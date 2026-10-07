/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

/** Shared batched config-propagation polling for replication throttle helpers. */
final class ThrottleConfigUtils {
  private static final int CONFIG_POLL_INTERVAL_MS = 1000;
  private ThrottleConfigUtils() {
  }

  static void waitForConfigs(AdminClient adminClient, Map<ConfigResource, Collection<AlterConfigOp>> changes,
                             int retries, long timeoutMs, BiPredicate<Config, Map<String, String>> configsEqual,
                             boolean retryQueryFailures) {
    Map<ConfigResource, Map<String, String>> expected = new HashMap<>();
    changes.forEach((resource, ops) -> {
      Map<String, String> values = new HashMap<>();
      ops.forEach(op -> values.put(op.configEntry().name(), op.configEntry().value()));
      expected.put(resource, values);
    });
    boolean applied = CruiseControlMetricsUtils.retry(() -> {
      try {
        Map<ConfigResource, Config> current = adminClient.describeConfigs(changes.keySet().stream()
            .sorted(Comparator.comparing(ConfigResource::name)).collect(Collectors.toList())).all()
            .get(timeoutMs, TimeUnit.MILLISECONDS);
        return expected.entrySet().stream().anyMatch(e -> current.get(e.getKey()) == null
            || !configsEqual.test(current.get(e.getKey()), e.getValue()));
      } catch (ExecutionException | InterruptedException | TimeoutException e) {
        // Preserve each helper's existing query-failure policy.
        return retryQueryFailures;
      }
    }, retries);
    if (!applied) {
      throw new IllegalStateException("The following configs " + changes + " were not applied within the time limit");
    }
  }

  static Map<ConfigResource, Exception> waitForConfigsPerResource(AdminClient adminClient,
                                                                Map<ConfigResource, Collection<AlterConfigOp>> changes,
                                                                int retries, long timeoutMs) {
    Map<ConfigResource, Map<String, String>> expected = new HashMap<>();
    changes.forEach((resource, ops) -> {
      Map<String, String> values = new HashMap<>();
      ops.forEach(op -> values.put(op.configEntry().name(), op.configEntry().value()));
      expected.put(resource, values);
    });
    Set<ConfigResource> pending = new HashSet<>(changes.keySet());
    Map<ConfigResource, Exception> failures = new HashMap<>();
    CruiseControlMetricsUtils.retry(() -> {
      if (pending.isEmpty()) {
        return false;
      }
      Map<ConfigResource, KafkaFuture<Config>> futures = adminClient.describeConfigs(pending.stream()
          .sorted(Comparator.comparing(ConfigResource::name)).collect(Collectors.toList())).values();
      for (ConfigResource resource : new HashSet<>(pending)) {
        try {
          if (!futures.containsKey(resource)) {
            throw new IllegalStateException("Missing throttle config response for " + resource);
          }
          Config current = futures.get(resource).get(timeoutMs, TimeUnit.MILLISECONDS);
          if (configsEqual(current, expected.get(resource), resource.type())) {
            pending.remove(resource);
            failures.remove(resource);
          } else {
            failures.put(resource, new IllegalStateException("Throttle configs did not converge for " + resource));
          }
        } catch (InterruptedException e) {
          pending.forEach(r -> failures.put(r, e));
          Thread.currentThread().interrupt();
          return false;
        } catch (ExecutionException | TimeoutException | IllegalStateException e) {
          failures.put(resource, e);
        }
      }
      return !pending.isEmpty();
    }, CONFIG_POLL_INTERVAL_MS, 1, retries, CONFIG_POLL_INTERVAL_MS);
    return failures;
  }

  static boolean configsEqual(Config configs, Map<String, String> expectedValues, ConfigResource.Type resourceType) {
    for (Map.Entry<String, String> entry : expectedValues.entrySet()) {
      ConfigEntry current = configs.get(entry.getKey());
      String actualValue = current == null ? null : current.value();
      if (entry.getValue() == null) {
        // DELETE removes the override for this resource. An inherited value is expected to remain visible.
        ConfigEntry.ConfigSource overrideSource = resourceType == ConfigResource.Type.TOPIC
            ? ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG : ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG;
        if (current != null && actualValue != null && !actualValue.isEmpty() && current.source() == overrideSource) {
          return false;
        }
      } else if (current == null || !Objects.equals(entry.getValue(), actualValue)) {
        return false;
      }
    }
    return true;
  }

}
