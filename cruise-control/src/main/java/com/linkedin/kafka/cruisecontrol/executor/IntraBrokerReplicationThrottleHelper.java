/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Helper class for managing Kafka's intra-broker replication throttle (log dir reassignment throttle).
 * This uses the broker config {@code replica.alter.log.dirs.io.max.bytes.per.second} to throttle
 * intra-broker replica movements (KIP-113).
 */
class IntraBrokerReplicationThrottleHelper {
  private static final Logger LOG = LoggerFactory.getLogger(IntraBrokerReplicationThrottleHelper.class);
  static final String REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG = "replica.alter.log.dirs.io.max.bytes.per.second";
  public static final long CLIENT_REQUEST_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
  static final int RETRIES = 30;

  private final AdminClient _adminClient;
  private final Long _throttleRate;
  private final int _retries;
  private final Set<Integer> _throttledBrokers;
  // Tracks the original throttle value per broker before this execution overwrote it (null means no prior value)
  private final Map<Integer, String> _originalThrottleValues;

  IntraBrokerReplicationThrottleHelper(AdminClient adminClient, Long throttleRate) {
    this(adminClient, throttleRate, RETRIES);
  }

  // for testing
  IntraBrokerReplicationThrottleHelper(AdminClient adminClient, Long throttleRate, int retries) {
    _adminClient = adminClient;
    _throttleRate = throttleRate;
    _retries = retries;
    _throttledBrokers = new HashSet<>();
    _originalThrottleValues = new HashMap<>();
  }

  void setThrottles(List<ExecutionTask> tasksToExecute)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (throttlingEnabled()) {
      LOG.info("Setting an intra-broker rebalance throttle of {} bytes/sec", _throttleRate);
      Set<Integer> participatingBrokers = getParticipatingBrokers(tasksToExecute);
      changeThrottles(participatingBrokers, true);
    }
  }

  void clearThrottles(List<ExecutionTask> completedTasks, List<ExecutionTask> inProgressTasks)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (throttlingEnabled()) {
      Set<Integer> brokersWithCompletedTasks = getParticipatingBrokers(
          completedTasks.stream()
              .filter(this::shouldRemoveThrottleForTask)
              .collect(Collectors.toList()));

      Set<Integer> brokersWithInProgressTasks = getParticipatingBrokers(
          inProgressTasks.stream()
              .filter(this::taskIsInProgress)
              .collect(Collectors.toList()));

      Set<Integer> brokersToRemoveThrottlesFrom = new TreeSet<>(brokersWithCompletedTasks);
      brokersToRemoveThrottlesFrom.removeAll(brokersWithInProgressTasks);

      LOG.info("Removing intra-broker replica movement throttles from brokers: {}", brokersToRemoveThrottlesFrom);
      changeThrottles(brokersToRemoveThrottlesFrom, false);
    }
  }

  /**
   * Remove throttle from all brokers that were throttled during this execution.
   * Used as a final cleanup to ensure no throttle configs are left behind.
   * Attempts all brokers even if some fail, then reports failures.
   */
  void clearAllThrottles() throws ExecutionException, InterruptedException, TimeoutException {
    clearAllThrottles(Collections.emptySet());
  }

  void clearAllThrottles(Set<Integer> brokersWithUnresolvedCopies)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (throttlingEnabled() && !_throttledBrokers.isEmpty()) {
      Set<Integer> brokersToClear = new TreeSet<>(_throttledBrokers);
      brokersToClear.removeAll(brokersWithUnresolvedCopies);
      LOG.info("Final cleanup: removing intra-broker throttles from settled brokers: {}", brokersToClear);
      changeThrottles(brokersToClear, false);
    }
  }

  private boolean throttlingEnabled() {
    return _throttleRate != null;
  }

  private boolean shouldRemoveThrottleForTask(ExecutionTask task) {
    return task.state() != ExecutionTaskState.IN_PROGRESS
        && task.state() != ExecutionTaskState.PENDING
        && task.type() == ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION;
  }

  private boolean taskIsInProgress(ExecutionTask task) {
    return task.state() == ExecutionTaskState.IN_PROGRESS
        && task.type() == ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION;
  }

  private Set<Integer> getParticipatingBrokers(List<ExecutionTask> tasks) {
    Set<Integer> participatingBrokers = new TreeSet<>();
    for (ExecutionTask task : tasks) {
      participatingBrokers.add(task.brokerId());
    }
    return participatingBrokers;
  }

  private void changeThrottles(Set<Integer> brokers, boolean setting)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (setting && _throttleRate == null) {
      throw new IllegalStateException("Throttle rate cannot be null when setting throttles.");
    }
    if (brokers.isEmpty()) {
      return;
    }
    List<ConfigResource> resources = brokers.stream().map(b -> new ConfigResource(ConfigResource.Type.BROKER, b.toString()))
        .collect(Collectors.toList());
    Map<ConfigResource, org.apache.kafka.common.KafkaFuture<Config>> futures = _adminClient.describeConfigs(resources).values();
    Map<ConfigResource, Collection<AlterConfigOp>> changes = new HashMap<>();
    Exception firstFailure = null;
    Set<Integer> retrievedBrokers = new HashSet<>();
    for (ConfigResource resource : resources) {
      try {
        if (!futures.containsKey(resource)) {
          throw new IllegalStateException("Missing throttle config response for " + resource);
        }
        Config config = futures.get(resource).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        int broker = Integer.parseInt(resource.name());
        retrievedBrokers.add(broker);
        ConfigEntry current = config.get(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG);
        if (setting) {
          if (!_originalThrottleValues.containsKey(broker)) {
            _originalThrottleValues.put(broker, current != null && current.source() == ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG
                ? current.value() : null);
          }
          // Track before issuing SET so cleanup can compensate for failures after the write is accepted.
          _throttledBrokers.add(broker);
          if (current == null || !String.valueOf(_throttleRate).equals(current.value())) {
            changes.put(resource, throttleOperation(String.valueOf(_throttleRate)));
          }
        } else if (current == null || current.value() == null || current.value().isEmpty()) {
          if (_originalThrottleValues.containsKey(broker)) {
            changes.put(resource, throttleOperation(_originalThrottleValues.get(broker)));
          }
        } else if (_originalThrottleValues.containsKey(broker) && current.source() != ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG) {
          changes.put(resource, throttleOperation(_originalThrottleValues.get(broker)));
        }
      } catch (ExecutionException | InterruptedException | TimeoutException | IllegalStateException e) {
        LOG.warn("Failed to read intra-broker throttle config for {}", resource, e);
        if (firstFailure == null) {
          firstFailure = e;
        }
      }
    }
    // Remove only brokers whose requested mutation and propagation were confirmed.
    Map<ConfigResource, Exception> mutationFailures = changeBrokerConfigs(changes);
    if (!setting) {
      mutationFailures.keySet().forEach(r -> retrievedBrokers.remove(Integer.parseInt(r.name())));
      _throttledBrokers.removeAll(retrievedBrokers);
    }
    for (Map.Entry<ConfigResource, Exception> failure : mutationFailures.entrySet()) {
      LOG.warn("Failed to apply intra-broker throttle config for {}", failure.getKey(), failure.getValue());
      if (firstFailure == null) {
        firstFailure = failure.getValue();
      }
    }
    if (firstFailure instanceof ExecutionException) {
      throw (ExecutionException) firstFailure;
    } else if (firstFailure instanceof InterruptedException) {
      throw (InterruptedException) firstFailure;
    } else if (firstFailure instanceof TimeoutException) {
      throw (TimeoutException) firstFailure;
    } else if (firstFailure != null) {
      throw (IllegalStateException) firstFailure;
    }
  }

  private Collection<AlterConfigOp> throttleOperation(String value) {
    return Collections.singletonList(new AlterConfigOp(new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, value),
                                                        value == null ? AlterConfigOp.OpType.DELETE : AlterConfigOp.OpType.SET));
  }

  private Map<ConfigResource, Exception> changeBrokerConfigs(Map<ConfigResource, Collection<AlterConfigOp>> changes) {
    Map<ConfigResource, Exception> failures = new HashMap<>();
    Map<ConfigResource, Collection<AlterConfigOp>> accepted = new HashMap<>();
    if (!changes.isEmpty()) {
      Map<ConfigResource, org.apache.kafka.common.KafkaFuture<Void>> results = _adminClient.incrementalAlterConfigs(changes).values();
      for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> change : changes.entrySet()) {
        try {
          if (!results.containsKey(change.getKey())) {
            throw new IllegalStateException("Missing throttle mutation response for " + change.getKey());
          }
          results.get(change.getKey()).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          accepted.put(change.getKey(), change.getValue());
        } catch (ExecutionException | InterruptedException | TimeoutException | IllegalStateException e) {
          failures.put(change.getKey(), e);
        }
      }
      failures.putAll(ThrottleConfigUtils.waitForConfigsPerResource(_adminClient, accepted, _retries, CLIENT_REQUEST_TIMEOUT_MS));
    }
    return failures;
  }

  void changeBrokerConfigs(int brokerId, Collection<AlterConfigOp> ops)
      throws ExecutionException, InterruptedException, TimeoutException {
    Map<ConfigResource, Exception> failures =
        changeBrokerConfigs(Collections.singletonMap(new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId)), ops));
    if (!failures.isEmpty()) {
      Exception failure = failures.values().iterator().next();
      if (failure instanceof ExecutionException) {
        throw (ExecutionException) failure;
      } else if (failure instanceof InterruptedException) {
        throw (InterruptedException) failure;
      } else if (failure instanceof TimeoutException) {
        throw (TimeoutException) failure;
      }
      throw (IllegalStateException) failure;
    }
  }

  void waitForConfigs(ConfigResource cf, Collection<AlterConfigOp> ops) {
    ThrottleConfigUtils.waitForConfigs(_adminClient, Collections.singletonMap(cf, ops), _retries, CLIENT_REQUEST_TIMEOUT_MS,
                                      IntraBrokerReplicationThrottleHelper::configsEqual, true);
  }

  static boolean configsEqual(Config configs, Map<String, String> expectedValues) {
    return ThrottleConfigUtils.configsEqual(configs, expectedValues, ConfigResource.Type.BROKER);
  }
}
