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

package com.linkedin.kafka.cruisecontrol.config;

import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class BrokerCapacityConfigResolverCompatibilityTest {
  @Test
  public void testLegacyResolverDoesNotNeedJbodDetectionMethod() {
    // Implements only the upstream interface, as existing third-party resolvers do.
    BrokerCapacityConfigResolver resolver = new BrokerCapacityConfigResolver() {
      @Override
      public void configure(Map<String, ?> configs) {
      }

      @Override
      public BrokerCapacityInfo capacityForBroker(String rack, String host, int brokerId, long timeoutMs,
                                                  boolean allowCapacityEstimation) {
        return new BrokerCapacityInfo(Map.of());
      }

      @Override
      public void close() {
      }
    };
    assertFalse(resolver.isJbodKafkaCluster());
  }
}
