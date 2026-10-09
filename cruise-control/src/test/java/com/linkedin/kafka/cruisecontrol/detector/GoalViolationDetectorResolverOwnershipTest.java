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

package com.linkedin.kafka.cruisecontrol.detector;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GoalViolationDetectorResolverOwnershipTest {
  public static class CountingResolver implements BrokerCapacityConfigResolver {
    private static final AtomicInteger INSTANCES = new AtomicInteger();
    private boolean _jbod = true;
    private boolean _failQuery;
    private int _queries;

    public CountingResolver() {
      INSTANCES.incrementAndGet();
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public BrokerCapacityInfo capacityForBroker(String rack, String host, int brokerId, long timeoutMs, boolean estimate) {
      return null;
    }

    @Override
    public boolean isJbodKafkaCluster() {
      ++_queries;
      if (_failQuery) {
        throw new IllegalStateException("resolver temporarily unavailable");
      }
      return _jbod;
    }

    @Override
    public void close() {
    }
  }

  @Test
  public void testDetectorUsesMonitorResolverAndSurvivesItsQueryFailure() throws Exception {
    CountingResolver.INSTANCES.set(0);
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.put(MonitorConfig.BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG, CountingResolver.class);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(properties);
    CountingResolver ownedResolver = config.getConfiguredInstance(MonitorConfig.BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG,
                                                                   CountingResolver.class);
    // Execute the monitor's real JBOD delegation without starting its background sampling threads.
    LoadMonitor monitor = EasyMock.createMockBuilder(LoadMonitor.class)
        .addMockedMethod("clusterModelGeneration").createNiceMock();
    Field resolverField = LoadMonitor.class.getDeclaredField("_brokerCapacityConfigResolver");
    resolverField.setAccessible(true);
    resolverField.set(monitor, ownedResolver);
    EasyMock.replay(monitor);
    KafkaCruiseControl control = EasyMock.niceMock(KafkaCruiseControl.class);
    EasyMock.expect(control.config()).andReturn(config).anyTimes();
    EasyMock.expect(control.adminClient()).andReturn(EasyMock.niceMock(AdminClient.class)).anyTimes();
    // No access during construction: the real manager is built before the monitor is initialized.
    EasyMock.expect(control.loadMonitor()).andReturn(monitor).times(3);
    EasyMock.replay(control);
    GoalViolationDetector detector = new GoalViolationDetector(new LinkedBlockingQueue<>(), control, new MetricRegistry()) {
      @Override
      protected AnomalyDetectionStatus getGoalViolationDetectionStatus() {
        return AnomalyDetectionStatus.SKIP_MODEL_GENERATION_NOT_CHANGED;
      }
    };
    assertEquals(1, CountingResolver.INSTANCES.get());
    detector.run();
    detector.refreshCombinedBalancednessScore(Map.of(false, List.of("IntraBrokerDiskCapacityGoal")), true);
    double previousScore = detector.balancednessScore();
    ownedResolver._failQuery = true;
    detector.run();
    assertEquals(previousScore, detector.balancednessScore(), 0.00001);
    ownedResolver._failQuery = false;
    ownedResolver._jbod = false;
    detector.run();
    assertEquals(100.0, detector.balancednessScore(), 0.00001);
    assertEquals(3, ownedResolver._queries);
    assertEquals(1, CountingResolver.INSTANCES.get());
    EasyMock.verify(control, monitor);
  }
}
