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
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.admin.AdminClient;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GoalViolationDetectorStateTest {
  @Test
  public void testHealthyIntraBrokerPassPreservesInterBrokerState() throws Exception {
    GoalViolationDetector detector = detector(true);
    detector.refreshCombinedBalancednessScore(Map.of(false, List.of("DiskCapacityGoal")), true);
    detector.refreshCombinedBalancednessScore(Map.of(true, List.of("DiskCapacityGoal")), false);
    double interBrokerScore = detector.balancednessScore();
    assertTrue(interBrokerScore < 100);
    Field provision = GoalViolationDetector.class.getDeclaredField("_provisionResponse");
    provision.setAccessible(true);
    provision.set(detector, new ProvisionResponse(ProvisionStatus.UNDER_PROVISIONED));
    Method detect = GoalViolationDetector.class.getDeclaredMethod("detectIntraBrokerGoalViolations");
    detect.setAccessible(true);
    detect.invoke(detector);
    Field intra = GoalViolationDetector.class.getDeclaredField("_intraBrokerViolations");
    intra.setAccessible(true);
    assertTrue(((Map<?, ?>) intra.get(detector)).isEmpty());
    assertEquals(ProvisionStatus.UNDER_PROVISIONED, detector.provisionStatus());
    assertEquals(interBrokerScore, detector.balancednessScore(), 0.00001);
  }

  @Test
  public void testBothPassesContributeAndCanRecoverIndependently() {
    GoalViolationDetector detector = detector(false);
    detector.refreshCombinedBalancednessScore(Map.of(true, List.of("DiskCapacityGoal")), false);
    double interBrokerScore = detector.balancednessScore();
    detector.refreshCombinedBalancednessScore(Map.of(false, List.of("IntraBrokerDiskCapacityGoal")), true);
    double bothScore = detector.balancednessScore();
    assertTrue(bothScore < interBrokerScore);
    detector.refreshCombinedBalancednessScore(Map.of(), false);
    double intraBrokerScore = detector.balancednessScore();
    assertTrue(intraBrokerScore > bothScore);
    assertTrue(intraBrokerScore < 100);
    detector.refreshCombinedBalancednessScore(Map.of(), true);
    assertEquals(100.0, detector.balancednessScore(), 0.00001);
  }

  private static GoalViolationDetector detector(boolean emptyIntraGoals) {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.setProperty(AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG, "false");
    properties.setProperty(AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG, "false");
    properties.setProperty(AnomalyDetectorConfig.PROVISIONER_ENABLE_CONFIG, "false");
    if (emptyIntraGoals) {
      properties.put(AnomalyDetectorConfig.ANOMALY_DETECTION_INTRA_BROKER_GOALS_CONFIG, List.of());
    }
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(properties);
    KafkaCruiseControl control = EasyMock.niceMock(KafkaCruiseControl.class);
    EasyMock.expect(control.config()).andReturn(config).anyTimes();
    EasyMock.expect(control.adminClient()).andReturn(EasyMock.niceMock(AdminClient.class)).anyTimes();
    EasyMock.replay(control);
    return new GoalViolationDetector(new LinkedBlockingQueue<>(), control, new MetricRegistry());
  }
}
