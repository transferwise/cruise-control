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
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
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
    Field provision = GoalViolationDetector.class.getDeclaredField("_interBrokerProvisionResponse");
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
  public void testBothPassesContributeAndCanRecoverIndependently() throws Exception {
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

  @Test
  public void testIntraBrokerDeadBrokerMarksCombinedStateUnreliable() throws Exception {
    assertOfflineIntraBrokerPassPreservesInterBrokerState(true);
  }

  @Test
  public void testIntraBrokerBadDiskMarksCombinedStateUnreliable() throws Exception {
    assertOfflineIntraBrokerPassPreservesInterBrokerState(false);
  }

  private static void assertOfflineIntraBrokerPassPreservesInterBrokerState(boolean deadBroker) throws Exception {
    ClusterModel model = EasyMock.mock(ClusterModel.class);
    SortedSet<Broker> offlineBrokers = new TreeSet<>();
    offlineBrokers.add(EasyMock.niceMock(Broker.class));
    EasyMock.expect(model.deadBrokers()).andReturn(deadBroker ? offlineBrokers : new TreeSet<>()).anyTimes();
    EasyMock.expect(model.brokersWithBadDisks()).andReturn(deadBroker ? new TreeSet<>() : offlineBrokers).anyTimes();
    EasyMock.expect(model.generation()).andReturn(new ModelGeneration(1, 1)).anyTimes();
    EasyMock.replay(model);
    GoalViolationDetector detector = detector(false, model, false);
    detector.refreshCombinedBalancednessScore(Map.of(true, List.of("DiskCapacityGoal")), false);
    detector.refreshCombinedBalancednessScore(Map.of(false, List.of("IntraBrokerDiskCapacityGoal")), true);
    setField(detector, "_interBrokerProvisionResponse", new ProvisionResponse(ProvisionStatus.UNDER_PROVISIONED));
    Method detect = GoalViolationDetector.class.getDeclaredMethod("detectIntraBrokerGoalViolations");
    detect.setAccessible(true);
    detect.invoke(detector);
    assertEquals(ProvisionStatus.UNDECIDED, detector.provisionStatus());
    assertEquals(GoalViolationDetector.BALANCEDNESS_SCORE_WITH_OFFLINE_REPLICAS, detector.balancednessScore(), 0.00001);
    // A subsequent healthy inter-broker result cannot certify the failed disk as healthy.
    detector.refreshCombinedBalancednessScore(Map.of(), false);
    Method publish = GoalViolationDetector.class.getDeclaredMethod("publishDetectionState");
    publish.setAccessible(true);
    publish.invoke(detector);
    assertEquals(ProvisionStatus.UNDECIDED, detector.provisionStatus());
    assertEquals(GoalViolationDetector.BALANCEDNESS_SCORE_WITH_OFFLINE_REPLICAS, detector.balancednessScore(), 0.00001);
    Field controlField = AbstractAnomalyDetector.class.getDeclaredField("_kafkaCruiseControl");
    controlField.setAccessible(true);
    KafkaCruiseControl control = (KafkaCruiseControl) controlField.get(detector);
    EasyMock.verify(control, control.loadMonitor());
    // The inter-broker/global offline path must retain its existing behavior.
    assertTrue(detector.skipDueToOfflineReplicas(model));
    assertEquals(ProvisionStatus.UNDECIDED, detector.provisionStatus());
    assertEquals(GoalViolationDetector.BALANCEDNESS_SCORE_WITH_OFFLINE_REPLICAS, detector.balancednessScore(), 0.00001);
    EasyMock.verify(model);
  }

  @Test
  public void testNonJbodClearsIntraBrokerViolationsWithUnchangedModel() throws Exception {
    GoalViolationDetector detector = detector(false, null, true);
    LoadMonitor monitor = monitor(detector);
    EasyMock.reset(monitor);
    EasyMock.expect(monitor.isJbodKafkaCluster()).andReturn(true);
    EasyMock.expect(monitor.isJbodKafkaCluster()).andReturn(false);
    EasyMock.replay(monitor);
    detector.refreshCombinedBalancednessScore(Map.of(true, List.of("DiskCapacityGoal")), false);
    double interBrokerScore = detector.balancednessScore();
    detector.refreshCombinedBalancednessScore(Map.of(false, List.of("IntraBrokerDiskCapacityGoal")), true);
    double combinedScore = detector.balancednessScore();
    detector.run();
    assertEquals(combinedScore, detector.balancednessScore(), 0.00001);
    detector.run();
    assertEquals(interBrokerScore, detector.balancednessScore(), 0.00001);
    EasyMock.verify(monitor);
  }

  @Test
  public void testHealthyIntraBrokerPassClearsItsOfflineSentinel() throws Exception {
    ClusterModel model = EasyMock.mock(ClusterModel.class);
    EasyMock.expect(model.deadBrokers()).andReturn(new TreeSet<>()).anyTimes();
    EasyMock.expect(model.brokersWithBadDisks()).andReturn(new TreeSet<>()).anyTimes();
    EasyMock.expect(model.generation()).andReturn(new ModelGeneration(2, 2)).anyTimes();
    EasyMock.expect(model.topics()).andReturn(java.util.Set.of()).anyTimes();
    EasyMock.replay(model);
    GoalViolationDetector detector = detector(false, model, false);
    setField(detector, "_intraBrokerHasOfflineReplicas", true);
    Method publish = GoalViolationDetector.class.getDeclaredMethod("publishDetectionState");
    publish.setAccessible(true);
    publish.invoke(detector);
    assertEquals(GoalViolationDetector.BALANCEDNESS_SCORE_WITH_OFFLINE_REPLICAS, detector.balancednessScore(), 0.00001);
    Method detect = GoalViolationDetector.class.getDeclaredMethod("detectIntraBrokerGoalViolations");
    detect.setAccessible(true);
    detect.invoke(detector);
    assertEquals(100.0, detector.balancednessScore(), 0.00001);
    // Clearing the disk pass must still preserve an independent inter-broker offline observation.
    detector.setBalancednessWithOfflineReplicas();
    detector.refreshCombinedBalancednessScore(Map.of(), true);
    publish.invoke(detector);
    assertEquals(GoalViolationDetector.BALANCEDNESS_SCORE_WITH_OFFLINE_REPLICAS, detector.balancednessScore(), 0.00001);
    EasyMock.verify(model);
  }

  @Test
  public void testNonJbodTransitionPreservesOfflineSentinel() throws Exception {
    GoalViolationDetector detector = detector(false, null, true);
    LoadMonitor monitor = monitor(detector);
    EasyMock.reset(monitor);
    EasyMock.expect(monitor.isJbodKafkaCluster()).andReturn(false);
    EasyMock.replay(monitor);
    detector.refreshCombinedBalancednessScore(Map.of(false, List.of("IntraBrokerDiskCapacityGoal")), true);
    detector.setBalancednessWithOfflineReplicas();
    detector.run();
    assertEquals(GoalViolationDetector.BALANCEDNESS_SCORE_WITH_OFFLINE_REPLICAS, detector.balancednessScore(), 0.00001);
    assertEquals(ProvisionStatus.UNDECIDED, detector.provisionStatus());
    Field intra = GoalViolationDetector.class.getDeclaredField("_intraBrokerViolations");
    intra.setAccessible(true);
    assertTrue(((Map<?, ?>) intra.get(detector)).isEmpty());
    EasyMock.verify(monitor);
  }

  private static void setField(GoalViolationDetector detector, String name, Object value) throws Exception {
    Field field = GoalViolationDetector.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(detector, value);
  }

  @Test
  public void testSkippedInterBrokerPassPreservesLastObservedState() throws Exception {
    assertSkippedPassPreservesState(false);
  }

  @Test
  public void testSkippedIntraBrokerPassPreservesLastObservedState() throws Exception {
    assertSkippedPassPreservesState(true);
  }

  private static void assertSkippedPassPreservesState(boolean intraBroker) throws Exception {
    GoalViolationDetector detector = detector(false);
    Field controlField = AbstractAnomalyDetector.class.getDeclaredField("_kafkaCruiseControl");
    controlField.setAccessible(true);
    KafkaCruiseControl control = (KafkaCruiseControl) controlField.get(detector);
    EasyMock.reset(control);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    LoadMonitor monitor = EasyMock.mock(LoadMonitor.class);
    EasyMock.expect(monitor.meetCompletenessRequirements(EasyMock.anyObject())).andReturn(false).anyTimes();
    EasyMock.replay(monitor);
    EasyMock.expect(control.config()).andReturn(config).anyTimes();
    EasyMock.expect(control.loadMonitor()).andReturn(monitor).anyTimes();
    EasyMock.replay(control);
    String prefix = intraBroker ? "_intraBroker" : "_interBroker";
    ProvisionResponse previous = new ProvisionResponse(ProvisionStatus.UNDER_PROVISIONED);
    setField(detector, prefix + "ProvisionResponse", previous);
    setField(detector, prefix + "HasExcessiveReplicationFactor", true);
    detector.refreshCombinedBalancednessScore(Map.of(true, List.of(intraBroker ? "IntraBrokerDiskCapacityGoal" : "DiskCapacityGoal")),
                                             intraBroker);
    Method publish = GoalViolationDetector.class.getDeclaredMethod("publishDetectionState");
    publish.setAccessible(true);
    publish.invoke(detector);
    double previousScore = detector.balancednessScore();
    Method detect = GoalViolationDetector.class.getDeclaredMethod(intraBroker ? "detectIntraBrokerGoalViolations"
                                                                            : "detectInterBrokerGoalViolations");
    detect.setAccessible(true);
    detect.invoke(detector);
    assertTrue(detector.hasPartitionsWithRFGreaterThanNumRacks());
    assertEquals(ProvisionStatus.UNDER_PROVISIONED, detector.provisionStatus());
    assertEquals(previousScore, detector.balancednessScore(), 0.00001);
    Field rf = GoalViolationDetector.class.getDeclaredField(prefix + "HasExcessiveReplicationFactor");
    rf.setAccessible(true);
    assertEquals(true, rf.get(detector));
    EasyMock.verify(control, monitor);
  }

  @Test
  public void testLegacyScoreMethodRemainsCompatible() throws Exception {
    GoalViolationDetector detector = detector(false);
    detector.refreshBalancednessScore(List.of("DiskCapacityGoal", "IntraBrokerDiskCapacityGoal"));
    double expectedScore = detector.balancednessScore();
    detector.refreshBalancednessScore(Map.of(true, List.of("DiskCapacityGoal"),
                                            false, List.of("IntraBrokerDiskCapacityGoal")));
    assertEquals(expectedScore, detector.balancednessScore(), 0.00001);
  }

  @Test
  public void testNonJbodClearsOnlyIntraBrokerProvisioningAndRackState() throws Exception {
    GoalViolationDetector detector = detector(false, null, true);
    LoadMonitor monitor = monitor(detector);
    EasyMock.reset(monitor);
    EasyMock.expect(monitor.isJbodKafkaCluster()).andReturn(false).times(2);
    EasyMock.replay(monitor);
    ProvisionResponse inter = new ProvisionResponse(ProvisionStatus.RIGHT_SIZED);
    setField(detector, "_interBrokerProvisionResponse", inter);
    setField(detector, "_intraBrokerProvisionResponse", new ProvisionResponse(ProvisionStatus.UNDER_PROVISIONED));
    setField(detector, "_intraBrokerHasExcessiveReplicationFactor", true);
    Method publish = GoalViolationDetector.class.getDeclaredMethod("publishDetectionState");
    publish.setAccessible(true);
    publish.invoke(detector);
    assertEquals(ProvisionStatus.UNDER_PROVISIONED, detector.provisionStatus());
    assertEquals(ProvisionStatus.RIGHT_SIZED, inter.status());
    detector.run();
    assertEquals(ProvisionStatus.RIGHT_SIZED, detector.provisionStatus());
    assertTrue(!detector.hasPartitionsWithRFGreaterThanNumRacks());
    assertEquals(ProvisionStatus.RIGHT_SIZED, inter.status());
    setField(detector, "_interBrokerHasExcessiveReplicationFactor", true);
    detector.run();
    assertTrue(detector.hasPartitionsWithRFGreaterThanNumRacks());
    EasyMock.verify(monitor);
  }

  @Test
  public void testDuplicateGoalPenaltySurvivesOnePassClearing() throws Exception {
    GoalViolationDetector detector = detector(false);
    detector.refreshCombinedBalancednessScore(Map.of(true, List.of("DiskCapacityGoal")), false);
    double expectedScore = detector.balancednessScore();
    detector.refreshCombinedBalancednessScore(Map.of(false, List.of("DiskCapacityGoal")), true);
    assertEquals(expectedScore, detector.balancednessScore(), 0.00001);
    detector.refreshCombinedBalancednessScore(Map.of(), false);
    assertEquals(expectedScore, detector.balancednessScore(), 0.00001);
    detector.refreshCombinedBalancednessScore(Map.of(), true);
    assertEquals(100.0, detector.balancednessScore(), 0.00001);
  }

  @Test
  public void testNonJbodUsesOnlyInterBrokerScoreWeights() throws Exception {
    GoalViolationDetector detector = detector(false, null, true);
    LoadMonitor monitor = monitor(detector);
    EasyMock.reset(monitor);
    EasyMock.expect(monitor.isJbodKafkaCluster()).andReturn(true);
    EasyMock.expect(monitor.isJbodKafkaCluster()).andReturn(false);
    EasyMock.replay(monitor);
    Field goalsField = GoalViolationDetector.class.getDeclaredField("_detectionGoals");
    goalsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal> goals =
        (List<com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal>) goalsField.get(detector);
    List<String> names = goals.stream().map(com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal::name).toList();
    detector.refreshCombinedBalancednessScore(Map.of(false, names), false);
    assertEquals(0.0, detector.balancednessScore(), 0.00001);
    detector.run();
    assertTrue(detector.balancednessScore() > 0);
    detector.run();
    assertEquals(0.0, detector.balancednessScore(), 0.00001);
    EasyMock.verify(monitor);
  }

  @Test
  public void testOverlappingDetectionGoalsNormalizeUniqueGoalWeights() throws Exception {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.setProperty(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal,"
            + "com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal");
    GoalViolationDetector detector = detector(false, null, true, properties);
    setField(detector, "_isJbodCluster", true);
    detector.refreshCombinedBalancednessScore(Map.of(false, List.of("DiskCapacityGoal", "IntraBrokerDiskCapacityGoal")), false);
    detector.refreshCombinedBalancednessScore(
        Map.of(false, List.of("IntraBrokerDiskCapacityGoal", "IntraBrokerDiskUsageDistributionGoal")), true);
    assertEquals(0.0, detector.balancednessScore(), 0.00001);
    // Recovering one pass must leave the shared goal penalized until both passes recover.
    detector.refreshCombinedBalancednessScore(Map.of(), false);
    double intraScore = detector.balancednessScore();
    assertTrue(intraScore > 0);
    assertTrue(intraScore < 100);
    detector.refreshCombinedBalancednessScore(Map.of(), true);
    assertEquals(100.0, detector.balancednessScore(), 0.00001);
  }

  private static LoadMonitor monitor(GoalViolationDetector detector) throws Exception {
    Field field = AbstractAnomalyDetector.class.getDeclaredField("_kafkaCruiseControl");
    field.setAccessible(true);
    return ((KafkaCruiseControl) field.get(detector)).loadMonitor();
  }

  private static GoalViolationDetector detector(boolean emptyIntraGoals) throws Exception {
    return detector(emptyIntraGoals, null, false);
  }

  private static GoalViolationDetector detector(boolean emptyIntraGoals, ClusterModel model, boolean unchangedModel) throws Exception {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    return detector(emptyIntraGoals, model, unchangedModel, properties);
  }

  private static GoalViolationDetector detector(boolean emptyIntraGoals, ClusterModel model, boolean unchangedModel,
                                                Properties properties) throws Exception {
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
    LoadMonitor monitor = EasyMock.mock(LoadMonitor.class);
    EasyMock.expect(control.loadMonitor()).andReturn(monitor).anyTimes();
    if (model != null) {
      EasyMock.expect(monitor.meetCompletenessRequirements(EasyMock.anyObject())).andReturn(true).anyTimes();

      EasyMock.expect(control.acquireForModelGeneration(EasyMock.anyObject()))
              .andReturn(EasyMock.niceMock(LoadMonitor.AutoCloseableSemaphore.class)).once();
      EasyMock.expect(control.clusterModel(EasyMock.anyObject(), EasyMock.anyBoolean(), EasyMock.anyObject(), EasyMock.eq(true)))
              .andReturn(model).once();
    }
    EasyMock.replay(monitor, control);
    if (unchangedModel) {
      return new GoalViolationDetector(new LinkedBlockingQueue<>(), control, new MetricRegistry()) {
        @Override
        protected AnomalyDetectionStatus getGoalViolationDetectionStatus() {
          return AnomalyDetectionStatus.SKIP_MODEL_GENERATION_NOT_CHANGED;
        }
      };
    }
    return new GoalViolationDetector(new LinkedBlockingQueue<>(), control, new MetricRegistry());
  }
}
