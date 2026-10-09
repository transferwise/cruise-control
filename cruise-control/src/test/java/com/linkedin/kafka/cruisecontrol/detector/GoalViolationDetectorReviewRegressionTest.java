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
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.lang.reflect.Field;
import java.util.Properties;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GoalViolationDetectorReviewRegressionTest {
  private static void field(Object target, String name, Object value) throws Exception {
    Field f = GoalViolationDetector.class.getDeclaredField(name);
    f.setAccessible(true);
    f.set(target, value);
  }
  private static Properties properties() {
    Properties p = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    p.setProperty(AnomalyDetectorConfig.PROVISIONER_ENABLE_CONFIG, "false");
    p.setProperty(AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG, "false");
    p.setProperty(AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG, "false");
    return p;
  }
  @Test
  public void testSupportedSingleIntraGoalMustStillStart() {
    Properties p = properties();
    p.setProperty(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal");
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(p);
    assertEquals(java.util.List.of("IntraBrokerDiskCapacityGoal"),
        AnomalyDetectorUtils.getSelfHealingIntraBrokerGoalNames(config));
  }
  @Test(expected = org.apache.kafka.common.config.ConfigException.class)
  public void testExplicitUnsupportedSelfHealingGoalStillRejected() {
    Properties p = properties();
    p.setProperty(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal");
    p.setProperty(AnomalyDetectorConfig.SELF_HEALING_INTRA_BROKER_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal");
    new KafkaCruiseControlConfig(p);
  }

  @Test
  public void testExplicitEmptySelfHealingIntraGoalsUsesConfiguredDiskGoals() {
    Properties p = properties();
    p.setProperty(AnalyzerConfig.INTRA_BROKER_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal");
    p.setProperty(AnomalyDetectorConfig.SELF_HEALING_INTRA_BROKER_GOALS_CONFIG, "");
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(p);
    assertEquals(List.of("IntraBrokerDiskCapacityGoal"), AnomalyDetectorUtils.getSelfHealingIntraBrokerGoalNames(config));
  }

  @Test
  public void testUpstreamExecutionSignatureMustRemainAvailable() throws Exception {
    KafkaCruiseControl.class.getMethod("executeProposals", Set.class, Set.class, boolean.class,
        Integer.class, Integer.class, Integer.class, Integer.class, Integer.class,
        Long.class, com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy.class,
        Long.class, boolean.class, String.class, boolean.class);
    com.linkedin.kafka.cruisecontrol.executor.Executor.class.getMethod("executeProposals",
        java.util.Collection.class, Set.class, Set.class, LoadMonitor.class,
        Integer.class, Integer.class, Integer.class, Integer.class, Integer.class,
        Long.class, com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy.class,
        Long.class, boolean.class, String.class, boolean.class, boolean.class);
  }
  @Test
  public void testUpstreamDetectorOverrideStillInterceptsInterBrokerPass() throws Exception {
    AtomicInteger overrideCalls = new AtomicInteger();
    GoalViolationDetector detector = detector(false, new AtomicInteger(), new AtomicInteger(), overrideCalls);
    detector.run();
    org.junit.Assert.assertTrue("Legacy subclass must intercept inter-broker optimization", overrideCalls.get() > 0);
  }

  private static GoalViolationDetector detector(boolean interOffline, AtomicInteger intraCalls, AtomicInteger interCalls) throws Exception {
    return detector(interOffline, intraCalls, interCalls, new AtomicInteger());
  }

  private static GoalViolationDetector detector(boolean interOffline, AtomicInteger intraCalls, AtomicInteger interCalls,
                                                AtomicInteger overrideCalls) throws Exception {
    return detector(interOffline, intraCalls, interCalls, overrideCalls, null);
  }

  private static GoalViolationDetector detector(boolean interOffline, AtomicInteger intraCalls, AtomicInteger interCalls,
                                                AtomicInteger overrideCalls, AtomicBoolean complete) throws Exception {
    Properties properties = properties();
    if (complete != null) {
      properties.put(AnomalyDetectorConfig.PROVISIONER_ENABLE_CONFIG, "true");
      properties.put(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG,
          List.of("com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal",
                  "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal"));
      properties.put(AnomalyDetectorConfig.ANOMALY_DETECTION_INTRA_BROKER_GOALS_CONFIG,
          List.of("com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal",
                  "com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal"));
    }
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(properties);
    KafkaCruiseControl control = EasyMock.niceMock(KafkaCruiseControl.class);
    LoadMonitor monitor = EasyMock.niceMock(LoadMonitor.class);
    AtomicReference<ModelGeneration> generation = new AtomicReference<>(new ModelGeneration(1, 1));
    ClusterModel healthy = EasyMock.niceMock(ClusterModel.class);
    EasyMock.expect(healthy.deadBrokers()).andReturn(new TreeSet<>()).anyTimes();
    EasyMock.expect(healthy.brokersWithBadDisks()).andReturn(new TreeSet<>()).anyTimes();
    EasyMock.expect(healthy.topics()).andReturn(Set.of()).anyTimes();
    EasyMock.expect(healthy.generation()).andAnswer(generation::get).anyTimes();
    EasyMock.replay(healthy);
    ClusterModel interModel = healthy;
    if (interOffline) {
      interModel = EasyMock.niceMock(ClusterModel.class);
      SortedSet<Broker> brokers = new TreeSet<>();
      brokers.add(EasyMock.niceMock(Broker.class));
      EasyMock.expect(interModel.deadBrokers()).andReturn(brokers).anyTimes();
      EasyMock.replay(interModel);
    }
    EasyMock.expect(monitor.clusterModelGeneration()).andAnswer(generation::get).anyTimes();
    EasyMock.expect(monitor.brokersWithOfflineReplicas(EasyMock.anyLong())).andReturn(Set.of()).anyTimes();
    AtomicInteger completenessChecks = new AtomicInteger();
    EasyMock.expect(monitor.meetCompletenessRequirements(EasyMock.anyObject()))
        .andAnswer(() -> complete == null || completenessChecks.getAndIncrement() % 2 == 0 || complete.get()).anyTimes();
    EasyMock.expect(monitor.isJbodKafkaCluster()).andReturn(true).anyTimes();
    EasyMock.replay(monitor);
    EasyMock.expect(control.config()).andReturn(config).anyTimes();
    EasyMock.expect(control.adminClient()).andReturn(EasyMock.niceMock(AdminClient.class)).anyTimes();
    EasyMock.expect(control.loadMonitor()).andReturn(monitor).anyTimes();
    EasyMock.expect(control.getLoadMonitorTaskRunnerState())
        .andReturn(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING).anyTimes();
    EasyMock.expect(control.executionState()).andReturn(ExecutorState.State.NO_TASK_IN_PROGRESS).anyTimes();
    AtomicInteger rightsizingCalls = new AtomicInteger();
    if (complete != null) {
      Provisioner provisioner = EasyMock.mock(Provisioner.class);
      EasyMock.expect(provisioner.rightsize(EasyMock.anyObject(), EasyMock.anyObject())).andAnswer(() -> {
        rightsizingCalls.incrementAndGet();
        return null;
      }).anyTimes();
      EasyMock.replay(provisioner);
      EasyMock.expect(control.provisioner()).andReturn(provisioner).anyTimes();
    }
    LoadMonitor.AutoCloseableSemaphore semaphore = EasyMock.niceMock(LoadMonitor.AutoCloseableSemaphore.class);
    EasyMock.replay(semaphore);
    EasyMock.expect(control.acquireForModelGeneration(EasyMock.anyObject())).andReturn(semaphore).anyTimes();
    ClusterModel interModelForAnswer = interModel;
    EasyMock.expect(control.clusterModel(EasyMock.anyObject(), EasyMock.anyBoolean(), EasyMock.anyObject()))
        .andAnswer(() -> {
          interCalls.incrementAndGet();
          return interOffline ? interModelForAnswer : healthy;
        }).anyTimes();
    EasyMock.expect(control.clusterModel(EasyMock.anyObject(), EasyMock.anyBoolean(), EasyMock.anyObject(), EasyMock.eq(true)))
        .andAnswer(() -> {
          int count = intraCalls.incrementAndGet();
          if (complete == null && !interOffline && count == 1) {
            throw new TimeoutException("transient intra-only timeout");
          }
          return healthy;
        }).anyTimes();
    EasyMock.replay(control);
    return new ReviewDetector(control, overrideCalls, complete, rightsizingCalls, generation);
  }

  private static class ReviewDetector extends GoalViolationDetector {
    private final AtomicInteger _overrideCalls;
    private final AtomicBoolean _complete;
    private final AtomicInteger _rightsizingCalls;
    private final AtomicReference<ModelGeneration> _generation;

    ReviewDetector(KafkaCruiseControl control, AtomicInteger overrideCalls, AtomicBoolean complete, AtomicInteger rightsizingCalls,
                   AtomicReference<ModelGeneration> generation) {
      super(new LinkedBlockingQueue<>(), control, new MetricRegistry());
      _overrideCalls = overrideCalls;
      _complete = complete;
      _rightsizingCalls = rightsizingCalls;
      _generation = generation;
    }

      @Override
      protected boolean optimizeForGoal(ClusterModel model, com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal goal,
                                         GoalViolations violations, Set<Integer> excludedLeadership,
                                         Set<Integer> excludedReplicaMove, boolean checkRacks)
          throws com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException {
        _overrideCalls.incrementAndGet();
        if (_complete != null) {
          violations.addViolation(goal.name(), true);
          return false;
        }
        return super.optimizeForGoal(model, goal, violations, excludedLeadership, excludedReplicaMove, checkRacks);
      }

      @Override
      protected boolean optimizeForGoal(ClusterModel model, com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal goal,
                                         IntraBrokerGoalViolations violations, Set<Integer> excludedLeadership,
                                         Set<Integer> excludedReplicaMove, boolean checkRacks, boolean intraBroker)
          throws com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException {
        if (_complete != null) {
          _overrideCalls.incrementAndGet();
          violations.addViolation(goal.name(), true);
          return false;
        }
        return super.optimizeForGoal(model, goal, violations, excludedLeadership, excludedReplicaMove, checkRacks, intraBroker);
      }
  }

  @Test
  public void testPartialInterBrokerPassRetriesWithoutDuplicateAnomalies() throws Exception {
    assertPartialPassRetriesWithoutDuplicateAnomalies(false);
  }

  @Test
  public void testPartialIntraBrokerPassRetriesWithoutDuplicateAnomalies() throws Exception {
    assertPartialPassRetriesWithoutDuplicateAnomalies(true);
  }

  private void assertPartialPassRetriesWithoutDuplicateAnomalies(boolean intraBroker) throws Exception {
    AtomicBoolean complete = new AtomicBoolean();
    AtomicInteger calls = new AtomicInteger();
    GoalViolationDetector detector = detector(false, new AtomicInteger(), new AtomicInteger(), calls, complete);
    java.lang.reflect.Method detect = GoalViolationDetector.class.getDeclaredMethod(
        intraBroker ? "detectIntraBrokerGoalViolations" : "detectInterBrokerGoalViolations");
    detect.setAccessible(true);
    Field anomaliesField = AbstractAnomalyDetector.class.getDeclaredField("_anomalies");
    anomaliesField.setAccessible(true);
    java.util.Queue<?> anomalies = (java.util.Queue<?>) anomaliesField.get(detector);
    detect.invoke(detector);
    detect.invoke(detector);
    assertEquals("unchecked goals must remain eligible for retries", 2, calls.get());
    assertEquals("same partial result must only be enqueued once", 1, anomalies.size());
    assertEquals("same partial result must not repeat rightsizing", 1, ((ReviewDetector) detector)._rightsizingCalls.get());
    complete.set(true);
    detect.invoke(detector);
    assertEquals("both goals must be checked when completeness recovers", 4, calls.get());
    assertEquals("newly discovered violation must be published", 2, anomalies.size());
    assertEquals("changed result remains eligible for rightsizing", 2, ((ReviewDetector) detector)._rightsizingCalls.get());
    Field generation = GoalViolationDetector.class.getDeclaredField(
        intraBroker ? "_lastCheckedIntraBrokerModelGeneration" : "_lastCheckedModelGeneration");
    generation.setAccessible(true);
    assertEquals(new ModelGeneration(1, 1), generation.get(detector));
    detect.invoke(detector);
    assertEquals(2, anomalies.size());
    assertEquals(2, ((ReviewDetector) detector)._rightsizingCalls.get());
    ((ReviewDetector) detector)._generation.set(new ModelGeneration(2, 2));
    detect.invoke(detector);
    assertEquals("a new model generation must permit the same violations to be reported again", 3, anomalies.size());
    assertEquals(3, ((ReviewDetector) detector)._rightsizingCalls.get());
  }
  @Test
  public void testHealthyIntraPassMustNotEraseInterOfflineSentinel() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    GoalViolationDetector detector = detector(true, calls, new AtomicInteger());
    detector.run();
    assertEquals("intra pass must not certify inter-broker health", 0, calls.get());
    assertEquals(-1.0, detector.balancednessScore(), 0.00001);
  }
  @Test
  public void testJbodResolverFailureDoesNotDisableInterBrokerDetection() throws Exception {
    AtomicInteger interCalls = new AtomicInteger();
    AtomicInteger intraCalls = new AtomicInteger();
    GoalViolationDetector detector = detector(false, intraCalls, interCalls);
    Field controlField = AbstractAnomalyDetector.class.getDeclaredField("_kafkaCruiseControl");
    controlField.setAccessible(true);
    LoadMonitor monitor = ((KafkaCruiseControl) controlField.get(detector)).loadMonitor();
    EasyMock.reset(monitor);
    EasyMock.expect(monitor.isJbodKafkaCluster()).andThrow(new IllegalStateException("JBOD query unavailable"));
    EasyMock.expect(monitor.isJbodKafkaCluster()).andReturn(true).anyTimes();
    EasyMock.expect(monitor.clusterModelGeneration()).andReturn(new ModelGeneration(1, 1)).anyTimes();
    EasyMock.expect(monitor.brokersWithOfflineReplicas(EasyMock.anyLong())).andReturn(Set.of()).anyTimes();
    EasyMock.expect(monitor.meetCompletenessRequirements(EasyMock.anyObject())).andReturn(true).anyTimes();
    EasyMock.replay(monitor);
    detector.run();
    assertEquals("JBOD detection failure must not skip the independent inter-broker model", 1, interCalls.get());
    assertEquals(0, intraCalls.get());
    detector.run();
    detector.run();
    assertEquals("resolver recovery must allow intra-broker detection to retry", 2, intraCalls.get());
    assertEquals("a completed inter-broker pass need not be rebuilt", 1, interCalls.get());
    EasyMock.verify(monitor);
  }

  @Test
  public void testTransientIntraFailureMustRetryWithoutNewLoad() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger interCalls = new AtomicInteger();
    GoalViolationDetector detector = detector(false, calls, interCalls);
    detector.run();
    assertEquals(1, calls.get());
    detector.run();
    assertEquals("failed intra pass should retry", 2, calls.get());
    assertEquals("completed inter pass should not repeat", 1, interCalls.get());
  }
}
