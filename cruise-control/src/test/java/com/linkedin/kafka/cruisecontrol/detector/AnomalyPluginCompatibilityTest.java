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
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotifier;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.easymock.EasyMock;
import org.junit.Test;
import org.apache.kafka.common.utils.Time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AnomalyPluginCompatibilityTest {
  @Test
  public void testLegacyNotifierDefaultsToIgnoringNewAnomaly() {
    // A plugin implementing only upstream callbacks must inherit a safe default for the added callback.
    AnomalyNotifier notifier = (AnomalyNotifier) Proxy.newProxyInstance(AnomalyNotifier.class.getClassLoader(),
        new Class<?>[]{AnomalyNotifier.class}, (proxy, method, args) -> {
          assertTrue("New callback must have a default implementation", method.isDefault());
          return InvocationHandler.invokeDefault(proxy, method, args);
        });
    assertEquals(AnomalyNotificationResult.Action.IGNORE,
                 notifier.onIntraBrokerGoalViolation(new IntraBrokerGoalViolations()).action());
  }

  @Test
  public void testLegacyNotifierWithoutNewAnomalyKeyHasDisabledGauge() throws Exception {
    AnomalyNotifier notifier = EasyMock.mock(AnomalyNotifier.class);
    EasyMock.expect(notifier.selfHealingEnabled()).andReturn(Map.of(KafkaAnomalyType.GOAL_VIOLATION, true)).times(2);
    EasyMock.replay(notifier);
    AnomalyDetectorManager manager = manager(EasyMock.niceMock(KafkaCruiseControl.class), notifier);
    try {
      MetricRegistry registry = new MetricRegistry();
      Method register = AnomalyDetectorManager.class.getDeclaredMethod("registerSensors", MetricRegistry.class);
      register.setAccessible(true);
      register.invoke(manager, registry);
      assertEquals(0, registry.getGauges().get("AnomalyDetector.intra_broker_goal_violation-self-healing-enabled").getValue());
      assertEquals(1, registry.getGauges().get("AnomalyDetector.goal_violation-self-healing-enabled").getValue());
      EasyMock.verify(notifier);
    } finally {
      manager.shutdown();
    }
  }

  @Test
  public void testIntraBrokerReadinessDoesNotFallBackToInterBrokerGoals() throws Exception {
    KafkaCruiseControl control = EasyMock.mock(KafkaCruiseControl.class);
    List<String> intraGoals = List.of("IntraBrokerDiskCapacityGoal");
    EasyMock.expect(control.getLoadMonitorTaskRunnerState()).andReturn(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING).times(2);
    EasyMock.expect(control.meetCompletenessRequirements(intraGoals)).andReturn(false).once();
    EasyMock.expect(control.meetCompletenessRequirements(intraGoals)).andReturn(true).once();
    EasyMock.replay(control);
    AnomalyDetectorManager manager = manager(control, EasyMock.niceMock(AnomalyNotifier.class));
    try {
      setField(manager, "_selfHealingIntraBrokerGoals", intraGoals);
      setField(manager, "_selfHealingGoals", List.of("DiskCapacityGoal"));
      AnomalyDetectorState state = EasyMock.niceMock(AnomalyDetectorState.class);
      EasyMock.replay(state);
      setField(manager, "_anomalyDetectorState", state);
      AnomalyDetectorManager.AnomalyHandlerTask handler = manager.new AnomalyHandlerTask();
      Method readiness = handler.getClass().getDeclaredMethod("isAnomalyInProgressReadyToFix", AnomalyType.class);
      readiness.setAccessible(true);
      assertFalse((Boolean) readiness.invoke(handler, KafkaAnomalyType.INTRA_BROKER_GOAL_VIOLATION));
      assertTrue((Boolean) readiness.invoke(handler, KafkaAnomalyType.INTRA_BROKER_GOAL_VIOLATION));
      EasyMock.verify(control);
    } finally {
      manager.shutdown();
    }
  }

  @Test
  public void testUnfixableGoalContributionsAndGaugesAreIndependent() {
    MetricRegistry registry = new MetricRegistry();
    AnomalyDetectorState state = new AnomalyDetectorState(Time.SYSTEM, EasyMock.niceMock(AnomalyNotifier.class), 10, registry);
    GoalViolations inter = EasyMock.mock(GoalViolations.class);
    IntraBrokerGoalViolations intra = EasyMock.mock(IntraBrokerGoalViolations.class);
    EasyMock.expect(inter.violatedGoalsByFixability()).andReturn(Map.of(false, List.of("RackAwareGoal")));
    EasyMock.expect(intra.violatedGoalsByFixability()).andReturn(Map.of(true, List.of("IntraBrokerDiskUsageDistributionGoal")));
    EasyMock.expect(intra.violatedGoalsByFixability()).andReturn(Map.of(false, List.of("IntraBrokerDiskCapacityGoal")));
    EasyMock.expect(inter.violatedGoalsByFixability()).andReturn(Map.of(true, List.of("DiskUsageDistributionGoal")));
    EasyMock.replay(inter, intra);
    state.refreshHasUnfixableGoal(inter);
    state.refreshHasUnfixableGoal(intra);
    assertTrue(state.hasUnfixableGoals());
    assertEquals(1, registry.getGauges().get("AnomalyDetector.GOAL_VIOLATION-has-unfixable-goals").getValue());
    assertEquals(0, registry.getGauges().get("AnomalyDetector.INTRA_BROKER_GOAL_VIOLATION-has-unfixable-goals").getValue());
    state.refreshHasUnfixableGoal(intra);
    state.refreshHasUnfixableGoal(inter);
    assertTrue(state.hasUnfixableGoals());
    assertEquals(0, registry.getGauges().get("AnomalyDetector.GOAL_VIOLATION-has-unfixable-goals").getValue());
    assertEquals(1, registry.getGauges().get("AnomalyDetector.INTRA_BROKER_GOAL_VIOLATION-has-unfixable-goals").getValue());
    state.resetHasUnfixableGoals();
    assertFalse(state.hasUnfixableGoals());
    assertEquals(0, registry.getGauges().get("AnomalyDetector.INTRA_BROKER_GOAL_VIOLATION-has-unfixable-goals").getValue());
    EasyMock.verify(inter, intra);
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static AnomalyDetectorManager manager(KafkaCruiseControl control, AnomalyNotifier notifier) {
    GoalViolationDetector goals = EasyMock.niceMock(GoalViolationDetector.class);
    KafkaBrokerFailureDetector brokers = EasyMock.niceMock(KafkaBrokerFailureDetector.class);
    MetricAnomalyDetector metrics = EasyMock.niceMock(MetricAnomalyDetector.class);
    DiskFailureDetector disks = EasyMock.niceMock(DiskFailureDetector.class);
    TopicAnomalyDetector topics = EasyMock.niceMock(TopicAnomalyDetector.class);
    MaintenanceEventDetector maintenance = EasyMock.niceMock(MaintenanceEventDetector.class);
    ScheduledExecutorService scheduler = EasyMock.niceMock(ScheduledExecutorService.class);
    EasyMock.replay(goals, brokers, metrics, disks, topics, maintenance, scheduler);
    return new AnomalyDetectorManager(new PriorityBlockingQueue<>(1, AnomalyDetectorUtils.anomalyComparator()), 1000,
        control, notifier, goals, brokers, metrics, disks, topics, maintenance, scheduler);
  }
}
