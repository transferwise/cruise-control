/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.AddBrokerParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.FixOfflineReplicasParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveBrokerParameters;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class InterBrokerOperationGoalsTest {
  private static final String GOAL_PACKAGE = "com.linkedin.kafka.cruisecontrol.analyzer.goals.";
  private final String _operation;
  private final boolean _diskCapacityCheck;

  public InterBrokerOperationGoalsTest(String operation, boolean diskCapacityCheck) {
    _operation = operation;
    _diskCapacityCheck = diskCapacityCheck;
  }

  /**
   * @return Each broker operation with disk-capacity checking disabled and enabled.
   */
  @Parameterized.Parameters(name = "{0}, disk checks={1}")
  public static Object[][] parameters() {
    return new Object[][] {{"add", false}, {"remove", false}, {"repair", false},
        {"add", true}, {"remove", true}, {"repair", true}};
  }

  private KafkaCruiseControlConfig config(boolean onlyDiskGoals) {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.setProperty(AnalyzerConfig.DEFAULT_GOALS_CONFIG, onlyDiskGoals
        ? GOAL_PACKAGE + "IntraBrokerDiskUsageDistributionGoal"
        : GOAL_PACKAGE + "DiskCapacityGoal," + GOAL_PACKAGE + "IntraBrokerDiskUsageDistributionGoal,"
            + GOAL_PACKAGE + "ReplicaCapacityGoal");
    properties.put(AnalyzerConfig.HARD_GOALS_CONFIG, onlyDiskGoals ? List.of() : List.of(GOAL_PACKAGE + "DiskCapacityGoal"));
    properties.setProperty(AnalyzerConfig.INTER_BROKER_DISK_CAPACITY_CHECK_ENABLED_CONFIG, Boolean.toString(_diskCapacityCheck));
    return new KafkaCruiseControlConfig(properties);
  }

  private KafkaCruiseControl control(KafkaCruiseControlConfig config) {
    KafkaCruiseControl control = EasyMock.niceMock(KafkaCruiseControl.class);
    EasyMock.expect(control.config()).andReturn(config).anyTimes();
    EasyMock.replay(control);
    return control;
  }

  private GoalBasedOperationRunnable userRunnable(KafkaCruiseControl control, List<String> goals, boolean stopOngoingExecution) {
    OperationFuture future = new OperationFuture("broker operation");
    switch (_operation) {
      case "add":
        AddBrokerParameters add = EasyMock.niceMock(AddBrokerParameters.class);
        EasyMock.expect(add.goals()).andReturn(goals).anyTimes();
        EasyMock.expect(add.skipHardGoalCheck()).andReturn(true).anyTimes();
        EasyMock.expect(add.stopOngoingExecution()).andReturn(stopOngoingExecution).anyTimes();
        EasyMock.replay(add);
        return new AddBrokersRunnable(control, future, add, "broker-operation");
      case "remove":
        RemoveBrokerParameters remove = EasyMock.niceMock(RemoveBrokerParameters.class);
        EasyMock.expect(remove.goals()).andReturn(goals).anyTimes();
        EasyMock.expect(remove.skipHardGoalCheck()).andReturn(true).anyTimes();
        EasyMock.expect(remove.stopOngoingExecution()).andReturn(stopOngoingExecution).anyTimes();
        EasyMock.replay(remove);
        return new RemoveBrokersRunnable(control, future, remove, "broker-operation");
      default:
        FixOfflineReplicasParameters repair = EasyMock.niceMock(FixOfflineReplicasParameters.class);
        EasyMock.expect(repair.goals()).andReturn(goals).anyTimes();
        EasyMock.expect(repair.skipHardGoalCheck()).andReturn(true).anyTimes();
        EasyMock.expect(repair.stopOngoingExecution()).andReturn(stopOngoingExecution).anyTimes();
        EasyMock.replay(repair);
        return new FixOfflineReplicasRunnable(control, future, repair, "broker-operation");
    }
  }

  private GoalBasedOperationRunnable healingRunnable(KafkaCruiseControl control, List<String> goals) {
    switch (_operation) {
      case "add":
        return new AddBrokersRunnable(control, Set.of(0), goals, true, false, false, "healing", () -> "healing", false);
      case "remove":
        return new RemoveBrokersRunnable(control, Set.of(0), goals, true, false, false, "healing", () -> "healing", false);
      default:
        return new FixOfflineReplicasRunnable(control, goals, true, false, false, "healing", () -> "healing", false);
    }
  }

  @Test
  public void testExplicitDiskGoalRejectedBeforeStoppingOrGeneratingModel() {
    KafkaCruiseControlConfig config = config(false);
    KafkaCruiseControl control = EasyMock.strictMock(KafkaCruiseControl.class);
    EasyMock.expect(control.config()).andReturn(config).anyTimes();
    control.sanityCheckDryRun(false, true);
    EasyMock.expect(control.config()).andReturn(config);
    EasyMock.replay(control);
    GoalBasedOperationRunnable runnable = userRunnable(control,
        List.of("DiskCapacityGoal", "intrabrokerdiskusagedistributiongoal"), true);
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, runnable::init);
    assertTrue(failure.getMessage().contains("IntraBrokerDiskUsageDistributionGoal"));
    assertTrue(failure.getMessage().contains("rebalance_disk=true"));
    EasyMock.verify(control);
  }

  @Test
  public void testMixedDefaultGoalsKeepInterBrokerPriority() {
    KafkaCruiseControlConfig config = config(false);
    List<Goal> goals = userRunnable(control(config), List.of(), false).goalsForOperation(config);
    assertEquals(List.of("DiskCapacityGoal", "ReplicaCapacityGoal"), goals.stream().map(Goal::name).toList());
  }

  @Test
  public void testExplicitInterBrokerGoalsKeepRequestOrder() {
    KafkaCruiseControlConfig config = config(false);
    List<Goal> goals = userRunnable(control(config), List.of("ReplicaCapacityGoal", "DiskCapacityGoal"), false)
        .goalsForOperation(config);
    assertEquals(List.of("ReplicaCapacityGoal", "DiskCapacityGoal"), goals.stream().map(Goal::name).toList());
  }

  @Test
  public void testDiskOnlyDefaultsRejected() {
    KafkaCruiseControlConfig config = config(true);
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> userRunnable(control(config), List.of(), false).goalsForOperation(config));
    assertTrue(failure.getMessage().contains("at least one inter-broker"));
  }

  @Test
  public void testAutomatedHealingFiltersDiskGoals() {
    KafkaCruiseControlConfig config = config(false);
    List<Goal> goals = healingRunnable(control(config),
        List.of("ReplicaCapacityGoal", "IntraBrokerDiskUsageDistributionGoal", "DiskCapacityGoal")).goalsForOperation(config);
    assertEquals(List.of("ReplicaCapacityGoal", "DiskCapacityGoal"), goals.stream().map(Goal::name).toList());
  }

  @Test
  public void testAutomatedBrokerHealingRejectsDiskOnlyGoals() {
    KafkaCruiseControlConfig config = config(false);
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> healingRunnable(control(config), List.of("IntraBrokerDiskUsageDistributionGoal")).goalsForOperation(config));
    assertTrue(failure.getMessage().contains("at least one inter-broker"));
  }
}
