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

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskManager;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.executor.strategy.StrategyOptions;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicConfigurationParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.TopicReplicationFactorChangeParameters;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.*;

public class UpdateTopicConfigurationRunnableTest {
  private static final String GOAL_PACKAGE = "com.linkedin.kafka.cruisecontrol.analyzer.goals.";

  private static KafkaCruiseControlConfig config() {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.setProperty(AnalyzerConfig.DEFAULT_GOALS_CONFIG,
        GOAL_PACKAGE + "DiskCapacityGoal," + GOAL_PACKAGE + "IntraBrokerDiskUsageDistributionGoal");
    properties.setProperty(AnalyzerConfig.HARD_GOALS_CONFIG, GOAL_PACKAGE + "DiskCapacityGoal");
    properties.setProperty(AnalyzerConfig.INTER_BROKER_DISK_CAPACITY_CHECK_ENABLED_CONFIG, "true");
    return new KafkaCruiseControlConfig(properties);
  }

  private static UpdateTopicConfigurationRunnable userRunnable(KafkaCruiseControl control, List<String> goals) {
    TopicReplicationFactorChangeParameters rfParameters = EasyMock.niceMock(TopicReplicationFactorChangeParameters.class);
    EasyMock.expect(rfParameters.skipHardGoalCheck()).andReturn(true).anyTimes();
    EasyMock.replay(rfParameters);
    TopicConfigurationParameters parameters = EasyMock.niceMock(TopicConfigurationParameters.class);
    EasyMock.expect(parameters.goals()).andReturn(goals).anyTimes();
    EasyMock.expect(parameters.dryRun()).andReturn(true).anyTimes();
    EasyMock.expect(parameters.topicReplicationFactorChangeParameters()).andReturn(rfParameters).anyTimes();
    EasyMock.replay(parameters);
    return new UpdateTopicConfigurationRunnable(control, new OperationFuture("RF update"), "rf-update", parameters);
  }

  @Test
  public void testDefaultGoalsExcludeDiskBalancing() {
    List<Goal> goals = userRunnable(EasyMock.niceMock(KafkaCruiseControl.class), List.of()).goalsForOperation(config());
    assertEquals(List.of("DiskCapacityGoal"), goals.stream().map(Goal::name).toList());
  }

  @Test
  public void testExplicitDiskGoalRejectedBeforeModelGeneration() {
    KafkaCruiseControlConfig config = config();
    KafkaCruiseControl control = EasyMock.strictMock(KafkaCruiseControl.class);
    control.sanityCheckDryRun(true, false);
    EasyMock.expect(control.config()).andReturn(config);
    EasyMock.replay(control);
    UpdateTopicConfigurationRunnable runnable = userRunnable(control,
        List.of("DiskCapacityGoal", "intrabrokerdiskusagedistributiongoal"));
    try {
      runnable.init();
      fail("RF requests must reject explicit disk balancing before building or modifying a model");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("IntraBrokerDiskUsageDistributionGoal"));
      assertTrue(e.getMessage().contains("rebalance_disk=true"));
    }
    EasyMock.verify(control);
  }

  @Test
  public void testExplicitInterBrokerGoalOrderPreserved() {
    List<Goal> goals = userRunnable(EasyMock.niceMock(KafkaCruiseControl.class),
        List.of("ReplicaCapacityGoal", "DiskCapacityGoal")).goalsForOperation(config());
    assertEquals(List.of("ReplicaCapacityGoal", "DiskCapacityGoal"), goals.stream().map(Goal::name).toList());
  }

  @Test
  public void testAutomatedRfHealingUsesInterBrokerSubset() {
    KafkaCruiseControl control = EasyMock.niceMock(KafkaCruiseControl.class);
    KafkaCruiseControlConfig config = config();
    EasyMock.expect(control.config()).andReturn(config).anyTimes();
    EasyMock.replay(control);
    UpdateTopicConfigurationRunnable runnable = new UpdateTopicConfigurationRunnable(control,
        Map.of((short) 2, Pattern.compile("grow")), List.of("DiskCapacityGoal", "IntraBrokerDiskUsageDistributionGoal"),
        true, false, false, "rf-healing", () -> "RF self-healing", false, false);
    assertEquals(List.of("DiskCapacityGoal"), runnable.goalsForOperation(config).stream().map(Goal::name).toList());
  }

  @Test
  public void testDefaultsWithoutInterBrokerGoalsRejected() {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.setProperty(AnalyzerConfig.DEFAULT_GOALS_CONFIG, GOAL_PACKAGE + "IntraBrokerDiskUsageDistributionGoal");
    properties.put(AnalyzerConfig.HARD_GOALS_CONFIG, List.of());
    try {
      userRunnable(EasyMock.niceMock(KafkaCruiseControl.class), List.of())
          .goalsForOperation(new KafkaCruiseControlConfig(properties));
      fail("RF optimization needs an inter-broker goal");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("at least one inter-broker"));
    }
  }

  @Test
  public void testRfIncreaseWithMixedDefaultsKeepsDiskPlacementAndExecutableAdditions() throws Exception {
    assertRfPlan((short) 2);
  }

  @Test
  public void testRfDecreaseWithMixedDefaultsPreservesReplicaRemoval() throws Exception {
    assertRfPlan((short) 1);
  }

  private static void assertRfPlan(short targetRf) throws Exception {
    KafkaCruiseControlConfig config = config();
    ClusterModel model = new ClusterModel(new ModelGeneration(0, 0), 1.0);
    Map<Resource, Double> capacity = Map.of(Resource.DISK, 2000.0, Resource.CPU, 100.0,
        Resource.NW_IN, 10000.0, Resource.NW_OUT, 10000.0);
    for (int broker = 0; broker < 2; broker++) {
      model.createRack(Integer.toString(broker));
      model.createBroker(Integer.toString(broker), "host" + broker, broker,
          new BrokerCapacityInfo(capacity, Map.of("/a", 1000.0, "/b", 1000.0)), true);
      model.broker(broker).disks().forEach(disk -> disk.setReportedUtilization(0));
    }
    for (int partition = 0; partition < 10; partition++) {
      addReplica(model, "historical", partition, 0, 0, true, 50);
    }
    addReplica(model, "grow", 0, 0, 0, true, 10);
    if (targetRf == 1) {
      addReplica(model, "grow", 0, 1, 1, false, 10);
    }
    model.broker(1).disk("/a").setReportedUtilization(799);
    model.enableInterBrokerDiskCapacityCheck(0.8);
    Cluster initial = DeterministicCluster.generateClusterFromClusterModel(model);
    Map<TopicPartition, ReplicaPlacementInfo> initialLeaders = model.getLeaderDistribution();
    KafkaCruiseControl control = EasyMock.niceMock(KafkaCruiseControl.class);
    EasyMock.expect(control.clusterModel(EasyMock.anyObject(), EasyMock.anyBoolean(), EasyMock.anyObject())).andReturn(model);
    EasyMock.expect(control.executorState()).andReturn(ExecutorState.noTaskInProgress(Set.of(), Set.of()));
    EasyMock.expect(control.excludedTopics(EasyMock.same(model), EasyMock.anyObject())).andReturn(Set.of());
    EasyMock.expect(control.optimizations(EasyMock.same(model), EasyMock.anyObject(), EasyMock.anyObject(),
        EasyMock.anyObject(), EasyMock.anyObject())).andAnswer(() -> {
          List<Goal> goals = EasyMock.getCurrentArgument(1);
          Map<TopicPartition, List<ReplicaPlacementInfo>> initialReplicas = EasyMock.getCurrentArgument(3);
          OptimizationOptions options = EasyMock.getCurrentArgument(4);
          Set<Goal> optimizedGoals = new HashSet<>();
          for (Goal goal : goals) {
            goal.optimize(model, optimizedGoals, options);
            optimizedGoals.add(goal);
          }
          OptimizerResult result = EasyMock.mock(OptimizerResult.class);
          EasyMock.expect(result.goalProposals()).andReturn(AnalyzerUtils.getDiff(initialReplicas, initialLeaders, model, true)).anyTimes();
          EasyMock.replay(result);
          return result;
        });
    EasyMock.replay(control);
    UpdateTopicConfigurationRunnable runnable = userRunnable(control, List.of());
    runnable._cluster = initial;
    runnable._topicsToChangeByReplicationFactor = Map.of(targetRf, Set.of("grow"));
    runnable._goalsByPriority = runnable.goalsForOperation(config);
    runnable._operationProgress = new OperationProgress();
    runnable._combinedCompletenessRequirements = new ModelCompletenessRequirements(1, 0, true);
    Set<ExecutionProposal> proposals = runnable.workWithClusterModel().goalProposals();
    assertEquals(1, proposals.size());
    ExecutionProposal proposal = proposals.iterator().next();
    assertEquals(new TopicPartition("grow", 0), proposal.topicPartition());
    assertTrue(proposal.replicasToMoveBetweenDisksByBroker().isEmpty());
    if (targetRf == 2) {
      assertEquals(1, proposal.replicasToAdd().size());
      assertEquals("/b", proposal.replicasToAdd().iterator().next().logdir());
      assertFalse(proposal.destinationDiskCapacityByBroker().isEmpty());
    } else {
      assertEquals(1, proposal.replicasToRemove().size());
    }
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    EasyMock.replay(admin);
    ExecutionTaskManager manager = new ExecutionTaskManager(admin, new MetricRegistry(), Time.SYSTEM, config);
    manager.getExecutionConcurrencyManager().initialize(Set.of(0, 1), null, 0, null, null);
    manager.addExecutionProposals(proposals, Set.of(), new StrategyOptions.Builder(initial).build(), null);
    assertEquals(0, manager.numRemainingIntraBrokerPartitionMovements());
    assertEquals(1, manager.numRemainingInterBrokerPartitionMovements());
    assertEquals(1, manager.getInterBrokerReplicaMovementTasks().size());
    EasyMock.verify(control, admin);
  }

  private static void addReplica(ClusterModel model, String topic, int partition, int broker, int index, boolean leader, double size) {
    TopicPartition tp = new TopicPartition(topic, partition);
    model.createReplica(Integer.toString(broker), broker, tp, index, leader, false, "/a", false);
    AggregatedMetricValues load = new AggregatedMetricValues();
    for (Resource resource : Resource.cachedValues()) {
      KafkaCruiseControlUnitTestUtils.setValueForResource(load, resource, resource == Resource.DISK ? size : 0);
    }
    model.setReplicaLoad(Integer.toString(broker), broker, tp, load, List.of(1L));
  }
}
