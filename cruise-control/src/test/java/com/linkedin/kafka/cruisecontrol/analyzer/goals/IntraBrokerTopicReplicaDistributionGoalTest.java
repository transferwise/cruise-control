/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import com.codahale.metrics.MetricRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Unit tests for {@link IntraBrokerTopicReplicaDistributionGoal}.
 */
public class IntraBrokerTopicReplicaDistributionGoalTest {

  private static final String RACK_A = "A::0";
  private static final String RACK_B = "B::0";

  private KafkaCruiseControlConfig createConfig() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    return new KafkaCruiseControlConfig(props);
  }

  private ClusterModel createJbodCluster(Map<Integer, String> brokerToRack) {
    ClusterModel cluster = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    BrokerCapacityInfo capacityInfo = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, TestConstants.DISK_CAPACITY);
    brokerToRack.values().stream().distinct().forEach(cluster::createRack);
    brokerToRack.forEach((broker, rack) -> cluster.createBroker(
        rack, Integer.toString(broker), broker, capacityInfo,
        capacityInfo.diskCapacityByLogDir() != null));
    return cluster;
  }

  /**
   * Test that replicas of the same topic are rebalanced across disks.
   * Broker 0: disk0 has 6 replicas of topicA, disk1 has 0 replicas of topicA -> should rebalance topicA.
   */
  @Test
  public void testTopicReplicaDistributionBalancing() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    // Place 6 partitions of "topicA" all on disk0 of broker 0
    List<Long> windows = Collections.singletonList(1L);
    for (int i = 0; i < 6; i++) {
      TopicPartition tp = new TopicPartition("topicA", i);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    Broker broker0 = cluster.broker(0);
    Disk disk0 = broker0.disk(TestConstants.LOGDIR0);
    Disk disk1 = broker0.disk(TestConstants.LOGDIR1);

    // Before optimization: all 6 topicA replicas on disk0
    assertEquals(6, disk0.numReplicasOfTopic("topicA"));
    assertEquals(0, disk1.numReplicasOfTopic("topicA"));

    // Run the goal
    IntraBrokerTopicReplicaDistributionGoal goal = new IntraBrokerTopicReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress());

    // After optimization: topicA replicas should be more balanced across disks
    int disk0TopicA = broker0.disk(TestConstants.LOGDIR0).numReplicasOfTopic("topicA");
    int disk1TopicA = broker0.disk(TestConstants.LOGDIR1).numReplicasOfTopic("topicA");
    assertTrue("Disk0 should have fewer than 6 topicA replicas after rebalance, got " + disk0TopicA, disk0TopicA < 6);
    assertTrue("Disk1 should have more than 0 topicA replicas after rebalance, got " + disk1TopicA, disk1TopicA > 0);
    assertEquals("Total topicA replicas should be preserved", 6, disk0TopicA + disk1TopicA);
  }

  /**
   * Test with multiple topics: topicA all on disk0, topicB all on disk1 -> both should be rebalanced.
   */
  @Test
  public void testMultipleTopicRebalancing() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    List<Long> windows = Collections.singletonList(1L);
    // topicA: 4 partitions all on disk0
    for (int i = 0; i < 4; i++) {
      TopicPartition tp = new TopicPartition("topicA", i);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }
    // topicB: 4 partitions all on disk1
    for (int i = 0; i < 4; i++) {
      TopicPartition tp = new TopicPartition("topicB", i);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    Broker broker0 = cluster.broker(0);

    // Before: topicA all on disk0, topicB all on disk1
    assertEquals(4, broker0.disk(TestConstants.LOGDIR0).numReplicasOfTopic("topicA"));
    assertEquals(0, broker0.disk(TestConstants.LOGDIR1).numReplicasOfTopic("topicA"));
    assertEquals(0, broker0.disk(TestConstants.LOGDIR0).numReplicasOfTopic("topicB"));
    assertEquals(4, broker0.disk(TestConstants.LOGDIR1).numReplicasOfTopic("topicB"));

    IntraBrokerTopicReplicaDistributionGoal goal = new IntraBrokerTopicReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress());

    // After: both topics should be more balanced
    int disk0TopicA = broker0.disk(TestConstants.LOGDIR0).numReplicasOfTopic("topicA");
    int disk1TopicA = broker0.disk(TestConstants.LOGDIR1).numReplicasOfTopic("topicA");
    int disk0TopicB = broker0.disk(TestConstants.LOGDIR0).numReplicasOfTopic("topicB");
    int disk1TopicB = broker0.disk(TestConstants.LOGDIR1).numReplicasOfTopic("topicB");

    assertTrue("TopicA should have replicas on disk1 after rebalance", disk1TopicA > 0);
    assertTrue("TopicB should have replicas on disk0 after rebalance", disk0TopicB > 0);
    assertEquals("Total topicA replicas should be preserved", 4, disk0TopicA + disk1TopicA);
    assertEquals("Total topicB replicas should be preserved", 4, disk0TopicB + disk1TopicB);
  }

  /**
   * Test that already balanced per-topic distribution produces no proposals.
   */
  @Test
  public void testAlreadyBalancedTopicReplicas() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    List<Long> windows = Collections.singletonList(1L);
    // topicA: 2 partitions on disk0, 2 on disk1
    for (int i = 0; i < 2; i++) {
      TopicPartition tp = new TopicPartition("topicA", i);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }
    for (int i = 2; i < 4; i++) {
      TopicPartition tp = new TopicPartition("topicA", i);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    Broker broker0 = cluster.broker(0);
    assertEquals(2, broker0.disk(TestConstants.LOGDIR0).numReplicasOfTopic("topicA"));
    assertEquals(2, broker0.disk(TestConstants.LOGDIR1).numReplicasOfTopic("topicA"));

    IntraBrokerTopicReplicaDistributionGoal goal = new IntraBrokerTopicReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    Set<com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal> proposals =
        goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress()).goalProposals();

    assertEquals("No proposals expected for balanced topic replicas", 0, proposals.size());
  }

  /**
   * Test actionAcceptance: same-topic swap always accepted (no per-topic count change).
   */
  @Test
  public void testActionAcceptanceSameTopicSwap() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    // topicA: 3 on disk0, 1 on disk1
    List<Long> windows = Collections.singletonList(1L);
    for (int i = 0; i < 3; i++) {
      TopicPartition tp = new TopicPartition("topicA", i);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }
    TopicPartition tpA3 = new TopicPartition("topicA", 3);
    cluster.createReplica(RACK_A, 0, tpA3, 0, true, false, TestConstants.LOGDIR1, false);
    cluster.setReplicaLoad(RACK_A, 0, tpA3, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
    cluster.createReplica(RACK_B, 1, tpA3, 1, false, false, TestConstants.LOGDIR1, false);
    cluster.setReplicaLoad(RACK_B, 1, tpA3, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);

    IntraBrokerTopicReplicaDistributionGoal goal = new IntraBrokerTopicReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress());

    Broker broker0 = cluster.broker(0);
    Disk disk0 = broker0.disk(TestConstants.LOGDIR0);
    Disk disk1 = broker0.disk(TestConstants.LOGDIR1);

    // Same-topic swap should always be accepted
    TopicPartition tpA0 = new TopicPartition("topicA", 0);
    BalancingAction sameTopicSwap = new BalancingAction(tpA0, disk0, disk1, ActionType.INTRA_BROKER_REPLICA_SWAP, tpA3);
    assertEquals(ActionAcceptance.ACCEPT, goal.actionAcceptance(sameTopicSwap, cluster));
  }

  /**
   * Test that goal handles a single-disk broker gracefully.
   */
  @Test
  public void testSingleDiskBroker() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    ClusterModel cluster = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    Map<String, Double> singleDiskCapacity = Map.of(TestConstants.LOGDIR0, TestConstants.LARGE_BROKER_CAPACITY);
    BrokerCapacityInfo capacityInfo = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, singleDiskCapacity);
    cluster.createRack(RACK_A);
    cluster.createRack(RACK_B);
    cluster.createBroker(RACK_A, "0", 0, capacityInfo, true);
    // Second broker also single-disk to avoid proposals from broker 1
    cluster.createBroker(RACK_B, "1", 1, capacityInfo, true);

    List<Long> windows = Collections.singletonList(1L);
    for (int i = 0; i < 4; i++) {
      TopicPartition tp = new TopicPartition("topicA", i);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    IntraBrokerTopicReplicaDistributionGoal goal = new IntraBrokerTopicReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    Set<com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal> proposals =
        goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress()).goalProposals();

    Broker broker0 = cluster.broker(0);
    assertEquals(1, broker0.disks().size());
    assertEquals(4, broker0.disk(TestConstants.LOGDIR0).numReplicasOfTopic("topicA"));
    assertEquals("No proposals expected when all brokers have single disk", 0, proposals.size());
  }

  /**
   * Test actionAcceptance: cross-topic swap that worsens balance for one topic is rejected.
   * disk0: 3 topicA + 1 topicB, disk1: 1 topicA + 3 topicB
   * Swapping topicA(disk1) with topicB(disk0) would worsen both topics -> should reject.
   */
  @Test
  public void testActionAcceptanceCrossTopicSwapRejected() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    List<Long> windows = Collections.singletonList(1L);
    // disk0: 3 topicA replicas
    for (int i = 0; i < 3; i++) {
      TopicPartition tp = new TopicPartition("topicA", i);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }
    // disk0: 1 topicB replica
    TopicPartition tpB0 = new TopicPartition("topicB", 0);
    cluster.createReplica(RACK_A, 0, tpB0, 0, true, false, TestConstants.LOGDIR0, false);
    cluster.setReplicaLoad(RACK_A, 0, tpB0, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
    cluster.createReplica(RACK_B, 1, tpB0, 1, false, false, TestConstants.LOGDIR0, false);
    cluster.setReplicaLoad(RACK_B, 1, tpB0, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);

    // disk1: 1 topicA replica
    TopicPartition tpA3 = new TopicPartition("topicA", 3);
    cluster.createReplica(RACK_A, 0, tpA3, 0, true, false, TestConstants.LOGDIR1, false);
    cluster.setReplicaLoad(RACK_A, 0, tpA3, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
    cluster.createReplica(RACK_B, 1, tpA3, 1, false, false, TestConstants.LOGDIR1, false);
    cluster.setReplicaLoad(RACK_B, 1, tpA3, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);

    // disk1: 3 topicB replicas
    for (int i = 1; i < 4; i++) {
      TopicPartition tp = new TopicPartition("topicB", i);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    IntraBrokerTopicReplicaDistributionGoal goal = new IntraBrokerTopicReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress());

    Broker broker0 = cluster.broker(0);
    Disk disk0 = broker0.disk(TestConstants.LOGDIR0);
    Disk disk1 = broker0.disk(TestConstants.LOGDIR1);

    // Swapping topicA(disk1) with topicB(disk0) worsens topicA distribution:
    // topicA: disk0 goes 3->4, disk1 goes 1->0 (worse)
    // topicB: disk0 goes 1->0, disk1 goes 3->4 (worse)
    BalancingAction crossTopicSwap = new BalancingAction(tpA3, disk1, disk0, ActionType.INTRA_BROKER_REPLICA_SWAP, tpB0);
    assertEquals(ActionAcceptance.REPLICA_REJECT, goal.actionAcceptance(crossTopicSwap, cluster));
  }

  /**
   * Test actionAcceptance: movement that worsens per-topic balance is rejected.
   */
  @Test
  public void testActionAcceptanceRejectsWorseningMove() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    List<Long> windows = Collections.singletonList(1L);
    // disk0: 1 topicA replica, disk1: 3 topicA replicas
    TopicPartition tpA0 = new TopicPartition("topicA", 0);
    cluster.createReplica(RACK_A, 0, tpA0, 0, true, false, TestConstants.LOGDIR0, false);
    cluster.setReplicaLoad(RACK_A, 0, tpA0, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
    cluster.createReplica(RACK_B, 1, tpA0, 1, false, false, TestConstants.LOGDIR0, false);
    cluster.setReplicaLoad(RACK_B, 1, tpA0, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);

    for (int i = 1; i < 4; i++) {
      TopicPartition tp = new TopicPartition("topicA", i);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    IntraBrokerTopicReplicaDistributionGoal goal = new IntraBrokerTopicReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress());

    Broker broker0 = cluster.broker(0);
    Disk disk0 = broker0.disk(TestConstants.LOGDIR0);
    Disk disk1 = broker0.disk(TestConstants.LOGDIR1);

    // Moving topicA from disk0 (1 replica) to disk1 (3 replicas) worsens balance -> should reject
    BalancingAction worseningMove = new BalancingAction(tpA0, disk0, disk1, ActionType.INTRA_BROKER_REPLICA_MOVEMENT);
    assertEquals(ActionAcceptance.REPLICA_REJECT, goal.actionAcceptance(worseningMove, cluster));
  }

  @Test
  public void testIsSoftGoal() {
    IntraBrokerTopicReplicaDistributionGoal goal = new IntraBrokerTopicReplicaDistributionGoal();
    assertFalse("IntraBrokerTopicReplicaDistributionGoal should be a soft goal", goal.isHardGoal());
  }

  /**
   * Test that a dead disk is skipped during topic replica rebalancing.
   */
  @Test
  public void testDeadDiskIsSkipped() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    // Place 4 replicas of topicA on disk0, 0 on disk1
    List<Long> windows = Collections.singletonList(1L);
    for (int i = 0; i < 4; i++) {
      TopicPartition tp = new TopicPartition("topicA", i);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    // Mark disk1 as dead
    Broker broker0 = cluster.broker(0);
    Disk disk0 = broker0.disk(TestConstants.LOGDIR0);
    Disk disk1 = broker0.disk(TestConstants.LOGDIR1);
    disk1.setState(Disk.State.DEAD);
    assertFalse("disk1 should be dead", disk1.isAlive());

    IntraBrokerTopicReplicaDistributionGoal goal = new IntraBrokerTopicReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress());

    // Dead disk should not participate - disk0 keeps all replicas
    assertEquals("disk0 should still have 4 topicA replicas", 4, disk0.numReplicasOfTopic("topicA"));
    assertEquals("disk1 (dead) should have 0 topicA replicas", 0, disk1.numReplicasOfTopic("topicA"));
  }

  /**
   * Test that the topic goal uses cross-topic swaps when one-way moves are blocked by a previously
   * optimized IntraBrokerReplicaDistributionGoal.
   *
   * Scenario: Broker has equal total replica counts per disk (4 each) but skewed per-topic:
   *   disk0: 4 topicA, 0 topicB
   *   disk1: 0 topicA, 4 topicB
   *
   * The IntraBrokerReplicaDistributionGoal runs first and locks in equal total counts per disk.
   * Moving a topicA from disk0->disk1 would create 3 vs 5 total replicas, which the replica goal rejects.
   * The topic goal should use swaps (topicA on disk0 <-> topicB on disk1) to fix per-topic balance.
   */
  @Test
  public void testCrossTopicSwapWhenMovesBlockedByReplicaGoal() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    List<Long> windows = Collections.singletonList(1L);
    // disk0: 4 topicA replicas
    for (int i = 0; i < 4; i++) {
      TopicPartition tp = new TopicPartition("topicA", i);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }
    // disk1: 4 topicB replicas
    for (int i = 0; i < 4; i++) {
      TopicPartition tp = new TopicPartition("topicB", i);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    Broker broker0 = cluster.broker(0);
    // Before: balanced total (4+4), but skewed per-topic
    assertEquals(4, broker0.disk(TestConstants.LOGDIR0).replicas().size());
    assertEquals(4, broker0.disk(TestConstants.LOGDIR1).replicas().size());
    assertEquals(4, broker0.disk(TestConstants.LOGDIR0).numReplicasOfTopic("topicA"));
    assertEquals(0, broker0.disk(TestConstants.LOGDIR1).numReplicasOfTopic("topicA"));

    // Run with replica distribution goal first (which locks total counts), then topic goal.
    IntraBrokerReplicaDistributionGoal replicaGoal = new IntraBrokerReplicaDistributionGoal(constraint);
    replicaGoal.configure(config.mergedConfigValues());
    IntraBrokerTopicReplicaDistributionGoal topicGoal = new IntraBrokerTopicReplicaDistributionGoal(constraint);
    topicGoal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Arrays.asList(replicaGoal, topicGoal), new OperationProgress());

    // After: total counts per disk should still be equal (4 each) and per-topic should be improved.
    Disk disk0 = broker0.disk(TestConstants.LOGDIR0);
    Disk disk1 = broker0.disk(TestConstants.LOGDIR1);
    assertEquals("Total replicas on disk0 should remain 4", 4, disk0.replicas().size());
    assertEquals("Total replicas on disk1 should remain 4", 4, disk1.replicas().size());
    assertTrue("topicA should now have replicas on disk1", disk1.numReplicasOfTopic("topicA") > 0);
    assertTrue("topicB should now have replicas on disk0", disk0.numReplicasOfTopic("topicB") > 0);
    assertEquals("Total topicA replicas preserved", 4,
        disk0.numReplicasOfTopic("topicA") + disk1.numReplicasOfTopic("topicA"));
    assertEquals("Total topicB replicas preserved", 4,
        disk0.numReplicasOfTopic("topicB") + disk1.numReplicasOfTopic("topicB"));
  }
}
