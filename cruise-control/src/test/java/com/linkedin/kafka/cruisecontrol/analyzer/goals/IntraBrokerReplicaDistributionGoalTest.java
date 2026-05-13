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
 * Unit tests for {@link IntraBrokerReplicaDistributionGoal}.
 */
public class IntraBrokerReplicaDistributionGoalTest {

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
   * Test that the goal moves replicas from a disk with many replicas to a disk with few replicas.
   * Broker 0: disk0 has 6 replicas, disk1 has 0 replicas -> should rebalance to ~3 each.
   */
  @Test
  public void testReplicaDistributionBalancing() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    // Place 6 replicas on disk0 of broker 0, none on disk1
    List<Long> windows = Collections.singletonList(1L);
    for (int i = 0; i < 6; i++) {
      TopicPartition tp = new TopicPartition("topic" + i, 0);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      // Also create follower on broker 1 so partition is valid
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    Broker broker0 = cluster.broker(0);
    Disk disk0 = broker0.disk(TestConstants.LOGDIR0);
    Disk disk1 = broker0.disk(TestConstants.LOGDIR1);

    // Before optimization: all 6 replicas on disk0
    assertEquals(6, disk0.replicas().size());
    assertEquals(0, disk1.replicas().size());

    // Run the goal
    IntraBrokerReplicaDistributionGoal goal = new IntraBrokerReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress());

    // After optimization: replicas should be more balanced
    int disk0Count = broker0.disk(TestConstants.LOGDIR0).replicas().size();
    int disk1Count = broker0.disk(TestConstants.LOGDIR1).replicas().size();
    assertTrue("Disk0 should have fewer than 6 replicas after rebalance, got " + disk0Count, disk0Count < 6);
    assertTrue("Disk1 should have more than 0 replicas after rebalance, got " + disk1Count, disk1Count > 0);
    assertEquals("Total replicas should be preserved", 6, disk0Count + disk1Count);
  }

  /**
   * Test that already balanced disks are not touched.
   */
  @Test
  public void testAlreadyBalancedCluster() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    // Place 3 replicas on each disk of broker 0
    List<Long> windows = Collections.singletonList(1L);
    for (int i = 0; i < 3; i++) {
      TopicPartition tp = new TopicPartition("topic" + i, 0);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }
    for (int i = 3; i < 6; i++) {
      TopicPartition tp = new TopicPartition("topic" + i, 0);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    Broker broker0 = cluster.broker(0);
    assertEquals(3, broker0.disk(TestConstants.LOGDIR0).replicas().size());
    assertEquals(3, broker0.disk(TestConstants.LOGDIR1).replicas().size());

    IntraBrokerReplicaDistributionGoal goal = new IntraBrokerReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    Set<com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal> proposals =
        goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress()).goalProposals();

    // No proposals expected since the cluster is already balanced.
    assertEquals("No proposals expected for balanced cluster", 0, proposals.size());
  }

  /**
   * Test actionAcceptance: swap always accepted, movement from high to low accepted, movement worsening balance rejected.
   */
  @Test
  public void testActionAcceptance() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    // disk0: 4 replicas, disk1: 2 replicas
    List<Long> windows = Collections.singletonList(1L);
    for (int i = 0; i < 4; i++) {
      TopicPartition tp = new TopicPartition("topic" + i, 0);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }
    for (int i = 4; i < 6; i++) {
      TopicPartition tp = new TopicPartition("topic" + i, 0);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    IntraBrokerReplicaDistributionGoal goal = new IntraBrokerReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress());

    Broker broker0 = cluster.broker(0);
    Disk disk0 = broker0.disk(TestConstants.LOGDIR0);
    Disk disk1 = broker0.disk(TestConstants.LOGDIR1);

    // Swap between disks should always be accepted (no replica count change)
    TopicPartition tp0 = new TopicPartition("topic0", 0);
    TopicPartition tp4 = new TopicPartition("topic4", 0);
    BalancingAction swapAction = new BalancingAction(tp0, disk0, disk1, ActionType.INTRA_BROKER_REPLICA_SWAP, tp4);
    assertEquals(ActionAcceptance.ACCEPT, goal.actionAcceptance(swapAction, cluster));
  }

  /**
   * Test that goal handles a single-disk broker gracefully (no rebalancing needed/possible).
   */
  @Test
  public void testSingleDiskBroker() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    ClusterModel cluster = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    // Create a broker with only one disk
    Map<String, Double> singleDiskCapacity = Map.of(TestConstants.LOGDIR0, TestConstants.LARGE_BROKER_CAPACITY);
    BrokerCapacityInfo capacityInfo = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, singleDiskCapacity);
    cluster.createRack(RACK_A);
    cluster.createRack(RACK_B);
    cluster.createBroker(RACK_A, "0", 0, capacityInfo, true);
    // Second broker also single-disk to avoid proposals from broker 1
    cluster.createBroker(RACK_B, "1", 1, capacityInfo, true);

    List<Long> windows = Collections.singletonList(1L);
    for (int i = 0; i < 4; i++) {
      TopicPartition tp = new TopicPartition("topic" + i, 0);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    IntraBrokerReplicaDistributionGoal goal = new IntraBrokerReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    Set<com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal> proposals =
        goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress()).goalProposals();

    // Single-disk brokers: no intra-broker proposals possible
    Broker broker0 = cluster.broker(0);
    assertEquals(1, broker0.disks().size());
    assertEquals(4, broker0.disk(TestConstants.LOGDIR0).replicas().size());
    assertEquals("No proposals expected when all brokers have single disk", 0, proposals.size());
  }

  /**
   * Test that actionAcceptance rejects a movement that worsens balance (moving from low-count disk to high-count disk).
   */
  @Test
  public void testActionAcceptanceRejectsWorseningMove() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    // disk0: 2 replicas, disk1: 4 replicas (disk1 already has more)
    List<Long> windows = Collections.singletonList(1L);
    for (int i = 0; i < 2; i++) {
      TopicPartition tp = new TopicPartition("topic" + i, 0);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }
    for (int i = 2; i < 6; i++) {
      TopicPartition tp = new TopicPartition("topic" + i, 0);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    IntraBrokerReplicaDistributionGoal goal = new IntraBrokerReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress());

    Broker broker0 = cluster.broker(0);
    Disk disk0 = broker0.disk(TestConstants.LOGDIR0);
    Disk disk1 = broker0.disk(TestConstants.LOGDIR1);

    // Moving from disk0 (fewer replicas) to disk1 (more replicas) worsens balance -> should be rejected
    TopicPartition tp0 = new TopicPartition("topic0", 0);
    BalancingAction worseningMove = new BalancingAction(tp0, disk0, disk1, ActionType.INTRA_BROKER_REPLICA_MOVEMENT);
    assertEquals(ActionAcceptance.REPLICA_REJECT, goal.actionAcceptance(worseningMove, cluster));
  }

  /**
   * Test that replicas are moved IN to an underloaded disk (the rebalanceByMovingReplicasIn path).
   * Broker 0: disk0 has 0 replicas, disk1 has 6 replicas -> disk0 should receive replicas.
   */
  @Test
  public void testRebalanceByMovingReplicasIn() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    // Place all 6 replicas on disk1 of broker 0, none on disk0
    List<Long> windows = Collections.singletonList(1L);
    for (int i = 0; i < 6; i++) {
      TopicPartition tp = new TopicPartition("topic" + i, 0);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    Broker broker0 = cluster.broker(0);
    assertEquals(0, broker0.disk(TestConstants.LOGDIR0).replicas().size());
    assertEquals(6, broker0.disk(TestConstants.LOGDIR1).replicas().size());

    IntraBrokerReplicaDistributionGoal goal = new IntraBrokerReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress());

    int disk0Count = broker0.disk(TestConstants.LOGDIR0).replicas().size();
    int disk1Count = broker0.disk(TestConstants.LOGDIR1).replicas().size();
    assertTrue("Disk0 should have gained replicas after rebalance, got " + disk0Count, disk0Count > 0);
    assertTrue("Disk1 should have fewer than 6 replicas after rebalance, got " + disk1Count, disk1Count < 6);
    assertEquals("Total replicas should be preserved", 6, disk0Count + disk1Count);
  }

  @Test
  public void testIsSoftGoal() {
    IntraBrokerReplicaDistributionGoal goal = new IntraBrokerReplicaDistributionGoal();
    assertFalse("IntraBrokerReplicaDistributionGoal should be a soft goal", goal.isHardGoal());
  }

  /**
   * Test that a dead disk is skipped during rebalancing. Replicas on the dead disk should not be moved,
   * and the alive disk should not receive more replicas from the dead disk.
   */
  @Test
  public void testDeadDiskIsSkipped() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    // Place 4 replicas on disk0 and 2 on disk1 of broker 0
    List<Long> windows = Collections.singletonList(1L);
    for (int i = 0; i < 4; i++) {
      TopicPartition tp = new TopicPartition("topic" + i, 0);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }
    for (int i = 4; i < 6; i++) {
      TopicPartition tp = new TopicPartition("topic" + i, 0);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR1, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    // Mark disk1 as dead
    Broker broker0 = cluster.broker(0);
    Disk disk0 = broker0.disk(TestConstants.LOGDIR0);
    Disk disk1 = broker0.disk(TestConstants.LOGDIR1);
    disk1.setState(Disk.State.DEAD);
    assertFalse("disk1 should be dead", disk1.isAlive());

    IntraBrokerReplicaDistributionGoal goal = new IntraBrokerReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress());

    // Dead disk should not participate in rebalancing - disk0 keeps its replicas, disk1 is untouched
    assertEquals("disk0 should still have 4 replicas (no target to move to)", 4, disk0.replicas().size());
    assertEquals("disk1 (dead) should still have 2 replicas", 2, disk1.replicas().size());
  }

  /**
   * Test that balanced distribution is achieved within limits for a simple case (6 replicas, 2 disks).
   * With default threshold 1.10 and BALANCE_MARGIN 0.9, effective margin is 0.09.
   * For avg=3: upper=ceil(3*1.09)=4, lower=floor(3*0.91)=2. Distribution must be within [2, 4].
   */
  @Test
  public void testBalancedDistributionWithinLimits() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    // Place 6 replicas on disk0 of broker 0, none on disk1
    List<Long> windows = Collections.singletonList(1L);
    for (int i = 0; i < 6; i++) {
      TopicPartition tp = new TopicPartition("topic" + i, 0);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    IntraBrokerReplicaDistributionGoal goal = new IntraBrokerReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress());

    Broker broker0 = cluster.broker(0);
    int disk0Count = broker0.disk(TestConstants.LOGDIR0).replicas().size();
    int disk1Count = broker0.disk(TestConstants.LOGDIR1).replicas().size();
    assertEquals("Total replicas should be preserved", 6, disk0Count + disk1Count);
    assertTrue("Disk0 should be within balance limits [2, 4], got " + disk0Count,
               disk0Count >= 2 && disk0Count <= 4);
    assertTrue("Disk1 should be within balance limits [2, 4], got " + disk1Count,
               disk1Count >= 2 && disk1Count <= 4);
  }
}
