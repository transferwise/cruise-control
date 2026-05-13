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
 * Unit tests for {@link IntraBrokerLeaderReplicaDistributionGoal}.
 */
public class IntraBrokerLeaderReplicaDistributionGoalTest {

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
   * Test that leader replicas are rebalanced across disks.
   * Broker 0: disk0 has 6 leader replicas, disk1 has 0 leader replicas -> should rebalance.
   */
  @Test
  public void testLeaderReplicaDistributionBalancing() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    // Place 6 leader replicas on disk0 of broker 0, with followers on broker 1
    List<Long> windows = Collections.singletonList(1L);
    for (int i = 0; i < 6; i++) {
      TopicPartition tp = new TopicPartition("topic" + i, 0);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    Broker broker0 = cluster.broker(0);
    Disk disk0 = broker0.disk(TestConstants.LOGDIR0);
    Disk disk1 = broker0.disk(TestConstants.LOGDIR1);

    // Before optimization: all 6 leader replicas on disk0
    assertEquals(6, disk0.numLeaderReplicas());
    assertEquals(0, disk1.numLeaderReplicas());

    // Run the goal
    IntraBrokerLeaderReplicaDistributionGoal goal = new IntraBrokerLeaderReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress());

    // After optimization: leader replicas should be more balanced across disks
    int disk0Leaders = broker0.disk(TestConstants.LOGDIR0).numLeaderReplicas();
    int disk1Leaders = broker0.disk(TestConstants.LOGDIR1).numLeaderReplicas();
    assertTrue("Disk0 should have fewer than 6 leaders after rebalance, got " + disk0Leaders, disk0Leaders < 6);
    assertTrue("Disk1 should have more than 0 leaders after rebalance, got " + disk1Leaders, disk1Leaders > 0);
    assertEquals("Total leader replicas should be preserved", 6, disk0Leaders + disk1Leaders);
  }

  /**
   * Test that already balanced leader replicas across disks produce no proposals.
   */
  @Test
  public void testAlreadyBalancedLeaders() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    // Place 3 leader replicas on each disk of broker 0
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
    assertEquals(3, broker0.disk(TestConstants.LOGDIR0).numLeaderReplicas());
    assertEquals(3, broker0.disk(TestConstants.LOGDIR1).numLeaderReplicas());

    IntraBrokerLeaderReplicaDistributionGoal goal = new IntraBrokerLeaderReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    Set<com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal> proposals =
        goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress()).goalProposals();

    assertEquals("No proposals expected for balanced leaders", 0, proposals.size());
  }

  /**
   * Test actionAcceptance: moving a follower is always accepted (does not change leader counts).
   */
  @Test
  public void testActionAcceptanceFollowerMove() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    // disk0: 3 leaders + 1 follower, disk1: 3 leaders
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
    // Add one follower on disk0 of broker 0 (leader on broker 1)
    TopicPartition followerTp = new TopicPartition("topicFollower", 0);
    cluster.createReplica(RACK_B, 1, followerTp, 0, true, false, TestConstants.LOGDIR0, false);
    cluster.setReplicaLoad(RACK_B, 1, followerTp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
    cluster.createReplica(RACK_A, 0, followerTp, 1, false, false, TestConstants.LOGDIR0, false);
    cluster.setReplicaLoad(RACK_A, 0, followerTp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);

    IntraBrokerLeaderReplicaDistributionGoal goal = new IntraBrokerLeaderReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress());

    Broker broker0 = cluster.broker(0);
    Disk disk0 = broker0.disk(TestConstants.LOGDIR0);
    Disk disk1 = broker0.disk(TestConstants.LOGDIR1);

    // Moving a follower between disks should always be accepted (no leader count change)
    BalancingAction followerMove = new BalancingAction(followerTp, disk0, disk1, ActionType.INTRA_BROKER_REPLICA_MOVEMENT);
    assertEquals(ActionAcceptance.ACCEPT, goal.actionAcceptance(followerMove, cluster));
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
      TopicPartition tp = new TopicPartition("topic" + i, 0);
      cluster.createReplica(RACK_A, 0, tp, 0, true, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_A, 0, tp, getAggregatedMetricValues(10.0, 10.0, 10.0, 1000.0), windows);
      cluster.createReplica(RACK_B, 1, tp, 1, false, false, TestConstants.LOGDIR0, false);
      cluster.setReplicaLoad(RACK_B, 1, tp, getAggregatedMetricValues(5.0, 5.0, 5.0, 1000.0), windows);
    }

    IntraBrokerLeaderReplicaDistributionGoal goal = new IntraBrokerLeaderReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    Set<com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal> proposals =
        goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress()).goalProposals();

    Broker broker0 = cluster.broker(0);
    assertEquals(1, broker0.disks().size());
    assertEquals(4, broker0.disk(TestConstants.LOGDIR0).numLeaderReplicas());
    assertEquals("No proposals expected when all brokers have single disk", 0, proposals.size());
  }

  @Test
  public void testIsSoftGoal() {
    IntraBrokerLeaderReplicaDistributionGoal goal = new IntraBrokerLeaderReplicaDistributionGoal();
    assertFalse("IntraBrokerLeaderReplicaDistributionGoal should be a soft goal", goal.isHardGoal());
  }

  /**
   * Test that a dead disk is skipped during leader rebalancing.
   */
  @Test
  public void testDeadDiskIsSkipped() throws Exception {
    KafkaCruiseControlConfig config = createConfig();
    BalancingConstraint constraint = new BalancingConstraint(config);

    Map<Integer, String> brokerToRack = Map.of(0, RACK_A, 1, RACK_B);
    ClusterModel cluster = createJbodCluster(brokerToRack);

    // Place 4 leaders on disk0 and 2 leaders on disk1
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

    IntraBrokerLeaderReplicaDistributionGoal goal = new IntraBrokerLeaderReplicaDistributionGoal(constraint);
    goal.configure(config.mergedConfigValues());

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(),
                                                     EasyMock.mock(Executor.class), EasyMock.mock(AdminClient.class));
    goalOptimizer.optimizations(cluster, Collections.singletonList(goal), new OperationProgress());

    // Dead disk should not participate - no moves possible
    assertEquals("disk0 should still have 4 leaders", 4, disk0.numLeaderReplicas());
    assertEquals("disk1 (dead) should still have 2 leaders", 2, disk1.numLeaderReplicas());
  }
}
