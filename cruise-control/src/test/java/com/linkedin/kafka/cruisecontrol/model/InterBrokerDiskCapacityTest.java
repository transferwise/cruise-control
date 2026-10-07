/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.*;

public class InterBrokerDiskCapacityTest {
  private static final String SOURCE = "/source";
  private static final String SMALL = "/small";
  private static final String LARGE = "/large";

  private ClusterModel cluster() {
    ClusterModel model = new ClusterModel(new ModelGeneration(0, 0), 1.0);
    Map<Resource, Double> capacity = Map.of(Resource.DISK, 1100.0, Resource.CPU, 100.0,
                                          Resource.NW_IN, 10000.0, Resource.NW_OUT, 10000.0);
    model.createRack("0");
    model.createRack("1");
    model.createBroker("0", "host0", 0, new BrokerCapacityInfo(capacity, Map.of(SOURCE, 1100.0)), true);
    model.createBroker("1", "host1", 1, new BrokerCapacityInfo(capacity, Map.of(SMALL, 100.0, LARGE, 1000.0)), true);
    model.broker(0).disk(SOURCE).setReportedUtilization(0);
    model.broker(1).disk(SMALL).setReportedUtilization(79);
    model.broker(1).disk(LARGE).setReportedUtilization(600);
    return model;
  }

  private Replica addReplica(ClusterModel model, int partition, double size) {
    return addReplica(model, partition, size, 0, SOURCE);
  }

  private Replica addReplica(ClusterModel model, int partition, double size, int broker, String logdir) {
    TopicPartition tp = new TopicPartition("topic", partition);
    model.createReplica(Integer.toString(broker), broker, tp, 0, true, false, logdir, false);
    AggregatedMetricValues load = new AggregatedMetricValues();
    for (Resource resource : Resource.cachedValues()) {
      KafkaCruiseControlUnitTestUtils.setValueForResource(load, resource, resource == Resource.DISK ? size : 0);
    }
    model.setReplicaLoad(Integer.toString(broker), broker, tp, load, List.of(1L));
    return model.broker(broker).replica(tp);
  }

  private void increaseReplicationFactor(ClusterModel model) {
    model.createOrDeleteReplicas(Map.of((short) 2, Set.of("topic")),
        Map.of("0", List.of(0), "1", List.of(1)), Map.of(0, "0", 1, "1"),
        com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.generateClusterFromClusterModel(model));
  }

  @Test
  public void testDeletingFutureReplicaReleasesOnlyItsPlannedReservation() {
    ClusterModel model = cluster();
    addReplica(model, 0, 150);
    model.enableInterBrokerDiskCapacityCheck(0.8);
    increaseReplicationFactor(model);
    Replica incoming = addReplica(model, 1, 150);
    assertNull(model.destinationDisk(incoming, model.broker(1)));
    Replica deleted = model.broker(1).replica(new TopicPartition("topic", 0));
    model.deleteReplica(new TopicPartition("topic", 0), 1);
    assertFalse(model.broker(1).disk(LARGE).replicas().contains(deleted));
    assertNotNull(model.destinationDisk(incoming, model.broker(1)));
  }

  @Test
  public void testZeroThresholdRemainsEnabledAndRejectsIncomingCopies() {
    ClusterModel model = cluster();
    Replica incoming = addReplica(model, 0, 10);
    model.enableInterBrokerDiskCapacityCheck(0);
    assertTrue(model.interBrokerDiskCapacityCheckEnabled());
    assertNull(model.destinationDisk(incoming, model.broker(1)));
  }

  @Test
  public void testDisabledCheckAllowsBrokerOnlyRelocationWhenAllDestinationDisksDead() {
    ClusterModel model = cluster();
    Replica incoming = addReplica(model, 0, 10);
    model.broker(1).disks().forEach(d -> d.setState(Disk.State.DEAD));
    model.relocateReplica(incoming.topicPartition(), 0, 1);
    assertSame(model.broker(1), incoming.broker());
    assertNull(incoming.disk());
    assertFalse(model.broker(0).disk(SOURCE).replicas().contains(incoming));
  }

  @Test
  public void testSupersededCopyReleasesReservationWithoutCreditingLiveSource() {
    ClusterModel model = cluster();
    Replica first = addReplica(model, 0, 150);
    Replica second = addReplica(model, 1, 150);
    model.enableInterBrokerDiskCapacityCheck(0.8);
    model.relocateReplica(first.topicPartition(), 0, 1);
    assertNull(model.destinationDisk(second, model.broker(1)));
    model.relocateReplica(first.topicPartition(), 1, 0);
    assertNotNull(model.destinationDisk(second, model.broker(1)));
    model.relocateReplica(second.topicPartition(), 0, 1);
    assertEquals(LARGE, second.disk().logDir());
  }

  @Test
  public void testReplicationFactorIncreaseSkipsFullBrokerInSameRack() {
    ClusterModel model = cluster();
    addReplica(model, 0, 200);
    Map<Resource, Double> capacity = Map.of(Resource.DISK, 1000.0, Resource.CPU, 100.0,
                                          Resource.NW_IN, 10000.0, Resource.NW_OUT, 10000.0);
    model.createBroker("1", "host2", 2, new BrokerCapacityInfo(capacity, Map.of(LARGE, 1000.0)), true);
    model.broker(2).disk(LARGE).setReportedUtilization(0);
    model.enableInterBrokerDiskCapacityCheck(0.8);
    model.createOrDeleteReplicas(Map.of((short) 2, Set.of("topic")),
        Map.of("0", List.of(0), "1", List.of(1, 2)), Map.of(0, "0", 1, "1", 2, "1"),
        com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.generateClusterFromClusterModel(model));
    assertNull(model.broker(1).replica(new TopicPartition("topic", 0)));
    assertEquals(LARGE, model.broker(2).replica(new TopicPartition("topic", 0)).disk().logDir());
  }

  @Test
  public void testReplicationFactorIncreaseAssignsAndReservesDiskWithoutRelocation() {
    ClusterModel model = cluster();
    addReplica(model, 0, 150);
    Map<TopicPartition, List<ReplicaPlacementInfo>> initialReplicas = model.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initialLeaders = model.getLeaderDistribution();
    model.enableInterBrokerDiskCapacityCheck(0.8);
    increaseReplicationFactor(model);
    Replica created = model.broker(1).replica(new TopicPartition("topic", 0));
    assertEquals(LARGE, created.disk().logDir());
    assertTrue(model.broker(1).disk(LARGE).replicas().contains(created));
    assertEquals(150, created.disk().utilization(), 0.001);
    Set<ExecutionProposal> proposals = AnalyzerUtils.getDiff(initialReplicas, initialLeaders, model, true);
    assertEquals(1, proposals.size());
    assertEquals(Map.of(1, 1000.0), proposals.iterator().next().destinationDiskCapacityByBroker());
    assertEquals(LARGE, proposals.iterator().next().replicasToAdd().iterator().next().logdir());
    Replica next = addReplica(model, 1, 150);
    assertNull(model.destinationDisk(next, model.broker(1)));
  }

  @Test
  public void testReplicationFactorIncreaseRejectsFullDisksBeforeAddingReplica() {
    ClusterModel model = cluster();
    addReplica(model, 0, 200);
    model.enableInterBrokerDiskCapacityCheck(0.8);
    assertThrows(IllegalStateException.class, () -> increaseReplicationFactor(model));
    assertNull(model.broker(1).replica(new TopicPartition("topic", 0)));
    assertEquals(1, model.partition(new TopicPartition("topic", 0)).replicas().size());
  }

  @Test
  public void testReplicationFactorIncreaseRemainsCompatibleWhenCheckDisabled() {
    ClusterModel model = cluster();
    addReplica(model, 0, 200);
    increaseReplicationFactor(model);
    assertNull(model.broker(1).replica(new TopicPartition("topic", 0)).disk());
    assertEquals(2, model.partition(new TopicPartition("topic", 0)).replicas().size());
  }

  @Test
  public void testSelectsRoomyDiskAndCarriesPlacementIntoProposal() {
    ClusterModel model = cluster();
    Replica replica = addReplica(model, 0, 30);
    Map<TopicPartition, List<ReplicaPlacementInfo>> initialReplicas = model.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initialLeaders = model.getLeaderDistribution();
    model.enableInterBrokerDiskCapacityCheck(0.8);
    assertTrue(GoalUtils.legitMove(replica, model.broker(1), model, ActionType.INTER_BROKER_REPLICA_MOVEMENT));
    model.relocateReplica(replica.topicPartition(), 0, 1);
    assertEquals(LARGE, replica.disk().logDir());
    assertFalse(model.broker(0).disk(SOURCE).replicas().contains(replica));
    assertTrue(model.broker(1).disk(LARGE).replicas().contains(replica));
    assertEquals(30, model.broker(1).disk(LARGE).utilization(), 0.001);
    ExecutionProposal proposal = AnalyzerUtils.getDiff(initialReplicas, initialLeaders, model).iterator().next();
    assertEquals(LARGE, proposal.replicasToAdd().iterator().next().logdir());
    assertEquals(Map.of(1, 1000.0), proposal.destinationDiskCapacityByBroker());
    assertEquals(Map.of(1, LARGE), proposal.getJsonStructure().get("destinationLogDirs"));
    assertTrue(proposal.replicasToMoveBetweenDisksByBroker().isEmpty());
  }

  @Test
  public void testReservesAllIncomingCopiesAndRejectsWhenNoDiskFits() {
    ClusterModel model = cluster();
    Replica first = addReplica(model, 0, 150);
    Replica second = addReplica(model, 1, 150);
    model.enableInterBrokerDiskCapacityCheck(0.8);
    model.relocateReplica(first.topicPartition(), 0, 1);
    assertFalse(GoalUtils.legitMove(second, model.broker(1), model, ActionType.INTER_BROKER_REPLICA_MOVEMENT));
    assertThrows(IllegalStateException.class, () -> model.relocateReplica(second.topicPartition(), 0, 1));
    assertSame(model.broker(0), second.broker());
  }

  @Test
  public void testMixedGoalsReserveIntraBrokerCopiesBeforeLaterInterBrokerMoves() {
    ClusterModel model = cluster();
    Replica intraBroker = addReplica(model, 99, 150, 1, SMALL);
    Replica incoming = addReplica(model, 0, 150);
    model.enableInterBrokerDiskCapacityCheck(0.8);
    model.relocateReplica(intraBroker.topicPartition(), 1, LARGE);
    assertNull(model.destinationDisk(incoming, model.broker(1)));
  }

  @Test
  public void testInterBrokerGoalsRespectDiskPlacementThroughoutOptimization() throws Exception {
    ClusterModel model = cluster();
    for (int partition = 0; partition < 6; partition++) {
      addReplica(model, partition, 150);
    }
    model.broker(0).disk(SOURCE).setReportedUtilization(900);
    model.enableInterBrokerDiskCapacityCheck(0.8);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    Set<Goal> optimized = new HashSet<>();
    OptimizationOptions options = new OptimizationOptions(Set.of(), Set.of(), Set.of());
    for (Goal goal : List.of(new DiskCapacityGoal(), new DiskUsageDistributionGoal())) {
      goal.configure(config.mergedConfigValues());
      goal.optimize(model, optimized, options);
      optimized.add(goal);
    }
    assertEquals(1, model.broker(1).replicas().size());
    assertEquals(LARGE, model.broker(1).replicas().iterator().next().disk().logDir());
    assertEquals(750, model.broker(0).load().expectedUtilizationFor(Resource.DISK), 0.001);
  }

  @Test
  public void testUsesLiveReplicaSizeAndExcludesDeadAndZeroCapacityDisks() {
    ClusterModel model = cluster();
    Replica replica = addReplica(model, 0, 1);
    model.recordReplicaDiskSize(replica.topicPartition(), 201);
    model.enableInterBrokerDiskCapacityCheck(0.8);
    assertNull(model.destinationDisk(replica, model.broker(1)));
    model.broker(1).disk(LARGE).setState(Disk.State.DEAD);
    model.broker(1).disk(SMALL).markDiskForRemoval();
    assertFalse(model.canMoveReplicaToBroker(replica, model.broker(1)));
  }

  @Test
  public void testSelectsByProjectedPercentageForHeterogeneousDisks() {
    ClusterModel model = cluster();
    Replica replica = addReplica(model, 0, 10);
    model.broker(1).disk(SMALL).setReportedUtilization(20);
    model.broker(1).disk(LARGE).setReportedUtilization(100);
    model.enableInterBrokerDiskCapacityCheck(0.8);
    assertEquals(LARGE, model.destinationDisk(replica, model.broker(1)).logDir());
  }

  @Test
  public void testDoesNotCreditOutgoingSpace() {
    ClusterModel model = cluster();
    Replica outgoing = addReplica(model, 0, 150);
    Replica incoming = addReplica(model, 1, 100, 1, LARGE);
    model.broker(0).disk(SOURCE).setReportedUtilization(850);
    model.enableInterBrokerDiskCapacityCheck(0.8);
    model.relocateReplica(outgoing.topicPartition(), 0, 1);
    assertNull(model.destinationDisk(incoming, model.broker(0)));
  }

  @Test
  public void testFailsClosedWhenUsageMissing() {
    ClusterModel model = cluster();
    model.broker(1).disk(LARGE).setReportedUtilization(0);
    model.createRack("2");
    model.createBroker("2", "host2", 2, new BrokerCapacityInfo(Map.of(Resource.DISK, 100.0, Resource.CPU, 100.0,
                                                                  Resource.NW_IN, 100.0, Resource.NW_OUT, 100.0),
                                                            Map.of("/unknown", 100.0)), true);
    assertThrows(IllegalStateException.class, () -> model.enableInterBrokerDiskCapacityCheck(0.8));
  }

  @Test
  public void testDefaultJbodRelocationMaintainsDiskMembershipAcrossRepeatedMoves() {
    ClusterModel model = cluster();
    Replica replica = addReplica(model, 0, 30);
    model.relocateReplica(replica.topicPartition(), 0, 1);
    assertSame(model.broker(1), replica.disk().broker());
    assertFalse(model.broker(0).disk(SOURCE).replicas().contains(replica));
    assertTrue(replica.disk().replicas().contains(replica));
    model.relocateReplica(replica.topicPartition(), 1, 0);
    assertSame(model.broker(0), replica.disk().broker());
    assertEquals(30, model.broker(0).disk(SOURCE).utilization(), 0.001);
    assertTrue(model.broker(1).disks().stream().noneMatch(d -> d.replicas().contains(replica)));
  }

  @Test
  public void testIntraBrokerMoveRejectsFullDiskBeforeMutationAndCarriesCapacityIntoProposal() {
    ClusterModel model = cluster();
    Replica replica = addReplica(model, 0, 30, 1, LARGE);
    model.enableInterBrokerDiskCapacityCheck(0.8);
    assertFalse(model.canMoveReplicaToDisk(replica, model.broker(1).disk(SMALL)));
    assertThrows(IllegalStateException.class, () -> model.relocateReplica(replica.topicPartition(), 1, SMALL));
    assertEquals(LARGE, replica.disk().logDir());
    assertTrue(replica.disk().replicas().contains(replica));
  }

  @Test
  public void testIntraBrokerProposalCarriesDestinationCapacity() {
    ClusterModel model = cluster();
    Replica replica = addReplica(model, 0, 1, 1, SMALL);
    Map<TopicPartition, List<ReplicaPlacementInfo>> initialReplicas = model.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initialLeaders = model.getLeaderDistribution();
    model.enableInterBrokerDiskCapacityCheck(0.8);
    model.relocateReplica(replica.topicPartition(), 1, LARGE);
    ExecutionProposal proposal = AnalyzerUtils.getDiff(initialReplicas, initialLeaders, model).iterator().next();
    assertEquals(Map.of(1, 1000.0), proposal.destinationDiskCapacityByBroker());
    assertEquals(Map.of(1, LARGE), proposal.getJsonStructure().get("destinationLogDirs"));
    assertTrue(proposal.replicasToAdd().isEmpty());
  }

  @Test
  public void testDefaultBrokerOnlyModelRemainsCompatible() {
    ClusterModel model = new ClusterModel(new ModelGeneration(0, 0), 1.0);
    Map<Resource, Double> capacity = Map.of(Resource.DISK, 100.0, Resource.CPU, 100.0,
                                          Resource.NW_IN, 100.0, Resource.NW_OUT, 100.0);
    model.createRack("0");
    model.createRack("1");
    model.createBroker("0", "host0", 0, new BrokerCapacityInfo(capacity), false);
    model.createBroker("1", "host1", 1, new BrokerCapacityInfo(capacity), false);
    TopicPartition tp = new TopicPartition("topic", 0);
    model.createReplica("0", 0, tp, 0, true);
    assertTrue(model.canMoveReplicaToBroker(model.broker(0).replica(tp), model.broker(1)));
    model.relocateReplica(tp, 0, 1);
    assertNull(model.broker(1).replica(tp).disk());
    assertThrows(IllegalStateException.class, () -> model.enableInterBrokerDiskCapacityCheck(0.8));
  }
}
