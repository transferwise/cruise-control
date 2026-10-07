/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class LeaderCpuUsageDistributionGoalTest {
  private static final List<Long> WINDOWS = Collections.singletonList(1L);

  @Test
  public void testLeadershipOnlyCpuDistribution() throws OptimizationFailureException {
    ClusterModel clusterModel = leaderSkewedCluster();
    Map<TopicPartition, List<ReplicaPlacementInfo>> initialReplicaDistribution = clusterModel.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initialLeaderDistribution = clusterModel.getLeaderDistribution();

    LeaderCpuUsageDistributionGoal goal = new LeaderCpuUsageDistributionGoal(balancingConstraint());
    assertTrue(goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet(),
                                                                                          Collections.emptySet(),
                                                                                          Collections.emptySet())));

    assertEquals(replicaPlacementSets(initialReplicaDistribution), replicaPlacementSets(clusterModel.getReplicaDistribution()));
    assertNotEquals(initialLeaderDistribution, clusterModel.getLeaderDistribution());
    assertEquals(ProvisionStatus.RIGHT_SIZED, goal.provisionResponse().status());
  }

  private static BalancingConstraint balancingConstraint() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.CPU_BALANCE_THRESHOLD_CONFIG, Double.toString(TestConstants.HIGH_BALANCE_PERCENTAGE));
    return new BalancingConstraint(new KafkaCruiseControlConfig(props));
  }

  private static ClusterModel leaderSkewedCluster() {
    Map<Integer, Integer> rackByBroker = Map.of(0, 0, 1, 1);
    Map<Resource, Double> brokerCapacity = Map.of(Resource.CPU, 100.0,
                                                  Resource.DISK, 1000.0,
                                                  Resource.NW_IN, 1000.0,
                                                  Resource.NW_OUT, 1000.0);
    ClusterModel clusterModel = DeterministicCluster.getHomogeneousCluster(rackByBroker, brokerCapacity, null);

    createLeaderSkewedPartition(clusterModel, new TopicPartition("topic", 0));
    createLeaderSkewedPartition(clusterModel, new TopicPartition("topic", 1));
    return clusterModel;
  }

  private static void createLeaderSkewedPartition(ClusterModel clusterModel, TopicPartition topicPartition) {
    clusterModel.createReplica("0", 0, topicPartition, 0, true);
    clusterModel.createReplica("1", 1, topicPartition, 1, false);
    clusterModel.setReplicaLoad("0", 0, topicPartition, getAggregatedMetricValues(20.0, 10.0, 190.0, 10.0), WINDOWS);
    clusterModel.setReplicaLoad("1", 1, topicPartition, getAggregatedMetricValues(5.0, 10.0, 0.0, 10.0), WINDOWS);
  }

  private static Map<TopicPartition, Set<ReplicaPlacementInfo>> replicaPlacementSets(
      Map<TopicPartition, List<ReplicaPlacementInfo>> replicaDistribution) {
    Map<TopicPartition, Set<ReplicaPlacementInfo>> placementSets = new HashMap<>();
    replicaDistribution.forEach((topicPartition, replicas) -> placementSets.put(topicPartition, new HashSet<>(replicas)));
    return placementSets;
  }
}
