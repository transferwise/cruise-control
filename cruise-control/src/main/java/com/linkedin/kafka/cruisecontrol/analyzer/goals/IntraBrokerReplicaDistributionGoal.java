/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;
import static com.linkedin.kafka.cruisecontrol.common.Resource.DISK;


/**
 * Soft goal to distribute replicas evenly across all alive disks within each broker.
 * <ul>
 * <li>Under: (the average number of replicas per alive disk) * (1 + intra broker replica count balance percentage)</li>
 * <li>Above: (the average number of replicas per alive disk) * Math.max(0, 1 - intra broker replica count balance percentage)</li>
 * </ul>
 */
public class IntraBrokerReplicaDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(IntraBrokerReplicaDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  private final Map<Broker, Integer> _balanceUpperLimitByBroker;
  private final Map<Broker, Integer> _balanceLowerLimitByBroker;

  public IntraBrokerReplicaDistributionGoal() {
    _balanceUpperLimitByBroker = new HashMap<>();
    _balanceLowerLimitByBroker = new HashMap<>();
  }

  IntraBrokerReplicaDistributionGoal(BalancingConstraint constraint) {
    this();
    _balancingConstraint = constraint;
  }

  @Override
  public boolean isHardGoal() {
    return false;
  }

  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    double balancePercentageWithMargin = (_balancingConstraint.intraBrokerReplicaBalancePercentage() - 1) * BALANCE_MARGIN;
    for (Broker broker : brokersToBalance(clusterModel)) {
      long numAliveDisks = broker.disks().stream().filter(Disk::isAlive).count();
      if (numAliveDisks == 0) {
        continue;
      }
      long replicasOnAliveDisks = broker.disks().stream().filter(Disk::isAlive).mapToLong(d -> d.replicas().size()).sum();
      double avgReplicasPerDisk = (double) replicasOnAliveDisks / numAliveDisks;
      _balanceUpperLimitByBroker.put(broker, (int) Math.ceil(avgReplicasPerDisk * (1 + balancePercentageWithMargin)));
      _balanceLowerLimitByBroker.put(broker, (int) Math.floor(avgReplicasPerDisk * Math.max(0, (1 - balancePercentageWithMargin))));
    }

    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectOnlineReplicas())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .addPriorityFunc(ReplicaSortFunctionFactory.prioritizeDiskImmigrants())
                              .setScoreFunc(ReplicaSortFunctionFactory.reverseSortByMetricGroupValue(DISK.name()))
                              .trackSortedReplicasFor(replicaSortName(this, true, false), clusterModel);
  }

  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    List<String> disksAboveLimit = new ArrayList<>();
    List<String> disksBelowLimit = new ArrayList<>();
    for (Broker broker : brokersToBalance(clusterModel)) {
      if (!_balanceUpperLimitByBroker.containsKey(broker)) {
        continue;
      }
      int upperLimit = _balanceUpperLimitByBroker.get(broker);
      int lowerLimit = _balanceLowerLimitByBroker.get(broker);
      for (Disk disk : broker.disks()) {
        if (disk.isAlive()) {
          int replicaCount = disk.replicas().size();
          if (replicaCount > upperLimit) {
            disksAboveLimit.add(broker.id() + ":" + disk.logDir());
          }
          if (replicaCount < lowerLimit) {
            disksBelowLimit.add(broker.id() + ":" + disk.logDir());
          }
        }
      }
    }
    if (!disksAboveLimit.isEmpty()) {
      LOG.warn("Disks {} are above intra-broker replica balance upper limit after optimization.", disksAboveLimit);
      _succeeded = false;
    }
    if (!disksBelowLimit.isEmpty()) {
      LOG.warn("Disks {} are below intra-broker replica balance lower limit after optimization.", disksBelowLimit);
      _succeeded = false;
    }
    finish();
  }

  @Override
  protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
    return new TreeSet<>(clusterModel.aliveBrokers());
  }

  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    // Leadership movements do not carry logdir info and are always neutral for replica counts.
    if (action.balancingAction() == com.linkedin.kafka.cruisecontrol.analyzer.ActionType.LEADERSHIP_MOVEMENT) {
      return ACCEPT;
    }
    if (action.sourceBrokerLogdir() == null || action.destinationBrokerLogdir() == null) {
      throw new IllegalArgumentException(this.getClass().getSimpleName() + " does not support balancing action not specifying logdir.");
    }
    Broker broker = clusterModel.broker(action.sourceBrokerId());
    Disk sourceDisk = broker.disk(action.sourceBrokerLogdir());
    Disk destinationDisk = broker.disk(action.destinationBrokerLogdir());

    switch (action.balancingAction()) {
      case INTRA_BROKER_REPLICA_SWAP:
        // Swap does not change replica counts.
        return ACCEPT;
      case INTRA_BROKER_REPLICA_MOVEMENT:
        // Source loses one replica, destination gains one.
        int upperLimit = _balanceUpperLimitByBroker.getOrDefault(broker, Integer.MAX_VALUE);
        int lowerLimit = _balanceLowerLimitByBroker.getOrDefault(broker, 0);
        int sourceCount = sourceDisk.replicas().size();
        int destCount = destinationDisk.replicas().size();
        // Reject if destination goes above upper limit or source goes below lower limit (when they were within limits).
        if (destCount + 1 > upperLimit && destCount <= upperLimit) {
          return REPLICA_REJECT;
        }
        if (sourceCount - 1 < lowerLimit && sourceCount >= lowerLimit) {
          return REPLICA_REJECT;
        }
        return isGettingMoreBalanced(sourceDisk.replicas().size(), destinationDisk.replicas().size()) ? ACCEPT : REPLICA_REJECT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    // Reject swaps because they don't change replica counts per disk, so they can never help this goal converge.
    return action.balancingAction() != com.linkedin.kafka.cruisecontrol.analyzer.ActionType.INTRA_BROKER_REPLICA_SWAP
           && actionAcceptance(action, clusterModel) == ACCEPT;
  }

  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) {
    if (!_balanceUpperLimitByBroker.containsKey(broker)) {
      return;
    }
    int upperLimit = _balanceUpperLimitByBroker.get(broker);
    int lowerLimit = _balanceLowerLimitByBroker.get(broker);
    LOG.debug("Balancing broker {} [limits] lower: {} upper: {}.", broker.id(), lowerLimit, upperLimit);

    for (Disk disk : broker.disks()) {
      if (!disk.isAlive()) {
        continue;
      }
      if (disk.replicas().size() > upperLimit) {
        rebalanceByMovingReplicasOut(disk, clusterModel, optimizedGoals, upperLimit, lowerLimit);
      }
      if (disk.replicas().size() < lowerLimit) {
        rebalanceByMovingReplicasIn(disk, clusterModel, optimizedGoals, lowerLimit);
      }
    }
  }

  private void rebalanceByMovingReplicasOut(Disk disk,
                                            ClusterModel clusterModel,
                                            Set<Goal> optimizedGoals,
                                            int upperLimit,
                                            int lowerLimit) {
    Broker broker = disk.broker();
    // Note: PQ ordering may become stale as replicas move between disks; the re-insertion check after each
    // successful move partially mitigates this. Exact ordering is not required for correctness of a soft goal.
    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(Comparator.comparingInt(d -> d.replicas().size()));
    for (Disk candidateDisk : broker.disks()) {
      if (candidateDisk.isAlive() && candidateDisk != disk && candidateDisk.replicas().size() < upperLimit) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      for (Iterator<Replica> iterator = disk.trackedSortedReplicas(replicaSortName(this, true, false)).sortedReplicas(true).iterator();
          iterator.hasNext(); ) {
        Replica replica = iterator.next();
        Disk d = maybeMoveReplicaBetweenDisks(clusterModel, replica, Collections.singleton(candidateDisk), optimizedGoals);
        if (d != null) {
          if (disk.replicas().size() <= upperLimit) {
            return;
          }
          iterator.remove();
          if (!candidateDiskPQ.isEmpty() && candidateDisk.replicas().size() > candidateDiskPQ.peek().replicas().size()) {
            candidateDiskPQ.add(candidateDisk);
            break;
          }
        }
      }
    }
  }

  private void rebalanceByMovingReplicasIn(Disk disk,
                                           ClusterModel clusterModel,
                                           Set<Goal> optimizedGoals,
                                           int lowerLimit) {
    Broker broker = disk.broker();
    long numAliveDisks = broker.disks().stream().filter(Disk::isAlive).count();
    long replicasOnAliveDisks = broker.disks().stream().filter(Disk::isAlive).mapToLong(d -> d.replicas().size()).sum();
    double avgReplicaCount = (double) replicasOnAliveDisks / numAliveDisks;

    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(
        (d1, d2) -> Integer.compare(d2.replicas().size(), d1.replicas().size()));
    for (Disk candidateDisk : broker.disks()) {
      if (candidateDisk.isAlive() && candidateDisk != disk && candidateDisk.replicas().size() > avgReplicaCount) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      for (Iterator<Replica> iterator = candidateDisk.trackedSortedReplicas(replicaSortName(this, true, false)).sortedReplicas(true).iterator();
          iterator.hasNext(); ) {
        Replica replica = iterator.next();
        Disk d = maybeMoveReplicaBetweenDisks(clusterModel, replica, Collections.singleton(disk), optimizedGoals);
        if (d != null) {
          if (disk.replicas().size() >= lowerLimit) {
            return;
          }
          iterator.remove();
          if (!candidateDiskPQ.isEmpty() && candidateDisk.replicas().size() < candidateDiskPQ.peek().replicas().size()) {
            candidateDiskPQ.add(candidateDisk);
            break;
          }
        }
      }
    }
  }

  private boolean isGettingMoreBalanced(int sourceCount, int destCount) {
    int prevDiff = Math.abs(sourceCount - destCount);
    int nextDiff = Math.abs((sourceCount - 1) - (destCount + 1));
    return nextDiff < prevDiff;
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    // ClusterModelStats does not track per-disk replica count distribution, so we always consider
    // the post-optimization state as acceptable (return 1 means stats2 is not worse than stats1).
    return new ClusterModelStatsComparator() {
      @Override
      public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
        return 1;
      }

      @Override
      public String explainLastComparison() {
        return null;
      }
    };
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(1, _minMonitoredPartitionPercentage, true);
  }
}
