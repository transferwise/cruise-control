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
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;
import static com.linkedin.kafka.cruisecontrol.common.Resource.DISK;


/**
 * Soft goal to distribute leader replicas evenly across all alive disks within each broker.
 * <p>
 * Since leadership movement is a broker-level concept (moving leadership to a follower on another broker),
 * this goal balances leader replicas across disks by moving leader replicas (via {@code INTRA_BROKER_REPLICA_MOVEMENT})
 * from disks with too many leaders to disks with fewer leaders, or by swapping leader replicas with follower replicas
 * between disks (via {@code INTRA_BROKER_REPLICA_SWAP}).
 * <ul>
 * <li>Under: (avg leader replicas per alive disk) * (1 + intra broker leader replica count balance percentage)</li>
 * <li>Above: (avg leader replicas per alive disk) * Math.max(0, 1 - intra broker leader replica count balance percentage)</li>
 * </ul>
 */
public class IntraBrokerLeaderReplicaDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(IntraBrokerLeaderReplicaDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  private static final long PER_DISK_SWAP_TIMEOUT_MS = 500L;
  private final Map<Broker, Integer> _balanceUpperLimitByBroker;
  private final Map<Broker, Integer> _balanceLowerLimitByBroker;

  public IntraBrokerLeaderReplicaDistributionGoal() {
    _balanceUpperLimitByBroker = new HashMap<>();
    _balanceLowerLimitByBroker = new HashMap<>();
  }

  IntraBrokerLeaderReplicaDistributionGoal(BalancingConstraint constraint) {
    this();
    _balancingConstraint = constraint;
  }

  @Override
  public boolean isHardGoal() {
    return false;
  }

  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    double balancePercentageWithMargin = (_balancingConstraint.intraBrokerLeaderReplicaBalancePercentage() - 1) * BALANCE_MARGIN;
    for (Broker broker : brokersToBalance(clusterModel)) {
      long numAliveDisks = broker.disks().stream().filter(Disk::isAlive).count();
      if (numAliveDisks == 0) {
        continue;
      }
      long leadersOnAliveDisks = broker.disks().stream().filter(Disk::isAlive).mapToLong(Disk::numLeaderReplicas).sum();
      double avgLeadersPerDisk = (double) leadersOnAliveDisks / numAliveDisks;
      _balanceUpperLimitByBroker.put(broker, (int) Math.ceil(avgLeadersPerDisk * (1 + balancePercentageWithMargin)));
      _balanceLowerLimitByBroker.put(broker, (int) Math.floor(avgLeadersPerDisk * Math.max(0, (1 - balancePercentageWithMargin))));
    }

    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    // Sort replicas for moving leaders out (descending disk utilization - move larger leaders first)
    new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectOnlineReplicas())
                              .addSelectionFunc(ReplicaSortFunctionFactory.selectLeaders())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .addPriorityFunc(ReplicaSortFunctionFactory.prioritizeDiskImmigrants())
                              .setScoreFunc(ReplicaSortFunctionFactory.reverseSortByMetricGroupValue(DISK.name()))
                              .trackSortedReplicasFor(replicaSortName(this, true, false), clusterModel);
    // Sort replicas for swapping (ascending disk utilization - swap smaller replicas)
    new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectOnlineReplicas())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .addPriorityFunc(ReplicaSortFunctionFactory.prioritizeDiskImmigrants())
                              .setScoreFunc(ReplicaSortFunctionFactory.sortByMetricGroupValue(DISK.name()))
                              .trackSortedReplicasFor(replicaSortName(this, false, false), clusterModel);
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
          int leaderCount = disk.numLeaderReplicas();
          if (leaderCount > upperLimit) {
            disksAboveLimit.add(broker.id() + ":" + disk.logDir());
          }
          if (leaderCount < lowerLimit) {
            disksBelowLimit.add(broker.id() + ":" + disk.logDir());
          }
        }
      }
    }
    if (!disksAboveLimit.isEmpty()) {
      LOG.warn("Disks {} are above intra-broker leader replica balance upper limit after optimization.", disksAboveLimit);
      _succeeded = false;
    }
    if (!disksBelowLimit.isEmpty()) {
      LOG.warn("Disks {} are below intra-broker leader replica balance lower limit after optimization.", disksBelowLimit);
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
    if (action.balancingAction() == com.linkedin.kafka.cruisecontrol.analyzer.ActionType.LEADERSHIP_MOVEMENT) {
      return isLeadershipMovementAcceptable(action, clusterModel);
    }
    if (action.sourceBrokerLogdir() == null || action.destinationBrokerLogdir() == null) {
      throw new IllegalArgumentException(this.getClass().getSimpleName() + " does not support balancing action not specifying logdir.");
    }
    Broker broker = clusterModel.broker(action.sourceBrokerId());
    Disk sourceDisk = broker.disk(action.sourceBrokerLogdir());
    Disk destinationDisk = broker.disk(action.destinationBrokerLogdir());
    Replica sourceReplica = broker.replica(action.topicPartition());

    switch (action.balancingAction()) {
      case INTRA_BROKER_REPLICA_SWAP:
        Replica destinationReplica = broker.replica(action.destinationTopicPartition());
        if (sourceReplica.isLeader() == destinationReplica.isLeader()) {
          // Swapping two leaders or two followers does not change leader count per disk.
          return ACCEPT;
        }
        // One is a leader and one is a follower - check if the swap improves balance.
        Disk leaderDisk = sourceReplica.isLeader() ? sourceDisk : destinationDisk;
        Disk followerDisk = sourceReplica.isLeader() ? destinationDisk : sourceDisk;
        return isLeaderSwapImproving(leaderDisk, followerDisk) ? ACCEPT : REPLICA_REJECT;
      case INTRA_BROKER_REPLICA_MOVEMENT:
        if (!sourceReplica.isLeader()) {
          // Moving a follower does not change leader counts.
          return ACCEPT;
        }
        return isLeaderMoveAcceptable(broker, sourceDisk, destinationDisk) ? ACCEPT : REPLICA_REJECT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  // A leadership movement transfers leadership from source broker to destination broker.
  // On the source broker, the leader's disk loses a leader; on the destination broker,
  // the follower's disk gains a leader. Reject if this pushes either disk outside limits.
  private ActionAcceptance isLeadershipMovementAcceptable(BalancingAction action, ClusterModel clusterModel) {
    // Check source broker: its leader replica becomes a follower.
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    Replica sourceReplica = sourceBroker.replica(action.topicPartition());
    if (sourceReplica != null && sourceReplica.disk() != null) {
      int lowerLimit = _balanceLowerLimitByBroker.getOrDefault(sourceBroker, 0);
      Disk sourceDisk = sourceReplica.disk();
      if (sourceDisk.numLeaderReplicas() - 1 < lowerLimit && sourceDisk.numLeaderReplicas() >= lowerLimit) {
        return REPLICA_REJECT;
      }
    }
    // Check destination broker: its follower replica becomes a leader.
    Broker destBroker = clusterModel.broker(action.destinationBrokerId());
    Replica destReplica = destBroker.replica(action.topicPartition());
    if (destReplica != null && destReplica.disk() != null) {
      int upperLimit = _balanceUpperLimitByBroker.getOrDefault(destBroker, Integer.MAX_VALUE);
      Disk destDisk = destReplica.disk();
      if (destDisk.numLeaderReplicas() + 1 > upperLimit && destDisk.numLeaderReplicas() <= upperLimit) {
        return REPLICA_REJECT;
      }
    }
    return ACCEPT;
  }

  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    if (action.balancingAction() == com.linkedin.kafka.cruisecontrol.analyzer.ActionType.INTRA_BROKER_REPLICA_SWAP) {
      // A swap only helps this goal if one replica is a leader and the other is a follower.
      Broker broker = clusterModel.broker(action.sourceBrokerId());
      Replica sourceReplica = broker.replica(action.topicPartition());
      Replica destReplica = broker.replica(action.destinationTopicPartition());
      if (sourceReplica.isLeader() == destReplica.isLeader()) {
        return false;
      }
    }
    return actionAcceptance(action, clusterModel) == ACCEPT;
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
    LOG.debug("Balancing broker {} leader replicas across disks [limits] lower: {} upper: {}.", broker.id(), lowerLimit, upperLimit);

    for (Disk disk : broker.disks()) {
      if (!disk.isAlive()) {
        continue;
      }
      // Move leaders out of overloaded disks
      if (disk.numLeaderReplicas() > upperLimit) {
        if (rebalanceByMovingLeadersOut(disk, clusterModel, optimizedGoals, upperLimit)) {
          rebalanceBySwappingLeadersOut(disk, clusterModel, optimizedGoals, upperLimit);
        }
      }
      // Move leaders into underloaded disks
      if (disk.numLeaderReplicas() < lowerLimit) {
        if (rebalanceByMovingLeadersIn(disk, clusterModel, optimizedGoals, lowerLimit)) {
          rebalanceBySwappingLeadersIn(disk, clusterModel, optimizedGoals, lowerLimit);
        }
      }
    }
  }

  private boolean rebalanceByMovingLeadersOut(Disk disk,
                                              ClusterModel clusterModel,
                                              Set<Goal> optimizedGoals,
                                              int upperLimit) {
    Broker broker = disk.broker();
    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(Comparator.comparingInt(d -> d.numLeaderReplicas()));
    for (Disk candidateDisk : broker.disks()) {
      if (candidateDisk.isAlive() && candidateDisk != disk && candidateDisk.numLeaderReplicas() < upperLimit) {
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
          if (disk.numLeaderReplicas() <= upperLimit) {
            return false;
          }
          iterator.remove();
          if (!candidateDiskPQ.isEmpty() && candidateDisk.numLeaderReplicas() > candidateDiskPQ.peek().numLeaderReplicas()) {
            candidateDiskPQ.add(candidateDisk);
            break;
          }
        }
      }
    }
    return true;
  }

  private boolean rebalanceByMovingLeadersIn(Disk disk,
                                             ClusterModel clusterModel,
                                             Set<Goal> optimizedGoals,
                                             int lowerLimit) {
    Broker broker = disk.broker();
    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(
        (d1, d2) -> Integer.compare(d2.numLeaderReplicas(), d1.numLeaderReplicas()));
    long numAliveDisks = broker.disks().stream().filter(Disk::isAlive).count();
    long leadersOnAliveDisks = broker.disks().stream().filter(Disk::isAlive).mapToLong(Disk::numLeaderReplicas).sum();
    double avgLeaders = (double) leadersOnAliveDisks / numAliveDisks;
    for (Disk candidateDisk : broker.disks()) {
      if (candidateDisk.isAlive() && candidateDisk != disk && candidateDisk.numLeaderReplicas() > avgLeaders) {
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
          if (disk.numLeaderReplicas() >= lowerLimit) {
            return false;
          }
          iterator.remove();
          if (!candidateDiskPQ.isEmpty() && candidateDisk.numLeaderReplicas() < candidateDiskPQ.peek().numLeaderReplicas()) {
            candidateDiskPQ.add(candidateDisk);
            break;
          }
        }
      }
    }
    return true;
  }

  private void rebalanceBySwappingLeadersOut(Disk disk,
                                             ClusterModel clusterModel,
                                             Set<Goal> optimizedGoals,
                                             int upperLimit) {
    long swapStartTimeMs = System.currentTimeMillis();
    Broker broker = disk.broker();

    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(Comparator.comparingInt(d -> d.numLeaderReplicas()));
    for (Disk candidateDisk : broker.disks()) {
      if (candidateDisk.isAlive() && candidateDisk != disk && candidateDisk.numLeaderReplicas() < upperLimit) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      // Filter candidate replicas to followers only — maybeSwapReplicaBetweenDisks returns null immediately
      // on selfSatisfied failure, so leaders in the set would prevent valid followers from being tried.
      SortedSet<Replica> followerCandidates = candidateDisk.trackedSortedReplicas(replicaSortName(this, false, false))
                                                           .sortedReplicas(false)
                                                           .stream()
                                                           .filter(r -> !r.isLeader())
                                                           .collect(Collectors.toCollection(TreeSet::new));
      boolean swapped = false;
      for (Replica sourceReplica : disk.trackedSortedReplicas(replicaSortName(this, true, false)).sortedReplicas(false)) {
        Replica swappedIn = maybeSwapReplicaBetweenDisks(clusterModel, sourceReplica, followerCandidates, optimizedGoals);
        if (swappedIn != null) {
          swapped = true;
          if (disk.numLeaderReplicas() <= upperLimit) {
            return;
          }
          break;
        }
      }
      if (remainingPerDiskSwapTimeMs(swapStartTimeMs) <= 0) {
        LOG.debug("Swap leaders out timeout for disk {}.", disk.logDir());
        break;
      }
      if (swapped && candidateDisk.numLeaderReplicas() < upperLimit) {
        candidateDiskPQ.add(candidateDisk);
      }
    }
  }

  private void rebalanceBySwappingLeadersIn(Disk disk,
                                            ClusterModel clusterModel,
                                            Set<Goal> optimizedGoals,
                                            int lowerLimit) {
    long swapStartTimeMs = System.currentTimeMillis();
    Broker broker = disk.broker();

    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(
        (d1, d2) -> Integer.compare(d2.numLeaderReplicas(), d1.numLeaderReplicas()));
    for (Disk candidateDisk : broker.disks()) {
      if (candidateDisk.isAlive() && candidateDisk != disk && candidateDisk.numLeaderReplicas() > lowerLimit) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      // Swap follower replicas on this disk with leader replicas on the candidate disk.
      // Filter source to followers only — swapping a leader with a leader doesn't help this goal.
      boolean swapped = false;
      for (Replica sourceReplica : disk.trackedSortedReplicas(replicaSortName(this, false, false)).sortedReplicas(false)) {
        if (sourceReplica.isLeader()) {
          continue;
        }
        Replica swappedIn = maybeSwapReplicaBetweenDisks(clusterModel,
                                                         sourceReplica,
                                                         candidateDisk.trackedSortedReplicas(replicaSortName(this, true, false))
                                                                      .sortedReplicas(false),
                                                         optimizedGoals);
        if (swappedIn != null) {
          swapped = true;
          if (disk.numLeaderReplicas() >= lowerLimit) {
            return;
          }
          break;
        }
      }
      if (remainingPerDiskSwapTimeMs(swapStartTimeMs) <= 0) {
        LOG.debug("Swap leaders in timeout for disk {}.", disk.logDir());
        break;
      }
      if (swapped && candidateDisk.numLeaderReplicas() > lowerLimit) {
        candidateDiskPQ.add(candidateDisk);
      }
    }
  }

  private boolean isLeaderMoveAcceptable(Broker broker, Disk sourceDisk, Disk destinationDisk) {
    int upperLimit = _balanceUpperLimitByBroker.getOrDefault(broker, Integer.MAX_VALUE);
    int lowerLimit = _balanceLowerLimitByBroker.getOrDefault(broker, 0);
    int sourceLeadersAfter = sourceDisk.numLeaderReplicas() - 1;
    int destLeadersAfter = destinationDisk.numLeaderReplicas() + 1;
    if (destLeadersAfter > upperLimit && destinationDisk.numLeaderReplicas() <= upperLimit) {
      return false;
    }
    if (sourceLeadersAfter < lowerLimit && sourceDisk.numLeaderReplicas() >= lowerLimit) {
      return false;
    }
    return isGettingMoreBalanced(sourceDisk.numLeaderReplicas(), destinationDisk.numLeaderReplicas());
  }

  private boolean isLeaderSwapImproving(Disk leaderDisk, Disk followerDisk) {
    int leaderDiskLeaders = leaderDisk.numLeaderReplicas();
    int followerDiskLeaders = followerDisk.numLeaderReplicas();
    // After swap: leaderDisk loses a leader, followerDisk gains a leader
    int prevDiff = Math.abs(leaderDiskLeaders - followerDiskLeaders);
    int nextDiff = Math.abs((leaderDiskLeaders - 1) - (followerDiskLeaders + 1));
    return nextDiff < prevDiff;
  }

  private boolean isGettingMoreBalanced(int sourceCount, int destCount) {
    int prevDiff = Math.abs(sourceCount - destCount);
    int nextDiff = Math.abs((sourceCount - 1) - (destCount + 1));
    return nextDiff < prevDiff;
  }

  private long remainingPerDiskSwapTimeMs(long swapStartTimeMs) {
    return PER_DISK_SWAP_TIMEOUT_MS - (System.currentTimeMillis() - swapStartTimeMs);
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    // ClusterModelStats does not track per-disk leader replica count distribution, so we always consider
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
