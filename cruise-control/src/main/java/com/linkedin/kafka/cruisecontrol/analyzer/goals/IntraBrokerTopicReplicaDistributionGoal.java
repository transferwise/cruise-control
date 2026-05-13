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
 * Soft goal to balance collocations of replicas of the same topic evenly across all alive disks within each broker.
 * <ul>
 * <li>Under: (avg topic replicas per alive disk) * (1 + intra broker topic replica count balance percentage)</li>
 * <li>Above: (avg topic replicas per alive disk) * Math.max(0, 1 - intra broker topic replica count balance percentage)</li>
 * </ul>
 */
public class IntraBrokerTopicReplicaDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(IntraBrokerTopicReplicaDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  private static final long PER_DISK_SWAP_TIMEOUT_MS = 500L;
  // Map from broker to topic to upper/lower limits
  private final Map<Broker, Map<String, Integer>> _balanceUpperLimitByBrokerByTopic;
  private final Map<Broker, Map<String, Integer>> _balanceLowerLimitByBrokerByTopic;

  public IntraBrokerTopicReplicaDistributionGoal() {
    _balanceUpperLimitByBrokerByTopic = new HashMap<>();
    _balanceLowerLimitByBrokerByTopic = new HashMap<>();
  }

  IntraBrokerTopicReplicaDistributionGoal(BalancingConstraint constraint) {
    this();
    _balancingConstraint = constraint;
  }

  @Override
  public boolean isHardGoal() {
    return false;
  }

  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    double balancePercentageWithMargin = (_balancingConstraint.intraBrokerTopicReplicaBalancePercentage() - 1) * BALANCE_MARGIN;
    Set<String> excludedTopics = optimizationOptions.excludedTopics();

    for (Broker broker : brokersToBalance(clusterModel)) {
      long numAliveDisks = broker.disks().stream().filter(Disk::isAlive).count();
      if (numAliveDisks == 0) {
        continue;
      }
      Map<String, Integer> upperLimitByTopic = new HashMap<>();
      Map<String, Integer> lowerLimitByTopic = new HashMap<>();
      for (String topic : broker.topics()) {
        if (excludedTopics.contains(topic)) {
          continue;
        }
        int numTopicReplicasOnAliveDisks = (int) broker.disks().stream()
            .filter(Disk::isAlive).mapToLong(d -> d.numReplicasOfTopic(topic)).sum();
        double avgTopicReplicasPerDisk = (double) numTopicReplicasOnAliveDisks / numAliveDisks;
        upperLimitByTopic.put(topic, (int) Math.ceil(avgTopicReplicasPerDisk * (1 + balancePercentageWithMargin)));
        lowerLimitByTopic.put(topic, (int) Math.floor(avgTopicReplicasPerDisk * Math.max(0, (1 - balancePercentageWithMargin))));
      }
      _balanceUpperLimitByBrokerByTopic.put(broker, upperLimitByTopic);
      _balanceLowerLimitByBrokerByTopic.put(broker, lowerLimitByTopic);
    }

    new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectOnlineReplicas())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics),
                                                     !excludedTopics.isEmpty())
                              .addPriorityFunc(ReplicaSortFunctionFactory.prioritizeDiskImmigrants())
                              .setScoreFunc(ReplicaSortFunctionFactory.reverseSortByMetricGroupValue(DISK.name()))
                              .trackSortedReplicasFor(replicaSortName(this, true, false), clusterModel);
    // Sort replicas for swapping (ascending disk utilization - swap smaller replicas to minimize disk usage impact)
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
      Map<String, Integer> upperLimitByTopic = _balanceUpperLimitByBrokerByTopic.get(broker);
      Map<String, Integer> lowerLimitByTopic = _balanceLowerLimitByBrokerByTopic.get(broker);
      if (upperLimitByTopic == null) {
        continue;
      }
      for (Disk disk : broker.disks()) {
        if (!disk.isAlive()) {
          continue;
        }
        for (String topic : broker.topics()) {
          int topicReplicaCount = disk.numReplicasOfTopic(topic);
          Integer upperLimit = upperLimitByTopic.get(topic);
          Integer lowerLimit = lowerLimitByTopic.get(topic);
          if (upperLimit != null && topicReplicaCount > upperLimit) {
            disksAboveLimit.add(broker.id() + ":" + disk.logDir() + ":" + topic);
          }
          if (lowerLimit != null && topicReplicaCount < lowerLimit) {
            disksBelowLimit.add(broker.id() + ":" + disk.logDir() + ":" + topic);
          }
        }
      }
    }
    if (!disksAboveLimit.isEmpty()) {
      LOG.warn("Disks {} are above intra-broker topic replica balance upper limit after optimization.", disksAboveLimit);
      _succeeded = false;
    }
    if (!disksBelowLimit.isEmpty()) {
      LOG.warn("Disks {} are below intra-broker topic replica balance lower limit after optimization.", disksBelowLimit);
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
    // Leadership movements do not carry logdir info and are always neutral for per-topic replica counts.
    if (action.balancingAction() == com.linkedin.kafka.cruisecontrol.analyzer.ActionType.LEADERSHIP_MOVEMENT) {
      return ACCEPT;
    }
    if (action.sourceBrokerLogdir() == null || action.destinationBrokerLogdir() == null) {
      throw new IllegalArgumentException(this.getClass().getSimpleName() + " does not support balancing action not specifying logdir.");
    }
    Broker broker = clusterModel.broker(action.sourceBrokerId());
    Disk sourceDisk = broker.disk(action.sourceBrokerLogdir());
    Disk destinationDisk = broker.disk(action.destinationBrokerLogdir());
    String sourceTopic = action.topicPartition().topic();

    Map<String, Integer> upperLimits = _balanceUpperLimitByBrokerByTopic.get(broker);
    Map<String, Integer> lowerLimits = _balanceLowerLimitByBrokerByTopic.get(broker);
    if (upperLimits == null) {
      return ACCEPT;
    }

    switch (action.balancingAction()) {
      case INTRA_BROKER_REPLICA_SWAP:
        String destTopic = action.destinationTopicPartition().topic();
        if (sourceTopic.equals(destTopic)) {
          // Same topic swap does not change per-topic counts.
          return ACCEPT;
        }
        // Check both topics
        if (!isTopicMoveAcceptable(broker, sourceTopic, sourceDisk, destinationDisk, upperLimits, lowerLimits)
            || !isTopicMoveAcceptable(broker, destTopic, destinationDisk, sourceDisk, upperLimits, lowerLimits)) {
          return REPLICA_REJECT;
        }
        return ACCEPT;
      case INTRA_BROKER_REPLICA_MOVEMENT:
        return isTopicMoveAcceptable(broker, sourceTopic, sourceDisk, destinationDisk, upperLimits, lowerLimits)
               ? ACCEPT : REPLICA_REJECT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  private boolean isTopicMoveAcceptable(Broker broker, String topic, Disk fromDisk, Disk toDisk,
                                        Map<String, Integer> upperLimits, Map<String, Integer> lowerLimits) {
    Integer upperLimit = upperLimits.get(topic);
    Integer lowerLimit = lowerLimits.get(topic);
    if (upperLimit == null || lowerLimit == null) {
      return true;
    }
    int fromCount = fromDisk.numReplicasOfTopic(topic);
    int toCount = toDisk.numReplicasOfTopic(topic);
    // Reject if destination goes above upper limit (when it was within) or source goes below lower limit (when it was within).
    if (toCount + 1 > upperLimit && toCount <= upperLimit) {
      return false;
    }
    if (fromCount - 1 < lowerLimit && fromCount >= lowerLimit) {
      return false;
    }
    // Must be getting more balanced for this topic.
    int prevDiff = Math.abs(fromCount - toCount);
    int nextDiff = Math.abs((fromCount - 1) - (toCount + 1));
    return nextDiff < prevDiff;
  }

  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    return actionAcceptance(action, clusterModel) == ACCEPT;
  }

  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) {
    Map<String, Integer> upperLimitByTopic = _balanceUpperLimitByBrokerByTopic.get(broker);
    Map<String, Integer> lowerLimitByTopic = _balanceLowerLimitByBrokerByTopic.get(broker);
    if (upperLimitByTopic == null) {
      return;
    }

    for (String topic : broker.topics()) {
      Integer upperLimit = upperLimitByTopic.get(topic);
      Integer lowerLimit = lowerLimitByTopic.get(topic);
      if (upperLimit == null || lowerLimit == null) {
        continue;
      }

      // Skip topics that are already balanced across all disks on this broker.
      boolean needsRebalance = false;
      for (Disk disk : broker.disks()) {
        if (disk.isAlive()) {
          int count = disk.numReplicasOfTopic(topic);
          if (count > upperLimit || count < lowerLimit) {
            needsRebalance = true;
            break;
          }
        }
      }
      if (!needsRebalance) {
        continue;
      }

      for (Disk disk : broker.disks()) {
        if (!disk.isAlive()) {
          continue;
        }
        if (disk.numReplicasOfTopic(topic) > upperLimit) {
          rebalanceByMovingTopicReplicasOut(disk, topic, clusterModel, optimizedGoals, upperLimit);
        }
        // If moves alone were insufficient, attempt cross-topic swaps.
        if (disk.numReplicasOfTopic(topic) > upperLimit) {
          rebalanceBySwappingTopicReplicasOut(disk, topic, clusterModel, optimizedGoals, upperLimit);
        }
        if (disk.numReplicasOfTopic(topic) < lowerLimit) {
          rebalanceByMovingTopicReplicasIn(disk, topic, clusterModel, optimizedGoals, lowerLimit);
        }
        // If moves alone were insufficient, attempt cross-topic swaps.
        if (disk.numReplicasOfTopic(topic) < lowerLimit) {
          rebalanceBySwappingTopicReplicasIn(disk, topic, clusterModel, optimizedGoals, lowerLimit);
        }
      }
    }
  }

  private void rebalanceByMovingTopicReplicasOut(Disk disk, String topic,
                                                 ClusterModel clusterModel,
                                                 Set<Goal> optimizedGoals,
                                                 int upperLimit) {
    Broker broker = disk.broker();
    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(Comparator.comparingInt(d -> d.numReplicasOfTopic(topic)));
    for (Disk candidateDisk : broker.disks()) {
      if (candidateDisk.isAlive() && candidateDisk != disk && candidateDisk.numReplicasOfTopic(topic) < upperLimit) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    // Get only the replicas of this topic from the sorted replicas on the disk.
    List<Replica> topicReplicas = disk.trackedSortedReplicas(replicaSortName(this, true, false))
                                     .sortedReplicas(true).stream()
                                     .filter(r -> r.topicPartition().topic().equals(topic))
                                     .collect(Collectors.toList());

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      Iterator<Replica> iterator = topicReplicas.iterator();
      while (iterator.hasNext()) {
        Replica replica = iterator.next();
        Disk d = maybeMoveReplicaBetweenDisks(clusterModel, replica, Collections.singleton(candidateDisk), optimizedGoals);
        if (d != null) {
          iterator.remove();
          if (disk.numReplicasOfTopic(topic) <= upperLimit) {
            return;
          }
          if (!candidateDiskPQ.isEmpty() && candidateDisk.numReplicasOfTopic(topic) > candidateDiskPQ.peek().numReplicasOfTopic(topic)) {
            candidateDiskPQ.add(candidateDisk);
            break;
          }
        }
      }
    }
  }

  private void rebalanceByMovingTopicReplicasIn(Disk disk, String topic,
                                                ClusterModel clusterModel,
                                                Set<Goal> optimizedGoals,
                                                int lowerLimit) {
    Broker broker = disk.broker();
    long numAliveDisks = broker.disks().stream().filter(Disk::isAlive).count();
    long topicReplicasOnAliveDisks = broker.disks().stream().filter(Disk::isAlive).mapToLong(d -> d.numReplicasOfTopic(topic)).sum();
    double avgTopicReplicas = (double) topicReplicasOnAliveDisks / numAliveDisks;

    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(
        (d1, d2) -> Integer.compare(d2.numReplicasOfTopic(topic), d1.numReplicasOfTopic(topic)));
    for (Disk candidateDisk : broker.disks()) {
      if (candidateDisk.isAlive() && candidateDisk != disk && candidateDisk.numReplicasOfTopic(topic) > avgTopicReplicas) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      List<Replica> topicReplicas = candidateDisk.trackedSortedReplicas(replicaSortName(this, true, false))
                                                 .sortedReplicas(true).stream()
                                                 .filter(r -> r.topicPartition().topic().equals(topic))
                                                 .collect(Collectors.toList());
      Iterator<Replica> iterator = topicReplicas.iterator();
      while (iterator.hasNext()) {
        Replica replica = iterator.next();
        Disk d = maybeMoveReplicaBetweenDisks(clusterModel, replica, Collections.singleton(disk), optimizedGoals);
        if (d != null) {
          iterator.remove();
          if (disk.numReplicasOfTopic(topic) >= lowerLimit) {
            return;
          }
          if (!candidateDiskPQ.isEmpty()
              && candidateDisk.numReplicasOfTopic(topic) < candidateDiskPQ.peek().numReplicasOfTopic(topic)) {
            candidateDiskPQ.add(candidateDisk);
            break;
          }
        }
      }
    }
  }

  // Swap replicas of the overloaded topic on this disk with replicas of other topics on candidate disks.
  // Handles the case where one-way moves are blocked by a previously optimized replica-count goal.
  private void rebalanceBySwappingTopicReplicasOut(Disk disk, String topic,
                                                    ClusterModel clusterModel,
                                                    Set<Goal> optimizedGoals,
                                                    int upperLimit) {
    long swapStartTimeMs = System.currentTimeMillis();
    Broker broker = disk.broker();

    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(Comparator.comparingInt(d -> d.numReplicasOfTopic(topic)));
    for (Disk candidateDisk : broker.disks()) {
      if (candidateDisk.isAlive() && candidateDisk != disk && candidateDisk.numReplicasOfTopic(topic) < upperLimit) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      // Source replicas: replicas of the overloaded topic on this disk.
      List<Replica> sourceReplicas = disk.trackedSortedReplicas(replicaSortName(this, false, false))
                                         .sortedReplicas(false).stream()
                                         .filter(r -> r.topicPartition().topic().equals(topic))
                                         .collect(Collectors.toList());
      // Group candidate replicas by topic to avoid early-termination in
      // AbstractGoal.maybeSwapReplicaBetweenDisks: if the first candidate's topic fails selfSatisfied,
      // the entire candidate set is abandoned. By trying each topic group separately, a bad topic
      // won't block valid swaps with other topics.
      Map<String, SortedSet<Replica>> candidatesByTopic = new HashMap<>();
      for (Replica r : candidateDisk.trackedSortedReplicas(replicaSortName(this, false, false)).sortedReplicas(false)) {
        String rTopic = r.topicPartition().topic();
        if (!rTopic.equals(topic)
            && candidateDisk.numReplicasOfTopic(rTopic) > disk.numReplicasOfTopic(rTopic)) {
          candidatesByTopic.computeIfAbsent(rTopic, k -> new TreeSet<>()).add(r);
        }
      }
      boolean swapped = false;
      for (Replica sourceReplica : sourceReplicas) {
        for (SortedSet<Replica> topicCandidates : candidatesByTopic.values()) {
          Replica swappedIn = maybeSwapReplicaBetweenDisks(clusterModel, sourceReplica, topicCandidates, optimizedGoals);
          if (swappedIn != null) {
            swapped = true;
            break;
          }
        }
        if (swapped) {
          if (disk.numReplicasOfTopic(topic) <= upperLimit) {
            return;
          }
          break;
        }
      }
      if (System.currentTimeMillis() - swapStartTimeMs > PER_DISK_SWAP_TIMEOUT_MS) {
        LOG.debug("Swap topic replicas out timeout for disk {} topic {}.", disk.logDir(), topic);
        break;
      }
      // Only re-insert the candidate disk if a swap was made (counts changed) and it's still valid.
      if (swapped && candidateDisk.numReplicasOfTopic(topic) < upperLimit) {
        candidateDiskPQ.add(candidateDisk);
      }
    }
  }

  // Swap replicas of other topics on this disk with replicas of the underloaded topic on candidate disks.
  private void rebalanceBySwappingTopicReplicasIn(Disk disk, String topic,
                                                   ClusterModel clusterModel,
                                                   Set<Goal> optimizedGoals,
                                                   int lowerLimit) {
    long swapStartTimeMs = System.currentTimeMillis();
    Broker broker = disk.broker();

    PriorityQueue<Disk> candidateDiskPQ = new PriorityQueue<>(
        (d1, d2) -> Integer.compare(d2.numReplicasOfTopic(topic), d1.numReplicasOfTopic(topic)));
    for (Disk candidateDisk : broker.disks()) {
      if (candidateDisk.isAlive() && candidateDisk != disk && candidateDisk.numReplicasOfTopic(topic) > lowerLimit) {
        candidateDiskPQ.add(candidateDisk);
      }
    }

    while (!candidateDiskPQ.isEmpty()) {
      Disk candidateDisk = candidateDiskPQ.poll();
      // Source replicas on this disk: replicas of OTHER topics whose movement to candidateDisk
      // would also improve that topic's balance (prevents selfSatisfied early-termination).
      List<Replica> sourceReplicas = disk.trackedSortedReplicas(replicaSortName(this, false, false))
                                         .sortedReplicas(false).stream()
                                         .filter(r -> {
                                           String rTopic = r.topicPartition().topic();
                                           if (rTopic.equals(topic)) {
                                             return false;
                                           }
                                           return disk.numReplicasOfTopic(rTopic) > candidateDisk.numReplicasOfTopic(rTopic);
                                         })
                                         .collect(Collectors.toList());
      // Candidate replicas on the candidate disk: replicas of the needed topic (to bring in).
      SortedSet<Replica> candidateReplicas = candidateDisk.trackedSortedReplicas(replicaSortName(this, false, false))
                                                          .sortedReplicas(false).stream()
                                                          .filter(r -> r.topicPartition().topic().equals(topic))
                                                          .collect(Collectors.toCollection(TreeSet::new));
      boolean swapped = false;
      for (Replica sourceReplica : sourceReplicas) {
        Replica swappedIn = maybeSwapReplicaBetweenDisks(clusterModel, sourceReplica, candidateReplicas, optimizedGoals);
        if (swappedIn != null) {
          swapped = true;
          if (disk.numReplicasOfTopic(topic) >= lowerLimit) {
            return;
          }
          break;
        }
      }
      if (System.currentTimeMillis() - swapStartTimeMs > PER_DISK_SWAP_TIMEOUT_MS) {
        LOG.debug("Swap topic replicas in timeout for disk {} topic {}.", disk.logDir(), topic);
        break;
      }
      // Only re-insert the candidate disk if a swap was made (counts changed) and it's still valid.
      if (swapped && candidateDisk.numReplicasOfTopic(topic) > lowerLimit) {
        candidateDiskPQ.add(candidateDisk);
      }
    }
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    // ClusterModelStats does not track per-disk topic replica count distribution, so we always consider
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
