/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.common.DiskCapacityUtils;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;

/** Validates disk-aware destinations and installs Kafka placement preferences before inter-broker copies start. */
final class InterBrokerDiskCapacityValidator {
  static final class ValidationException extends IllegalStateException {
    ValidationException(RuntimeException cause) {
      super("Inter-broker disk validation rejected reassignment before submission: " + cause.getMessage(), cause);
    }
  }

  private InterBrokerDiskCapacityValidator() {
  }

  static void prepare(AdminClient adminClient, Collection<ExecutionTask> tasks, Collection<ExecutionTask> activeTasks,
                      double threshold, long timeoutMs) {
    Map<TopicPartitionReplica, String> destinations = new HashMap<>();
    for (ExecutionTask task : tasks) {
      if (task.state() == ExecutionTaskState.IN_PROGRESS && !task.proposal().destinationDiskCapacityByBroker().isEmpty()) {
        task.proposal().replicasToAdd().forEach(r -> destinations.put(replicaKey(task.proposal(), r), r.logdir()));
      }
    }
    if (destinations.isEmpty()) {
      return;
    }
    try {
      validateCapacity(adminClient, tasks, activeTasks, threshold, timeoutMs);
      // Kafka records the desired log directory even if the replica does not exist yet. In that case
      // REPLICA_NOT_AVAILABLE is the expected acknowledgement, rather than a placement failure.
      Map<TopicPartitionReplica, KafkaFuture<Void>> placementResults = adminClient.alterReplicaLogDirs(destinations).values();
      for (TopicPartitionReplica replica : destinations.keySet()) {
        if (!placementResults.containsKey(replica)) {
          throw new IllegalStateException("Missing placement acknowledgement for " + replica);
        }
        try {
          placementResults.get(replica).get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
          if (!(e.getCause() instanceof ReplicaNotAvailableException)) {
            throw e;
          }
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while validating inter-broker destination disks.", e);
    } catch (ExecutionException | TimeoutException e) {
      throw new IllegalStateException("Cannot validate or select inter-broker destination disks; reassignment was not submitted.", e);
    }
  }

  static void validateIntraBroker(AdminClient adminClient, Collection<ExecutionTask> tasks, Collection<ExecutionTask> activeTasks,
                                  double threshold, long timeoutMs) {
    if (tasks.stream().noneMatch(t -> t.type() == ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION
        && t.state() == ExecutionTaskState.IN_PROGRESS && !t.proposal().destinationDiskCapacityByBroker().isEmpty())) {
      return;
    }
    try {
      validateCapacity(adminClient, tasks, activeTasks, threshold, timeoutMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while validating intra-broker destination disks.", e);
    } catch (ExecutionException | TimeoutException e) {
      throw new IllegalStateException("Cannot validate intra-broker destination disks; disk movement was not submitted.", e);
    }
  }

  private static void validateCapacity(AdminClient adminClient, Collection<ExecutionTask> tasks, Collection<ExecutionTask> activeTasks,
                                       double threshold, long timeoutMs) throws ExecutionException, InterruptedException, TimeoutException {
    Set<ExecutionTask> reservations = new HashSet<>(activeTasks);
    reservations.addAll(tasks);
    reservations.removeIf(t -> t.proposal().destinationDiskCapacityByBroker().isEmpty()
                              || (t.type() != ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION
                                  && t.type() != ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION)
                              || (t.state() != ExecutionTaskState.IN_PROGRESS && t.state() != ExecutionTaskState.ABORTING));
    Set<Integer> brokers = new HashSet<>();
    Set<Integer> requiredBrokers = new HashSet<>();
    for (ExecutionTask task : reservations) {
      // Source sizes may have grown since planning. Query them as well as destination usage;
      // source failures are tolerated for self-healing, destination failures are not.
      task.proposal().oldReplicas().forEach(r -> brokers.add(r.brokerId()));
      destinationReplicas(task).forEach(r -> {
        brokers.add(r.brokerId());
        requiredBrokers.add(r.brokerId());
      });
    }
    Map<Integer, Map<String, LogDirDescription>> descriptions = new HashMap<>();
    Map<TopicPartition, Double> replicaSizes = new HashMap<>();
    Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> futures = adminClient.describeLogDirs(brokers).descriptions();
    for (int broker : brokers) {
      if (!futures.containsKey(broker)) {
        if (requiredBrokers.contains(broker)) {
          throw new IllegalStateException("Missing log-directory response for broker " + broker);
        }
        continue;
      }
      Map<String, LogDirDescription> dirs;
      try {
        dirs = futures.get(broker).get(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (ExecutionException | TimeoutException e) {
        // Dead source brokers are expected during self-healing. Destination usage must always be known.
        if (requiredBrokers.contains(broker)) {
          throw e;
        }
        continue;
      }
      descriptions.put(broker, dirs);
      for (LogDirDescription dir : dirs.values()) {
        if (dir.error() == null) {
          dir.replicaInfos().forEach((tp, info) -> replicaSizes.merge(tp, DiskCapacityUtils.replicaSize(info), Math::max));
        }
      }
    }
    validateReservations(reservations, descriptions, replicaSizes, threshold);
  }

  private static Set<ReplicaPlacementInfo> destinationReplicas(ExecutionTask task) {
    if (task.type() == ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION) {
      ReplicaPlacementInfo destination = task.proposal().replicasToMoveBetweenDisksByBroker().get(task.brokerId());
      return destination == null ? Set.of() : Set.of(destination);
    }
    return task.proposal().replicasToAdd();
  }

  static void validateReservations(Collection<ExecutionTask> reservations,
                                   Map<Integer, Map<String, LogDirDescription>> descriptions,
                                   Map<TopicPartition, Double> replicaSizes,
                                   double threshold) {
    Map<ReplicaPlacementInfo, Double> projectedUsage = new HashMap<>();
    Map<ReplicaPlacementInfo, Double> capacities = new HashMap<>();
    for (ExecutionTask task : reservations) {
      ExecutionProposal proposal = task.proposal();
      for (ReplicaPlacementInfo replica : destinationReplicas(task)) {
        Map<String, LogDirDescription> dirs = descriptions.get(replica.brokerId());
        LogDirDescription destination = dirs == null ? null : dirs.get(replica.logdir());
        if (destination == null || destination.error() != null) {
          throw new IllegalStateException("Destination log directory is unavailable: " + replica);
        }
        double capacity = DiskCapacityUtils.capacity(proposal.destinationDiskCapacityByBroker().get(replica.brokerId()), destination);
        Double previousCapacity = capacities.putIfAbsent(replica, capacity);
        if (previousCapacity != null && Double.compare(previousCapacity, capacity) != 0) {
          throw new IllegalStateException("Inconsistent destination disk capacities for " + replica);
        }
        projectedUsage.computeIfAbsent(replica, r -> DiskCapacityUtils.utilization(destination));
        for (Map.Entry<String, LogDirDescription> dir : dirs.entrySet()) {
          if (!dir.getKey().equals(replica.logdir()) && dir.getValue().error() == null
              && dir.getValue().replicaInfos().containsKey(proposal.topicPartition())
              && !(task.type() == ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION
                   && proposal.oldReplicas().contains(new ReplicaPlacementInfo(replica.brokerId(), dir.getKey())))) {
            throw new IllegalStateException("Replica " + replicaKey(proposal, replica) + " already exists on another log directory.");
          }
        }
        ReplicaInfo existing = destination.replicaInfos().get(proposal.topicPartition());
        double copiedSize = existing == null ? 0 : DiskCapacityUtils.replicaSize(existing);
        double plannedSize = task.type() == ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION
            ? (double) proposal.intraBrokerDataToMoveInMB()
            : (double) proposal.interBrokerDataToMoveInMB() / proposal.replicasToAdd().size();
        double requiredSize = Math.max(plannedSize, replicaSizes.getOrDefault(proposal.topicPartition(), 0.0));
        projectedUsage.merge(replica, Math.max(0, requiredSize - copiedSize), Double::sum);
      }
    }
    for (Map.Entry<ReplicaPlacementInfo, Double> usage : projectedUsage.entrySet()) {
      double limit = capacities.get(usage.getKey()) * threshold;
      if (!Double.isFinite(usage.getValue()) || usage.getValue() >= limit) {
        throw new IllegalStateException("Destination " + usage.getKey() + " would exceed disk capacity threshold: "
                                        + usage.getValue() + " MB (limit " + limit + " MB).");
      }
    }
  }

  static boolean placementCompleted(AdminClient adminClient, ExecutionProposal proposal, long timeoutMs) {
    return placementsCompleted(adminClient, Set.of(proposal), timeoutMs).contains(proposal);
  }

  static Set<ExecutionProposal> placementsCompleted(AdminClient adminClient, Collection<ExecutionProposal> proposals, long timeoutMs) {
    Set<TopicPartitionReplica> replicas = new HashSet<>();
    for (ExecutionProposal proposal : proposals) {
      if (!proposal.destinationDiskCapacityByBroker().isEmpty()) {
        proposal.destinationReplicas().forEach(r -> replicas.add(replicaKey(proposal, r)));
      }
    }
    Map<TopicPartitionReplica, ReplicaLogDirInfo> placement = new HashMap<>();
    if (!replicas.isEmpty()) {
      Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> futures = adminClient.describeReplicaLogDirs(replicas).values();
      try {
        for (TopicPartitionReplica replica : replicas) {
          if (!futures.containsKey(replica)) {
            throw new IllegalStateException("Missing final log-directory response for " + replica);
          }
          placement.put(replica, futures.get(replica).get(timeoutMs, TimeUnit.MILLISECONDS));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while verifying destination log directories.", e);
      } catch (ExecutionException | TimeoutException e) {
        throw new IllegalStateException("Cannot verify final destination log directories.", e);
      }
    }
    Set<ExecutionProposal> completed = new HashSet<>();
    for (ExecutionProposal proposal : proposals) {
      if (proposalPlacementCompleted(proposal, placement)) {
        completed.add(proposal);
      }
    }
    return completed;
  }

  private static boolean proposalPlacementCompleted(ExecutionProposal proposal, Map<TopicPartitionReplica, ReplicaLogDirInfo> placement) {
    if (proposal.destinationDiskCapacityByBroker().isEmpty()) {
      return true;
    }
    for (ReplicaPlacementInfo replica : proposal.destinationReplicas()) {
      TopicPartitionReplica key = replicaKey(proposal, replica);
      ReplicaLogDirInfo info = placement.get(key);
      if (!replica.logdir().equals(info.getCurrentReplicaLogDir())) {
        if (replica.logdir().equals(info.getFutureReplicaLogDir())) {
          return false;
        }
        throw new IllegalStateException("Replica " + key + " is on " + info.getCurrentReplicaLogDir()
                                        + " instead of selected log directory " + replica.logdir());
      }
      if (info.getFutureReplicaLogDir() != null) {
        return false;
      }
    }
    return true;
  }

  private static TopicPartitionReplica replicaKey(ExecutionProposal proposal, ReplicaPlacementInfo replica) {
    return new TopicPartitionReplica(proposal.topic(), proposal.partitionId(), replica.brokerId());
  }

}
