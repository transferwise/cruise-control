/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

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

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.LogDirNotFoundException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;

public final class ExecutorAdminUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorAdminUtils.class);

  /** A pre-submission rejection: none of the requested disk copies have been submitted. */
  static final class DiskCapacityValidationException extends IllegalStateException {
    DiskCapacityValidationException(RuntimeException cause) {
      super("Intra-broker disk capacity validation failed before submission.", cause);
    }
  }

  private ExecutorAdminUtils() {

  }

  /**
   * Fetch the logdir information for subject replicas in intra-broker replica movement tasks.
   *
   * @param tasks The tasks to check.
   * @param adminClient The adminClient to send describeReplicaLogDirs request.
   * @param config The config object that holds all the Cruise Control related configs
   * @return Replica logdir information by task.
   */
  static Map<ExecutionTask, ReplicaLogDirInfo> getLogdirInfoForExecutionTask(Collection<ExecutionTask> tasks,
                                                                             AdminClient adminClient,
                                                                             KafkaCruiseControlConfig config) {
    return getLogdirInfoForExecutionTask(tasks, adminClient, config, null);
  }

  /**
   * Fetch the logdir information for subject replicas in intra-broker replica movement tasks.
   * Optionally populates a set of tasks that had confirmed disk failures.
   *
   * @param tasks The tasks to check.
   * @param adminClient The adminClient to send describeReplicaLogDirs request.
   * @param config The config object that holds all the Cruise Control related configs.
   * @param nonRetriableFailures If non-null, populated with tasks whose logdir query failed with a non-retriable error
   *                             (e.g. {@link LogDirNotFoundException}, {@link KafkaStorageException}).
   * @return Replica logdir information by task.
   */
  static Map<ExecutionTask, ReplicaLogDirInfo> getLogdirInfoForExecutionTask(Collection<ExecutionTask> tasks,
                                                                             AdminClient adminClient,
                                                                             KafkaCruiseControlConfig config,
                                                                             Set<ExecutionTask> nonRetriableFailures) {
    return getLogdirInfoForExecutionTask(tasks, adminClient, config, nonRetriableFailures, null);
  }

  /**
   * Fetch placement while distinguishing confirmed disk failures from unavailable replicas and transient errors.
   * @param tasks tasks to query
   * @param adminClient Kafka admin client
   * @param config operator configuration
   * @param nonRetriableFailures optional destination for non-retriable replica or disk errors
   * @param failedDisks optional destination for storage or missing-directory errors only
   * @return successfully queried placements
   */
  static Map<ExecutionTask, ReplicaLogDirInfo> getLogdirInfoForExecutionTask(Collection<ExecutionTask> tasks,
                                                                             AdminClient adminClient,
                                                                             KafkaCruiseControlConfig config,
                                                                             Set<ExecutionTask> nonRetriableFailures,
                                                                             Set<ExecutionTask> failedDisks) {
    Set<TopicPartitionReplica> replicasToCheck = new HashSet<>();
    Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask = new HashMap<>();
    Map<TopicPartitionReplica, ExecutionTask> taskByReplica = new HashMap<>();
    tasks.forEach(t -> {
      TopicPartitionReplica tpr = new TopicPartitionReplica(t.proposal().topic(), t.proposal().partitionId(), t.brokerId());
      replicasToCheck.add(tpr);
      taskByReplica.put(tpr, t);
    });
    Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> logDirsByReplicas = adminClient.describeReplicaLogDirs(replicasToCheck).values();
    for (Map.Entry<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> entry : logDirsByReplicas.entrySet()) {
      try {
        ReplicaLogDirInfo info = entry.getValue().get(config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS);
        logdirInfoByTask.put(taskByReplica.get(entry.getKey()), info);
      } catch (ExecutionException e) {
        LOG.warn("Encounter exception {} when fetching logdir information for replica {}", e.getMessage(), entry.getKey());
        if (nonRetriableFailures != null && isNonRetriableLogDirError(e)) {
          nonRetriableFailures.add(taskByReplica.get(entry.getKey()));
        }
        if (failedDisks != null && (e.getCause() instanceof KafkaStorageException || e.getCause() instanceof LogDirNotFoundException)) {
          failedDisks.add(taskByReplica.get(entry.getKey()));
        }
      } catch (InterruptedException | TimeoutException e) {
        LOG.warn("Encounter exception {} when fetching logdir information for replica {}", e.getMessage(), entry.getKey());
      }
    }
    return logdirInfoByTask;
  }

  /**
   * Checks if an ExecutionException wraps a non-retriable logdir/replica error.
   *
   * @param e The execution exception to inspect.
   * @return {@code true} if the cause is a non-retriable disk/replica error, {@code false} otherwise.
   */
  private static boolean isNonRetriableLogDirError(ExecutionException e) {
    Throwable cause = e.getCause();
    return cause instanceof LogDirNotFoundException
        || cause instanceof KafkaStorageException;
  }

  /**
   * Execute intra-broker replica movement tasks by sending alterReplicaLogDirs request.
   *
   * @param tasksToExecute The tasks to execute.
   * @param adminClient The adminClient to send alterReplicaLogDirs request.
   * @param executionTaskManager The task manager to do bookkeeping for task execution state.
   * @param config The config object that holds all the Cruise Control related configs
   */
  static void executeIntraBrokerReplicaMovements(List<ExecutionTask> tasksToExecute,
                                                 AdminClient adminClient,
                                                 ExecutionTaskManager executionTaskManager,
                                                 KafkaCruiseControlConfig config) {
    executeIntraBrokerReplicaMovements(tasksToExecute, tasksToExecute, adminClient, executionTaskManager, config);
  }

  /**
   * Validate destination capacity for new and active copies before submitting disk moves.
   * @param tasksToExecute tasks to submit
   * @param activeTasks all active replica copy tasks
   * @param adminClient Kafka admin client
   * @param executionTaskManager task state manager
   * @param config operator configuration
   */
  static void executeIntraBrokerReplicaMovements(List<ExecutionTask> tasksToExecute,
                                                 Collection<ExecutionTask> activeTasks,
                                                 AdminClient adminClient,
                                                 ExecutionTaskManager executionTaskManager,
                                                 KafkaCruiseControlConfig config) {
    try {
      InterBrokerDiskCapacityValidator.validateIntraBroker(adminClient, tasksToExecute, activeTasks,
          config.getDouble(com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig.DISK_CAPACITY_THRESHOLD_CONFIG),
          config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG));
    } catch (RuntimeException e) {
      throw new DiskCapacityValidationException(e);
    }
    Map<TopicPartitionReplica, String> replicaAssignment = new HashMap<>();
    Map<TopicPartitionReplica, ExecutionTask> replicaToTask = new HashMap<>();
    tasksToExecute.forEach(t -> {
      TopicPartitionReplica tpr = new TopicPartitionReplica(t.proposal().topic(), t.proposal().partitionId(), t.brokerId());
      replicaAssignment.put(tpr, t.proposal().replicasToMoveBetweenDisksByBroker().get(t.brokerId()).logdir());
      replicaToTask.put(tpr, t);
    });
    Map<TopicPartitionReplica, KafkaFuture<Void>> acknowledgements = adminClient.alterReplicaLogDirs(replicaAssignment).values();
    for (TopicPartitionReplica replica : replicaAssignment.keySet()) {
      ExecutionTask task = replicaToTask.get(replica);
      if (!acknowledgements.containsKey(replica)) {
        LOG.warn("Missing disk-copy acknowledgement for {}; retaining task for placement polling.", replica);
        continue;
      }
      try {
        acknowledgements.get(replica).get(config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS);
      } catch (ExecutionException e) {
        if (isNonRetriableLogDirError(e)
            || (e.getCause() instanceof ApiException && !(e.getCause() instanceof RetriableException))) {
          LOG.warn("Disk-copy submission rejected for task {}.", task, e);
          executionTaskManager.markTaskAborting(task);
          executionTaskManager.markTaskDead(task);
        } else {
          LOG.warn("Disk-copy acknowledgement failed for {}; retaining task for placement polling.", task, e);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Interrupted awaiting disk-copy acknowledgement for {}; retaining task for placement polling.", task, e);
        // The entire batch was already submitted. Preserve every task for polling, but stop blocking on acknowledgements.
        return;
      } catch (TimeoutException e) {
        LOG.warn("Disk-copy acknowledgement timed out for {}; retaining task for placement polling.", task, e);
      }
    }
  }

  /**
   * Check whether there is ongoing intra-broker replica movement.
   * @param adminClient The adminClient to send describeLogDirs request.
   * @param config The config object that holds all the Cruise Control related configs
   * @return {@code true} if there is ongoing intra-broker replica movement.
   */
  static boolean hasOngoingIntraBrokerReplicaMovement(AdminClient adminClient,
                                                      KafkaCruiseControlConfig config)
      throws InterruptedException, ExecutionException, TimeoutException {
    Collection<Integer> brokersToCheck = adminClient.describeCluster().nodes().get().stream().map(Node::id).collect(Collectors.toSet());
    Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> logDirsByBrokerId = adminClient.describeLogDirs(brokersToCheck).descriptions();
    for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirDescription>>> entry : logDirsByBrokerId.entrySet()) {
      Map<String, LogDirDescription> logInfos = entry.getValue().get(config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS);
      for (LogDirDescription info : logInfos.values()) {
        if (info.error() == null) {
          if (info.replicaInfos().values().stream().anyMatch(ReplicaInfo::isFuture)) {
            return true;
          }
        }
      }
    }
    return false;
  }
}
