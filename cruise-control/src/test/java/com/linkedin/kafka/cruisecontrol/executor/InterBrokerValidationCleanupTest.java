/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.MetadataAdminClient;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorManager;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.*;

/** Exercises rejection, placement-query retries, rollback and throttle cleanup through the executor's real loop. */
public class InterBrokerValidationCleanupTest {
  private enum Failure { CAPACITY, ACKNOWLEDGEMENT, QUERY_RECOVERY, QUERY_EXHAUSTION, CANCELLATION_REJECTION }

  private ExecutionTask task(int partition) {
    ReplicaPlacementInfo source = new ReplicaPlacementInfo(0, "/source");
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition("topic", partition), 20, source,
        List.of(source), List.of(new ReplicaPlacementInfo(1, "/target")), Map.of(1, 1000.0));
    return new ExecutionTask(partition, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, 1000000);
  }

  private void setField(Object object, String name, Object value) throws Exception {
    Field field = object.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(object, value);
  }

  @Test
  public void testFirstBatchAcknowledgementFailureRemovesBrokerAndTopicThrottles() throws Exception {
    verifyCleanup(Failure.ACKNOWLEDGEMENT, false);
  }

  @Test
  public void testLaterBatchCapacityRejectionSettlesActiveReassignmentBeforeCleanup() throws Exception {
    verifyCleanup(Failure.CAPACITY, true);
  }

  @Test
  public void testTransientPlacementQueryRecoversWithoutCancellingExecution() throws Exception {
    verifyCleanup(Failure.QUERY_RECOVERY, false);
  }

  @Test
  public void testPersistentPlacementQueryFailureCancelsAndCleansUp() throws Exception {
    verifyCleanup(Failure.QUERY_EXHAUSTION, false);
  }

  @Test
  public void testFailedRollbackRetainsThrottlesEvenAfterTasksMarkedDead() throws Exception {
    verifyCleanup(Failure.CANCELLATION_REJECTION, true);
  }

  private void verifyCleanup(Failure failure, boolean previousCopy) throws Exception {
    ExecutionTask incoming = task(1);
    ExecutionTask active = task(0);
    Set<ExecutionTask> running = new HashSet<>();
    if (previousCopy) {
      active.inProgress(Time.SYSTEM.milliseconds());
      running.add(active);
    }
    AtomicBoolean settled = new AtomicBoolean(!previousCopy);
    AtomicInteger submissions = new AtomicInteger();
    AtomicInteger cancellations = new AtomicInteger();
    AtomicInteger queries = new AtomicInteger();
    Map<ConfigResource, Map<String, String>> configValues = new HashMap<>();
    AdminClient admin = EasyMock.mock(AdminClient.class);
    EasyMock.expect(admin.describeConfigs(EasyMock.anyObject())).andAnswer(() -> {
      Collection<ConfigResource> resources = EasyMock.getCurrentArgument(0);
      Map<ConfigResource, KafkaFuture<Config>> results = new HashMap<>();
      for (ConfigResource resource : resources) {
        List<ConfigEntry> entries = configValues.getOrDefault(resource, Map.of()).entrySet().stream()
            .map(e -> new ConfigEntry(e.getKey(), e.getValue())).collect(java.util.stream.Collectors.toList());
        results.put(resource, KafkaFuture.completedFuture(new Config(entries)));
      }
      DescribeConfigsResult result = EasyMock.mock(DescribeConfigsResult.class);
      EasyMock.expect(result.values()).andReturn(results).anyTimes();
      Map<ConfigResource, Config> configs = new HashMap<>();
      for (Map.Entry<ConfigResource, KafkaFuture<Config>> entry : results.entrySet()) {
        configs.put(entry.getKey(), entry.getValue().get());
      }
      EasyMock.expect(result.all()).andReturn(KafkaFuture.completedFuture(configs)).anyTimes();
      EasyMock.replay(result);
      return result;
    }).anyTimes();
    EasyMock.expect(admin.incrementalAlterConfigs(EasyMock.anyObject())).andAnswer(() -> {
      Map<ConfigResource, Collection<AlterConfigOp>> changes = EasyMock.getCurrentArgument(0);
      for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> change : changes.entrySet()) {
        Map<String, String> values = configValues.computeIfAbsent(change.getKey(), key -> new HashMap<>());
        for (AlterConfigOp op : change.getValue()) {
          if (op.opType() == AlterConfigOp.OpType.DELETE) {
            assertTrue("Throttle must remain until reassignment is settled", settled.get());
            values.remove(op.configEntry().name());
          } else {
            values.put(op.configEntry().name(), op.configEntry().value());
          }
        }
      }
      AlterConfigsResult result = EasyMock.mock(AlterConfigsResult.class);
      EasyMock.expect(result.all()).andReturn(KafkaFuture.completedFuture(null));
      EasyMock.replay(result);
      return result;
    }).anyTimes();
    EasyMock.expect(admin.describeLogDirs(Set.of(0, 1))).andAnswer(() -> {
      DescribeLogDirsResult result = EasyMock.mock(DescribeLogDirsResult.class);
      long usage = failure == Failure.CAPACITY || failure == Failure.CANCELLATION_REJECTION ? 790 : 600;
      EasyMock.expect(result.descriptions()).andReturn(Map.of(
          0, KafkaFuture.completedFuture(Map.of("/source", new LogDirDescription(null, Map.of()))),
          1, KafkaFuture.completedFuture(Map.of("/target", new LogDirDescription(null, Map.of(
              new TopicPartition("other", 0), new ReplicaInfo(usage * 1024 * 1024, 0, false)))))));
      EasyMock.replay(result);
      return result;
    }).anyTimes();
    TopicPartitionReplica destination = new TopicPartitionReplica("topic", 1, 1);
    EasyMock.expect(admin.alterReplicaLogDirs(Map.of(destination, "/target"))).andAnswer(() -> {
      AlterReplicaLogDirsResult result = EasyMock.mock(AlterReplicaLogDirsResult.class);
      KafkaFutureImpl<Void> ack = new KafkaFutureImpl<>();
      ack.completeExceptionally(failure == Failure.ACKNOWLEDGEMENT
          ? new org.apache.kafka.common.errors.KafkaStorageException()
          : new org.apache.kafka.common.errors.ReplicaNotAvailableException("not created yet"));
      EasyMock.expect(result.values()).andReturn(Map.of(destination, ack));
      EasyMock.replay(result);
      return result;
    }).anyTimes();
    EasyMock.expect(admin.alterPartitionReassignments(EasyMock.anyObject())).andAnswer(() -> {
      Map<TopicPartition, Optional<NewPartitionReassignment>> changes = EasyMock.getCurrentArgument(0);
      Map<TopicPartition, KafkaFuture<Void>> results = new HashMap<>();
      for (Map.Entry<TopicPartition, Optional<NewPartitionReassignment>> change : changes.entrySet()) {
        if (change.getValue().isEmpty()) {
          cancellations.incrementAndGet();
          if (failure == Failure.CANCELLATION_REJECTION) {
            KafkaFutureImpl<Void> rejected = new KafkaFutureImpl<>();
            rejected.completeExceptionally(new org.apache.kafka.common.errors.TimeoutException("rollback not acknowledged"));
            results.put(change.getKey(), rejected);
            continue;
          }
          settled.set(true);
        } else {
          submissions.incrementAndGet();
          settled.set(false);
        }
        results.put(change.getKey(), KafkaFuture.completedFuture(null));
      }
      AlterPartitionReassignmentsResult result = EasyMock.mock(AlterPartitionReassignmentsResult.class);
      EasyMock.expect(result.values()).andReturn(results).anyTimes();
      EasyMock.replay(result);
      return result;
    }).anyTimes();
    EasyMock.expect(admin.listPartitionReassignments()).andAnswer(() -> {
      ListPartitionReassignmentsResult result = EasyMock.mock(ListPartitionReassignmentsResult.class);
      Map<TopicPartition, PartitionReassignment> ongoing = settled.get() ? Map.of()
          : Map.of(incoming.proposal().topicPartition(), new PartitionReassignment(List.of(0, 1), List.of(1), List.of(0)));
      EasyMock.expect(result.reassignments()).andReturn(KafkaFuture.completedFuture(ongoing));
      EasyMock.replay(result);
      return result;
    }).anyTimes();
    EasyMock.expect(admin.describeReplicaLogDirs(Set.of(destination))).andAnswer(() -> {
      int count = queries.incrementAndGet();
      KafkaFutureImpl<ReplicaLogDirInfo> info = new KafkaFutureImpl<>();
      if (failure == Failure.QUERY_EXHAUSTION || count == 1) {
        info.completeExceptionally(new java.util.concurrent.TimeoutException("transient query timeout"));
      } else {
        ReplicaLogDirInfo placement = EasyMock.mock(ReplicaLogDirInfo.class);
        EasyMock.expect(placement.getCurrentReplicaLogDir()).andReturn("/target").anyTimes();
        EasyMock.expect(placement.getFutureReplicaLogDir()).andReturn(null).anyTimes();
        EasyMock.replay(placement);
        info.complete(placement);
      }
      DescribeReplicaLogDirsResult result = EasyMock.mock(DescribeReplicaLogDirsResult.class);
      EasyMock.expect(result.values()).andReturn(Map.of(destination, info));
      EasyMock.replay(result);
      return result;
    }).anyTimes();
    admin.close();
    EasyMock.expectLastCall();
    MetadataAdminClient metadata = EasyMock.niceMock(MetadataAdminClient.class);
    Node source = new Node(0, "localhost", 9092);
    Node target = new Node(1, "localhost", 9093);
    Cluster cluster = new Cluster("test", List.of(source, target), List.of(
        new PartitionInfo("topic", 0, source, new Node[]{source, target}, new Node[]{source, target}),
        new PartitionInfo("topic", 1, target, new Node[]{target}, new Node[]{target})), Set.of(), Set.of());
    EasyMock.expect(metadata.cluster()).andReturn(cluster).anyTimes();
    LoadMonitor monitor = EasyMock.mock(LoadMonitor.class);
    EasyMock.expect(monitor.deadBrokersWithReplicas(EasyMock.anyLong())).andReturn(Set.of());
    ExecutionTaskManager manager = EasyMock.niceMock(ExecutionTaskManager.class);
    EasyMock.expect(manager.numRemainingInterBrokerPartitionMovements()).andReturn(1).andReturn(0).anyTimes();
    EasyMock.expect(manager.getInterBrokerReplicaMovementTasks()).andReturn(List.of(incoming));
    manager.markTasksInProgress(List.of(incoming));
    EasyMock.expectLastCall().andAnswer(() -> {
      incoming.inProgress(Time.SYSTEM.milliseconds());
      running.add(incoming);
      return null;
    });
    manager.markTaskDead(EasyMock.anyObject());
    EasyMock.expectLastCall().andAnswer(() -> {
      ExecutionTask task = EasyMock.getCurrentArgument(0);
      task.kill(Time.SYSTEM.milliseconds());
      running.remove(task);
      return null;
    }).anyTimes();
    manager.markTaskDone(incoming);
    EasyMock.expectLastCall().andAnswer(() -> {
      incoming.completed(Time.SYSTEM.milliseconds());
      running.remove(incoming);
      settled.set(true);
      return null;
    }).anyTimes();
    EasyMock.expect(manager.inExecutionTasks()).andAnswer(() -> new HashSet<>(running)).anyTimes();
    EasyMock.expect(manager.inExecutionTasks(Set.of(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION)))
        .andAnswer(() -> new HashSet<>(running)).anyTimes();
    ExecutionTaskTracker tracker = new ExecutionTaskTracker(new MetricRegistry(), Time.SYSTEM);
    EasyMock.expect(manager.getExecutionTasksSummary(EasyMock.anyObject())).andReturn(tracker.getExecutionTasksSummary(Set.of())).anyTimes();
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    com.linkedin.kafka.cruisecontrol.executor.concurrency.ExecutionConcurrencyManager concurrency =
        new com.linkedin.kafka.cruisecontrol.executor.concurrency.ExecutionConcurrencyManager(config);
    EasyMock.expect(manager.getExecutionConcurrencyManager()).andReturn(concurrency).anyTimes();
    EasyMock.replay(admin, metadata, manager, monitor);
    Executor executor = new Executor(config, Time.SYSTEM, new MetricRegistry(), admin, metadata,
        EasyMock.niceMock(ExecutorNotifier.class), EasyMock.niceMock(AnomalyDetectorManager.class));
    setField(executor, "_executionTaskManager", manager);
    setField(executor, "_defaultExecutionProgressCheckIntervalMs", 1L);
    setField(executor, "_minExecutionProgressCheckIntervalMs", 1L);
    setField(executor, "_executionProgressCheckIntervalMs", 1L);
    setField(executor, "_reasonSupplier", (java.util.function.Supplier<String>) () -> "test");
    setField(executor, "_executorState", ExecutorState.operationInProgress(
        ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS, tracker.getExecutionTasksSummary(Set.of()),
        concurrency.getExecutionConcurrencySummary(), "test", "test", Set.of(), Set.of(), false));
    Class<?> runnableClass = Class.forName(Executor.class.getName() + "$ProposalExecutionRunnable");
    Constructor<?> constructor = runnableClass.getDeclaredConstructor(Executor.class, LoadMonitor.class,
        Collection.class, Collection.class, Long.class, Long.class, boolean.class);
    constructor.setAccessible(true);
    Object runnable = constructor.newInstance(executor, monitor, null, null, 1000L, null, false);
    Method move = runnableClass.getDeclaredMethod("interBrokerMoveReplicas");
    move.setAccessible(true);
    try {
      if (failure == Failure.CANCELLATION_REJECTION) {
        java.lang.reflect.InvocationTargetException rejected =
            assertThrows(java.lang.reflect.InvocationTargetException.class, () -> move.invoke(runnable));
        assertEquals(IllegalStateException.class, rejected.getCause().getClass());
        assertTrue(rejected.getCause().getMessage().contains("alterPartitionReassignments request timed out"));
        assertTrue(running.isEmpty());
        assertFalse(settled.get());
        assertTrue(configValues.values().stream().anyMatch(values -> !values.isEmpty()));
        assertEquals(1, cancellations.get());
        assertEquals(0, submissions.get());
      } else {
        move.invoke(runnable);
        assertTrue(running.isEmpty());
        assertTrue(configValues.values().stream().allMatch(Map::isEmpty));
        if (failure == Failure.QUERY_RECOVERY) {
          assertEquals(ExecutionTaskState.COMPLETED, incoming.state());
          assertEquals(2, queries.get());
          assertEquals(0, cancellations.get());
        } else {
          assertEquals(ExecutionTaskState.DEAD, incoming.state());
          assertEquals(previousCopy || failure == Failure.QUERY_EXHAUSTION ? 1 : 0, cancellations.get());
        }
        assertEquals(failure == Failure.QUERY_RECOVERY || failure == Failure.QUERY_EXHAUSTION ? 1 : 0, submissions.get());
        if (failure == Failure.QUERY_EXHAUSTION) {
          assertEquals(3, queries.get());
        }
      }
    } finally {
      executor.shutdown();
    }
    EasyMock.verify(admin, manager, metadata, monitor);
  }
}
