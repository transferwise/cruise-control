/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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
  private enum Failure {
    CAPACITY, ACKNOWLEDGEMENT, QUERY_RECOVERY, QUERY_DELAYED_RECOVERY, QUERY_PERSISTENT, QUERY_RESET_RECOVERY, CANCELLATION_REJECTION,
    SUBMISSION_AND_DRAIN, PERMANENT_QUERY, CLEANUP_REJECTION
  }

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
  public void testRepeatedTransientPlacementFailuresRecoverWithoutCancelling() throws Exception {
    verifyCleanup(Failure.QUERY_DELAYED_RECOVERY, false);
  }

  @Test(timeout = 15000)
  public void testPersistentPlacementFailuresCancelAfterTenQueries() throws Exception {
    verifyCleanup(Failure.QUERY_PERSISTENT, false);
  }

  @Test(timeout = 15000)
  public void testSuccessfulQueryResetsConsecutiveFailureBudget() throws Exception {
    verifyCleanup(Failure.QUERY_RESET_RECOVERY, false);
  }

  @Test
  public void testPermanentPlacementQueryFailureCancelsExecution() throws Exception {
    verifyCleanup(Failure.PERMANENT_QUERY, false);
  }

  @Test
  public void testFailedRollbackRetainsThrottlesEvenAfterTasksMarkedDead() throws Exception {
    verifyCleanup(Failure.CANCELLATION_REJECTION, true);
  }

  @Test
  public void testDrainFailureDoesNotReplaceSubmissionFailure() throws Exception {
    verifyCleanup(Failure.SUBMISSION_AND_DRAIN, true);
  }

  @Test
  public void testFailedThrottleCleanupIsRetriedBeforeNextExecution() throws Exception {
    verifyCleanup(Failure.CLEANUP_REJECTION, false);
  }

  @Test(timeout = 15000)
  public void testConfiguredPlacementFailureLimitIsEnforced() throws Exception {
    verifyCleanup(Failure.QUERY_PERSISTENT, false, 3);
  }

  @Test(expected = org.apache.kafka.common.config.ConfigException.class)
  public void testPlacementFailureLimitCannotDisableTheBound() {
    java.util.Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.setProperty(com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig
        .INTER_BROKER_DISK_PLACEMENT_MAX_CONSECUTIVE_QUERY_FAILURES_CONFIG, "0");
    new KafkaCruiseControlConfig(properties);
  }

  private void verifyCleanup(Failure failure, boolean previousCopy) throws Exception {
    verifyCleanup(failure, previousCopy, 10);
  }

  private void verifyCleanup(Failure failure, boolean previousCopy, int maxQueryFailures) throws Exception {
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
    AtomicInteger throttleWrites = new AtomicInteger();
    AtomicInteger cleanupAttempts = new AtomicInteger();
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
            if (failure == Failure.CLEANUP_REJECTION && cleanupAttempts.incrementAndGet() <= 2) {
              throw new IllegalStateException("throttle cleanup failed");
            }
            values.remove(op.configEntry().name());
          } else {
            values.put(op.configEntry().name(), op.configEntry().value());
          }
        }
      }
      if (failure == Failure.SUBMISSION_AND_DRAIN && throttleWrites.incrementAndGet() == 1) {
        throw new IllegalStateException("initial throttle write failed");
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
          if (failure == Failure.CANCELLATION_REJECTION || failure == Failure.SUBMISSION_AND_DRAIN) {
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
      // Delayed confirmation can outlive Kafka's reassignment itself; it must not trigger resubmission.
      Map<TopicPartition, PartitionReassignment> ongoing = settled.get() || failure == Failure.QUERY_DELAYED_RECOVERY
          || failure == Failure.QUERY_RESET_RECOVERY ? Map.of()
          : Map.of(incoming.proposal().topicPartition(), new PartitionReassignment(List.of(0, 1), List.of(1), List.of(0)));
      EasyMock.expect(result.reassignments()).andReturn(KafkaFuture.completedFuture(ongoing));
      EasyMock.replay(result);
      return result;
    }).anyTimes();
    EasyMock.expect(admin.describeReplicaLogDirs(Set.of(destination))).andAnswer(() -> {
      int count = queries.incrementAndGet();
      KafkaFutureImpl<ReplicaLogDirInfo> info = new KafkaFutureImpl<>();
      if (failure == Failure.PERMANENT_QUERY) {
        info.completeExceptionally(new org.apache.kafka.common.errors.KafkaStorageException("disk failed"));
      } else if (failure == Failure.QUERY_PERSISTENT
                 || (failure == Failure.QUERY_RESET_RECOVERY && count != 10 && count != 20)
                 || (failure == Failure.QUERY_DELAYED_RECOVERY && count <= 4) || count == 1) {
        info.completeExceptionally(new java.util.concurrent.TimeoutException("transient query timeout"));
      } else {
        ReplicaLogDirInfo placement = EasyMock.mock(ReplicaLogDirInfo.class);
        boolean inProgress = failure == Failure.QUERY_RESET_RECOVERY && count == 10;
        EasyMock.expect(placement.getCurrentReplicaLogDir()).andReturn(inProgress ? "/source" : "/target").anyTimes();
        EasyMock.expect(placement.getFutureReplicaLogDir()).andReturn(inProgress ? "/target" : null).anyTimes();
        EasyMock.replay(placement);
        info.complete(placement);
      }
      DescribeReplicaLogDirsResult result = EasyMock.mock(DescribeReplicaLogDirsResult.class);
      EasyMock.expect(result.values()).andReturn(Map.of(destination, info));
      EasyMock.replay(result);
      return result;
    }).anyTimes();
    org.apache.kafka.clients.admin.DescribeClusterResult brokers = EasyMock.mock(org.apache.kafka.clients.admin.DescribeClusterResult.class);
    EasyMock.expect(brokers.nodes()).andReturn(KafkaFuture.completedFuture(List.of(new Node(1, "localhost", 9093)))).anyTimes();
    EasyMock.replay(brokers);
    EasyMock.expect(admin.describeCluster()).andReturn(brokers).anyTimes();
    DescribeLogDirsResult idleDirectories = EasyMock.mock(DescribeLogDirsResult.class);
    EasyMock.expect(idleDirectories.descriptions()).andReturn(Map.of(1,
        KafkaFuture.completedFuture(Map.of("/target", new LogDirDescription(null, Map.of()))))).anyTimes();
    EasyMock.replay(idleDirectories);
    EasyMock.expect(admin.describeLogDirs(Set.of(1))).andReturn(idleDirectories).anyTimes();
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
    if (failure != Failure.SUBMISSION_AND_DRAIN) {
      manager.markTasksInProgress(List.of(incoming));
      EasyMock.expectLastCall().andAnswer(() -> {
        incoming.inProgress(Time.SYSTEM.milliseconds());
        running.add(incoming);
        return null;
      });
    }
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
    java.util.Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    if (maxQueryFailures != 10) {
      properties.setProperty(com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig
          .INTER_BROKER_DISK_PLACEMENT_MAX_CONSECUTIVE_QUERY_FAILURES_CONFIG, Integer.toString(maxQueryFailures));
    }
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(properties);
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
      if (failure == Failure.CANCELLATION_REJECTION || failure == Failure.SUBMISSION_AND_DRAIN) {
        if (failure == Failure.SUBMISSION_AND_DRAIN) {
          java.lang.reflect.InvocationTargetException rejected =
              assertThrows(java.lang.reflect.InvocationTargetException.class, () -> move.invoke(runnable));
          assertEquals("initial throttle write failed", rejected.getCause().getMessage());
          assertEquals(1, rejected.getCause().getSuppressed().length);
          assertTrue(rejected.getCause().getSuppressed()[0].getMessage().contains("alterPartitionReassignments request timed out"));
        } else {
          move.invoke(runnable);
          Field original = runnableClass.getDeclaredField("_executionException");
          original.setAccessible(true);
          Throwable rejected = (Throwable) original.get(runnable);
          assertTrue(rejected instanceof InterBrokerDiskCapacityValidator.ValidationException);
          assertEquals(1, rejected.getSuppressed().length);
          assertTrue(rejected.getSuppressed()[0].getMessage().contains("alterPartitionReassignments request timed out"));
        }
        assertTrue(running.isEmpty());
        assertFalse(settled.get());
        assertTrue(configValues.values().stream().anyMatch(values -> !values.isEmpty()));
        assertEquals(1, cancellations.get());
        assertEquals(0, submissions.get());
        Field pending = Executor.class.getDeclaredField("_pendingInterBrokerThrottleCleanup");
        pending.setAccessible(true);
        assertNotNull(pending.get(executor));
        Method sanity = Executor.class.getDeclaredMethod("sanityCheckOngoingMovement");
        sanity.setAccessible(true);
        assertThrows(java.lang.reflect.InvocationTargetException.class, () -> sanity.invoke(executor));
        assertNotNull(pending.get(executor));
        assertTrue(configValues.values().stream().anyMatch(values -> !values.isEmpty()));
        settled.set(true);
        sanity.invoke(executor);
        assertNull(pending.get(executor));
        assertTrue(configValues.values().stream().allMatch(Map::isEmpty));
      } else if (failure == Failure.CLEANUP_REJECTION) {
        java.lang.reflect.InvocationTargetException rejected =
            assertThrows(java.lang.reflect.InvocationTargetException.class, () -> move.invoke(runnable));
        assertEquals("throttle cleanup failed", rejected.getCause().getMessage());
        assertTrue(running.isEmpty());
        assertTrue(configValues.values().stream().anyMatch(values -> !values.isEmpty()));
        Field pending = Executor.class.getDeclaredField("_pendingInterBrokerThrottleCleanup");
        pending.setAccessible(true);
        assertNotNull(pending.get(executor));
        Method sanity = Executor.class.getDeclaredMethod("sanityCheckOngoingMovement");
        sanity.setAccessible(true);
        sanity.invoke(executor);
        assertNull(pending.get(executor));
        assertTrue(configValues.values().stream().allMatch(Map::isEmpty));
      } else {
        move.invoke(runnable);
        assertTrue(running.isEmpty());
        assertTrue(configValues.values().stream().allMatch(Map::isEmpty));
        if (failure == Failure.QUERY_RECOVERY || failure == Failure.QUERY_DELAYED_RECOVERY
            || failure == Failure.QUERY_RESET_RECOVERY) {
          assertEquals(ExecutionTaskState.COMPLETED, incoming.state());
          assertEquals(failure == Failure.QUERY_RECOVERY ? 2 : failure == Failure.QUERY_RESET_RECOVERY ? 20 : 5, queries.get());
          assertEquals(0, cancellations.get());
        } else {
          assertEquals(ExecutionTaskState.DEAD, incoming.state());
          assertEquals(previousCopy || failure == Failure.PERMANENT_QUERY || failure == Failure.QUERY_PERSISTENT ? 1 : 0, cancellations.get());
        }
        if (failure == Failure.QUERY_PERSISTENT) {
          assertEquals(maxQueryFailures, queries.get());
        }
        boolean submitted = failure == Failure.QUERY_RECOVERY || failure == Failure.QUERY_DELAYED_RECOVERY
            || failure == Failure.PERMANENT_QUERY || failure == Failure.QUERY_PERSISTENT || failure == Failure.QUERY_RESET_RECOVERY;
        assertEquals(submitted ? 1 : 0, submissions.get());
      }
    } finally {
      executor.shutdown();
    }
    EasyMock.verify(admin, manager, metadata, monitor);
  }
}
