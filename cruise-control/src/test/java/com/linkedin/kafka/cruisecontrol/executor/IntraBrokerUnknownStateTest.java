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
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.*;

/** Exercises the executor's real submission and drain loops with scripted Kafka responses. */
public class IntraBrokerUnknownStateTest {
  private static final String TARGET = "/target";
  private static final String THROTTLE = IntraBrokerReplicationThrottleHelper.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG;
  private static final ConfigResource BROKER = new ConfigResource(ConfigResource.Type.BROKER, "1");

  private ExecutionTask task(int partition) {
    ReplicaPlacementInfo source = new ReplicaPlacementInfo(1, "/source");
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition("topic", partition), 20, source,
        List.of(source), List.of(new ReplicaPlacementInfo(1, TARGET)), Map.of(1, 1000.0));
    return new ExecutionTask(partition, proposal, 1, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, 1000000);
  }

  private void setField(Object object, String name, Object value) throws Exception {
    Field field = object.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(object, value);
  }

  private void configs(AdminClient admin, Config config) {
    DescribeConfigsResult result = EasyMock.mock(DescribeConfigsResult.class);
    EasyMock.expect(admin.describeConfigs(List.of(BROKER))).andReturn(result);
    EasyMock.expect(result.values()).andReturn(Map.of(BROKER, KafkaFuture.completedFuture(config)));
    EasyMock.replay(result);
  }

  private void alterConfigs(AdminClient admin, ExecutionTask active, boolean clearing) {
    AlterConfigsResult result = EasyMock.mock(AlterConfigsResult.class);
    EasyMock.expect(admin.incrementalAlterConfigs(EasyMock.anyObject())).andAnswer(() -> {
      assertEquals(clearing ? ExecutionTaskState.DEAD : ExecutionTaskState.IN_PROGRESS, active.state());
      return result;
    });
    EasyMock.expect(result.values()).andReturn(Map.of(BROKER, KafkaFuture.completedFuture(null)));
    EasyMock.replay(result);
  }

  private void logdirs(AdminClient admin, Exception failure) {
    DescribeReplicaLogDirsResult result = EasyMock.mock(DescribeReplicaLogDirsResult.class);
    TopicPartitionReplica replica = new TopicPartitionReplica("topic", 0, 1);
    EasyMock.expect(admin.describeReplicaLogDirs(Set.of(replica))).andReturn(result);
    org.apache.kafka.common.internals.KafkaFutureImpl<ReplicaLogDirInfo> unknown =
        new org.apache.kafka.common.internals.KafkaFutureImpl<>();
    if (failure == null) {
      ReplicaLogDirInfo settled = EasyMock.niceMock(ReplicaLogDirInfo.class);
      EasyMock.expect(settled.getCurrentReplicaLogDir()).andReturn(TARGET).anyTimes();
      EasyMock.replay(settled);
      unknown.complete(settled);
    } else {
      unknown.completeExceptionally(failure);
    }
    EasyMock.expect(result.values()).andReturn(Map.of(replica, unknown));
    EasyMock.replay(result);
  }

  @Test
  public void testUnavailableCopyRetainsThrottleDuringNormalExecution() throws Exception {
    assertProgressPollingFailure(new org.apache.kafka.common.errors.ReplicaNotAvailableException("unavailable"), true);
  }

  @Test
  public void testConfirmedStorageFailurePermitsThrottleCleanup() throws Exception {
    assertProgressPollingFailure(new org.apache.kafka.common.errors.KafkaStorageException("failed disk"), false);
  }

  @Test
  public void testConfirmedMissingDirectoryPermitsThrottleCleanup() throws Exception {
    assertProgressPollingFailure(new org.apache.kafka.common.errors.LogDirNotFoundException("missing disk"), false);
  }

  @Test
  public void testTransientPlacementFailuresRecoverWithoutStoppingExecution() throws Exception {
    assertProgressPollingResult(new java.util.concurrent.TimeoutException("transient timeout"), false, true);
  }

  private void assertProgressPollingFailure(Exception queryFailure, boolean retainCopy) throws Exception {
    assertProgressPollingResult(queryFailure, retainCopy, false);
  }

  private void assertProgressPollingResult(Exception queryFailure, boolean retainCopy, boolean recover) throws Exception {
    ExecutionTask active = task(0);
    active.inProgress(Time.SYSTEM.milliseconds());
    Set<ExecutionTask> running = new HashSet<>(Set.of(active));
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    MetadataAdminClient metadata = EasyMock.niceMock(MetadataAdminClient.class);
    ExecutionTaskManager manager = EasyMock.niceMock(ExecutionTaskManager.class);
    if (recover) {
      manager.markTaskDone(active);
    } else {
      manager.markTaskDead(active);
    }
    EasyMock.expectLastCall().andAnswer(() -> {
      if (recover) {
        active.completed(Time.SYSTEM.milliseconds());
      } else {
        active.kill(Time.SYSTEM.milliseconds());
      }
      running.remove(active);
      return null;
    });
    EasyMock.expect(manager.inExecutionTasks()).andAnswer(() -> new HashSet<>(running)).anyTimes();
    EasyMock.expect(manager.inExecutionTasks(Set.of(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION)))
        .andAnswer(() -> new HashSet<>(running)).anyTimes();
    ExecutionTaskTracker tracker = new ExecutionTaskTracker(new MetricRegistry(), Time.SYSTEM);
    EasyMock.expect(manager.getExecutionTasksSummary(EasyMock.anyObject())).andReturn(tracker.getExecutionTasksSummary(Set.of())).anyTimes();
    EasyMock.expect(manager.getExecutionConcurrencyManager())
        .andReturn(new com.linkedin.kafka.cruisecontrol.executor.concurrency.ExecutionConcurrencyManager(config)).anyTimes();
    Node node = new Node(1, "localhost", 9092);
    Cluster cluster = new Cluster("test", List.of(node),
        List.of(new PartitionInfo("topic", 0, node, new Node[]{node}, new Node[]{node})), Set.of(), Set.of());
    EasyMock.expect(metadata.cluster()).andReturn(cluster).anyTimes();
    // Only the progress query counts towards the budget; retry inspection also queries each poll.
    int failedPolls = recover ? 2 : retainCopy ? 3 : 1;
    for (int i = 0; i < failedPolls * 2; i++) {
      logdirs(admin, queryFailure);
    }
    if (recover) {
      logdirs(admin, null);
      logdirs(admin, null);
    }
    admin.close();
    EasyMock.expectLastCall();
    EasyMock.replay(admin, metadata, manager);
    Executor executor = new Executor(config, Time.SYSTEM, new MetricRegistry(), admin, metadata,
        EasyMock.niceMock(ExecutorNotifier.class), EasyMock.niceMock(AnomalyDetectorManager.class));
    setField(executor, "_executionTaskManager", manager);
    setField(executor, "_executionProgressCheckIntervalMs", 1L);
    setField(executor, "_reasonSupplier", (java.util.function.Supplier<String>) () -> "test");
    setField(executor, "_executorState", ExecutorState.operationInProgress(
        ExecutorState.State.INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS, tracker.getExecutionTasksSummary(Set.of()),
        new com.linkedin.kafka.cruisecontrol.executor.concurrency.ExecutionConcurrencyManager(config).getExecutionConcurrencySummary(),
        "test", "test", Set.of(), Set.of(), false));
    Class<?> runnableClass = Class.forName(Executor.class.getName() + "$ProposalExecutionRunnable");
    Constructor<?> constructor = runnableClass.getDeclaredConstructor(Executor.class,
        com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor.class, Collection.class, Collection.class, Long.class, Long.class, boolean.class);
    constructor.setAccessible(true);
    Object runnable = constructor.newInstance(executor, null, null, null, null, 1000L, false);
    Method wait = runnableClass.getDeclaredMethod("waitForIntraBrokerReplicaTasksToFinish");
    wait.setAccessible(true);
    try {
      assertEquals(List.of(active), wait.invoke(runnable));
      assertEquals(recover ? ExecutionTaskState.COMPLETED : ExecutionTaskState.DEAD, active.state());
      if (recover) {
        Field stop = Executor.class.getDeclaredField("_stopSignal");
        stop.setAccessible(true);
        assertEquals(0, ((java.util.concurrent.atomic.AtomicInteger) stop.get(executor)).get());
      }
      Field retained = runnableClass.getDeclaredField("_intraBrokerCopiesWithUnknownState");
      retained.setAccessible(true);
      assertEquals(retainCopy ? Set.of(active) : Set.of(), retained.get(runnable));
      // Only positively settled copies may be handed to the throttle helper for cleanup.
      IntraBrokerReplicationThrottleHelper helper = EasyMock.strictMock(IntraBrokerReplicationThrottleHelper.class);
      helper.clearThrottles(retainCopy ? List.of() : List.of(active), List.of());
      EasyMock.expectLastCall();
      EasyMock.replay(helper);
      Method clear = runnableClass.getDeclaredMethod("clearSettledIntraBrokerThrottles",
          IntraBrokerReplicationThrottleHelper.class, List.class, List.class);
      clear.setAccessible(true);
      clear.invoke(runnable, helper, List.of(active), List.of());
      EasyMock.verify(helper);
    } finally {
      executor.shutdown();
    }
    EasyMock.verify(admin, metadata, manager);
  }

  @Test
  public void testUnknownCopyRetainsThrottleAfterBoundedStopWait() throws Exception {
    assertCopyRetainsThrottleAfterBoundedStopWait(new java.util.concurrent.TimeoutException("transient logdir timeout"));
  }

  @Test
  public void testUnavailableCopyRetainsThrottleAfterBoundedStopWait() throws Exception {
    assertCopyRetainsThrottleAfterBoundedStopWait(new org.apache.kafka.common.errors.ReplicaNotAvailableException("unavailable"));
  }

  private void assertCopyRetainsThrottleAfterBoundedStopWait(Exception queryFailure) throws Exception {
    ExecutionTask active = task(0);
    active.inProgress(Time.SYSTEM.milliseconds());
    ExecutionTask rejected = task(1);
    Set<ExecutionTask> running = new HashSet<>(Set.of(active));
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    MetadataAdminClient metadata = EasyMock.niceMock(MetadataAdminClient.class);
    ExecutionTaskManager manager = EasyMock.niceMock(ExecutionTaskManager.class);
    EasyMock.expect(manager.numRemainingIntraBrokerPartitionMovements()).andReturn(1);
    EasyMock.expect(manager.getIntraBrokerReplicaMovementTasks()).andReturn(List.of(rejected));
    manager.markTasksInProgress(List.of(rejected));
    EasyMock.expectLastCall().andAnswer(() -> {
      rejected.inProgress(Time.SYSTEM.milliseconds());
      running.add(rejected);
      return null;
    });
    manager.markTaskDead(rejected);
    EasyMock.expectLastCall().andAnswer(() -> {
      rejected.kill(Time.SYSTEM.milliseconds());
      running.remove(rejected);
      return null;
    });
    manager.markTaskDead(active);
    EasyMock.expectLastCall().andAnswer(() -> {
      active.kill(Time.SYSTEM.milliseconds());
      running.remove(active);
      return null;
    });
    EasyMock.expect(manager.inExecutionTasks()).andAnswer(() -> new HashSet<>(running)).anyTimes();
    EasyMock.expect(manager.inExecutionTasks(Set.of(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION)))
        .andAnswer(() -> new HashSet<>(running)).anyTimes();
    ExecutionTaskTracker tracker = new ExecutionTaskTracker(new MetricRegistry(), Time.SYSTEM);
    EasyMock.expect(manager.getExecutionTasksSummary(EasyMock.anyObject())).andReturn(tracker.getExecutionTasksSummary(Set.of())).anyTimes();
    EasyMock.expect(manager.getExecutionConcurrencyManager())
        .andReturn(new com.linkedin.kafka.cruisecontrol.executor.concurrency.ExecutionConcurrencyManager(config)).anyTimes();
    Node node = new Node(1, "localhost", 9092);
    Cluster cluster = new Cluster("test", List.of(node),
        List.of(new PartitionInfo("topic", 0, node, new Node[]{node}, new Node[]{node})), Set.of(), Set.of());
    EasyMock.expect(metadata.cluster()).andReturn(cluster).anyTimes();
    Config empty = new Config(List.of());
    Config throttled = new Config(List.of(new ConfigEntry(THROTTLE, "1000")));
    configs(admin, empty);
    alterConfigs(admin, active, false);
    configs(admin, throttled);
    DescribeLogDirsResult usage = EasyMock.mock(DescribeLogDirsResult.class);
    EasyMock.expect(admin.describeLogDirs(Set.of(1))).andReturn(usage);
    EasyMock.expect(usage.descriptions()).andReturn(Map.of(1, KafkaFuture.completedFuture(Map.of(
        TARGET, new LogDirDescription(null, Map.of(new TopicPartition("other", 0), new ReplicaInfo(790L * 1024 * 1024, 0, false))),
        "/source", new LogDirDescription(null, Map.of())))));
    EasyMock.replay(usage);
    // Repeated transient failures exhaust the stop-wait budget without confirming copy completion.
    for (int i = 0; i < 6; i++) {
      logdirs(admin, queryFailure);
    }
    // A later execution must positively observe idle copies before restoring the throttle.
    for (int attempt = 0; attempt < 2; attempt++) {
      ListPartitionReassignmentsResult reassignments = EasyMock.mock(ListPartitionReassignmentsResult.class);
      EasyMock.expect(admin.listPartitionReassignments()).andReturn(reassignments);
      EasyMock.expect(reassignments.reassignments()).andReturn(KafkaFuture.completedFuture(Map.of()));
      EasyMock.replay(reassignments);
      DescribeClusterResult description = EasyMock.mock(DescribeClusterResult.class);
      EasyMock.expect(admin.describeCluster()).andReturn(description);
      EasyMock.expect(description.nodes()).andReturn(KafkaFuture.completedFuture(List.of(node)));
      EasyMock.replay(description);
      DescribeLogDirsResult idle = EasyMock.mock(DescribeLogDirsResult.class);
      EasyMock.expect(admin.describeLogDirs(Set.of(1))).andReturn(idle);
      EasyMock.expect(idle.descriptions()).andReturn(Map.of(1, KafkaFuture.completedFuture(Map.of(
          TARGET, new LogDirDescription(null, Map.of())))));
      EasyMock.replay(idle);
      DescribeReplicaLogDirsResult completed = EasyMock.mock(DescribeReplicaLogDirsResult.class);
      TopicPartitionReplica retainedReplica = new TopicPartitionReplica("topic", 0, 1);
      EasyMock.expect(admin.describeReplicaLogDirs(Set.of(retainedReplica))).andReturn(completed);
      ReplicaLogDirInfo completedInfo = EasyMock.mock(ReplicaLogDirInfo.class);
      EasyMock.expect(completedInfo.getCurrentReplicaLogDir()).andReturn(TARGET).times(2);
      EasyMock.expect(completedInfo.getFutureReplicaLogDir()).andReturn(null);
      EasyMock.replay(completedInfo);
      EasyMock.expect(completed.values()).andReturn(attempt == 0 ? Map.of()
          : Map.of(retainedReplica, KafkaFuture.completedFuture(completedInfo)));
      EasyMock.replay(completed);
      if (attempt == 0) {
        DescribeTopicsResult topics = EasyMock.mock(DescribeTopicsResult.class);
        EasyMock.expect(admin.describeTopics(Set.of("topic"))).andReturn(topics);
        TopicDescription assigned = new TopicDescription("topic", false,
            List.of(new TopicPartitionInfo(0, node, List.of(node), List.of(node))));
        EasyMock.expect(topics.topicNameValues()).andReturn(Map.of("topic", KafkaFuture.completedFuture(assigned)));
        EasyMock.replay(topics);
      }
    }
    configs(admin, throttled);
    alterConfigs(admin, active, true);
    configs(admin, empty);
    admin.close();
    EasyMock.expectLastCall();
    EasyMock.replay(admin, metadata, manager);
    Executor executor = new Executor(config, Time.SYSTEM, new MetricRegistry(), admin, metadata,
        EasyMock.niceMock(ExecutorNotifier.class), EasyMock.niceMock(AnomalyDetectorManager.class));
    setField(executor, "_executionTaskManager", manager);
    setField(executor, "_executionProgressCheckIntervalMs", 1L);
    setField(executor, "_reasonSupplier", (java.util.function.Supplier<String>) () -> "test");
    Class<?> runnableClass = Class.forName(Executor.class.getName() + "$ProposalExecutionRunnable");
    Constructor<?> constructor = runnableClass.getDeclaredConstructor(Executor.class,
        com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor.class, Collection.class, Collection.class, Long.class, Long.class, boolean.class);
    constructor.setAccessible(true);
    Object runnable = constructor.newInstance(executor, null, null, null, null, 1000L, false);
    Method move = runnableClass.getDeclaredMethod("intraBrokerMoveReplicas");
    move.setAccessible(true);
    try {
      move.invoke(runnable);
      assertEquals(ExecutionTaskState.DEAD, active.state());
      Field retained = Executor.class.getDeclaredField("_unresolvedIntraBrokerCopies");
      retained.setAccessible(true);
      assertEquals(Set.of(active), retained.get(executor));
      Field pendingCleanup = Executor.class.getDeclaredField("_pendingIntraBrokerThrottleCleanup");
      pendingCleanup.setAccessible(true);
      assertTrue(pendingCleanup.get(executor) != null);
      assertEquals(ExecutionTaskState.DEAD, rejected.state());
      assertTrue(running.isEmpty());
      Field failure = runnableClass.getDeclaredField("_executionException");
      failure.setAccessible(true);
      assertEquals(ExecutorAdminUtils.DiskCapacityValidationException.class, failure.get(runnable).getClass());
      Method sanityCheck = Executor.class.getDeclaredMethod("sanityCheckOngoingMovement");
      sanityCheck.setAccessible(true);
      try {
        sanityCheck.invoke(executor);
        fail("Missing copy placement must block cleanup");
      } catch (InvocationTargetException e) {
        assertEquals(com.linkedin.kafka.cruisecontrol.exception.OngoingExecutionException.class, e.getCause().getClass());
      }
      assertEquals(Set.of(active), retained.get(executor));
      assertTrue(pendingCleanup.get(executor) != null);
      sanityCheck.invoke(executor);
      assertEquals(Set.of(), retained.get(executor));
      assertEquals(null, pendingCleanup.get(executor));
    } finally {
      executor.shutdown();
    }
    EasyMock.verify(admin, manager, metadata);
  }
}
