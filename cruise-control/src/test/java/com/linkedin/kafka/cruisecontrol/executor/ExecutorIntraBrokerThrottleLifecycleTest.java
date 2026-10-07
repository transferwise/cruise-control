/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class ExecutorIntraBrokerThrottleLifecycleTest {
  private static final long THROTTLE = 1000000L;
  private static final String THROTTLE_CONFIG =
      IntraBrokerReplicationThrottleHelper.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG;

  @Test
  public void testUnknownLogdirStateRemainsInProgressWithoutStop() throws Exception {
    Executor executor = EasyMock.createNiceMock(Executor.class);
    ExecutionTaskManager manager = EasyMock.createMock(ExecutionTaskManager.class);
    setField(executor, "_executionTaskManager", manager);
    setField(executor, "_stopSignal", new AtomicInteger(0));
    EasyMock.replay(executor, manager);
    Object runnable = runnable(executor);
    ExecutionTask task = task(0);
    task.inProgress(0);

    assertFalse(markDead(runnable, task, Collections.emptySet()));
    assertEquals(ExecutionTaskState.IN_PROGRESS, task.state());
    EasyMock.verify(manager);
  }

  @Test
  public void testFailedProgressQueryDoesNotResubmitUnknownCopy() throws Exception {
    Executor executor = EasyMock.createNiceMock(Executor.class);
    ExecutionTaskManager manager = EasyMock.createMock(ExecutionTaskManager.class);
    AdminClient admin = EasyMock.createMock(AdminClient.class);
    KafkaCruiseControlConfig config = EasyMock.createNiceMock(KafkaCruiseControlConfig.class);
    EasyMock.expect(config.getLong(EasyMock.anyString())).andReturn(1000L).anyTimes();
    ExecutionTask active = task(0);
    active.inProgress(0);
    EasyMock.expect(manager.inExecutionTasks(Collections.singleton(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION)))
        .andReturn(Collections.singleton(active));
    DescribeReplicaLogDirsResult result = EasyMock.createMock(DescribeReplicaLogDirsResult.class);
    KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo> failedQuery = new KafkaFutureImpl<>();
    failedQuery.completeExceptionally(new org.apache.kafka.common.errors.TimeoutException("transient query failure"));
    EasyMock.expect(result.values()).andReturn(Collections.singletonMap(new TopicPartitionReplica("test-topic", 0, 0), failedQuery));
    EasyMock.expect(admin.describeReplicaLogDirs(EasyMock.anyObject())).andReturn(result);
    setField(executor, "_executionTaskManager", manager);
    setField(executor, "_adminClient", admin);
    setField(executor, "_config", config);
    setField(executor, "_stopSignal", new AtomicInteger(0));
    EasyMock.replay(executor, manager, admin, config, result);
    Object runnable = runnable(executor);
    Method retry = runnable.getClass().getDeclaredMethod("maybeReexecuteIntraBrokerReplicaTasks");
    retry.setAccessible(true);

    retry.invoke(runnable);
    assertEquals(ExecutionTaskState.IN_PROGRESS, active.state());
    EasyMock.verify(manager, admin, result);
  }

  @Test
  public void testConfirmedDiskFailureStillMarksTaskDead() throws Exception {
    Executor executor = EasyMock.createNiceMock(Executor.class);
    ExecutionTaskManager manager = EasyMock.createMock(ExecutionTaskManager.class);
    ExecutionTask task = task(0);
    task.inProgress(0);
    manager.markTaskDead(task);
    EasyMock.expectLastCall().andAnswer(() -> {
      task.kill(1);
      return null;
    });
    setField(executor, "_executionTaskManager", manager);
    setField(executor, "_stopSignal", new AtomicInteger(0));
    EasyMock.replay(executor, manager);

    assertTrue(markDead(runnable(executor), task, Collections.singleton(task)));
    assertEquals(ExecutionTaskState.DEAD, task.state());
    EasyMock.verify(manager);
  }

  @Test
  public void testBatchFailureAttemptsDrainAndRetainsThrottleIfStateQueryFails() throws Exception {
    Executor executor = EasyMock.createNiceMock(Executor.class);
    ExecutionTaskManager manager = EasyMock.createNiceMock(ExecutionTaskManager.class);
    AdminClient admin = EasyMock.createStrictMock(AdminClient.class);
    ExecutionTask active = task(0);
    active.inProgress(0);
    Set<ExecutionTask> activeTasks = Collections.singleton(active);
    EasyMock.expect(executor.inExecutionTasks()).andReturn(activeTasks).anyTimes();
    EasyMock.expect(manager.inExecutionTasks(EasyMock.anyObject())).andReturn(activeTasks).anyTimes();
    EasyMock.expect(manager.numRemainingIntraBrokerPartitionMovements()).andReturn(2);
    EasyMock.expect(manager.getIntraBrokerReplicaMovementTasks()).andReturn(Arrays.asList(task(0), task(1)));

    // An earlier broker is already throttled; configuring a later broker fails.
    ConfigResource broker0 = new ConfigResource(ConfigResource.Type.BROKER, "0");
    DescribeConfigsResult configs = EasyMock.createMock(DescribeConfigsResult.class);
    EasyMock.expect(configs.all()).andReturn(KafkaFuture.completedFuture(Collections.singletonMap(broker0,
        new Config(Collections.singletonList(new ConfigEntry(THROTTLE_CONFIG, String.valueOf(THROTTLE)))))));
    EasyMock.expect(admin.describeConfigs(Collections.singletonList(broker0))).andReturn(configs);
    IllegalStateException batchFailure = new IllegalStateException("later batch config failed");
    EasyMock.expect(admin.describeConfigs(Collections.singletonList(new ConfigResource(ConfigResource.Type.BROKER, "1"))))
        .andThrow(batchFailure);
    IllegalStateException queryFailure = new IllegalStateException("state query failed");
    EasyMock.expect(admin.describeReplicaLogDirs(EasyMock.anyObject())).andThrow(queryFailure);
    // No config restoration is allowed while the active copy's state is unknown.
    setField(executor, "_executionTaskManager", manager);
    setField(executor, "_adminClient", admin);
    setField(executor, "_stopSignal", new AtomicInteger(0));
    EasyMock.replay(executor, manager, admin, configs);
    Object runnable = runnable(executor);
    Method move = runnable.getClass().getDeclaredMethod("intraBrokerMoveReplicas");
    move.setAccessible(true);

    InvocationTargetException thrown = assertThrows(InvocationTargetException.class, () -> move.invoke(runnable));
    assertSame(batchFailure, thrown.getCause());
    Field failure = runnable.getClass().getDeclaredField("_executionException");
    failure.setAccessible(true);
    assertSame(queryFailure, failure.get(runnable));
    assertEquals(ExecutionTaskState.IN_PROGRESS, active.state());
    EasyMock.verify(manager, admin, configs);
  }

  private static Object runnable(Executor executor) throws Exception {
    Class<?> type = Class.forName(Executor.class.getName() + "$ProposalExecutionRunnable");
    Constructor<?> constructor = type.getDeclaredConstructor(Executor.class, LoadMonitor.class, Collection.class,
        Collection.class, Long.class, Long.class, boolean.class);
    constructor.setAccessible(true);
    return constructor.newInstance(executor, null, null, null, null, THROTTLE, false);
  }

  private static boolean markDead(Object runnable, ExecutionTask task, Set<ExecutionTask> failures) throws Exception {
    Method method = runnable.getClass().getDeclaredMethod("maybeMarkTaskAsDead", Cluster.class,
        java.util.Map.class, ExecutionTask.class, Set.class, Set.class);
    method.setAccessible(true);
    return (boolean) method.invoke(runnable, null, Collections.emptyMap(), task, null, failures);
  }

  private static void setField(Executor executor, String name, Object value) throws Exception {
    Field field = Executor.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(executor, value);
  }

  private static ExecutionTask task(int broker) {
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition("test-topic", 0), 100,
        new ReplicaPlacementInfo(broker, "/old"), Collections.singletonList(new ReplicaPlacementInfo(broker, "/old")),
        Collections.singletonList(new ReplicaPlacementInfo(broker, "/new")));
    return new ExecutionTask(broker, proposal, broker, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, 1000);
  }
}
