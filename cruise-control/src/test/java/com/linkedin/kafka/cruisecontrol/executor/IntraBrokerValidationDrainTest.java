/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorManager;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
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
public class IntraBrokerValidationDrainTest {
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
      assertEquals(clearing ? ExecutionTaskState.COMPLETED : ExecutionTaskState.IN_PROGRESS, active.state());
      return result;
    });
    EasyMock.expect(result.values()).andReturn(Map.of(BROKER, KafkaFuture.completedFuture(null)));
    EasyMock.replay(result);
  }

  private void logdirs(AdminClient admin, boolean complete) {
    DescribeReplicaLogDirsResult result = EasyMock.mock(DescribeReplicaLogDirsResult.class);
    TopicPartitionReplica replica = new TopicPartitionReplica("topic", 0, 1);
    EasyMock.expect(admin.describeReplicaLogDirs(Set.of(replica))).andReturn(result);
    ReplicaLogDirInfo info = EasyMock.mock(ReplicaLogDirInfo.class);
    EasyMock.expect(info.getCurrentReplicaLogDir()).andReturn(complete ? TARGET : "/source").anyTimes();
    EasyMock.expect(info.getFutureReplicaLogDir()).andReturn(complete ? null : TARGET).anyTimes();
    EasyMock.replay(info);
    EasyMock.expect(result.values()).andReturn(Map.of(replica, KafkaFuture.completedFuture(info)));
    EasyMock.replay(result);
  }

  @Test
  public void testRejectedNewBatchDrainsPreviouslySubmittedCopyBeforeClearingThrottle() throws Exception {
    ExecutionTask active = task(0);
    active.inProgress(Time.SYSTEM.milliseconds());
    ExecutionTask rejected = task(1);
    Set<ExecutionTask> running = new HashSet<>(Set.of(active));
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    MetadataClient metadata = EasyMock.niceMock(MetadataClient.class);
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
    manager.markTaskDone(active);
    EasyMock.expectLastCall().andAnswer(() -> {
      active.completed(Time.SYSTEM.milliseconds());
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
    EasyMock.expect(metadata.refreshMetadata()).andReturn(new MetadataClient.ClusterAndGeneration(cluster, 0)).anyTimes();
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
    // The first poll observes an active future copy; the second confirms completion.
    logdirs(admin, false);
    logdirs(admin, false);
    logdirs(admin, false);
    logdirs(admin, true);
    configs(admin, throttled);
    alterConfigs(admin, active, true);
    configs(admin, empty);
    admin.close();
    EasyMock.expectLastCall();
    EasyMock.replay(admin, metadata, manager);
    Executor executor = new Executor(config, Time.SYSTEM, new MetricRegistry(), metadata,
        EasyMock.niceMock(ExecutorNotifier.class), EasyMock.niceMock(AnomalyDetectorManager.class));
    Field adminField = Executor.class.getDeclaredField("_adminClient");
    adminField.setAccessible(true);
    ((AdminClient) adminField.get(executor)).close();
    setField(executor, "_adminClient", admin);
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
      assertEquals(ExecutionTaskState.COMPLETED, active.state());
      assertEquals(ExecutionTaskState.DEAD, rejected.state());
      assertTrue(running.isEmpty());
      Field failure = runnableClass.getDeclaredField("_executionException");
      failure.setAccessible(true);
      assertEquals(ExecutorAdminUtils.DiskCapacityValidationException.class, failure.get(runnable).getClass());
    } finally {
      executor.shutdown();
    }
    EasyMock.verify(admin, manager, metadata);
  }
}
