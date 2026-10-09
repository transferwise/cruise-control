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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.*;

public class InterBrokerRetryReservationTest {
  private static ExecutionTask task(int id, int size) {
    ReplicaPlacementInfo source = new ReplicaPlacementInfo(0, "/source");
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition("topic", id), size, source,
        List.of(source), List.of(new ReplicaPlacementInfo(1, "/target")), Map.of(1, 1000.0));
    ExecutionTask task = new ExecutionTask(id, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, 1000000);
    task.inProgress(Time.SYSTEM.milliseconds());
    return task;
  }

  @Test
  public void testRetryRejectsCapacityReservedByCopyAwaitingPlacementConfirmation() throws Exception {
    assertRetryPreservesReservations(false, false, false);
  }

  @Test
  public void testRetryWithHeadroomSubmitsOnlyIncompleteBrokerReassignment() throws Exception {
    assertRetryPreservesReservations(true, false, false);
  }

  @Test
  public void testPartitionMissingDuringRetryFilteringWaitsForNextProgressPoll() throws Exception {
    assertRetryPreservesReservations(true, true, false);
  }

  @Test
  public void testMissingOtherPartitionDoesNotAbortHealthyRetry() throws Exception {
    assertRetryPreservesReservations(true, false, true);
  }

  private void assertRetryPreservesReservations(boolean sufficientCapacity, boolean missingRetry, boolean missingPending) throws Exception {
    ExecutionTask pendingPlacement = task(0, 150);
    ExecutionTask retry = task(1, 50);
    AtomicInteger submissions = new AtomicInteger();
    AdminClient admin = EasyMock.mock(AdminClient.class);
    ListPartitionReassignmentsResult ongoing = EasyMock.mock(ListPartitionReassignmentsResult.class);
    EasyMock.expect(ongoing.reassignments()).andReturn(KafkaFuture.completedFuture(Map.of())).anyTimes();
    EasyMock.replay(ongoing);
    EasyMock.expect(admin.listPartitionReassignments()).andReturn(ongoing).anyTimes();
    // Broker membership is complete for partition 0, but its disk copy still needs 130 MB.
    // The retry needs 50 MB. With 650 MB already used, their total exceeds the 800 MB limit.
    long otherUsageMb = sufficientCapacity ? 400 : 630;
    Map<Integer, Map<String, LogDirDescription>> directories = Map.of(
        0, Map.of("/source", new LogDirDescription(null, Map.of(
            pendingPlacement.proposal().topicPartition(), new ReplicaInfo(150L * 1024 * 1024, 0, false),
            retry.proposal().topicPartition(), new ReplicaInfo(50L * 1024 * 1024, 0, false)))),
        1, Map.of("/target", new LogDirDescription(null, Map.of(
            pendingPlacement.proposal().topicPartition(), new ReplicaInfo(20L * 1024 * 1024, 0, true),
            new TopicPartition("other", 0), new ReplicaInfo(otherUsageMb * 1024 * 1024, 0, false)))));
    DescribeLogDirsResult logs = EasyMock.mock(DescribeLogDirsResult.class);
    EasyMock.expect(logs.descriptions()).andReturn(Map.of(0, KafkaFuture.completedFuture(directories.get(0)),
                                                        1, KafkaFuture.completedFuture(directories.get(1)))).anyTimes();
    EasyMock.replay(logs);
    EasyMock.expect(admin.describeLogDirs(Set.of(0, 1))).andReturn(logs).anyTimes();
    AlterReplicaLogDirsResult placement = EasyMock.mock(AlterReplicaLogDirsResult.class);
    EasyMock.expect(placement.values()).andReturn(Map.of(new TopicPartitionReplica("topic", 1, 1), KafkaFuture.completedFuture(null)))
        .anyTimes();
    EasyMock.replay(placement);
    EasyMock.expect(admin.alterReplicaLogDirs(Map.of(new TopicPartitionReplica("topic", 1, 1), "/target")))
        .andReturn(placement).anyTimes();
    EasyMock.expect(admin.alterPartitionReassignments(EasyMock.anyObject())).andAnswer(() -> {
      Map<TopicPartition, ?> changes = EasyMock.getCurrentArgument(0);
      assertEquals(Set.of(retry.proposal().topicPartition()), changes.keySet());
      submissions.incrementAndGet();
      AlterPartitionReassignmentsResult result = EasyMock.mock(AlterPartitionReassignmentsResult.class);
      EasyMock.expect(result.values()).andReturn(Map.of(retry.proposal().topicPartition(), KafkaFuture.completedFuture(null))).anyTimes();
      EasyMock.replay(result);
      return result;
    }).anyTimes();
    admin.close();
    EasyMock.expectLastCall();
    Node source = new Node(0, "localhost", 9092);
    Node destination = new Node(1, "localhost", 9093);
    List<PartitionInfo> partitions = new ArrayList<>();
    if (!missingPending) {
      partitions.add(new PartitionInfo("topic", 0, destination, new Node[]{destination}, new Node[]{destination}));
    }
    if (!missingRetry) {
      partitions.add(new PartitionInfo("topic", 1, source, new Node[]{source}, new Node[]{source}));
    }
    Cluster cluster = new Cluster("test", List.of(source, destination), partitions, Set.of(), Set.of());
    MetadataAdminClient metadata = EasyMock.niceMock(MetadataAdminClient.class);
    Cluster nextCluster = new Cluster("test", List.of(source, destination), List.of(
        new PartitionInfo("topic", 0, destination, new Node[]{destination}, new Node[]{destination}),
        new PartitionInfo("topic", 1, source, new Node[]{source}, new Node[]{source})), Set.of(), Set.of());
    EasyMock.expect(metadata.cluster()).andReturn(cluster).andReturn(nextCluster).anyTimes();
    ExecutionTaskManager manager = EasyMock.mock(ExecutionTaskManager.class);
    Set<ExecutionTask> remainingTasks = missingRetry ? Set.of(pendingPlacement)
        : missingPending ? Set.of(retry) : Set.of(pendingPlacement, retry);
    EasyMock.expect(manager.inExecutionTasks(Set.of(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION)))
        .andReturn(Set.of(pendingPlacement, retry)).andReturn(remainingTasks).anyTimes();
    if (!sufficientCapacity) {
      manager.setStopRequested();
      EasyMock.expectLastCall();
    }
    EasyMock.replay(admin, metadata, manager);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    Executor executor = new Executor(config, Time.SYSTEM, new MetricRegistry(), admin, metadata,
        EasyMock.niceMock(ExecutorNotifier.class), EasyMock.niceMock(AnomalyDetectorManager.class));
    Field managerField = Executor.class.getDeclaredField("_executionTaskManager");
    managerField.setAccessible(true);
    managerField.set(executor, manager);
    Class<?> runnableClass = Class.forName(Executor.class.getName() + "$ProposalExecutionRunnable");
    Constructor<?> constructor = runnableClass.getDeclaredConstructor(Executor.class, LoadMonitor.class, Collection.class,
        Collection.class, Long.class, Long.class, boolean.class);
    constructor.setAccessible(true);
    Object runnable = constructor.newInstance(executor, EasyMock.niceMock(LoadMonitor.class), null, null, null, null, false);
    Method method = runnableClass.getDeclaredMethod("maybeReexecuteInterBrokerReplicaTasks", Set.class, Set.class);
    method.setAccessible(true);
    try {
      Set<TopicPartition> deleted = new HashSet<>();
      Set<TopicPartition> dead = new HashSet<>();
      method.invoke(runnable, deleted, dead);
      assertTrue(deleted.isEmpty());
      assertTrue(dead.isEmpty());
      assertEquals(sufficientCapacity && !missingRetry && !missingPending ? 1 : 0, submissions.get());
      Field failure = runnableClass.getDeclaredField("_executionException");
      failure.setAccessible(true);
      if (sufficientCapacity) {
        assertNull(failure.get(runnable));
      } else {
        assertTrue(failure.get(runnable) instanceof InterBrokerDiskCapacityValidator.ValidationException);
        assertTrue(((Throwable) failure.get(runnable)).getMessage().contains("disk capacity threshold"));
      }
      assertEquals(ExecutionTaskState.IN_PROGRESS, pendingPlacement.state());
      assertEquals(ExecutionTaskState.IN_PROGRESS, retry.state());
      if (missingRetry || missingPending) {
        // Simulate the next progress poll settling the missing task and removing it from active tracking.
        ExecutionTask missingTask = missingRetry ? retry : pendingPlacement;
        missingTask.abort();
        missingTask.aborted(Time.SYSTEM.milliseconds());
        method.invoke(runnable, deleted, dead);
        assertEquals(missingRetry ? 0 : 1, submissions.get());
        assertNull(failure.get(runnable));
        assertTrue(deleted.isEmpty());
        assertTrue(dead.isEmpty());
      }
    } finally {
      executor.shutdown();
    }
    EasyMock.verify(admin, metadata, manager);
  }
}
