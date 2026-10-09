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

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExecutorSubmissionAcknowledgementTest {
  @Test
  public void testInterruptStopsBatchAcknowledgementWaitsAndPreservesTasks() throws Exception {
    ReplicaPlacementInfo source = new ReplicaPlacementInfo(1, "/source");
    ExecutionTask first = new ExecutionTask(1, new ExecutionProposal(new TopicPartition("topic", 0), 20, source,
        List.of(source), List.of(new ReplicaPlacementInfo(1, "/target"))),
        1, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, 1000000);
    ExecutionTask second = new ExecutionTask(2, new ExecutionProposal(new TopicPartition("topic", 1), 20, source,
        List.of(source), List.of(new ReplicaPlacementInfo(1, "/target"))),
        1, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, 1000000);
    first.inProgress(Time.SYSTEM.milliseconds());
    second.inProgress(Time.SYSTEM.milliseconds());
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    AlterReplicaLogDirsResult result = EasyMock.mock(AlterReplicaLogDirsResult.class);
    KafkaFuture<Void> interrupted = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(interrupted.get(EasyMock.anyLong(), EasyMock.eq(TimeUnit.MILLISECONDS)))
        .andThrow(new InterruptedException("executor interrupted"));
    EasyMock.expect(admin.alterReplicaLogDirs(Map.of(new TopicPartitionReplica("topic", 0, 1), "/target",
        new TopicPartitionReplica("topic", 1, 1), "/target"))).andReturn(result);
    // Both entries share a future so either iteration order must stop after the first wait.
    EasyMock.expect(result.values()).andReturn(Map.of(new TopicPartitionReplica("topic", 0, 1), interrupted,
        new TopicPartitionReplica("topic", 1, 1), interrupted));
    ExecutionTaskManager manager = EasyMock.strictMock(ExecutionTaskManager.class);
    EasyMock.replay(admin, result, interrupted, manager);
    try {
      ExecutorAdminUtils.executeIntraBrokerReplicaMovements(List.of(first, second), admin, manager,
          new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));
      assertTrue(Thread.currentThread().isInterrupted());
      assertEquals(ExecutionTaskState.IN_PROGRESS, first.state());
      assertEquals(ExecutionTaskState.IN_PROGRESS, second.state());
      EasyMock.verify(admin, result, interrupted, manager);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void testTimeoutRemainsInProgress() {
    assertSubmissionState(new TimeoutException("acknowledgement lost"), false, ExecutionTaskState.IN_PROGRESS);
  }

  @Test
  public void testTransientKafkaErrorRemainsInProgress() {
    assertSubmissionState(new NetworkException("connection lost"), false, ExecutionTaskState.IN_PROGRESS);
  }

  @Test
  public void testUnavailableReplicaAcknowledgementRemainsInProgress() {
    org.apache.kafka.common.errors.ReplicaNotAvailableException unavailable =
        new org.apache.kafka.common.errors.ReplicaNotAvailableException("replica metadata temporarily unavailable");
    assertTrue(org.apache.kafka.common.errors.RetriableException.class.isAssignableFrom(unavailable.getClass()));
    assertSubmissionState(unavailable, false, ExecutionTaskState.IN_PROGRESS);
  }

  @Test
  public void testMissingAcknowledgementRemainsInProgress() {
    assertSubmissionState(null, true, ExecutionTaskState.IN_PROGRESS);
  }

  @Test
  public void testStorageRejectionMarksTaskDead() {
    assertSubmissionState(new KafkaStorageException("failed disk"), false, ExecutionTaskState.DEAD);
  }

  @Test
  public void testAuthorizationRejectionMarksTaskDead() {
    assertSubmissionState(new ClusterAuthorizationException("not authorized"), false, ExecutionTaskState.DEAD);
  }

  private void assertSubmissionState(Exception failure, boolean missing, ExecutionTaskState expected) {
    ReplicaPlacementInfo source = new ReplicaPlacementInfo(1, "/source");
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition("topic", 0), 20, source,
        List.of(source), List.of(new ReplicaPlacementInfo(1, "/target")));
    ExecutionTask task = new ExecutionTask(1, proposal, 1, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, 1000000);
    task.inProgress(Time.SYSTEM.milliseconds());
    AdminClient admin = EasyMock.mock(AdminClient.class);
    AlterReplicaLogDirsResult result = EasyMock.mock(AlterReplicaLogDirsResult.class);
    KafkaFutureImpl<Void> acknowledgement = new KafkaFutureImpl<>();
    if (failure != null) {
      acknowledgement.completeExceptionally(failure);
    }
    EasyMock.expect(admin.alterReplicaLogDirs(Map.of(new TopicPartitionReplica("topic", 0, 1), "/target"))).andReturn(result);
    EasyMock.expect(result.values()).andReturn(missing ? Map.of() : Map.of(new TopicPartitionReplica("topic", 0, 1), acknowledgement));
    ExecutionTaskManager manager = EasyMock.mock(ExecutionTaskManager.class);
    if (expected == ExecutionTaskState.DEAD) {
      manager.markTaskAborting(task);
      EasyMock.expectLastCall().andAnswer(() -> {
        task.abort();
        return null;
      });
      manager.markTaskDead(task);
      EasyMock.expectLastCall().andAnswer(() -> {
        task.kill(Time.SYSTEM.milliseconds());
        return null;
      });
    }
    EasyMock.replay(admin, result, manager);
    ExecutorAdminUtils.executeIntraBrokerReplicaMovements(List.of(task), admin, manager,
        new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));
    assertEquals(expected, task.state());
    EasyMock.verify(admin, result, manager);
  }
}
