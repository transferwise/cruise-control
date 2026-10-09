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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RedirectedIntraBrokerCopyTest {
  @Test
  public void testStopKeepsRedirectedCopyTracked() throws Exception {
    verifyCopy("/source", "/other", true, ExecutionTaskState.IN_PROGRESS, false);
  }

  @Test
  public void testTargetWithAnotherFutureCopyIsNotComplete() throws Exception {
    verifyCopy("/target", "/other", true, ExecutionTaskState.IN_PROGRESS, false);
  }

  @Test
  public void testNormalExecutionDoesNotOverwriteActiveRedirectedCopy() throws Exception {
    verifyCopy("/source", "/other", false, ExecutionTaskState.IN_PROGRESS, false);
  }

  @Test
  public void testStopMarksSettledNonTargetCopyDead() throws Exception {
    verifyCopy("/other", null, true, ExecutionTaskState.DEAD, false);
  }

  @Test
  public void testTargetWithoutFutureCopyCanComplete() throws Exception {
    verifyCopy("/target", null, true, ExecutionTaskState.IN_PROGRESS, true);
  }

  @Test
  public void testPersistentRedirectStopsNormalExecutionWithoutClearingThrottle() throws Exception {
    verifyCopy("/source", "/other", false, ExecutionTaskState.DEAD, false, List.of("/other", "/other", "/other"));
  }

  @Test
  public void testPersistentRedirectBoundsStopWait() throws Exception {
    verifyCopy("/source", "/other", true, ExecutionTaskState.DEAD, false, List.of("/other", "/other", "/other"));
  }

  @Test
  public void testCurrentTargetDoesNotHidePersistentRedirect() throws Exception {
    verifyCopy("/target", "/other", false, ExecutionTaskState.DEAD, false, List.of("/other", "/other", "/other"));
  }

  @Test
  public void testChangingWrongFutureDirectoriesDoesNotResetMismatchBudget() throws Exception {
    verifyCopy("/source", "/other", false, ExecutionTaskState.DEAD, false, List.of("/other", "/another", "/other"));
  }

  @Test
  public void testExpectedFutureDirectoryResetsMismatchBudget() throws Exception {
    verifyCopy("/source", "/other", false, ExecutionTaskState.IN_PROGRESS, false,
        List.of("/other", "/other", "/target", "/other", "/other"));
  }

  @Test
  public void testSettledCopyResetsMismatchBudget() throws Exception {
    verifyCopy("/source", "/other", false, ExecutionTaskState.IN_PROGRESS, false,
        Arrays.asList("/other", "/other", null, "/other", "/other"));
  }

  @Test
  public void testExpectedActiveCopyHasNoMismatchDeadline() throws Exception {
    verifyCopy("/source", "/target", false, ExecutionTaskState.IN_PROGRESS, false,
        List.of("/target", "/target", "/target", "/target"));
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static void verifyCopy(String current, String future, boolean stopping,
                                 ExecutionTaskState expectedState, boolean complete) throws Exception {
    verifyCopy(current, future, stopping, expectedState, complete, List.of());
  }

  private static void verifyCopy(String current, String future, boolean stopping,
                                 ExecutionTaskState expectedState, boolean complete, List<String> observations) throws Exception {
    ReplicaPlacementInfo source = new ReplicaPlacementInfo(1, "/source");
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition("topic", 0), 20, source,
        List.of(source), List.of(new ReplicaPlacementInfo(1, "/target")));
    ExecutionTask task = new ExecutionTask(0, proposal, 1, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, 1000000);
    task.inProgress(Time.SYSTEM.milliseconds());
    Set<ExecutionTask> running = new HashSet<>(Set.of(task));
    TopicPartitionReplica replica = new TopicPartitionReplica("topic", 0, 1);
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    DescribeReplicaLogDirsResult result = EasyMock.mock(DescribeReplicaLogDirsResult.class);
    ReplicaLogDirInfo info = EasyMock.mock(ReplicaLogDirInfo.class);
    EasyMock.expect(info.getCurrentReplicaLogDir()).andReturn(current).anyTimes();
    AtomicReference<String> observedFuture = new AtomicReference<>(future);
    EasyMock.expect(info.getFutureReplicaLogDir()).andAnswer(observedFuture::get).anyTimes();
    EasyMock.expect(admin.describeReplicaLogDirs(Set.of(replica))).andReturn(result);
    EasyMock.expect(result.values()).andReturn(Map.of(replica, KafkaFuture.completedFuture(info)));
    admin.close();
    EasyMock.expectLastCall();
    ExecutionTaskManager manager = EasyMock.mock(ExecutionTaskManager.class);
    EasyMock.expect(manager.inExecutionTasks(Set.of(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION)))
        .andAnswer(() -> new HashSet<>(running));
    if (expectedState == ExecutionTaskState.DEAD) {
      if (!stopping && !observations.isEmpty()) {
        manager.setStopRequested();
        EasyMock.expectLastCall();
      }
      manager.markTaskDead(task);
      EasyMock.expectLastCall().andAnswer(() -> {
        task.kill(Time.SYSTEM.milliseconds());
        running.remove(task);
        return null;
      });
    }
    EasyMock.replay(admin, result, info, manager);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    Executor executor = new Executor(config, Time.SYSTEM, new MetricRegistry(), admin,
        EasyMock.niceMock(MetadataAdminClient.class), EasyMock.niceMock(ExecutorNotifier.class),
        EasyMock.niceMock(AnomalyDetectorManager.class));
    try {
      setField(executor, "_executionTaskManager", manager);
      AtomicInteger stopSignal = new AtomicInteger(stopping ? 1 : 0);
      setField(executor, "_stopSignal", stopSignal);
      Class<?> runnableClass = Class.forName(Executor.class.getName() + "$ProposalExecutionRunnable");
      Constructor<?> constructor = runnableClass.getDeclaredConstructor(Executor.class, LoadMonitor.class,
          Collection.class, Collection.class, Long.class, Long.class, boolean.class);
      constructor.setAccessible(true);
      Object runnable = constructor.newInstance(executor, null, null, null, null, 1000L, false);
      Method retry = runnableClass.getDeclaredMethod("maybeReexecuteIntraBrokerReplicaTasks");
      retry.setAccessible(true);
      retry.invoke(runnable);
      if (!observations.isEmpty()) {
        Method check = runnableClass.getDeclaredMethod("maybeMarkTaskAsDead", Cluster.class, Map.class,
            ExecutionTask.class, Set.class, Set.class);
        check.setAccessible(true);
        for (int i = 0; i < observations.size(); i++) {
          observedFuture.set(observations.get(i));
          boolean dead = (boolean) check.invoke(runnable, null, Map.of(task, info), task, null, Set.of());
          assertEquals("Only the final poll can exhaust the mismatch budget",
              expectedState == ExecutionTaskState.DEAD && i == observations.size() - 1, dead);
        }
        observedFuture.set(future);
        Field unresolvedField = runnableClass.getDeclaredField("_intraBrokerCopiesWithUnknownState");
        unresolvedField.setAccessible(true);
        assertEquals(expectedState == ExecutionTaskState.DEAD, ((Set<?>) unresolvedField.get(runnable)).contains(task));
        if (expectedState == ExecutionTaskState.DEAD) {
          assertTrue(stopSignal.get() != 0);
          Field failureField = runnableClass.getDeclaredField("_executionException");
          failureField.setAccessible(true);
          assertTrue(((Throwable) failureField.get(runnable)).getMessage().contains("instead of /target"));
          IntraBrokerReplicationThrottleHelper throttleHelper = EasyMock.strictMock(IntraBrokerReplicationThrottleHelper.class);
          throttleHelper.clearThrottles(List.of(), List.of());
          EasyMock.expectLastCall();
          EasyMock.replay(throttleHelper);
          Method cleanup = runnableClass.getDeclaredMethod("clearSettledIntraBrokerThrottles",
              IntraBrokerReplicationThrottleHelper.class, List.class, List.class);
          cleanup.setAccessible(true);
          cleanup.invoke(runnable, throttleHelper, List.of(task), List.of());
          EasyMock.verify(throttleHelper);
        }
      }
      assertEquals(expectedState, task.state());
      assertEquals(expectedState == ExecutionTaskState.IN_PROGRESS, running.contains(task));
      assertEquals(complete, ExecutionUtils.isIntraBrokerReplicaActionDone(Map.of(task, info), task));
      if (future != null) {
        assertFalse(ExecutionUtils.isIntraBrokerReplicaActionDone(Map.of(task, info), task));
        if (observations.isEmpty()) {
          assertTrue("Active redirected copy must prevent throttle cleanup", running.contains(task));
        }
      }
    } finally {
      executor.shutdown();
    }
    EasyMock.verify(admin, result, info, manager);
  }
}
