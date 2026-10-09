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
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorManager;
import com.linkedin.kafka.cruisecontrol.exception.OngoingExecutionException;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.LogDirNotFoundException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class RetainedIntraBrokerCopyCleanupTest {
  @Test
  public void testConfirmedTopicDeletionAllowsCleanup() throws Exception {
    assertCleanup(new UnknownTopicOrPartitionException("deleted"), false, null);
  }

  @Test
  public void testReplicaRemovalConfirmationUsesAdminTimeoutAndAllowsCleanup() throws Exception {
    assertCleanup(null, false, null);
  }

  @Test
  public void testMissingCachedTopicDoesNotProveDeletion() throws Exception {
    assertCleanup(null, true, OngoingExecutionException.class);
  }

  @Test
  public void testTopicQueryTimeoutRetainsCopyRecords() throws Exception {
    assertCleanup(new TimeoutException("topic metadata unavailable"), false, IllegalStateException.class);
  }

  @Test
  public void testPermanentStorageFailureAllowsCleanupWhileReplicaStillAssigned() throws Exception {
    assertCleanup(null, true, null, new KafkaStorageException("disk permanently failed"), false);
  }

  @Test
  public void testMissingDirectoryAllowsCleanupWhileReplicaStillAssigned() throws Exception {
    assertCleanup(null, true, null, new LogDirNotFoundException("directory removed"), false);
  }

  @Test
  public void testTransientPlacementTimeoutRetainsCopyRecords() throws Exception {
    assertCleanup(null, true, OngoingExecutionException.class, new TimeoutException("placement unknown"), false);
  }

  @Test
  public void testActiveCopyStillBlocksCleanupDespiteFailedDisk() throws Exception {
    assertCleanup(null, true, OngoingExecutionException.class, new KafkaStorageException("disk permanently failed"), true);
  }

  private void assertCleanup(Exception topicError, boolean assignedToRetainedBroker, Class<?> expectedFailure) throws Exception {
    assertCleanup(topicError, assignedToRetainedBroker, expectedFailure,
        new ReplicaNotAvailableException("replica unavailable"), false);
  }

  private void assertCleanup(Exception topicError, boolean assignedToRetainedBroker, Class<?> expectedFailure,
                             Exception placementError, boolean activeCopy) throws Exception {
    Properties properties = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    properties.setProperty(ExecutorConfig.ADMIN_CLIENT_REQUEST_TIMEOUT_MS_CONFIG, "12345");
    properties.setProperty(ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG, "100");
    boolean failedDisk = placementError instanceof KafkaStorageException || placementError instanceof LogDirNotFoundException;
    ReplicaPlacementInfo source = new ReplicaPlacementInfo(1, "/source");
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition("topic", 0), 20, source,
        List.of(source), List.of(new ReplicaPlacementInfo(1, "/target")));
    ExecutionTask task = new ExecutionTask(1, proposal, 1, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, 1000000);
    task.inProgress(Time.SYSTEM.milliseconds());
    task.kill(Time.SYSTEM.milliseconds());
    AdminClient admin = EasyMock.niceMock(AdminClient.class);
    ListPartitionReassignmentsResult reassignments = EasyMock.mock(ListPartitionReassignmentsResult.class);
    EasyMock.expect(admin.listPartitionReassignments()).andReturn(reassignments);
    EasyMock.expect(reassignments.reassignments()).andReturn(KafkaFuture.completedFuture(Map.of()));
    DescribeClusterResult cluster = EasyMock.mock(DescribeClusterResult.class);
    Node retainedBroker = new Node(1, "localhost", 9092);
    EasyMock.expect(admin.describeCluster()).andReturn(cluster);
    EasyMock.expect(cluster.nodes()).andReturn(KafkaFuture.completedFuture(List.of(retainedBroker)));
    DescribeLogDirsResult directories = EasyMock.mock(DescribeLogDirsResult.class);
    EasyMock.expect(admin.describeLogDirs(Set.of(1))).andReturn(directories);
    Map<TopicPartition, ReplicaInfo> liveCopies = activeCopy
        ? Map.of(new TopicPartition("other-topic", 0), new ReplicaInfo(1, 0, true)) : Map.of();
    EasyMock.expect(directories.descriptions()).andReturn(Map.of(1,
        KafkaFuture.completedFuture(Map.of("/source", new LogDirDescription(failedDisk ? (ApiException) placementError : null, Map.of()),
            "/target", new LogDirDescription(null, liveCopies)))));
    DescribeReplicaLogDirsResult placements = EasyMock.mock(DescribeReplicaLogDirsResult.class);
    KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo> missing = new KafkaFutureImpl<>();
    missing.completeExceptionally(placementError);
    if (!activeCopy) {
      EasyMock.expect(admin.describeReplicaLogDirs(Set.of(new TopicPartitionReplica("topic", 0, 1)))).andReturn(placements);
      EasyMock.expect(placements.values()).andReturn(Map.of(new TopicPartitionReplica("topic", 0, 1), missing));
    }
    DescribeTopicsResult topics = EasyMock.mock(DescribeTopicsResult.class);
    KafkaFutureImpl<TopicDescription> description = new KafkaFutureImpl<>() {
      @Override
      public TopicDescription get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        assertEquals("Topic metadata confirmation must use the AdminClient request budget", 12345L, timeout);
        assertEquals(TimeUnit.MILLISECONDS, unit);
        return super.get(timeout, unit);
      }
    };
    if (topicError != null) {
      description.completeExceptionally(topicError);
    } else {
      Node broker = assignedToRetainedBroker ? retainedBroker : new Node(2, "localhost", 9093);
      description.complete(new TopicDescription("topic", false,
          List.of(new TopicPartitionInfo(0, broker, List.of(broker), List.of(broker)))));
    }
    if (!activeCopy && !failedDisk) {
      EasyMock.expect(admin.describeTopics(Set.of("topic"))).andReturn(topics);
      EasyMock.expect(topics.topicNameValues()).andReturn(Map.of("topic", description));
    }
    MetadataAdminClient metadata = EasyMock.niceMock(MetadataAdminClient.class);
    EasyMock.expect(metadata.cluster()).andReturn(Cluster.empty()).anyTimes();
    EasyMock.replay(admin, reassignments, cluster, directories, placements, topics, metadata);
    Executor executor = new Executor(new KafkaCruiseControlConfig(properties),
        Time.SYSTEM, new MetricRegistry(), admin, metadata,
        EasyMock.niceMock(ExecutorNotifier.class), EasyMock.niceMock(AnomalyDetectorManager.class));
    Field helper = Executor.class.getDeclaredField("_pendingIntraBrokerThrottleCleanup");
    helper.setAccessible(true);
    IntraBrokerReplicationThrottleHelper cleanup = EasyMock.mock(IntraBrokerReplicationThrottleHelper.class);
    if (expectedFailure == null) {
      cleanup.clearAllThrottles();
      EasyMock.expectLastCall().once();
    }
    EasyMock.replay(cleanup);
    helper.set(executor, cleanup);
    Field retained = Executor.class.getDeclaredField("_unresolvedIntraBrokerCopies");
    retained.setAccessible(true);
    retained.set(executor, Set.of(task));
    Method check = Executor.class.getDeclaredMethod("sanityCheckOngoingMovement");
    check.setAccessible(true);
    try {
      if (expectedFailure == null) {
        check.invoke(executor);
        assertNull(helper.get(executor));
        assertEquals(Set.of(), retained.get(executor));
      } else {
        try {
          check.invoke(executor);
          fail("Unknown or still-assigned replicas must block cleanup");
        } catch (InvocationTargetException e) {
          assertEquals(expectedFailure, e.getCause().getClass());
        }
        assertNotNull(helper.get(executor));
        assertEquals(Set.of(task), retained.get(executor));
      }
    } finally {
      executor.shutdown();
    }
    EasyMock.verify(admin, reassignments, cluster, directories, placements, topics, metadata, cleanup);
  }
}
