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

import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCContainerizedKraftCluster;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaClientsIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/** Exercises disk capacity rejection and pre-creation directory selection against real Kafka brokers. */
public class InterBrokerDiskPlacementIntegrationTest extends CCKafkaClientsIntegrationTestHarness {
  private static final String FIRST_DIR = "/tmp/disk-aware-logs-1";
  private static final String SECOND_DIR = "/tmp/disk-aware-logs-2";
  private static final String TOPIC = "disk-aware-placement";
  private CCContainerizedKraftCluster _cluster;

  @Override
  public int clusterSize() {
    return 3;
  }

  @Override
  protected Map<Object, Object> overridingProps() {
    return Map.of("log.dirs", FIRST_DIR + "," + SECOND_DIR, "metadata.log.dir", "/tmp/disk-aware-metadata",
                  "listener.security.protocol.map", "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT");
  }

  @Override
  @Before
  public void setUp() {
    _cluster = new CCContainerizedKraftCluster(clusterSize(), buildBrokerConfigs(), new Properties());
    _cluster.start();
    _bootstrapUrl = _cluster.getExternalBootstrapAddress();
  }

  @Override
  @After
  public void tearDown() {
    if (_cluster != null) {
      _cluster.stop();
    }
  }

  @Test
  public void testRejectsFullDiskThenCreatesReplicaOnSelectedDirectory() throws Exception {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, _bootstrapUrl))) {
      admin.createTopics(List.of(new NewTopic(TOPIC, Map.of(0, List.of(0))))).all().get(30, TimeUnit.SECONDS);
      _cluster.waitForTopicMetadata(List.of(TOPIC), Duration.ofSeconds(30), Duration.ofSeconds(60),
          d -> d.partitions().get(0).leader() != null);
      try (Producer<String, String> producer = createProducer(new Properties())) {
        producer.send(new ProducerRecord<>(TOPIC, "key", "value".repeat(10000))).get(30, TimeUnit.SECONDS);
      }
      TopicPartitionReplica sourceKey = new TopicPartitionReplica(TOPIC, 0, 0);
      ReplicaLogDirInfo source = admin.describeReplicaLogDirs(List.of(sourceKey)).values().get(sourceKey).get(30, TimeUnit.SECONDS);
      assertNotNull(source.getCurrentReplicaLogDir());
      ReplicaPlacementInfo original = new ReplicaPlacementInfo(0, source.getCurrentReplicaLogDir());

      ExecutionTask rejected = task(tp, original, FIRST_DIR, 0.001);
      assertThrows(IllegalStateException.class,
                   () -> ExecutionUtils.submitReplicaReassignmentTasks(admin, List.of(rejected), Set.of(), 0.8, 30000));
      assertTrue(admin.listPartitionReassignments(Set.of(tp)).reassignments().get(30, TimeUnit.SECONDS).isEmpty());
      assertEquals(0, admin.describeTopics(List.of(TOPIC)).topicNameValues().get(TOPIC).get(30, TimeUnit.SECONDS)
                           .partitions().get(0).replicas().get(0).id());

      // The added replica does not exist yet. Kafka must record its directory preference before reassignment creates it.
      ExecutionTask accepted = task(tp, original, SECOND_DIR, 1_000_000_000.0);
      ExecutionUtils.submitReplicaReassignmentTasks(admin, List.of(accepted), Set.of(), 0.8, 30000)
                    .all().get(30, TimeUnit.SECONDS);
      _cluster.waitForTopicMetadata(List.of(TOPIC), Duration.ofSeconds(30), Duration.ofSeconds(90), d -> d.partitions().get(0).replicas().size() == 1
          && d.partitions().get(0).replicas().get(0).id() == 1 && d.partitions().get(0).isr().size() == 1);
      TopicPartitionReplica destinationKey = new TopicPartitionReplica(TOPIC, 0, 1);
      ReplicaLogDirInfo destination = admin.describeReplicaLogDirs(List.of(destinationKey)).values().get(destinationKey)
                                          .get(30, TimeUnit.SECONDS);
      assertEquals(SECOND_DIR, destination.getCurrentReplicaLogDir());
      assertTrue(InterBrokerDiskCapacityValidator.placementCompleted(admin, accepted.proposal(), 30000));
    }
  }

  private ExecutionTask task(TopicPartition tp, ReplicaPlacementInfo original, String destination, double capacity) {
    ExecutionProposal proposal = new ExecutionProposal(tp, 1, original, List.of(original),
        List.of(new ReplicaPlacementInfo(1, destination)), Map.of(1, capacity));
    ExecutionTask task = new ExecutionTask(0, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, 1000);
    task.inProgress(0);
    return task;
  }

  @Test
  public void testIntraBrokerMoveRejectsInsufficientCapacityThenMovesToSelectedDirectory() throws Exception {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    TopicPartitionReplica replicaKey = new TopicPartitionReplica(TOPIC, 0, 0);
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, _bootstrapUrl))) {
      admin.createTopics(List.of(new NewTopic(TOPIC, Map.of(0, List.of(0))))).all().get(30, TimeUnit.SECONDS);
      _cluster.waitForTopicMetadata(List.of(TOPIC), Duration.ofSeconds(30), Duration.ofSeconds(60),
          d -> d.partitions().get(0).leader() != null);
      try (Producer<String, String> producer = createProducer(new Properties())) {
        producer.send(new ProducerRecord<>(TOPIC, "key", "value".repeat(10000))).get(30, TimeUnit.SECONDS);
      }
      String sourceDir = admin.describeReplicaLogDirs(List.of(replicaKey)).values().get(replicaKey)
          .get(30, TimeUnit.SECONDS).getCurrentReplicaLogDir();
      String target = FIRST_DIR.equals(sourceDir) ? SECOND_DIR : FIRST_DIR;
      ReplicaPlacementInfo source = new ReplicaPlacementInfo(0, sourceDir);
      ReplicaPlacementInfo destination = new ReplicaPlacementInfo(0, target);
      ExecutionTaskManager manager = org.easymock.EasyMock.strictMock(ExecutionTaskManager.class);
      org.easymock.EasyMock.replay(manager);
      com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig config =
          new com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig(
              com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
      ExecutionProposal rejectedProposal = new ExecutionProposal(tp, 1, source, List.of(source), List.of(destination), Map.of(0, 0.001));
      ExecutionTask rejected = new ExecutionTask(0, rejectedProposal, 0, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, 1000);
      rejected.inProgress(0);
      assertThrows(IllegalStateException.class,
                   () -> ExecutorAdminUtils.executeIntraBrokerReplicaMovements(List.of(rejected), Set.of(), admin, manager, config));
      assertEquals(sourceDir, admin.describeReplicaLogDirs(List.of(replicaKey)).values().get(replicaKey)
          .get(30, TimeUnit.SECONDS).getCurrentReplicaLogDir());

      ExecutionProposal acceptedProposal = new ExecutionProposal(tp, 1, source, List.of(source), List.of(destination),
                                                                 Map.of(0, 1_000_000_000.0));
      ExecutionTask accepted = new ExecutionTask(1, acceptedProposal, 0, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, 1000);
      accepted.inProgress(0);
      ExecutorAdminUtils.executeIntraBrokerReplicaMovements(List.of(accepted), Set.of(), admin, manager, config);
      com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.waitUntilTrue(() -> {
        try {
          ReplicaLogDirInfo info = admin.describeReplicaLogDirs(List.of(replicaKey)).values().get(replicaKey)
              .get(30, TimeUnit.SECONDS);
          return target.equals(info.getCurrentReplicaLogDir()) && info.getFutureReplicaLogDir() == null;
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }, "Replica did not reach the selected directory", 90000, 100);
      assertTrue(InterBrokerDiskCapacityValidator.placementCompleted(admin, acceptedProposal, 30000));
      org.easymock.EasyMock.verify(manager);
    }
  }

  @Test
  public void testBatchedThrottleCleanupRestoresOperatorValue() throws Exception {
    String key = IntraBrokerReplicationThrottleHelper.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG;
    ConfigResource first = new ConfigResource(ConfigResource.Type.BROKER, "0");
    ConfigResource second = new ConfigResource(ConfigResource.Type.BROKER, "1");
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, _bootstrapUrl))) {
      List<AlterConfigOp> original = List.of(new AlterConfigOp(new ConfigEntry(key, "500000"), AlterConfigOp.OpType.SET));
      admin.incrementalAlterConfigs(Map.of(first, original)).all().get(30, TimeUnit.SECONDS);
      ThrottleConfigUtils.waitForConfigs(admin, Map.of(first, original), 3, 30000,
                                        IntraBrokerReplicationThrottleHelper::configsEqual, true);
      List<ExecutionTask> tasks = new java.util.ArrayList<>();
      for (int broker : List.of(0, 1)) {
        ReplicaPlacementInfo source = new ReplicaPlacementInfo(broker, FIRST_DIR);
        ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC, broker), 1, source, List.of(source),
            List.of(new ReplicaPlacementInfo(broker, SECOND_DIR)));
        tasks.add(new ExecutionTask(broker, proposal, broker, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, 1000));
      }
      IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(admin, 1_000_000L, 3);
      helper.setThrottles(tasks);
      Map<ConfigResource, Config> throttled = admin.describeConfigs(List.of(first, second)).all().get(30, TimeUnit.SECONDS);
      assertEquals("1000000", throttled.get(first).get(key).value());
      assertEquals("1000000", throttled.get(second).get(key).value());
      helper.clearAllThrottles();
      Map<ConfigResource, Config> restored = admin.describeConfigs(List.of(first, second)).all().get(30, TimeUnit.SECONDS);
      assertEquals("500000", restored.get(first).get(key).value());
      assertEquals(ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG, restored.get(first).get(key).source());
      ConfigEntry cleared = restored.get(second).get(key);
      assertTrue(cleared == null || cleared.source() != ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);
    }
  }
}
