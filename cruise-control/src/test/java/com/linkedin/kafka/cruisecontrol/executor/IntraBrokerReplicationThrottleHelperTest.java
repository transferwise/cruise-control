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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.easymock.EasyMock;
import org.junit.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutorTestUtils.EXECUTION_ALERTING_THRESHOLD_MS;
import static com.linkedin.kafka.cruisecontrol.executor.IntraBrokerReplicationThrottleHelper.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class IntraBrokerReplicationThrottleHelperTest {

  private static final Config EMPTY_CONFIG = new Config(Collections.emptyList());

  @Test
  public void testDoesNotDeleteUntrackedOperatorThrottle() throws Exception {
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    Config operator = new Config(Collections.singletonList(mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
                                                                         "500", ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG)));
    expectDescribeBrokerConfigs(admin, 0, operator);
    EasyMock.replay(admin);
    ExecutionTask completed = createIntraBrokerTask(0, 0);
    completed.inProgress(0);
    completed.completed(1);
    new IntraBrokerReplicationThrottleHelper(admin, 100L, 3).clearThrottles(Collections.singletonList(completed), Collections.emptyList());
    EasyMock.verify(admin);
  }

  @Test
  public void testIsNoOpWhenThrottleIsNull() throws Exception {
    AdminClient mockAdminClient = EasyMock.strictMock(AdminClient.class);
    EasyMock.replay(mockAdminClient);

    // If throttle is null, no admin client interactions should happen
    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, null);
    ExecutionTask task = createIntraBrokerTask(0, 1);
    task.inProgress(0);
    task.completed(1);

    helper.setThrottles(Collections.singletonList(task));
    helper.clearThrottles(Collections.singletonList(task), Collections.emptyList());
    helper.clearAllThrottles();
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testSetThrottles() throws Exception {
    final long throttleRate = 1000000L;
    final int brokerId0 = 0;
    final int brokerId1 = 1;

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);

    expectDescribeBrokerConfigs(mockAdminClient, Map.of(brokerId0, EMPTY_CONFIG, brokerId1, EMPTY_CONFIG));
    expectIncrementalAlterBrokerConfigs(mockAdminClient, Set.of(brokerId0, brokerId1));
    Config configAfterSet = new Config(Collections.singletonList(
        new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, String.valueOf(throttleRate))));
    expectDescribeBrokerConfigs(mockAdminClient, Map.of(brokerId0, configAfterSet, brokerId1, configAfterSet));

    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, throttleRate, 3);

    ExecutionTask task0 = createIntraBrokerTask(0, brokerId0);
    ExecutionTask task1 = createIntraBrokerTask(1, brokerId1);

    helper.setThrottles(Arrays.asList(task0, task1));
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testSetThrottleSkipsWhenAlreadySet() throws Exception {
    final long throttleRate = 1000000L;
    final int brokerId = 0;

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);

    // Broker already has the correct throttle rate set
    Config existingConfig = new Config(Collections.singletonList(
        new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, String.valueOf(throttleRate))));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId, existingConfig);
    // No incrementalAlterConfigs expected since throttle is already set

    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, throttleRate, 3);

    ExecutionTask task = createIntraBrokerTask(0, brokerId);
    helper.setThrottles(Collections.singletonList(task));
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testClearThrottlesForCompletedTasks() throws Exception {
    final long throttleRate = 1000000L;
    final int brokerId0 = 0;
    final int brokerId1 = 1;

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);

    expectDescribeBrokerConfigs(mockAdminClient, brokerId0, EMPTY_CONFIG);
    expectIncrementalAlterBrokerConfigs(mockAdminClient, brokerId0);
    expectDescribeBrokerConfigs(mockAdminClient, brokerId0, new Config(Collections.singletonList(
        new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, String.valueOf(throttleRate)))));
    // Broker 0 has dynamic throttle config to be removed
    Config dynamicThrottleConfig = new Config(Collections.singletonList(
        mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
            String.valueOf(throttleRate), ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG)));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId0, dynamicThrottleConfig);
    expectIncrementalAlterBrokerConfigs(mockAdminClient, brokerId0);
    // waitForConfigs - return empty after delete
    expectDescribeBrokerConfigs(mockAdminClient, brokerId0, EMPTY_CONFIG);

    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, throttleRate, 3);

    // Task on broker 0 is completed, task on broker 1 is still in progress
    ExecutionTask completedTask = createIntraBrokerTask(0, brokerId0);
    helper.setThrottles(Collections.singletonList(completedTask));
    completedTask.inProgress(0);
    completedTask.completed(1);

    ExecutionTask inProgressTask = createIntraBrokerTask(1, brokerId1);
    inProgressTask.inProgress(0);

    helper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testClearThrottlesSkipsStaticConfig() throws Exception {
    final long throttleRate = 1000000L;
    final int brokerId = 0;

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);

    // Broker has static throttle config - should not be removed
    Config staticThrottleConfig = new Config(Collections.singletonList(
        mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
            String.valueOf(throttleRate), ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId, staticThrottleConfig);
    // No incrementalAlterConfigs expected since it's a static config

    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, throttleRate, 3);

    ExecutionTask completedTask = createIntraBrokerTask(0, brokerId);
    completedTask.inProgress(0);
    completedTask.completed(1);

    helper.clearThrottles(Collections.singletonList(completedTask), Collections.emptyList());
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testClearAllThrottles() throws Exception {
    final long throttleRate = 1000000L;
    final int brokerId0 = 0;
    final int brokerId1 = 1;

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);

    expectDescribeBrokerConfigs(mockAdminClient, Map.of(brokerId0, EMPTY_CONFIG, brokerId1, EMPTY_CONFIG));
    expectIncrementalAlterBrokerConfigs(mockAdminClient, Set.of(brokerId0, brokerId1));
    Config configAfterSet = new Config(Collections.singletonList(
        new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, String.valueOf(throttleRate))));
    expectDescribeBrokerConfigs(mockAdminClient, Map.of(brokerId0, configAfterSet, brokerId1, configAfterSet));

    Config dynamicConfig = new Config(Collections.singletonList(
        mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
            String.valueOf(throttleRate), ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG)));
    expectDescribeBrokerConfigs(mockAdminClient, Map.of(brokerId0, dynamicConfig, brokerId1, dynamicConfig));
    expectIncrementalAlterBrokerConfigs(mockAdminClient, Set.of(brokerId0, brokerId1));
    expectDescribeBrokerConfigs(mockAdminClient, Map.of(brokerId0, EMPTY_CONFIG, brokerId1, EMPTY_CONFIG));

    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, throttleRate, 3);

    ExecutionTask task0 = createIntraBrokerTask(0, brokerId0);
    ExecutionTask task1 = createIntraBrokerTask(1, brokerId1);

    helper.setThrottles(Arrays.asList(task0, task1));
    helper.clearAllThrottles();
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testDoNotRemoveThrottleForBrokerWithInProgressTask() throws Exception {
    final long throttleRate = 1000000L;
    final int brokerId = 0;

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);
    // No interactions expected since broker 0 has both completed and in-progress tasks
    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, throttleRate, 3);

    // Both tasks are on the same broker
    ExecutionTask completedTask = createIntraBrokerTask(0, brokerId);
    completedTask.inProgress(0);
    completedTask.completed(1);

    ExecutionTask inProgressTask = createIntraBrokerTask(1, brokerId);
    inProgressTask.inProgress(0);

    // Since broker 0 still has an in-progress task, throttle should not be removed
    helper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testWaitForConfigsThrowsOnTimeout() {
    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);
    int retries = 2;

    // Return empty config repeatedly (never matches expected), triggering timeout
    for (int i = 0; i <= retries; i++) {
      expectDescribeBrokerConfigs(mockAdminClient, 0, EMPTY_CONFIG);
    }

    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, 100L, retries);
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, "0");
    assertThrows(IllegalStateException.class, () -> helper.waitForConfigs(cf, Collections.singletonList(
        new AlterConfigOp(new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, "100"),
            AlterConfigOp.OpType.SET))));
  }

  @Test
  public void testConfigsEqual() {
    Map<String, String> expectedConfigs = new HashMap<>();

    assertTrue(IntraBrokerReplicationThrottleHelper.configsEqual(EMPTY_CONFIG, expectedConfigs));

    expectedConfigs.put(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, "1000000");
    assertFalse(IntraBrokerReplicationThrottleHelper.configsEqual(EMPTY_CONFIG, expectedConfigs));

    Config matchingConfig = new Config(Collections.singletonList(
        new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, "1000000")));
    assertTrue(IntraBrokerReplicationThrottleHelper.configsEqual(matchingConfig, expectedConfigs));

    Config mismatchConfig = new Config(Collections.singletonList(
        new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, "500000")));
    assertFalse(IntraBrokerReplicationThrottleHelper.configsEqual(mismatchConfig, expectedConfigs));

    // Null expected value with empty config value should be treated as equal
    Map<String, String> nullExpected = new HashMap<>();
    nullExpected.put(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, null);
    assertTrue(IntraBrokerReplicationThrottleHelper.configsEqual(EMPTY_CONFIG, nullExpected));

    // Static broker config with null expected should be treated as cleared
    ConfigEntry mockStaticEntry = mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
        "1000000", ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG);
    Config staticConfig = new Config(Collections.singletonList(mockStaticEntry));
    assertTrue(IntraBrokerReplicationThrottleHelper.configsEqual(staticConfig, nullExpected));
    EasyMock.verify(mockStaticEntry);

    // Dynamic default broker config with null expected should be treated as cleared (inherited value exposed after DELETE)
    ConfigEntry mockDefaultEntry = mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
        "500000", ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG);
    Config defaultConfig = new Config(Collections.singletonList(mockDefaultEntry));
    assertTrue(IntraBrokerReplicationThrottleHelper.configsEqual(defaultConfig, nullExpected));
    EasyMock.verify(mockDefaultEntry);

    // Dynamic broker-specific config with null expected should NOT be treated as cleared (override still exists)
    ConfigEntry mockDynamicBrokerEntry = mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
        "750000", ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);
    Config dynamicBrokerConfig = new Config(Collections.singletonList(mockDynamicBrokerEntry));
    assertFalse(IntraBrokerReplicationThrottleHelper.configsEqual(dynamicBrokerConfig, nullExpected));
    EasyMock.verify(mockDynamicBrokerEntry);
  }

  @Test
  public void testRestoresPreExistingThrottleOnCleanup() throws Exception {
    final long throttleRate = 2000000L;
    final String preExistingRate = "500000";
    final int brokerId = 0;

    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);

    // setThrottles: describeConfigs returns pre-existing dynamic broker config
    Config preExistingConfig = new Config(Collections.singletonList(
        mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
            preExistingRate, ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG)));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId, preExistingConfig);
    // Should overwrite with our throttle rate
    expectIncrementalAlterBrokerConfigs(mockAdminClient, brokerId);
    // waitForConfigs verification
    Config configAfterSet = new Config(Collections.singletonList(
        new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, String.valueOf(throttleRate))));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId, configAfterSet);

    // clearAllThrottles: describeConfigs returns our throttle (dynamic broker config)
    Config ourConfig = new Config(Collections.singletonList(
        mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
            String.valueOf(throttleRate), ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG)));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId, ourConfig);
    // Should restore the original value (SET, not DELETE)
    expectIncrementalAlterBrokerConfigs(mockAdminClient, brokerId);
    // waitForConfigs verification - returns the restored value
    Config restoredConfig = new Config(Collections.singletonList(
        new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, preExistingRate)));
    expectDescribeBrokerConfigs(mockAdminClient, brokerId, restoredConfig);

    EasyMock.replay(mockAdminClient);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(mockAdminClient, throttleRate, 3);

    ExecutionTask task = createIntraBrokerTask(0, brokerId);
    helper.setThrottles(Collections.singletonList(task));
    helper.clearAllThrottles();
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testBatchedCleanupContinuesAfterBrokerFailureAndRetainsFailedBrokerForRetry() throws Exception {
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    Config setConfig = new Config(Collections.singletonList(new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
                                                                           "100")));
    Config dynamicConfig = new Config(Collections.singletonList(mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
                                                                               "100", ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG)));
    expectDescribeBrokerConfigs(admin, Map.of(0, EMPTY_CONFIG, 1, EMPTY_CONFIG));
    expectIncrementalAlterBrokerConfigs(admin, Set.of(0, 1));
    expectDescribeBrokerConfigs(admin, Map.of(0, setConfig, 1, setConfig));

    ConfigResource failedBroker = new ConfigResource(ConfigResource.Type.BROKER, "0");
    ConfigResource healthyBroker = new ConfigResource(ConfigResource.Type.BROKER, "1");
    org.apache.kafka.common.internals.KafkaFutureImpl<Config> failed = new org.apache.kafka.common.internals.KafkaFutureImpl<>();
    failed.completeExceptionally(new org.apache.kafka.common.errors.TimeoutException("unavailable broker"));
    DescribeConfigsResult result = EasyMock.strictMock(DescribeConfigsResult.class);
    EasyMock.expect(result.values()).andReturn(Map.of(failedBroker, failed, healthyBroker, KafkaFuture.completedFuture(dynamicConfig)));
    EasyMock.expect(admin.describeConfigs(Arrays.asList(failedBroker, healthyBroker))).andReturn(result);
    EasyMock.replay(result);
    expectIncrementalAlterBrokerConfigs(admin, 1);
    expectDescribeBrokerConfigs(admin, 1, EMPTY_CONFIG);
    // The failed broker remains tracked, while the successfully cleaned broker needs no further request.
    expectDescribeBrokerConfigs(admin, 0, dynamicConfig);
    expectIncrementalAlterBrokerConfigs(admin, 0);
    expectDescribeBrokerConfigs(admin, 0, EMPTY_CONFIG);
    EasyMock.replay(admin);

    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(admin, 100L, 3);
    helper.setThrottles(Arrays.asList(createIntraBrokerTask(0, 0), createIntraBrokerTask(1, 1)));
    assertThrows(java.util.concurrent.ExecutionException.class, helper::clearAllThrottles);
    helper.clearAllThrottles();
    helper.clearAllThrottles();
    EasyMock.verify(admin, result);
  }

  @Test
  public void testClearCompletedTasksBatchesBothBrokers() throws Exception {
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    Config dynamicConfig = new Config(Collections.singletonList(mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
                                                                               "100", ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG)));
    expectDescribeBrokerConfigs(admin, Map.of(0, EMPTY_CONFIG, 1, EMPTY_CONFIG));
    expectIncrementalAlterBrokerConfigs(admin, Set.of(0, 1));
    expectDescribeBrokerConfigs(admin, Map.of(0, dynamicConfig, 1, dynamicConfig));
    expectDescribeBrokerConfigs(admin, Map.of(0, dynamicConfig, 1, dynamicConfig));
    expectIncrementalAlterBrokerConfigs(admin, Set.of(0, 1));
    expectDescribeBrokerConfigs(admin, Map.of(0, EMPTY_CONFIG, 1, EMPTY_CONFIG));
    EasyMock.replay(admin);
    ExecutionTask first = createIntraBrokerTask(0, 0);
    ExecutionTask second = createIntraBrokerTask(1, 1);
    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(admin, 100L, 3);
    helper.setThrottles(Arrays.asList(first, second));
    for (ExecutionTask task : Arrays.asList(first, second)) {
      task.inProgress(0);
      task.completed(1);
    }
    helper.clearThrottles(Arrays.asList(first, second), Collections.emptyList());
    EasyMock.verify(admin);
  }

  @Test
  public void testCleanupRetainsOnlyBrokerWhoseMutationFailed() throws Exception {
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    Config set = new Config(Collections.singletonList(new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, "100")));
    Config dynamic = new Config(Collections.singletonList(mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
                                                                         "100", ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG)));
    expectDescribeBrokerConfigs(admin, Map.of(0, EMPTY_CONFIG, 1, EMPTY_CONFIG));
    expectIncrementalAlterBrokerConfigs(admin, Set.of(0, 1));
    expectDescribeBrokerConfigs(admin, Map.of(0, set, 1, set));
    expectDescribeBrokerConfigs(admin, Map.of(0, dynamic, 1, dynamic));
    ConfigResource first = new ConfigResource(ConfigResource.Type.BROKER, "0");
    ConfigResource second = new ConfigResource(ConfigResource.Type.BROKER, "1");
    org.apache.kafka.common.internals.KafkaFutureImpl<Void> failed = new org.apache.kafka.common.internals.KafkaFutureImpl<>();
    failed.completeExceptionally(new org.apache.kafka.common.errors.TimeoutException("mutation failed"));
    AlterConfigsResult result = EasyMock.strictMock(AlterConfigsResult.class);
    EasyMock.expect(result.values()).andReturn(Map.of(first, failed, second, KafkaFuture.completedFuture(null)));
    EasyMock.replay(result);
    EasyMock.expect(admin.incrementalAlterConfigs(EasyMock.anyObject())).andReturn(result);
    expectDescribeBrokerConfigs(admin, 1, EMPTY_CONFIG);
    expectDescribeBrokerConfigs(admin, 0, dynamic);
    expectIncrementalAlterBrokerConfigs(admin, 0);
    expectDescribeBrokerConfigs(admin, 0, EMPTY_CONFIG);
    EasyMock.replay(admin);
    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(admin, 100L, 1);
    helper.setThrottles(Arrays.asList(createIntraBrokerTask(0, 0), createIntraBrokerTask(1, 1)));
    assertThrows(java.util.concurrent.ExecutionException.class, helper::clearAllThrottles);
    helper.clearAllThrottles();
    helper.clearAllThrottles();
    EasyMock.verify(admin, result);
  }

  @Test
  public void testCleanupRetainsOnlyBrokerWhosePropagationFailed() throws Exception {
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    Config set = new Config(Collections.singletonList(new ConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, "100")));
    Config dynamic = new Config(Collections.singletonList(mockConfigEntry(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG,
                                                                         "100", ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG)));
    expectDescribeBrokerConfigs(admin, Map.of(0, EMPTY_CONFIG, 1, EMPTY_CONFIG));
    expectIncrementalAlterBrokerConfigs(admin, Set.of(0, 1));
    expectDescribeBrokerConfigs(admin, Map.of(0, set, 1, set));
    expectDescribeBrokerConfigs(admin, Map.of(0, dynamic, 1, dynamic));
    expectIncrementalAlterBrokerConfigs(admin, Set.of(0, 1));
    expectDescribeBrokerConfigs(admin, Map.of(0, dynamic, 1, EMPTY_CONFIG));
    // Once broker 1 converges, subsequent polls and cleanup retries must query broker 0 only.
    expectDescribeBrokerConfigs(admin, 0, dynamic);
    expectDescribeBrokerConfigs(admin, 0, dynamic);
    expectIncrementalAlterBrokerConfigs(admin, 0);
    expectDescribeBrokerConfigs(admin, 0, EMPTY_CONFIG);
    EasyMock.replay(admin);
    IntraBrokerReplicationThrottleHelper helper = new IntraBrokerReplicationThrottleHelper(admin, 100L, 2);
    helper.setThrottles(Arrays.asList(createIntraBrokerTask(0, 0), createIntraBrokerTask(1, 1)));
    assertThrows(IllegalStateException.class, helper::clearAllThrottles);
    helper.clearAllThrottles();
    helper.clearAllThrottles();
    EasyMock.verify(admin);
  }

  // --- Helper methods ---

  private ExecutionTask createIntraBrokerTask(long executionId, int brokerId) {
    ExecutionProposal proposal = new ExecutionProposal(
        new TopicPartition("test-topic", 0),
        100,
        new com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo(brokerId),
        Collections.singletonList(new com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo(brokerId)),
        Collections.singletonList(new com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo(brokerId)));
    return new ExecutionTask(executionId, proposal, brokerId, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION,
        EXECUTION_ALERTING_THRESHOLD_MS);
  }

  private ConfigEntry mockConfigEntry(String name, String value, ConfigEntry.ConfigSource configSource) {
    ConfigEntry configEntry = EasyMock.mock(ConfigEntry.class);
    EasyMock.expect(configEntry.name()).andReturn(name).anyTimes();
    EasyMock.expect(configEntry.value()).andReturn(value).anyTimes();
    EasyMock.expect(configEntry.source()).andReturn(configSource).anyTimes();
    EasyMock.replay(configEntry);
    return configEntry;
  }

  private void expectDescribeBrokerConfigs(AdminClient adminClient, int brokerId, Config brokerConfig) {
    expectDescribeBrokerConfigs(adminClient, Collections.singletonMap(brokerId, brokerConfig));
  }

  private void expectDescribeBrokerConfigs(AdminClient adminClient, Map<Integer, Config> brokerConfigs) {
    Map<ConfigResource, Config> configs = new HashMap<>();
    brokerConfigs.forEach((broker, config) -> configs.put(new ConfigResource(ConfigResource.Type.BROKER, broker.toString()), config));
    DescribeConfigsResult result = EasyMock.mock(DescribeConfigsResult.class);
    Map<ConfigResource, KafkaFuture<Config>> futures = new HashMap<>();
    configs.forEach((resource, config) -> futures.put(resource, KafkaFuture.completedFuture(config)));
    EasyMock.expect(result.values()).andReturn(futures).anyTimes();
    EasyMock.expect(result.all()).andReturn(KafkaFuture.completedFuture(configs)).anyTimes();
    EasyMock.expect(adminClient.describeConfigs(brokerConfigs.keySet().stream().sorted()
        .map(id -> new ConfigResource(ConfigResource.Type.BROKER, id.toString())).collect(java.util.stream.Collectors.toList()))).andReturn(result);
    EasyMock.replay(result);
  }

  private void expectIncrementalAlterBrokerConfigs(AdminClient adminClient, int brokerId) {
    expectIncrementalAlterBrokerConfigs(adminClient, Collections.singleton(brokerId));
  }

  private void expectIncrementalAlterBrokerConfigs(AdminClient adminClient, Set<Integer> brokerIds) {
    AlterConfigsResult result = EasyMock.mock(AlterConfigsResult.class);
    Map<ConfigResource, KafkaFuture<Void>> futures = new HashMap<>();
    brokerIds.forEach(id -> futures.put(new ConfigResource(ConfigResource.Type.BROKER, id.toString()), KafkaFuture.completedFuture(null)));
    EasyMock.expect(result.values()).andReturn(futures);
    EasyMock.expect(adminClient.incrementalAlterConfigs(EasyMock.anyObject())).andAnswer(() -> {
      Map<ConfigResource, Collection<AlterConfigOp>> changes = EasyMock.getCurrentArgument(0);
      org.junit.Assert.assertEquals(brokerIds.stream().map(id -> new ConfigResource(ConfigResource.Type.BROKER, id.toString()))
          .collect(java.util.stream.Collectors.toSet()), changes.keySet());
      return result;
    });
    EasyMock.replay(result);
  }
}
