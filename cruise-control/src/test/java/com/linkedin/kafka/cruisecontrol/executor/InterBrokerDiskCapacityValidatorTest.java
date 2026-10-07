/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.easymock.EasyMock;
import org.easymock.Capture;
import org.junit.Test;

import static org.junit.Assert.*;

public class InterBrokerDiskCapacityValidatorTest {
  private static final String TARGET = "/target";
  private static final TopicPartition TP = new TopicPartition("topic", 0);
  private static final TopicPartition OTHER = new TopicPartition("other", 0);
  private static final TopicPartitionReplica DESTINATION = new TopicPartitionReplica("topic", 0, 1);
  private static final long MB = 1024 * 1024;

  private ExecutionTask task(int partition, long size) {
    TopicPartition tp = new TopicPartition("topic", partition);
    ReplicaPlacementInfo source = new ReplicaPlacementInfo(0, "/source");
    ExecutionProposal proposal = new ExecutionProposal(tp, size, source, List.of(source),
        List.of(new ReplicaPlacementInfo(1, TARGET)), Map.of(1, 1000.0));
    ExecutionTask task = new ExecutionTask(partition, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, 1000);
    task.inProgress(0);
    return task;
  }

  private Map<Integer, Map<String, LogDirDescription>> descriptions(long usage) {
    return Map.of(0, Map.of("/source", new LogDirDescription(null, Map.of(TP, replica(20)))),
                  1, Map.of(TARGET, new LogDirDescription(null, Map.of(OTHER, replica(usage)))));
  }

  private ReplicaInfo replica(long size) {
    return new ReplicaInfo(size * MB, 0, false);
  }

  private KafkaFuture<Void> failure(Throwable error) {
    KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
    future.completeExceptionally(error);
    return future;
  }

  private DescribeLogDirsResult expectDescriptions(AdminClient admin, Map<Integer, Map<String, LogDirDescription>> descriptions) {
    DescribeLogDirsResult result = EasyMock.mock(DescribeLogDirsResult.class);
    EasyMock.expect(admin.describeLogDirs(Set.of(0, 1))).andReturn(result);
    EasyMock.expect(result.descriptions()).andReturn(Map.of(0, KafkaFuture.completedFuture(descriptions.get(0)),
                                                          1, KafkaFuture.completedFuture(descriptions.get(1))));
    EasyMock.replay(result);
    return result;
  }

  @Test
  public void testMultiBrokerIntraProposalReservesFullReplicaSizeOnEachDisk() {
    ReplicaPlacementInfo first = new ReplicaPlacementInfo(0, "/source");
    ReplicaPlacementInfo second = new ReplicaPlacementInfo(1, "/source");
    ExecutionProposal proposal = new ExecutionProposal(TP, 40, first, List.of(first, second),
        List.of(new ReplicaPlacementInfo(0, TARGET), new ReplicaPlacementInfo(1, TARGET)), Map.of(0, 1000.0, 1, 1000.0));
    ExecutionTask task = new ExecutionTask(0, proposal, 1, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, 1000);
    task.inProgress(0);
    Map<Integer, Map<String, LogDirDescription>> dirs = Map.of(1, Map.of(
        "/source", new LogDirDescription(null, Map.of()),
        TARGET, new LogDirDescription(null, Map.of(OTHER, replica(770)))));
    assertThrows(IllegalStateException.class,
        () -> InterBrokerDiskCapacityValidator.validateReservations(List.of(task), dirs, Map.of(), 0.8));
  }

  @Test
  public void testSetsDiskPreferenceBeforeReassignmentAndAcceptsMissingReplicaAcknowledgement() {
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    DescribeLogDirsResult dirs = expectDescriptions(admin, descriptions(600));
    AlterReplicaLogDirsResult placement = EasyMock.mock(AlterReplicaLogDirsResult.class);
    EasyMock.expect(admin.alterReplicaLogDirs(Map.of(DESTINATION, TARGET))).andReturn(placement);
    EasyMock.expect(placement.values()).andReturn(Map.of(DESTINATION, failure(new ReplicaNotAvailableException("Replica not created yet"))));
    AlterPartitionReassignmentsResult reassignment = EasyMock.mock(AlterPartitionReassignmentsResult.class);
    Capture<Map<TopicPartition, java.util.Optional<NewPartitionReassignment>>> submitted = EasyMock.newCapture();
    EasyMock.expect(admin.alterPartitionReassignments(EasyMock.capture(submitted))).andReturn(reassignment);
    EasyMock.replay(admin, placement, reassignment);
    assertSame(reassignment, ExecutionUtils.submitReplicaReassignmentTasks(admin, List.of(task(0, 20)), Set.of(), 0.8, 1000));
    assertEquals(List.of(1), submitted.getValue().get(TP).get().targetReplicas());
    EasyMock.verify(admin, dirs, placement, reassignment);
  }

  @Test
  public void testPlacementFailurePreventsReassignment() {
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    DescribeLogDirsResult dirs = expectDescriptions(admin, descriptions(600));
    AlterReplicaLogDirsResult placement = EasyMock.mock(AlterReplicaLogDirsResult.class);
    EasyMock.expect(admin.alterReplicaLogDirs(Map.of(DESTINATION, TARGET))).andReturn(placement);
    EasyMock.expect(placement.values()).andReturn(Map.of(DESTINATION, failure(new KafkaStorageException())));
    EasyMock.replay(admin, placement);
    assertThrows(IllegalStateException.class,
                 () -> ExecutionUtils.submitReplicaReassignmentTasks(admin, List.of(task(0, 20)), Set.of(), 0.8, 1000));
    EasyMock.verify(admin, dirs, placement);
  }

  @Test
  public void testDiskBecomingFullPreventsPlacementAndReassignment() {
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    DescribeLogDirsResult dirs = expectDescriptions(admin, descriptions(790));
    EasyMock.replay(admin);
    assertThrows(IllegalStateException.class,
                 () -> ExecutionUtils.submitReplicaReassignmentTasks(admin, List.of(task(0, 20)), Set.of(), 0.8, 1000));
    EasyMock.verify(admin, dirs);
  }

  @Test
  public void testQueriesSourcesToCatchGrowthBeforeSubmitting() {
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    Map<Integer, Map<String, LogDirDescription>> dirs = Map.of(
        0, Map.of("/source", new LogDirDescription(null, Map.of(TP, replica(200)))),
        1, Map.of(TARGET, new LogDirDescription(null, Map.of(OTHER, replica(650)))));
    DescribeLogDirsResult result = expectDescriptions(admin, dirs);
    EasyMock.replay(admin);
    assertThrows(IllegalStateException.class,
                 () -> ExecutionUtils.submitReplicaReassignmentTasks(admin, List.of(task(0, 20)), Set.of(), 0.8, 1000));
    EasyMock.verify(admin, result);
  }

  @Test
  public void testReservesUncopiedPortionOfInFlightReplica() {
    ExecutionTask incoming = task(0, 20);
    ExecutionTask active = task(1, 40);
    Map<Integer, Map<String, LogDirDescription>> dirs = Map.of(1, Map.of(TARGET,
        new LogDirDescription(null, Map.of(OTHER, replica(740), active.proposal().topicPartition(), replica(10)))));
    assertThrows(IllegalStateException.class,
                 () -> InterBrokerDiskCapacityValidator.validateReservations(List.of(incoming, active), dirs, Map.of(), 0.8));
  }

  @Test
  public void testAccountsForReplicaGrowthSincePlanning() {
    assertThrows(IllegalStateException.class,
                 () -> InterBrokerDiskCapacityValidator.validateReservations(List.of(task(0, 20)), descriptions(650),
                                                                              Map.of(TP, 200.0), 0.8));
  }

  @Test
  public void testUsesFilesystemUsageAndActualVolumeCapacityWhenAvailable() {
    Map<Integer, Map<String, LogDirDescription>> externalUsage = Map.of(1, Map.of(TARGET,
        new LogDirDescription(null, Map.of(OTHER, replica(10)), 1000 * MB, 210 * MB)));
    assertThrows(IllegalStateException.class,
                 () -> InterBrokerDiskCapacityValidator.validateReservations(List.of(task(0, 20)), externalUsage, Map.of(), 0.8));
    Map<Integer, Map<String, LogDirDescription>> smallerVolume = Map.of(1, Map.of(TARGET,
        new LogDirDescription(null, Map.of(OTHER, replica(300)), 400 * MB, 100 * MB)));
    assertThrows(IllegalStateException.class,
                 () -> InterBrokerDiskCapacityValidator.validateReservations(List.of(task(0, 20)), smallerVolume, Map.of(), 0.8));
  }

  @Test
  public void testDoesNotCountAlreadyCopiedBytesTwice() {
    Map<Integer, Map<String, LogDirDescription>> dirs = Map.of(1, Map.of(TARGET,
        new LogDirDescription(null, Map.of(OTHER, replica(770), TP, replica(20)))));
    InterBrokerDiskCapacityValidator.validateReservations(List.of(task(0, 20)), dirs, Map.of(), 0.8);
  }

  @Test
  public void testRejectsMissingFailedAndWrongLogDirectories() {
    ExecutionTask incoming = task(0, 20);
    assertThrows(IllegalStateException.class,
                 () -> InterBrokerDiskCapacityValidator.validateReservations(List.of(incoming), Map.of(), Map.of(), 0.8));
    Map<Integer, Map<String, LogDirDescription>> failed = Map.of(1, Map.of(TARGET,
        new LogDirDescription(new KafkaStorageException(), Map.of())));
    assertThrows(IllegalStateException.class,
                 () -> InterBrokerDiskCapacityValidator.validateReservations(List.of(incoming), failed, Map.of(), 0.8));
    Map<Integer, Map<String, LogDirDescription>> misplaced = Map.of(1, Map.of(TARGET, new LogDirDescription(null, Map.of()),
        "/wrong", new LogDirDescription(null, Map.of(TP, replica(20)))));
    assertThrows(IllegalStateException.class,
                 () -> InterBrokerDiskCapacityValidator.validateReservations(List.of(incoming), misplaced, Map.of(), 0.8));
  }

  @Test
  public void testMultipleNewReplicasShareDiskReservations() {
    assertThrows(IllegalStateException.class,
                 () -> InterBrokerDiskCapacityValidator.validateReservations(List.of(task(0, 30), task(1, 30)),
                                                                              descriptions(750), Map.of(), 0.8));
  }

  private ReplicaLogDirInfo logDirInfo(String directory) throws Exception {
    java.lang.reflect.Constructor<ReplicaLogDirInfo> constructor =
        ReplicaLogDirInfo.class.getDeclaredConstructor(String.class, long.class, String.class, long.class);
    constructor.setAccessible(true);
    return constructor.newInstance(directory, 0L, null, -1L);
  }

  @Test
  public void testFinalPlacementIsVerified() throws Exception {
    AdminClient admin = EasyMock.mock(AdminClient.class);
    DescribeReplicaLogDirsResult result = EasyMock.mock(DescribeReplicaLogDirsResult.class);
    EasyMock.expect(admin.describeReplicaLogDirs(Set.of(DESTINATION))).andReturn(result).times(2);
    EasyMock.expect(result.values()).andReturn(Map.of(DESTINATION,
        KafkaFuture.completedFuture(logDirInfo(TARGET))));
    EasyMock.expect(result.values()).andReturn(Map.of(DESTINATION,
        KafkaFuture.completedFuture(logDirInfo("/wrong"))));
    EasyMock.replay(admin, result);
    assertTrue(InterBrokerDiskCapacityValidator.placementCompleted(admin, task(0, 20).proposal(), 1000));
    assertThrows(IllegalStateException.class,
                 () -> InterBrokerDiskCapacityValidator.placementCompleted(admin, task(0, 20).proposal(), 1000));
    EasyMock.verify(admin, result);
  }

  private ExecutionTask intraTask(int partition, long size) {
    TopicPartition tp = new TopicPartition("topic", partition);
    ReplicaPlacementInfo source = new ReplicaPlacementInfo(1, "/source");
    ExecutionProposal proposal = new ExecutionProposal(tp, size, source, List.of(source),
        List.of(new ReplicaPlacementInfo(1, TARGET)), Map.of(1, 1000.0));
    ExecutionTask task = new ExecutionTask(partition, proposal, 1, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, 1000);
    task.inProgress(0);
    return task;
  }

  @Test
  public void testLegacySubmissionRejectsDiskAwareTasksWithoutConfiguredSettings() {
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    EasyMock.replay(admin);
    assertThrows(IllegalArgumentException.class, () -> ExecutionUtils.submitReplicaReassignmentTasks(admin, List.of(task(0, 20))));
    EasyMock.verify(admin);
  }

  @Test
  public void testIntraBrokerExecutionUsesConfiguredThresholdBeforeSubmitting() {
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    ExecutionTaskManager manager = EasyMock.strictMock(ExecutionTaskManager.class);
    DescribeLogDirsResult result = EasyMock.strictMock(DescribeLogDirsResult.class);
    EasyMock.expect(admin.describeLogDirs(Set.of(1))).andReturn(result);
    EasyMock.expect(result.descriptions()).andReturn(Map.of(1, KafkaFuture.completedFuture(Map.of(
        "/source", new LogDirDescription(null, Map.of(TP, replica(20))),
        TARGET, new LogDirDescription(null, Map.of(OTHER, replica(650)))))));
    EasyMock.replay(admin, manager, result);
    java.util.Properties props = com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.put(com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig.DISK_CAPACITY_THRESHOLD_CONFIG, "0.6");
    com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig config =
        new com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig(props);
    assertThrows(IllegalStateException.class,
                 () -> ExecutorAdminUtils.executeIntraBrokerReplicaMovements(List.of(intraTask(0, 20)), Set.of(), admin, manager, config));
    EasyMock.verify(admin, manager, result);
  }

  @Test
  public void testIntraBrokerReservationsSubtractCopiedFutureBytesAndIncludeOtherActiveCopies() {
    ExecutionTask incoming = intraTask(0, 20);
    ExecutionTask active = intraTask(1, 40);
    Map<Integer, Map<String, LogDirDescription>> dirs = Map.of(1, Map.of(
        "/source", new LogDirDescription(null, Map.of(TP, replica(20))),
        TARGET, new LogDirDescription(null, Map.of(OTHER, replica(730),
            active.proposal().topicPartition(), new ReplicaInfo(10 * MB, 0, true)))));
    InterBrokerDiskCapacityValidator.validateReservations(List.of(incoming, active), dirs, Map.of(), 0.8);
    assertThrows(IllegalStateException.class,
                 () -> InterBrokerDiskCapacityValidator.validateReservations(List.of(incoming, active), dirs, Map.of(), 0.79));
  }

  @Test
  public void testCompletionQueriesAreBatchedAcrossPartitions() throws Exception {
    ExecutionProposal first = task(0, 20).proposal();
    ExecutionProposal second = task(1, 20).proposal();
    TopicPartitionReplica secondReplica = new TopicPartitionReplica("topic", 1, 1);
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    DescribeReplicaLogDirsResult result = EasyMock.strictMock(DescribeReplicaLogDirsResult.class);
    EasyMock.expect(admin.describeReplicaLogDirs(Set.of(DESTINATION, secondReplica))).andReturn(result);
    EasyMock.expect(result.values()).andReturn(Map.of(DESTINATION, KafkaFuture.completedFuture(logDirInfo(TARGET)),
                                                    secondReplica, KafkaFuture.completedFuture(logDirInfo(TARGET))));
    EasyMock.replay(admin, result);
    assertEquals(Set.of(first, second), InterBrokerDiskCapacityValidator.placementsCompleted(admin, List.of(first, second), 1000));
    EasyMock.verify(admin, result);
  }

  @Test
  public void testLegacyReassignmentsDoNotIssueDiskRequests() {
    ReplicaPlacementInfo source = new ReplicaPlacementInfo(0);
    ExecutionProposal proposal = new ExecutionProposal(TP, 20, source, List.of(source), List.of(new ReplicaPlacementInfo(1)));
    ExecutionTask task = new ExecutionTask(0, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, 1000);
    task.inProgress(0);
    AdminClient admin = EasyMock.strictMock(AdminClient.class);
    AlterPartitionReassignmentsResult result = EasyMock.mock(AlterPartitionReassignmentsResult.class);
    Capture<Map<TopicPartition, java.util.Optional<NewPartitionReassignment>>> submitted = EasyMock.newCapture();
    EasyMock.expect(admin.alterPartitionReassignments(EasyMock.capture(submitted))).andReturn(result);
    EasyMock.replay(admin, result);
    assertSame(result, ExecutionUtils.submitReplicaReassignmentTasks(admin, List.of(task)));
    assertEquals(List.of(1), submitted.getValue().get(TP).get().targetReplicas());
    assertTrue(InterBrokerDiskCapacityValidator.placementCompleted(admin, proposal, 1000));
    EasyMock.verify(admin, result);
  }
}
