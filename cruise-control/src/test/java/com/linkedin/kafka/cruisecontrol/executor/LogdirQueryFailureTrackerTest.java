/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.*;

public class LogdirQueryFailureTrackerTest {
  private ExecutionTask task(int id) {
    ReplicaPlacementInfo source = new ReplicaPlacementInfo(0, "/source");
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition("topic", id), 10, source, List.of(source),
                                                       List.of(new ReplicaPlacementInfo(0, "/destination")));
    ExecutionTask task = new ExecutionTask(id, proposal, 0, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, 1000);
    task.inProgress(0);
    return task;
  }

  @Test
  public void testRepeatedTransientErrorsDuringStopAreBoundedPerTask() {
    LogdirQueryFailureTracker tracker = new LogdirQueryFailureTracker();
    ExecutionTask first = task(0);
    ExecutionTask second = task(1);
    assertFalse(tracker.shouldMarkDead(first, false, true, false));
    assertFalse(tracker.shouldMarkDead(second, false, true, false));
    assertFalse(tracker.shouldMarkDead(first, false, true, false));
    assertTrue(tracker.shouldMarkDead(first, false, true, false));
    assertFalse(tracker.shouldMarkDead(second, false, true, false));
  }

  @Test
  public void testSuccessfulQueryResetsTransientFailureCount() {
    LogdirQueryFailureTracker tracker = new LogdirQueryFailureTracker();
    ExecutionTask task = task(0);
    assertFalse(tracker.shouldMarkDead(task, false, true, false));
    assertFalse(tracker.shouldMarkDead(task, false, true, false));
    assertFalse(tracker.shouldMarkDead(task, true, true, false));
    assertFalse(tracker.shouldMarkDead(task, false, true, false));
    assertFalse(tracker.shouldMarkDead(task, false, true, false));
    assertTrue(tracker.shouldMarkDead(task, false, true, false));
  }

  @Test
  public void testPermanentErrorsAndNormalExecutionKeepImmediateFailureBehavior() {
    LogdirQueryFailureTracker tracker = new LogdirQueryFailureTracker();
    assertTrue(tracker.shouldMarkDead(task(0), false, true, true));
    assertTrue(tracker.shouldMarkDead(task(1), false, false, false));
  }
}
