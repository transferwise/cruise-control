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
