/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.util.HashMap;
import java.util.Map;

/** Bounds consecutive unavailable placement queries during both normal execution and stop/drain. */
final class LogdirQueryFailureTracker {
  static final int MAX_CONSECUTIVE_FAILURES = 3;
  private final Map<ExecutionTask, Integer> _failures = new HashMap<>();

  boolean shouldMarkDead(ExecutionTask task, boolean querySucceeded, boolean stopRequested, boolean nonRetriable) {
    if (querySucceeded) {
      _failures.remove(task);
      return false;
    }
    return nonRetriable || _failures.merge(task, 1, Integer::sum) >= MAX_CONSECUTIVE_FAILURES;
  }
}
