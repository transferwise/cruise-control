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

import java.util.HashMap;
import java.util.Map;

/** Bounds stop-wait query failures while preserving immediate failure handling during normal execution. */
final class LogdirQueryFailureTracker {
  static final int MAX_CONSECUTIVE_FAILURES = 3;
  private final Map<ExecutionTask, Integer> _failures = new HashMap<>();

  boolean shouldMarkDead(ExecutionTask task, boolean querySucceeded, boolean stopRequested, boolean nonRetriable) {
    if (querySucceeded) {
      _failures.remove(task);
      return false;
    }
    return nonRetriable || !stopRequested || _failures.merge(task, 1, Integer::sum) >= MAX_CONSECUTIVE_FAILURES;
  }
}
