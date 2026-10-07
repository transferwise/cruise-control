/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
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

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorManager;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import java.util.Collections;
import java.util.List;


/**
 * Flags to indicate the type of an anomaly.
 * Each anomaly type has a priority, which determines order of anomalies being handled by {@link AnomalyDetectorManager}.
 * The smaller the priority value is, the higher priority the anomaly type has.
 *
 * Currently supported anomaly types are as follows (in descending order of priority).
 * <ul>
 *  <li>{@link #BROKER_FAILURE}: Fail-stop failure of brokers.</li>
 *  <li>{@link #MAINTENANCE_EVENT}: Planned maintenance event submitted via a store.</li>
 *  <li>{@link #DISK_FAILURE}: Fail-stop failure of disks.</li>
 *  <li>{@link #METRIC_ANOMALY}: Abnormal changes in broker metrics.</li>
 *  <li>{@link #GOAL_VIOLATION}: Violation of anomaly detection goals.</li>
 *  <li>{@link #TOPIC_ANOMALY}: Topic violating some desired properties.</li>
 * </ul>
 */
public enum KafkaAnomalyType implements AnomalyType {
  @JsonResponseField
  BROKER_FAILURE(0),
  @JsonResponseField
  MAINTENANCE_EVENT(1),
  @JsonResponseField
  DISK_FAILURE(2),
  @JsonResponseField
  METRIC_ANOMALY(3),
  @JsonResponseField
  INTRA_BROKER_GOAL_VIOLATION(4),
  @JsonResponseField
  GOAL_VIOLATION(5),
  @JsonResponseField
  TOPIC_ANOMALY(6);

  private static final List<KafkaAnomalyType> CACHED_VALUES = List.of(values());
  private final int _priority;

  KafkaAnomalyType(int priority) {
    _priority = priority;
  }

  @Override
  public int priority() {
    return _priority;
  }

  /**
   * Use this instead of values() because values() creates a new array each time.
   * @return enumerated values in the same order as values()
   */
  public static List<KafkaAnomalyType> cachedValues() {
    return Collections.unmodifiableList(CACHED_VALUES);
  }
}
