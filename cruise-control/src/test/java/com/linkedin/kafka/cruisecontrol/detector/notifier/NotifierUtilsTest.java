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
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NotifierUtilsTest {
  @Test
  public void testExistingKafkaAnomalySeverities() {
    assertEquals(AlertSeverity.CRITICAL, NotifierUtils.getAlertSeverity(KafkaAnomalyType.BROKER_FAILURE));
    assertEquals(AlertSeverity.MAJOR, NotifierUtils.getAlertSeverity(KafkaAnomalyType.MAINTENANCE_EVENT));
    assertEquals(AlertSeverity.MAJOR, NotifierUtils.getAlertSeverity(KafkaAnomalyType.DISK_FAILURE));
    assertEquals(AlertSeverity.MINOR, NotifierUtils.getAlertSeverity(KafkaAnomalyType.METRIC_ANOMALY));
    assertEquals(AlertSeverity.MINOR, NotifierUtils.getAlertSeverity(KafkaAnomalyType.GOAL_VIOLATION));
    assertEquals(AlertSeverity.WARNING, NotifierUtils.getAlertSeverity(KafkaAnomalyType.TOPIC_ANOMALY));
  }

  @Test
  public void testIntraBrokerGoalViolationSeverity() {
    assertEquals(AlertSeverity.MINOR, NotifierUtils.getAlertSeverity(KafkaAnomalyType.INTRA_BROKER_GOAL_VIOLATION));
  }

  @Test
  public void testCustomAnomalyTypesRetainPriorityBasedSeverity() {
    AlertSeverity[] expected = {AlertSeverity.CRITICAL, AlertSeverity.MAJOR, AlertSeverity.MAJOR,
                               AlertSeverity.MINOR, AlertSeverity.MINOR, AlertSeverity.WARNING};
    for (int priority = 0; priority < expected.length; priority++) {
      int customPriority = priority;
      AnomalyType customType = () -> customPriority;
      assertEquals(expected[priority], NotifierUtils.getAlertSeverity(customType));
    }
  }
}
