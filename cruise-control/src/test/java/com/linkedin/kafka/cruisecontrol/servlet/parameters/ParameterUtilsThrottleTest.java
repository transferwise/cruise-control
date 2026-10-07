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

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;

public class ParameterUtilsThrottleTest {
  @Test
  public void testIntraBrokerThrottleFallbackPrecedence() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.put(ExecutorConfig.DEFAULT_INTRA_BROKER_REPLICATION_THROTTLE_CONFIG, "500");
    props.put(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG, "200");
    assertEquals(Long.valueOf(500), ParameterUtils.resolveIntraBrokerReplicationThrottle(new KafkaCruiseControlConfig(props), 100L));
    props.remove(ExecutorConfig.DEFAULT_INTRA_BROKER_REPLICATION_THROTTLE_CONFIG);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);
    assertEquals(Long.valueOf(100), ParameterUtils.resolveIntraBrokerReplicationThrottle(config, 100L));
    assertEquals(Long.valueOf(0), ParameterUtils.resolveIntraBrokerReplicationThrottle(config, 0L));
    assertEquals(Long.valueOf(200), ParameterUtils.resolveIntraBrokerReplicationThrottle(config));
    props.remove(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG);
    assertNull(ParameterUtils.resolveIntraBrokerReplicationThrottle(new KafkaCruiseControlConfig(props)));
  }
}
