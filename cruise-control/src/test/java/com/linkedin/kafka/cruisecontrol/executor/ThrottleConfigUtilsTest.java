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

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.*;

public class ThrottleConfigUtilsTest {
  private Config config(ConfigEntry.ConfigSource source) {
    ConfigEntry entry = EasyMock.mock(ConfigEntry.class);
    EasyMock.expect(entry.name()).andReturn("throttle").anyTimes();
    EasyMock.expect(entry.value()).andReturn("100").anyTimes();
    EasyMock.expect(entry.source()).andReturn(source).anyTimes();
    EasyMock.replay(entry);
    return new Config(Collections.singletonList(entry));
  }

  @Test
  public void testBrokerDeleteAcceptsInheritedDefaultsButRejectsExistingBrokerOverride() {
    Map<String, String> delete = Collections.singletonMap("throttle", null);
    for (ConfigEntry.ConfigSource source : new ConfigEntry.ConfigSource[] {
        ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG, ConfigEntry.ConfigSource.DEFAULT_CONFIG,
        ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG}) {
      assertTrue(ThrottleConfigUtils.configsEqual(config(source), delete, ConfigResource.Type.BROKER));
      assertTrue(IntraBrokerReplicationThrottleHelper.configsEqual(config(source), delete));
      assertTrue(ReplicationThrottleHelper.configsEqual(config(source), delete));
    }
    assertFalse(ThrottleConfigUtils.configsEqual(config(ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG), delete,
                                                ConfigResource.Type.BROKER));
  }

  @Test
  public void testTopicDeleteRequiresRemovalOfTopicOverrideAndAcceptsInheritedBrokerValue() {
    Map<String, String> delete = Collections.singletonMap("throttle", null);
    assertFalse(ThrottleConfigUtils.configsEqual(config(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG), delete,
                                                ConfigResource.Type.TOPIC));
    assertTrue(ThrottleConfigUtils.configsEqual(config(ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG), delete,
                                               ConfigResource.Type.TOPIC));
  }
}
