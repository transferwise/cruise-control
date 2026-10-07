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

package com.linkedin.kafka.cruisecontrol.monitor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.*;

public class MonitorUtilsDiskCapacityTest {
  private static final long MB = 1024 * 1024;
  private static final TopicPartition TP = new TopicPartition("topic", 0);
  private static final TopicPartition FUTURE = new TopicPartition("topic", 1);

  private ClusterModel model() {
    ClusterModel model = new ClusterModel(new ModelGeneration(0, 0), 1.0);
    model.createRack("0");
    model.createBroker("0", "host", 0, new BrokerCapacityInfo(Map.of(Resource.DISK, 200.0, Resource.CPU, 100.0,
                                                                  Resource.NW_IN, 100.0, Resource.NW_OUT, 100.0),
                                                            Map.of("/one", 100.0, "/two", 100.0)), true);
    return model;
  }

  private KafkaCruiseControlConfig config(boolean enabled) {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.INTER_BROKER_DISK_CAPACITY_CHECK_ENABLED_CONFIG, Boolean.toString(enabled));
    return new KafkaCruiseControlConfig(props);
  }

  private Map<TopicPartition, Map<Integer, String>> fetch(ClusterModel model, Map<String, LogDirDescription> dirs, boolean enabled) {
    return fetch(model, dirs, enabled, enabled);
  }

  private Map<TopicPartition, Map<Integer, String>> fetch(ClusterModel model, Map<String, LogDirDescription> dirs,
                                                        boolean enabled, boolean diskAwareRequest) {
    AdminClient admin = EasyMock.mock(AdminClient.class);
    DescribeLogDirsResult result = EasyMock.mock(DescribeLogDirsResult.class);
    Cluster cluster = new Cluster("cluster", List.of(new Node(0, "host", 9092)), List.of(), Set.of(), Set.of());
    EasyMock.expect(admin.describeLogDirs(List.of(0))).andReturn(result);
    EasyMock.expect(result.descriptions()).andReturn(Map.of(0, KafkaFuture.completedFuture(dirs)));
    EasyMock.replay(admin, result);
    try {
      return MonitorUtils.getReplicaPlacementInfo(model, cluster, admin, config(enabled), diskAwareRequest);
    } finally {
      EasyMock.verify(admin, result);
    }
  }

  @Test
  public void testMissingPlacementOnFailedDiskIsPreservedAsOfflineReplica() throws Exception {
    ClusterModel model = model();
    Map<TopicPartition, Map<Integer, String>> placement = fetch(model, Map.of(
        "/one", new LogDirDescription(new org.apache.kafka.common.errors.KafkaStorageException(), Map.of()),
        "/two", new LogDirDescription(null, Map.of())), true);
    Node node = new Node(0, "host", 9092, "0");
    Cluster cluster = new Cluster("cluster", List.of(node),
        List.of(new org.apache.kafka.common.PartitionInfo("topic", 0, node, new Node[]{node}, new Node[]{node})), Set.of(), Set.of());
    com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver resolver =
        EasyMock.mock(com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver.class);
    EasyMock.expect(resolver.capacityForBroker(EasyMock.anyString(), EasyMock.anyString(), EasyMock.eq(0),
        EasyMock.anyLong(), EasyMock.anyBoolean())).andReturn(new BrokerCapacityInfo(Map.of(
            Resource.DISK, 200.0, Resource.CPU, 100.0, Resource.NW_IN, 100.0, Resource.NW_OUT, 100.0),
            Map.of("/one", 100.0, "/two", 100.0)));
    EasyMock.replay(resolver);
    com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues values =
        new com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues();
    for (Resource resource : Resource.cachedValues()) {
      KafkaCruiseControlUnitTestUtils.setValueForResource(values, resource, 10);
    }
    com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations samples =
        new com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations(values, Map.of());
    samples.setWindows(List.of(1L));
    MonitorUtils.populatePartitionLoad(cluster, model, TP, samples, placement, resolver, false, true);
    model.enableInterBrokerDiskCapacityCheck(0.8);
    assertTrue(model.broker(0).replica(TP).isCurrentOffline());
    assertFalse(model.broker(0).disk("/one").isAlive());
    assertTrue(model.broker(0).disk("/two").isAlive());
    EasyMock.verify(resolver);
  }

  @Test
  public void testUsageIncludesUnmonitoredAndFutureReplicas() {
    ClusterModel model = model();
    Map<TopicPartition, Map<Integer, String>> placement = fetch(model, Map.of(
        "/one", new LogDirDescription(null, Map.of(TP, new ReplicaInfo(60 * MB, 0, false),
                                                   FUTURE, new ReplicaInfo(10 * MB, 0, true))),
        "/two", new LogDirDescription(null, Map.of(TP, new ReplicaInfo(5 * MB, 0, true)))), true);
    assertEquals(70, model.broker(0).disk("/one").reportedUtilization(), 0.001);
    assertEquals(5, model.broker(0).disk("/two").reportedUtilization(), 0.001);
    assertEquals(Map.of(TP, Map.of(0, "/one")), placement);
    model.enableInterBrokerDiskCapacityCheck(0.8);
    model.createReplica("0", 0, TP, 0, true, false, "/one", false);
    assertEquals(60, model.replicaDiskSize(model.broker(0).replica(TP)), 0.001);
  }

  @Test
  public void testRejectsMissingConfiguredDirectoryAndUnknownCapacity() {
    assertThrows(IllegalStateException.class,
                 () -> fetch(model(), Map.of("/one", new LogDirDescription(null, Map.of())), true));
    assertThrows(IllegalStateException.class,
                 () -> fetch(model(), Map.of("/one", new LogDirDescription(null, Map.of()),
                                             "/two", new LogDirDescription(null, Map.of()),
                                             "/unknown", new LogDirDescription(null, Map.of())), true));
  }

  @Test
  public void testUsesPhysicalCapacityAndNonKafkaFilesystemUsage() {
    ClusterModel model = model();
    fetch(model, Map.of("/one", new LogDirDescription(null, Map.of(TP, new ReplicaInfo(10 * MB, 0, false)), 80 * MB, 10 * MB),
                        "/two", new LogDirDescription(null, Map.of(), 200 * MB, 150 * MB)), true);
    assertEquals(80, model.broker(0).disk("/one").capacity(), 0.001);
    assertEquals(70, model.broker(0).disk("/one").reportedUtilization(), 0.001);
    assertEquals(100, model.broker(0).disk("/two").capacity(), 0.001);
    assertEquals(50, model.broker(0).disk("/two").reportedUtilization(), 0.001);
  }

  @Test
  public void testDisabledModePreservesPlacementOnlyBehavior() {
    ClusterModel model = model();
    fetch(model, Map.of("/one", new LogDirDescription(null, Map.of(TP, new ReplicaInfo(60 * MB, 0, false)))), false);
    assertTrue(Double.isNaN(model.broker(0).disk("/one").reportedUtilization()));
    assertFalse(model.interBrokerDiskCapacityCheckEnabled());
    KafkaCruiseControlConfig defaults = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    assertFalse(defaults.getBoolean(AnalyzerConfig.INTER_BROKER_DISK_CAPACITY_CHECK_ENABLED_CONFIG));
  }

  @Test
  public void testLegacyPlacementOnlyRequestRemainsCompatibleWithFeatureConfigured() {
    ClusterModel model = model();
    fetch(model, Map.of("/one", new LogDirDescription(null, Map.of(TP, new ReplicaInfo(60 * MB, 0, false)))), true, false);
    assertTrue(Double.isNaN(model.broker(0).disk("/one").reportedUtilization()));
    assertFalse(model.interBrokerDiskCapacityCheckEnabled());
  }
}
