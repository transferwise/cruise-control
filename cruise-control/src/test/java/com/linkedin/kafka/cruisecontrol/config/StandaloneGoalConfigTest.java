/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderCpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class StandaloneGoalConfigTest {
  private static final String LEADER_CPU_USAGE_DISTRIBUTION_GOAL = LeaderCpuUsageDistributionGoal.class.getName();

  @Test
  public void testStandaloneGoalCanBeSupportedGoal() {
    new KafkaCruiseControlConfig(baseProperties());
  }

  @Test
  public void testStandaloneGoalCannotBeDefaultGoal() {
    assertStandaloneGoalRejected(AnalyzerConfig.DEFAULT_GOALS_CONFIG,
                                 TestConstants.DEFAULT_GOALS_VALUES + "," + LEADER_CPU_USAGE_DISTRIBUTION_GOAL);
  }

  @Test
  public void testStandaloneGoalCannotBeHardGoal() {
    assertStandaloneGoalRejected(AnalyzerConfig.HARD_GOALS_CONFIG,
                                 AnalyzerConfig.DEFAULT_HARD_GOALS + "," + LEADER_CPU_USAGE_DISTRIBUTION_GOAL);
  }

  @Test
  public void testStandaloneGoalCannotBeSelfHealingGoal() {
    assertStandaloneGoalRejected(AnomalyDetectorConfig.SELF_HEALING_GOALS_CONFIG, LEADER_CPU_USAGE_DISTRIBUTION_GOAL);
  }

  @Test
  public void testStandaloneGoalCannotBeAnomalyDetectionGoal() {
    assertStandaloneGoalRejected(AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG,
                                 AnomalyDetectorConfig.DEFAULT_ANOMALY_DETECTION_GOALS
                                 + "," + LEADER_CPU_USAGE_DISTRIBUTION_GOAL);
  }

  private static void assertStandaloneGoalRejected(String configName, String configValue) {
    Properties props = baseProperties();
    props.setProperty(configName, configValue);

    ConfigException exception = assertThrows(ConfigException.class, () -> new KafkaCruiseControlConfig(props));
    assertTrue(exception.getMessage().contains("cannot be configured in " + configName));
  }

  private static Properties baseProperties() {
    Properties props = new Properties();
    props.setProperty(MonitorConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(AnalyzerConfig.GOALS_CONFIG, TestConstants.GOALS_VALUES);
    props.setProperty(AnalyzerConfig.DEFAULT_GOALS_CONFIG, TestConstants.DEFAULT_GOALS_VALUES);
    return props;
  }
}
