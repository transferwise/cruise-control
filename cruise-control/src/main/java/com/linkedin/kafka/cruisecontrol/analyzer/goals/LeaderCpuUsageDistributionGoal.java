/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.LEADERSHIP_MOVEMENT;


/**
 * SOFT GOAL: Balance CPU distribution over alive brokers using only leadership movements.
 *
 * Unlike {@link CpuUsageDistributionGoal}, this goal never generates inter-broker replica moves or replica swaps. It
 * relies solely on leader elections to shift the leadership-fraction of CPU load between brokers, making it a
 * zero-data-movement way to correct CPU imbalance.
 *
 * <p>Because no replicas are physically moved, this goal cannot fix offline replicas or help with
 * broker decommissions. It is intended to run as a standalone, low-cost follow-on operation after hard placement goals
 * are already satisfied.
 */
public class LeaderCpuUsageDistributionGoal extends ResourceDistributionGoal {
  private static final Logger LOG = LoggerFactory.getLogger(LeaderCpuUsageDistributionGoal.class);

  public LeaderCpuUsageDistributionGoal() {
    super();
  }

  LeaderCpuUsageDistributionGoal(BalancingConstraint constraint) {
    super(constraint);
  }

  @Override
  protected Resource resource() {
    return Resource.CPU;
  }

  /**
   * Accept all non-leadership actions unconditionally. This goal does not constrain replica placements. For leadership
   * movements, delegate to the parent's balance-limit check.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    if (action.balancingAction() != LEADERSHIP_MOVEMENT) {
      return ACCEPT;
    }
    return super.actionAcceptance(action, clusterModel);
  }

  /**
   * Rebalance using only leadership movements. Replica moves and swaps are intentionally skipped.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) {
    if (broker.currentOfflineReplicas().isEmpty()) {
      boolean requireLessLoad = !isLoadUnderBalanceUpperLimit(broker);
      boolean requireMoreLoad = !isExcludedForReplicaMove(broker) && !isLoadAboveBalanceLowerLimit(broker);
      if (!requireMoreLoad && !requireLessLoad) {
        return;
      }

      if (requireLessLoad && !rebalanceByMovingLoadOut(broker, clusterModel, optimizedGoals,
                                                       LEADERSHIP_MOVEMENT, optimizationOptions)) {
        LOG.debug("Successfully balanced {} for broker {} by moving out leaders.", resource(), broker.id());
        requireLessLoad = false;
      }
      if (requireMoreLoad && !rebalanceByMovingLoadIn(broker, clusterModel, optimizedGoals,
                                                      LEADERSHIP_MOVEMENT, optimizationOptions, false)) {
        LOG.debug("Successfully balanced {} for broker {} by moving in leaders.", resource(), broker.id());
      }
      if (requireLessLoad) {
        LOG.debug("Failed to balance {} for broker {} by leadership movements alone.", resource(), broker.id());
      }
    }
  }
}
