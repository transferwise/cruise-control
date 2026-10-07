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

package com.linkedin.kafka.cruisecontrol.common;

import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;

/** Disk capacity and usage in MB, using filesystem information when the broker supplies it. */
public final class DiskCapacityUtils {
  private static final double BYTES_IN_MB = 1024.0 * 1024.0;

  private DiskCapacityUtils() {
  }

  /**
   * @param description live log-directory description
   * @return usage in MB, including future replicas and other filesystem usage when available
   */
  public static double utilization(LogDirDescription description) {
    validateVolume(description);
    double replicaUsage = description.replicaInfos().values().stream().mapToDouble(DiskCapacityUtils::replicaSize).sum();
    if (description.totalBytes().isPresent()) {
      double volumeUsage = (description.totalBytes().getAsLong() - description.usableBytes().getAsLong()) / BYTES_IN_MB;
      return Math.max(replicaUsage, volumeUsage);
    }
    return replicaUsage;
  }

  /**
   * @param configuredCapacity configured disk capacity in MB
   * @param description live log-directory description
   * @return configured capacity capped at the reported volume size when available
   */
  public static double capacity(double configuredCapacity, LogDirDescription description) {
    validateVolume(description);
    return description.totalBytes().isPresent() ? Math.min(configuredCapacity, description.totalBytes().getAsLong() / BYTES_IN_MB)
                                                : configuredCapacity;
  }

  /**
   * @param replica live replica information
   * @return replica size in MB
   */
  public static double replicaSize(ReplicaInfo replica) {
    if (replica.size() < 0) {
      throw new IllegalStateException("Unknown replica size in log-directory response.");
    }
    return replica.size() / BYTES_IN_MB;
  }

  private static void validateVolume(LogDirDescription description) {
    if (description.totalBytes().isPresent() != description.usableBytes().isPresent()
        || (description.totalBytes().isPresent()
            && (description.totalBytes().getAsLong() <= 0 || description.usableBytes().getAsLong() < 0
                || description.usableBytes().getAsLong() > description.totalBytes().getAsLong()))) {
      throw new IllegalStateException("Invalid filesystem capacity or usable space in log-directory response.");
    }
  }
}
