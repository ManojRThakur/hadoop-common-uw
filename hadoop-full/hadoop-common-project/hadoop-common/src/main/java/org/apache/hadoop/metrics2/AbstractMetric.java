/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metrics2;

import ostrusted.quals.OsUntrusted;
import com.google.common.base.Objects;
import static com.google.common.base.Preconditions.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The immutable metric
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AbstractMetric implements @OsUntrusted MetricsInfo {
  private final @OsUntrusted MetricsInfo info;

  /**
   * Construct the metric
   * @param info  about the metric
   */
  protected @OsUntrusted AbstractMetric(@OsUntrusted MetricsInfo info) {
    this.info = checkNotNull(info, "metric info");
  }

  @Override public @OsUntrusted String name(@OsUntrusted AbstractMetric this) {
    return info.name();
  }

  @Override public @OsUntrusted String description(@OsUntrusted AbstractMetric this) {
    return info.description();
  }

  protected @OsUntrusted MetricsInfo info(@OsUntrusted AbstractMetric this) {
    return info;
  }

  /**
   * Get the value of the metric
   * @return the value of the metric
   */
  public abstract @OsUntrusted Number value(@OsUntrusted AbstractMetric this);

  /**
   * Get the type of the metric
   * @return the type of the metric
   */
  public abstract @OsUntrusted MetricType type(@OsUntrusted AbstractMetric this);

  /**
   * Accept a visitor interface
   * @param visitor of the metric
   */
  public abstract void visit(@OsUntrusted AbstractMetric this, @OsUntrusted MetricsVisitor visitor);

  @Override public @OsUntrusted boolean equals(@OsUntrusted AbstractMetric this, @OsUntrusted Object obj) {
    if (obj instanceof @OsUntrusted AbstractMetric) {
      final @OsUntrusted AbstractMetric other = (@OsUntrusted AbstractMetric) obj;
      return Objects.equal(info, other.info()) &&
             Objects.equal(value(), other.value());
    }
    return false;
  }

  @Override public @OsUntrusted int hashCode(@OsUntrusted AbstractMetric this) {
    return Objects.hashCode(info, value());
  }

  @Override public @OsUntrusted String toString(@OsUntrusted AbstractMetric this) {
    return Objects.toStringHelper(this)
        .add("info", info)
        .add("value", value())
        .toString();
  }
}
