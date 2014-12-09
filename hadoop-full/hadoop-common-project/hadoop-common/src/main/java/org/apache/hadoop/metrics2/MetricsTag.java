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
 * Immutable tag for metrics (for grouping on host/queue/username etc.)
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricsTag implements @OsUntrusted MetricsInfo {
  private final @OsUntrusted MetricsInfo info;
  private final @OsUntrusted String value;

  /**
   * Construct the tag with name, description and value
   * @param info  of the tag
   * @param value of the tag
   */
  public @OsUntrusted MetricsTag(@OsUntrusted MetricsInfo info, @OsUntrusted String value) {
    this.info = checkNotNull(info, "tag info");
    this.value = value;
  }

  @Override public @OsUntrusted String name(@OsUntrusted MetricsTag this) {
    return info.name();
  }

  @Override public @OsUntrusted String description(@OsUntrusted MetricsTag this) {
    return info.description();
  }

  /**
   * @return the info object of the tag
   */
  public @OsUntrusted MetricsInfo info(@OsUntrusted MetricsTag this) {
    return info;
  }

  /**
   * Get the value of the tag
   * @return  the value
   */
  public @OsUntrusted String value(@OsUntrusted MetricsTag this) {
    return value;
  }

  @Override public @OsUntrusted boolean equals(@OsUntrusted MetricsTag this, @OsUntrusted Object obj) {
    if (obj instanceof @OsUntrusted MetricsTag) {
      final @OsUntrusted MetricsTag other = (@OsUntrusted MetricsTag) obj;
      return Objects.equal(info, other.info()) &&
             Objects.equal(value, other.value());
    }
    return false;
  }

  @Override public @OsUntrusted int hashCode(@OsUntrusted MetricsTag this) {
    return Objects.hashCode(info, value);
  }

  @Override public @OsUntrusted String toString(@OsUntrusted MetricsTag this) {
    return Objects.toStringHelper(this)
        .add("info", info)
        .add("value", value())
        .toString();
  }
}
