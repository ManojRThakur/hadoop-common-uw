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

package org.apache.hadoop.metrics2.lib;

import ostrusted.quals.OsUntrusted;
import com.google.common.base.Objects;
import static com.google.common.base.Preconditions.*;
import org.apache.hadoop.metrics2.MetricsInfo;

/**
 * Making implementing metric info a little easier
 */
class MetricsInfoImpl implements @OsUntrusted MetricsInfo {
  private final @OsUntrusted String name;
  private final @OsUntrusted String description;

  @OsUntrusted
  MetricsInfoImpl(@OsUntrusted String name, @OsUntrusted String description) {
    this.name = checkNotNull(name, "name");
    this.description = checkNotNull(description, "description");
  }

  @Override public @OsUntrusted String name(@OsUntrusted MetricsInfoImpl this) {
    return name;
  }

  @Override public @OsUntrusted String description(@OsUntrusted MetricsInfoImpl this) {
    return description;
  }

  @Override public @OsUntrusted boolean equals(@OsUntrusted MetricsInfoImpl this, @OsUntrusted Object obj) {
    if (obj instanceof @OsUntrusted MetricsInfo) {
      @OsUntrusted
      MetricsInfo other = (@OsUntrusted MetricsInfo) obj;
      return Objects.equal(name, other.name()) &&
             Objects.equal(description, other.description());
    }
    return false;
  }

  @Override public @OsUntrusted int hashCode(@OsUntrusted MetricsInfoImpl this) {
    return Objects.hashCode(name, description);
  }

  @Override public @OsUntrusted String toString(@OsUntrusted MetricsInfoImpl this) {
    return Objects.toStringHelper(this)
        .add("name", name).add("description", description)
        .toString();
  }
}
