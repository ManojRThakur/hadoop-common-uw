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

package org.apache.hadoop.metrics2.impl;

import ostrusted.quals.OsUntrusted;
import com.google.common.base.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;

/**
 * Metrics system related metrics info instances
 */
@InterfaceAudience.Private
public enum MsInfo implements @OsUntrusted MetricsInfo {

@OsUntrusted  NumActiveSources("Number of active metrics sources"),

@OsUntrusted  NumAllSources("Number of all registered metrics sources"),

@OsUntrusted  NumActiveSinks("Number of active metrics sinks"),

@OsUntrusted  NumAllSinks("Number of all registered metrics sinks"),

@OsUntrusted  Context("Metrics context"),

@OsUntrusted  Hostname("Local hostname"),

@OsUntrusted  SessionId("Session ID"),

@OsUntrusted  ProcessName("Process name");

  private final @OsUntrusted String desc;

  @OsUntrusted
  MsInfo(@OsUntrusted String desc) {
    this.desc = desc;
  }

  @Override public @OsUntrusted String description(@OsUntrusted MsInfo this) {
    return desc;
  }

  @Override public @OsUntrusted String toString(@OsUntrusted MsInfo this) {
    return Objects.toStringHelper(this)
        .add("name", name()).add("description", desc)
        .toString();
  }
}
