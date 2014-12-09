/*
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

package org.apache.hadoop.metrics2.source;

import ostrusted.quals.OsUntrusted;
import com.google.common.base.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;

/**
 * JVM and logging related metrics info instances
 */
@InterfaceAudience.Private
public enum JvmMetricsInfo implements @OsUntrusted MetricsInfo {

@OsUntrusted  JvmMetrics("JVM related metrics etc."), // record info
  // metrics

@OsUntrusted  MemNonHeapUsedM("Non-heap memory used in MB"),

@OsUntrusted  MemNonHeapCommittedM("Non-heap memory committed in MB"),

@OsUntrusted  MemNonHeapMaxM("Non-heap memory max in MB"),

@OsUntrusted  MemHeapUsedM("Heap memory used in MB"),

@OsUntrusted  MemHeapCommittedM("Heap memory committed in MB"),

@OsUntrusted  MemHeapMaxM("Heap memory max in MB"),

@OsUntrusted  MemMaxM("Max memory size in MB"),

@OsUntrusted  GcCount("Total GC count"),

@OsUntrusted  GcTimeMillis("Total GC time in milliseconds"),

@OsUntrusted  ThreadsNew("Number of new threads"),

@OsUntrusted  ThreadsRunnable("Number of runnable threads"),

@OsUntrusted  ThreadsBlocked("Number of blocked threads"),

@OsUntrusted  ThreadsWaiting("Number of waiting threads"),

@OsUntrusted  ThreadsTimedWaiting("Number of timed waiting threads"),

@OsUntrusted  ThreadsTerminated("Number of terminated threads"),

@OsUntrusted  LogFatal("Total number of fatal log events"),

@OsUntrusted  LogError("Total number of error log events"),

@OsUntrusted  LogWarn("Total number of warning log events"),

@OsUntrusted  LogInfo("Total number of info log events");

  private final @OsUntrusted String desc;

  @OsUntrusted
  JvmMetricsInfo(@OsUntrusted String desc) { this.desc = desc; }

  @Override public @OsUntrusted String description(@OsUntrusted JvmMetricsInfo this) { return desc; }

  @Override public @OsUntrusted String toString(@OsUntrusted JvmMetricsInfo this) {
  return Objects.toStringHelper(this)
      .add("name", name()).add("description", desc)
      .toString();
  }
}
