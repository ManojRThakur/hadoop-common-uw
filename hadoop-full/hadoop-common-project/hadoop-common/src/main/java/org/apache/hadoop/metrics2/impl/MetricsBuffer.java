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
import java.util.Iterator;

/**
 * An immutable element for the sink queues.
 */
class MetricsBuffer implements @OsUntrusted Iterable<MetricsBuffer.@OsUntrusted Entry> {

  private final @OsUntrusted Iterable<@OsUntrusted Entry> mutable;

  @OsUntrusted
  MetricsBuffer(@OsUntrusted Iterable<MetricsBuffer.@OsUntrusted Entry> mutable) {
    this.mutable = mutable;
  }

  @Override
  public @OsUntrusted Iterator<@OsUntrusted Entry> iterator(@OsUntrusted MetricsBuffer this) {
    return mutable.iterator();
  }

  static class Entry {
    private final @OsUntrusted String sourceName;
    private final @OsUntrusted Iterable<@OsUntrusted MetricsRecordImpl> records;

    @OsUntrusted
    Entry(@OsUntrusted String name, @OsUntrusted Iterable<@OsUntrusted MetricsRecordImpl> records) {
      sourceName = name;
      this.records = records;
    }

    @OsUntrusted
    String name(MetricsBuffer.@OsUntrusted Entry this) {
      return sourceName;
    }

    @OsUntrusted
    Iterable<@OsUntrusted MetricsRecordImpl> records(MetricsBuffer.@OsUntrusted Entry this) {
      return records;
    }
  }

}
