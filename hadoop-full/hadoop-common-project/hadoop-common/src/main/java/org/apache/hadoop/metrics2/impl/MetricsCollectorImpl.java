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
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsFilter;
import static org.apache.hadoop.metrics2.lib.Interns.*;

class MetricsCollectorImpl implements @OsUntrusted MetricsCollector,
    @OsUntrusted
    Iterable<@OsUntrusted MetricsRecordBuilderImpl> {

  private final @OsUntrusted List<@OsUntrusted MetricsRecordBuilderImpl> rbs = Lists.newArrayList();
  private @OsUntrusted MetricsFilter recordFilter;
  private @OsUntrusted MetricsFilter metricFilter;

  @Override
  public @OsUntrusted MetricsRecordBuilderImpl addRecord(@OsUntrusted MetricsCollectorImpl this, @OsUntrusted MetricsInfo info) {
    @OsUntrusted
    boolean acceptable = recordFilter == null ||
                         recordFilter.accepts(info.name());
    @OsUntrusted
    MetricsRecordBuilderImpl rb = new @OsUntrusted MetricsRecordBuilderImpl(this, info,
        recordFilter, metricFilter, acceptable);
    if (acceptable) rbs.add(rb);
    return rb;
  }

  @Override
  public @OsUntrusted MetricsRecordBuilderImpl addRecord(@OsUntrusted MetricsCollectorImpl this, @OsUntrusted String name) {
    return addRecord(info(name, name +" record"));
  }

  public @OsUntrusted List<@OsUntrusted MetricsRecordImpl> getRecords(@OsUntrusted MetricsCollectorImpl this) {
    @OsUntrusted
    List<@OsUntrusted MetricsRecordImpl> recs = Lists.newArrayListWithCapacity(rbs.size());
    for (@OsUntrusted MetricsRecordBuilderImpl rb : rbs) {
      @OsUntrusted
      MetricsRecordImpl mr = rb.getRecord();
      if (mr != null) {
        recs.add(mr);
      }
    }
    return recs;
  }

  @Override
  public @OsUntrusted Iterator<@OsUntrusted MetricsRecordBuilderImpl> iterator(@OsUntrusted MetricsCollectorImpl this) {
    return rbs.iterator();
  }

  void clear(@OsUntrusted MetricsCollectorImpl this) { rbs.clear(); }

  @OsUntrusted
  MetricsCollectorImpl setRecordFilter(@OsUntrusted MetricsCollectorImpl this, @OsUntrusted MetricsFilter rf) {
    recordFilter = rf;
    return this;
  }

  @OsUntrusted
  MetricsCollectorImpl setMetricFilter(@OsUntrusted MetricsCollectorImpl this, @OsUntrusted MetricsFilter mf) {
    metricFilter = mf;
    return this;
  }
}
