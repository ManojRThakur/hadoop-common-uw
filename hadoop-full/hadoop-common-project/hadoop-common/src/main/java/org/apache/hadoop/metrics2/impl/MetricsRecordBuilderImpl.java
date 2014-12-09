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
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.util.Time;

/**
 * {@link MetricsRecordBuilder} implementation used for building metrics records
 * by the {@link MetricsCollector}. It provides the following functionality:
 * <ul>
 * <li>Allows configuring filters for metrics.
 * </ul>
 *
 */
class MetricsRecordBuilderImpl extends @OsUntrusted MetricsRecordBuilder {
  private final @OsUntrusted MetricsCollector parent;
  private final @OsUntrusted long timestamp;
  private final @OsUntrusted MetricsInfo recInfo;
  private final @OsUntrusted List<@OsUntrusted AbstractMetric> metrics;
  private final @OsUntrusted List<@OsUntrusted MetricsTag> tags;
  private final @OsUntrusted MetricsFilter recordFilter, metricFilter;
  private final @OsUntrusted boolean acceptable;

  /**
   * @param parent {@link MetricsCollector} using this record builder
   * @param info metrics information
   * @param rf
   * @param mf
   * @param acceptable
   */
  @OsUntrusted
  MetricsRecordBuilderImpl(@OsUntrusted MetricsCollector parent, @OsUntrusted MetricsInfo info,
      @OsUntrusted
      MetricsFilter rf, @OsUntrusted MetricsFilter mf, @OsUntrusted boolean acceptable) {
    this.parent = parent;
    timestamp = Time.now();
    recInfo = info;
    metrics = Lists.newArrayList();
    tags = Lists.newArrayList();
    recordFilter = rf;
    metricFilter = mf;
    this.acceptable = acceptable;
  }

  @Override
  public @OsUntrusted MetricsCollector parent(@OsUntrusted MetricsRecordBuilderImpl this) { return parent; }

  @Override
  public @OsUntrusted MetricsRecordBuilderImpl tag(@OsUntrusted MetricsRecordBuilderImpl this, @OsUntrusted MetricsInfo info, @OsUntrusted String value) {
    if (acceptable) {
      tags.add(Interns.tag(info, value));
    }
    return this;
  }

  @Override
  public @OsUntrusted MetricsRecordBuilderImpl add(@OsUntrusted MetricsRecordBuilderImpl this, @OsUntrusted MetricsTag tag) {
    tags.add(tag);
    return this;
  }

  @Override
  public @OsUntrusted MetricsRecordBuilderImpl add(@OsUntrusted MetricsRecordBuilderImpl this, @OsUntrusted AbstractMetric metric) {
    metrics.add(metric);
    return this;
  }

  @Override
  public @OsUntrusted MetricsRecordBuilderImpl addCounter(@OsUntrusted MetricsRecordBuilderImpl this, @OsUntrusted MetricsInfo info, @OsUntrusted int value) {
    if (acceptable && (metricFilter == null ||
        metricFilter.accepts(info.name()))) {
      metrics.add(new @OsUntrusted MetricCounterInt(info, value));
    }
    return this;
  }

  @Override
  public @OsUntrusted MetricsRecordBuilderImpl addCounter(@OsUntrusted MetricsRecordBuilderImpl this, @OsUntrusted MetricsInfo info, @OsUntrusted long value) {
    if (acceptable && (metricFilter == null ||
        metricFilter.accepts(info.name()))) {
      metrics.add(new @OsUntrusted MetricCounterLong(info, value));
    }
    return this;
  }

  @Override
  public @OsUntrusted MetricsRecordBuilderImpl addGauge(@OsUntrusted MetricsRecordBuilderImpl this, @OsUntrusted MetricsInfo info, @OsUntrusted int value) {
    if (acceptable && (metricFilter == null ||
        metricFilter.accepts(info.name()))) {
      metrics.add(new @OsUntrusted MetricGaugeInt(info, value));
    }
    return this;
  }

  @Override
  public @OsUntrusted MetricsRecordBuilderImpl addGauge(@OsUntrusted MetricsRecordBuilderImpl this, @OsUntrusted MetricsInfo info, @OsUntrusted long value) {
    if (acceptable && (metricFilter == null ||
        metricFilter.accepts(info.name()))) {
      metrics.add(new @OsUntrusted MetricGaugeLong(info, value));
    }
    return this;
  }

  @Override
  public @OsUntrusted MetricsRecordBuilderImpl addGauge(@OsUntrusted MetricsRecordBuilderImpl this, @OsUntrusted MetricsInfo info, @OsUntrusted float value) {
    if (acceptable && (metricFilter == null ||
        metricFilter.accepts(info.name()))) {
      metrics.add(new @OsUntrusted MetricGaugeFloat(info, value));
    }
    return this;
  }

  @Override
  public @OsUntrusted MetricsRecordBuilderImpl addGauge(@OsUntrusted MetricsRecordBuilderImpl this, @OsUntrusted MetricsInfo info, @OsUntrusted double value) {
    if (acceptable && (metricFilter == null ||
        metricFilter.accepts(info.name()))) {
      metrics.add(new @OsUntrusted MetricGaugeDouble(info, value));
    }
    return this;
  }

  @Override
  public @OsUntrusted MetricsRecordBuilderImpl setContext(@OsUntrusted MetricsRecordBuilderImpl this, @OsUntrusted String value) {
    return tag(MsInfo.Context, value);
  }

  public @OsUntrusted MetricsRecordImpl getRecord(@OsUntrusted MetricsRecordBuilderImpl this) {
    if (acceptable && (recordFilter == null || recordFilter.accepts(tags))) {
      return new @OsUntrusted MetricsRecordImpl(recInfo, timestamp, tags(), metrics());
    }
    return null;
  }

  @OsUntrusted
  List<@OsUntrusted MetricsTag> tags(@OsUntrusted MetricsRecordBuilderImpl this) {
    return Collections.unmodifiableList(tags);
  }

  @OsUntrusted
  List<@OsUntrusted AbstractMetric> metrics(@OsUntrusted MetricsRecordBuilderImpl this) {
    return Collections.unmodifiableList(metrics);
  }
}
