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
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.base.Objects;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MsInfo;

/**
 * An optional metrics registry class for creating and maintaining a
 * collection of MetricsMutables, making writing metrics source easier.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricsRegistry {
  private final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted MutableMetric> metricsMap = Maps.newLinkedHashMap();
  private final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted MetricsTag> tagsMap = Maps.newLinkedHashMap();
  private final @OsUntrusted MetricsInfo metricsInfo;

  /**
   * Construct the registry with a record name
   * @param name  of the record of the metrics
   */
  public @OsUntrusted MetricsRegistry(@OsUntrusted String name) {
    metricsInfo = Interns.info(name, name);
  }

  /**
   * Construct the registry with a metadata object
   * @param info  the info object for the metrics record/group
   */
  public @OsUntrusted MetricsRegistry(@OsUntrusted MetricsInfo info) {
    metricsInfo = info;
  }

  /**
   * @return the info object of the metrics registry
   */
  public @OsUntrusted MetricsInfo info(@OsUntrusted MetricsRegistry this) {
    return metricsInfo;
  }

  /**
   * Get a metric by name
   * @param name  of the metric
   * @return the metric object
   */
  public synchronized @OsUntrusted MutableMetric get(@OsUntrusted MetricsRegistry this, @OsUntrusted String name) {
    return metricsMap.get(name);
  }

  /**
   * Get a tag by name
   * @param name  of the tag
   * @return the tag object
   */
  public synchronized @OsUntrusted MetricsTag getTag(@OsUntrusted MetricsRegistry this, @OsUntrusted String name) {
    return tagsMap.get(name);
  }

  /**
   * Create a mutable integer counter
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new counter object
   */
  public @OsUntrusted MutableCounterInt newCounter(@OsUntrusted MetricsRegistry this, @OsUntrusted String name, @OsUntrusted String desc, @OsUntrusted int iVal) {
    return newCounter(Interns.info(name, desc), iVal);
  }

  /**
   * Create a mutable integer counter
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new counter object
   */
  public synchronized @OsUntrusted MutableCounterInt newCounter(@OsUntrusted MetricsRegistry this, @OsUntrusted MetricsInfo info, @OsUntrusted int iVal) {
    checkMetricName(info.name());
    @OsUntrusted
    MutableCounterInt ret = new @OsUntrusted MutableCounterInt(info, iVal);
    metricsMap.put(info.name(), ret);
    return ret;
  }

  /**
   * Create a mutable long integer counter
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new counter object
   */
  public @OsUntrusted MutableCounterLong newCounter(@OsUntrusted MetricsRegistry this, @OsUntrusted String name, @OsUntrusted String desc, @OsUntrusted long iVal) {
    return newCounter(Interns.info(name, desc), iVal);
  }

  /**
   * Create a mutable long integer counter
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new counter object
   */
  public synchronized
  @OsUntrusted
  MutableCounterLong newCounter(@OsUntrusted MetricsRegistry this, @OsUntrusted MetricsInfo info, @OsUntrusted long iVal) {
    checkMetricName(info.name());
    @OsUntrusted
    MutableCounterLong ret = new @OsUntrusted MutableCounterLong(info, iVal);
    metricsMap.put(info.name(), ret);
    return ret;
  }

  /**
   * Create a mutable integer gauge
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new gauge object
   */
  public @OsUntrusted MutableGaugeInt newGauge(@OsUntrusted MetricsRegistry this, @OsUntrusted String name, @OsUntrusted String desc, @OsUntrusted int iVal) {
    return newGauge(Interns.info(name, desc), iVal);
  }
  /**
   * Create a mutable integer gauge
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new gauge object
   */
  public synchronized @OsUntrusted MutableGaugeInt newGauge(@OsUntrusted MetricsRegistry this, @OsUntrusted MetricsInfo info, @OsUntrusted int iVal) {
    checkMetricName(info.name());
    @OsUntrusted
    MutableGaugeInt ret = new @OsUntrusted MutableGaugeInt(info, iVal);
    metricsMap.put(info.name(), ret);
    return ret;
  }

  /**
   * Create a mutable long integer gauge
   * @param name  of the metric
   * @param desc  metric description
   * @param iVal  initial value
   * @return a new gauge object
   */
  public @OsUntrusted MutableGaugeLong newGauge(@OsUntrusted MetricsRegistry this, @OsUntrusted String name, @OsUntrusted String desc, @OsUntrusted long iVal) {
    return newGauge(Interns.info(name, desc), iVal);
  }

  /**
   * Create a mutable long integer gauge
   * @param info  metadata of the metric
   * @param iVal  initial value
   * @return a new gauge object
   */
  public synchronized @OsUntrusted MutableGaugeLong newGauge(@OsUntrusted MetricsRegistry this, @OsUntrusted MetricsInfo info, @OsUntrusted long iVal) {
    checkMetricName(info.name());
    @OsUntrusted
    MutableGaugeLong ret = new @OsUntrusted MutableGaugeLong(info, iVal);
    metricsMap.put(info.name(), ret);
    return ret;
  }

  /**
   * Create a mutable metric that estimates quantiles of a stream of values
   * @param name of the metric
   * @param desc metric description
   * @param sampleName of the metric (e.g., "Ops")
   * @param valueName of the metric (e.g., "Time" or "Latency")
   * @param interval rollover interval of estimator in seconds
   * @return a new quantile estimator object
   */
  public synchronized @OsUntrusted MutableQuantiles newQuantiles(@OsUntrusted MetricsRegistry this, @OsUntrusted String name, @OsUntrusted String desc,
      @OsUntrusted
      String sampleName, @OsUntrusted String valueName, @OsUntrusted int interval) {
    checkMetricName(name);
    @OsUntrusted
    MutableQuantiles ret = 
        new @OsUntrusted MutableQuantiles(name, desc, sampleName, valueName, interval);
    metricsMap.put(name, ret);
    return ret;
  }
  
  /**
   * Create a mutable metric with stats
   * @param name  of the metric
   * @param desc  metric description
   * @param sampleName  of the metric (e.g., "Ops")
   * @param valueName   of the metric (e.g., "Time" or "Latency")
   * @param extended    produce extended stat (stdev, min/max etc.) if true.
   * @return a new mutable stat metric object
   */
  public synchronized @OsUntrusted MutableStat newStat(@OsUntrusted MetricsRegistry this, @OsUntrusted String name, @OsUntrusted String desc,
      @OsUntrusted
      String sampleName, @OsUntrusted String valueName, @OsUntrusted boolean extended) {
    checkMetricName(name);
    @OsUntrusted
    MutableStat ret =
        new @OsUntrusted MutableStat(name, desc, sampleName, valueName, extended);
    metricsMap.put(name, ret);
    return ret;
  }

  /**
   * Create a mutable metric with stats
   * @param name  of the metric
   * @param desc  metric description
   * @param sampleName  of the metric (e.g., "Ops")
   * @param valueName   of the metric (e.g., "Time" or "Latency")
   * @return a new mutable metric object
   */
  public @OsUntrusted MutableStat newStat(@OsUntrusted MetricsRegistry this, @OsUntrusted String name, @OsUntrusted String desc,
                             @OsUntrusted
                             String sampleName, @OsUntrusted String valueName) {
    return newStat(name, desc, sampleName, valueName, false);
  }

  /**
   * Create a mutable rate metric
   * @param name  of the metric
   * @return a new mutable metric object
   */
  public @OsUntrusted MutableRate newRate(@OsUntrusted MetricsRegistry this, @OsUntrusted String name) {
    return newRate(name, name, false);
  }

  /**
   * Create a mutable rate metric
   * @param name  of the metric
   * @param description of the metric
   * @return a new mutable rate metric object
   */
  public @OsUntrusted MutableRate newRate(@OsUntrusted MetricsRegistry this, @OsUntrusted String name, @OsUntrusted String description) {
    return newRate(name, description, false);
  }

  /**
   * Create a mutable rate metric (for throughput measurement)
   * @param name  of the metric
   * @param desc  description
   * @param extended  produce extended stat (stdev/min/max etc.) if true
   * @return a new mutable rate metric object
   */
  public @OsUntrusted MutableRate newRate(@OsUntrusted MetricsRegistry this, @OsUntrusted String name, @OsUntrusted String desc, @OsUntrusted boolean extended) {
    return newRate(name, desc, extended, true);
  }

  @InterfaceAudience.Private
  public synchronized @OsUntrusted MutableRate newRate(@OsUntrusted MetricsRegistry this, @OsUntrusted String name, @OsUntrusted String desc,
      @OsUntrusted
      boolean extended, @OsUntrusted boolean returnExisting) {
    if (returnExisting) {
      @OsUntrusted
      MutableMetric rate = metricsMap.get(name);
      if (rate != null) {
        if (rate instanceof @OsUntrusted MutableRate) return (@OsUntrusted MutableRate) rate;
        throw new @OsUntrusted MetricsException("Unexpected metrics type "+ rate.getClass()
                                   +" for "+ name);
      }
    }
    checkMetricName(name);
    @OsUntrusted
    MutableRate ret = new @OsUntrusted MutableRate(name, desc, extended);
    metricsMap.put(name, ret);
    return ret;
  }

  synchronized void add(@OsUntrusted MetricsRegistry this, @OsUntrusted String name, @OsUntrusted MutableMetric metric) {
    checkMetricName(name);
    metricsMap.put(name, metric);
  }

  /**
   * Add sample to a stat metric by name.
   * @param name  of the metric
   * @param value of the snapshot to add
   */
  public synchronized void add(@OsUntrusted MetricsRegistry this, @OsUntrusted String name, @OsUntrusted long value) {
    @OsUntrusted
    MutableMetric m = metricsMap.get(name);

    if (m != null) {
      if (m instanceof @OsUntrusted MutableStat) {
        ((@OsUntrusted MutableStat) m).add(value);
      }
      else {
        throw new @OsUntrusted MetricsException("Unsupported add(value) for metric "+ name);
      }
    }
    else {
      metricsMap.put(name, newRate(name)); // default is a rate metric
      add(name, value);
    }
  }

  /**
   * Set the metrics context tag
   * @param name of the context
   * @return the registry itself as a convenience
   */
  public @OsUntrusted MetricsRegistry setContext(@OsUntrusted MetricsRegistry this, @OsUntrusted String name) {
    return tag(MsInfo.Context, name, true);
  }

  /**
   * Add a tag to the metrics
   * @param name  of the tag
   * @param description of the tag
   * @param value of the tag
   * @return the registry (for keep adding tags)
   */
  public @OsUntrusted MetricsRegistry tag(@OsUntrusted MetricsRegistry this, @OsUntrusted String name, @OsUntrusted String description, @OsUntrusted String value) {
    return tag(name, description, value, false);
  }

  /**
   * Add a tag to the metrics
   * @param name  of the tag
   * @param description of the tag
   * @param value of the tag
   * @param override  existing tag if true
   * @return the registry (for keep adding tags)
   */
  public @OsUntrusted MetricsRegistry tag(@OsUntrusted MetricsRegistry this, @OsUntrusted String name, @OsUntrusted String description, @OsUntrusted String value,
                             @OsUntrusted
                             boolean override) {
    return tag(Interns.info(name, description), value, override);
  }

  /**
   * Add a tag to the metrics
   * @param info  metadata of the tag
   * @param value of the tag
   * @param override existing tag if true
   * @return the registry (for keep adding tags etc.)
   */
  public synchronized
  @OsUntrusted
  MetricsRegistry tag(@OsUntrusted MetricsRegistry this, @OsUntrusted MetricsInfo info, @OsUntrusted String value, @OsUntrusted boolean override) {
    if (!override) checkTagName(info.name());
    tagsMap.put(info.name(), Interns.tag(info, value));
    return this;
  }

  public @OsUntrusted MetricsRegistry tag(@OsUntrusted MetricsRegistry this, @OsUntrusted MetricsInfo info, @OsUntrusted String value) {
    return tag(info, value, false);
  }

  @OsUntrusted
  Collection<@OsUntrusted MetricsTag> tags(@OsUntrusted MetricsRegistry this) {
    return tagsMap.values();
  }

  @OsUntrusted
  Collection<@OsUntrusted MutableMetric> metrics(@OsUntrusted MetricsRegistry this) {
    return metricsMap.values();
  }

  private void checkMetricName(@OsUntrusted MetricsRegistry this, @OsUntrusted String name) {
    if (metricsMap.containsKey(name)) {
      throw new @OsUntrusted MetricsException("Metric name "+ name +" already exists!");
    }
  }

  private void checkTagName(@OsUntrusted MetricsRegistry this, @OsUntrusted String name) {
    if (tagsMap.containsKey(name)) {
      throw new @OsUntrusted MetricsException("Tag "+ name +" already exists!");
    }
  }

  /**
   * Sample all the mutable metrics and put the snapshot in the builder
   * @param builder to contain the metrics snapshot
   * @param all get all the metrics even if the values are not changed.
   */
  public synchronized void snapshot(@OsUntrusted MetricsRegistry this, @OsUntrusted MetricsRecordBuilder builder, @OsUntrusted boolean all) {
    for (@OsUntrusted MetricsTag tag : tags()) {
      builder.add(tag);
    }
    for (@OsUntrusted MutableMetric metric : metrics()) {
      metric.snapshot(builder, all);
    }
  }

  @Override public @OsUntrusted String toString(@OsUntrusted MetricsRegistry this) {
    return Objects.toStringHelper(this)
        .add("info", metricsInfo).add("tags", tags()).add("metrics", metrics())
        .toString();
  }
}
