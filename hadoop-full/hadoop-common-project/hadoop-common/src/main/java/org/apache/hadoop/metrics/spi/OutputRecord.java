/*
 * OutputRecord.java
 *
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

package org.apache.hadoop.metrics.spi;

import ostrusted.quals.OsUntrusted;
import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap;

/**
 * Represents a record of metric data to be sent to a metrics system.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OutputRecord {
    
  private @OsUntrusted TagMap tagMap;
  private @OsUntrusted MetricMap metricMap;
    
  /** Creates a new instance of OutputRecord */
  @OsUntrusted
  OutputRecord(@OsUntrusted TagMap tagMap, @OsUntrusted MetricMap metricMap) {
    this.tagMap = tagMap;
    this.metricMap = metricMap;
  }
    
  /**
   * Returns the set of tag names
   */
  public @OsUntrusted Set<@OsUntrusted String> getTagNames(@OsUntrusted OutputRecord this) {
    return Collections.unmodifiableSet(tagMap.keySet());
  }
    
  /**
   * Returns a tag object which is can be a String, Integer, Short or Byte.
   *
   * @return the tag value, or null if there is no such tag
   */
  public @OsUntrusted Object getTag(@OsUntrusted OutputRecord this, @OsUntrusted String name) {
    return tagMap.get(name);
  }
    
  /**
   * Returns the set of metric names.
   */
  public @OsUntrusted Set<@OsUntrusted String> getMetricNames(@OsUntrusted OutputRecord this) {
    return Collections.unmodifiableSet(metricMap.keySet());
  }
    
  /**
   * Returns the metric object which can be a Float, Integer, Short or Byte.
   */
  public @OsUntrusted Number getMetric(@OsUntrusted OutputRecord this, @OsUntrusted String name) {
    return metricMap.get(name);
  }
  

  /**
   * Returns a copy of this record's tags.
   */
  public @OsUntrusted TagMap getTagsCopy(@OsUntrusted OutputRecord this) {
    return new @OsUntrusted TagMap(tagMap);
  }
  
  /**
   * Returns a copy of this record's metrics.
   */
  public @OsUntrusted MetricMap getMetricsCopy(@OsUntrusted OutputRecord this) {
    return new @OsUntrusted MetricMap(metricMap);
  }
}
