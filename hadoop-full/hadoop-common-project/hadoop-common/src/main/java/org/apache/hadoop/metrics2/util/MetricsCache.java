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

package org.apache.hadoop.metrics2.util;

import ostrusted.quals.OsUntrusted;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

/**
 * A metrics cache for sinks that don't support sparse updates.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricsCache {
  static final @OsUntrusted Log LOG = LogFactory.getLog(MetricsCache.class);
  static final @OsUntrusted int MAX_RECS_PER_NAME_DEFAULT = 1000;

  private final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted RecordCache> map = Maps.newHashMap();
  private final @OsUntrusted int maxRecsPerName;

  class RecordCache
      extends @OsUntrusted LinkedHashMap<@OsUntrusted Collection<@OsUntrusted MetricsTag>, @OsUntrusted Record> {
    private static final @OsUntrusted long serialVersionUID = 1L;
    private @OsUntrusted boolean gotOverflow = false;

    @Override
    protected @OsUntrusted boolean removeEldestEntry(@OsUntrusted MetricsCache.RecordCache this, Map.@OsUntrusted Entry<@OsUntrusted Collection<@OsUntrusted MetricsTag>,
                                                  @OsUntrusted
                                                  Record> eldest) {
      @OsUntrusted
      boolean overflow = size() > maxRecsPerName;
      if (overflow && !gotOverflow) {
        LOG.warn("Metrics cache overflow at "+ size() +" for "+ eldest);
        gotOverflow = true;
      }
      return overflow;
    }
  }

  /**
   * Cached record
   */
  public static class Record {
    final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> tags = Maps.newHashMap();
    final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted AbstractMetric> metrics = Maps.newHashMap();

    /**
     * Lookup a tag value
     * @param key name of the tag
     * @return the tag value
     */
    public @OsUntrusted String getTag(MetricsCache.@OsUntrusted Record this, @OsUntrusted String key) {
      return tags.get(key);
    }

    /**
     * Lookup a metric value
     * @param key name of the metric
     * @return the metric value
     */
    public @OsUntrusted Number getMetric(MetricsCache.@OsUntrusted Record this, @OsUntrusted String key) {
      @OsUntrusted
      AbstractMetric metric = metrics.get(key);
      return metric != null ? metric.value() : null;
    }

    /**
     * Lookup a metric instance
     * @param key name of the metric
     * @return the metric instance
     */
    public @OsUntrusted AbstractMetric getMetricInstance(MetricsCache.@OsUntrusted Record this, @OsUntrusted String key) {
      return metrics.get(key);
    }

    /**
     * @return the entry set of the tags of the record
     */
    public @OsUntrusted Set<Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted String>> tags(MetricsCache.@OsUntrusted Record this) {
      return tags.entrySet();
    }

    /**
     * @deprecated use metricsEntrySet() instead
     * @return entry set of metrics
     */
    @Deprecated
    public @OsUntrusted Set<Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted Number>> metrics(MetricsCache.@OsUntrusted Record this) {
      @OsUntrusted
      Map<@OsUntrusted String, @OsUntrusted Number> map = new @OsUntrusted LinkedHashMap<@OsUntrusted String, @OsUntrusted Number>(
          metrics.size());
      for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted AbstractMetric> mapEntry : metrics.entrySet()) {
        map.put(mapEntry.getKey(), mapEntry.getValue().value());
      }
      return map.entrySet();
    }

    /**
     * @return entry set of metrics
     */
    public @OsUntrusted Set<Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted AbstractMetric>> metricsEntrySet(MetricsCache.@OsUntrusted Record this) {
      return metrics.entrySet();
    }

    @Override public @OsUntrusted String toString(MetricsCache.@OsUntrusted Record this) {
      return Objects.toStringHelper(this)
          .add("tags", tags).add("metrics", metrics)
          .toString();
    }
  }

  public @OsUntrusted MetricsCache() {
    this(MAX_RECS_PER_NAME_DEFAULT);
  }

  /**
   * Construct a metrics cache
   * @param maxRecsPerName  limit of the number records per record name
   */
  public @OsUntrusted MetricsCache(@OsUntrusted int maxRecsPerName) {
    this.maxRecsPerName = maxRecsPerName;
  }

  /**
   * Update the cache and return the current cached record
   * @param mr the update record
   * @param includingTags cache tag values (for later lookup by name) if true
   * @return the updated cache record
   */
  public @OsUntrusted Record update(@OsUntrusted MetricsCache this, @OsUntrusted MetricsRecord mr, @OsUntrusted boolean includingTags) {
    @OsUntrusted
    String name = mr.name();
    @OsUntrusted
    RecordCache recordCache = map.get(name);
    if (recordCache == null) {
      recordCache = new @OsUntrusted RecordCache();
      map.put(name, recordCache);
    }
    @OsUntrusted
    Collection<@OsUntrusted MetricsTag> tags = mr.tags();
    @OsUntrusted
    Record record = recordCache.get(tags);
    if (record == null) {
      record = new @OsUntrusted Record();
      recordCache.put(tags, record);
    }
    for (@OsUntrusted AbstractMetric m : mr.metrics()) {
      record.metrics.put(m.name(), m);
    }
    if (includingTags) {
      // mostly for some sinks that include tags as part of a dense schema
      for (@OsUntrusted MetricsTag t : mr.tags()) {
        record.tags.put(t.name(), t.value());
      }
    }
    return record;
  }

  /**
   * Update the cache and return the current cache record
   * @param mr the update record
   * @return the updated cache record
   */
  public @OsUntrusted Record update(@OsUntrusted MetricsCache this, @OsUntrusted MetricsRecord mr) {
    return update(mr, false);
  }

  /**
   * Get the cached record
   * @param name of the record
   * @param tags of the record
   * @return the cached record or null
   */
  public @OsUntrusted Record get(@OsUntrusted MetricsCache this, @OsUntrusted String name, @OsUntrusted Collection<@OsUntrusted MetricsTag> tags) {
    @OsUntrusted
    RecordCache rc = map.get(name);
    if (rc == null) return null;
    return rc.get(tags);
  }
}
