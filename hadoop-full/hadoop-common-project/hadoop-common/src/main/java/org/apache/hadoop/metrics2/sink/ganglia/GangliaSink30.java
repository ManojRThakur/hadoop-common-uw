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

package org.apache.hadoop.metrics2.sink.ganglia;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.util.MetricsCache;
import org.apache.hadoop.metrics2.util.MetricsCache.Record;

/**
 * This code supports Ganglia 3.0
 * 
 */
public class GangliaSink30 extends @OsUntrusted AbstractGangliaSink {

  public final @OsUntrusted Log LOG = LogFactory.getLog(this.getClass());

  private static final @OsUntrusted String TAGS_FOR_PREFIX_PROPERTY_PREFIX = "tagsForPrefix.";
  
  private @OsUntrusted MetricsCache metricsCache = new @OsUntrusted MetricsCache();

  // a key with a NULL value means ALL
  private @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Set<@OsUntrusted String>> useTagsMap = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted Set<@OsUntrusted String>>();

  @Override
  @SuppressWarnings("unchecked")
  public void init(@OsUntrusted GangliaSink30 this, @OsUntrusted SubsetConfiguration conf) {
    super.init(conf);

    conf.setListDelimiter(',');
    @OsUntrusted
    Iterator<@OsUntrusted String> it = (@OsUntrusted Iterator<@OsUntrusted String>) conf.getKeys();
    while (it.hasNext()) {
      @OsUntrusted
      String propertyName = it.next();
      if (propertyName.startsWith(TAGS_FOR_PREFIX_PROPERTY_PREFIX)) {
        @OsUntrusted
        String contextName = propertyName.substring(TAGS_FOR_PREFIX_PROPERTY_PREFIX.length());
        @OsUntrusted
        String @OsUntrusted [] tags = conf.getStringArray(propertyName);
        @OsUntrusted
        boolean useAllTags = false;
        @OsUntrusted
        Set<@OsUntrusted String> set = null;
        if (tags.length > 0) {
          set = new @OsUntrusted HashSet<@OsUntrusted String>();
          for (@OsUntrusted String tag : tags) {
            tag = tag.trim();
            useAllTags |= tag.equals("*");
            if (tag.length() > 0) {
              set.add(tag);
            }
          }
          if (useAllTags) {
            set = null;
          }
        }
        useTagsMap.put(contextName, set);
      }
    }
  }

  @InterfaceAudience.Private
  public void appendPrefix(@OsUntrusted GangliaSink30 this, @OsUntrusted MetricsRecord record, @OsUntrusted StringBuilder sb) {
    @OsUntrusted
    String contextName = record.context();
    @OsUntrusted
    Collection<@OsUntrusted MetricsTag> tags = record.tags();
    if (useTagsMap.containsKey(contextName)) {
      @OsUntrusted
      Set<@OsUntrusted String> useTags = useTagsMap.get(contextName);
      for (@OsUntrusted MetricsTag t : tags) {
        if (useTags == null || useTags.contains(t.name())) {

          // the context is always skipped here because it is always added
          
          // the hostname is always skipped to avoid case-mismatches 
          // from different DNSes.

          if (t.info() != MsInfo.Context && t.info() != MsInfo.Hostname && t.value() != null) {
            sb.append('.').append(t.name()).append('=').append(t.value());
          }
        }
      }
    }          
  }
  
  @Override
  public void putMetrics(@OsUntrusted GangliaSink30 this, @OsUntrusted MetricsRecord record) {
    // The method handles both cases whether Ganglia support dense publish
    // of metrics of sparse (only on change) publish of metrics
    try {
      @OsUntrusted
      String recordName = record.name();
      @OsUntrusted
      String contextName = record.context();

      @OsUntrusted
      StringBuilder sb = new @OsUntrusted StringBuilder();
      sb.append(contextName);
      sb.append('.');
      sb.append(recordName);

      appendPrefix(record, sb);
      
      @OsUntrusted
      String groupName = sb.toString();
      sb.append('.');
      @OsUntrusted
      int sbBaseLen = sb.length();

      @OsUntrusted
      String type = null;
      @OsUntrusted
      GangliaSlope slopeFromMetric = null;
      @OsUntrusted
      GangliaSlope calculatedSlope = null;
      @OsUntrusted
      Record cachedMetrics = null;
      resetBuffer();  // reset the buffer to the beginning
      if (!isSupportSparseMetrics()) {
        // for sending dense metrics, update metrics cache
        // and get the updated data
        cachedMetrics = metricsCache.update(record);

        if (cachedMetrics != null && cachedMetrics.metricsEntrySet() != null) {
          for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted AbstractMetric> entry : cachedMetrics
              .metricsEntrySet()) {
            @OsUntrusted
            AbstractMetric metric = entry.getValue();
            sb.append(metric.name());
            @OsUntrusted
            String name = sb.toString();

            // visit the metric to identify the Ganglia type and
            // slope
            metric.visit(gangliaMetricVisitor);
            type = gangliaMetricVisitor.getType();
            slopeFromMetric = gangliaMetricVisitor.getSlope();

            @OsUntrusted
            GangliaConf gConf = getGangliaConfForMetric(name);
            calculatedSlope = calculateSlope(gConf, slopeFromMetric);

            // send metric to Ganglia
            emitMetric(groupName, name, type, metric.value().toString(), gConf,
                calculatedSlope);

            // reset the length of the buffer for next iteration
            sb.setLength(sbBaseLen);
          }
        }
      } else {
        // we support sparse updates

        @OsUntrusted
        Collection<@OsUntrusted AbstractMetric> metrics = (@OsUntrusted Collection<@OsUntrusted AbstractMetric>) record
            .metrics();
        if (metrics.size() > 0) {
          // we got metrics. so send the latest
          for (@OsUntrusted AbstractMetric metric : record.metrics()) {
            sb.append(metric.name());
            @OsUntrusted
            String name = sb.toString();

            // visit the metric to identify the Ganglia type and
            // slope
            metric.visit(gangliaMetricVisitor);
            type = gangliaMetricVisitor.getType();
            slopeFromMetric = gangliaMetricVisitor.getSlope();

            @OsUntrusted
            GangliaConf gConf = getGangliaConfForMetric(name);
            calculatedSlope = calculateSlope(gConf, slopeFromMetric);

            // send metric to Ganglia
            emitMetric(groupName, name, type, metric.value().toString(), gConf,
                calculatedSlope);

            // reset the length of the buffer for next iteration
            sb.setLength(sbBaseLen);
          }
        }
      }
    } catch (@OsUntrusted IOException io) {
      throw new @OsUntrusted MetricsException("Failed to putMetrics", io);
    }
  }

  // Calculate the slope from properties and metric
  private @OsUntrusted GangliaSlope calculateSlope(@OsUntrusted GangliaSink30 this, @OsUntrusted GangliaConf gConf,
      @OsUntrusted
      GangliaSlope slopeFromMetric) {
    if (gConf.getSlope() != null) {
      // if slope has been specified in properties, use that
      return gConf.getSlope();
    } else if (slopeFromMetric != null) {
      // slope not specified in properties, use derived from Metric
      return slopeFromMetric;
    } else {
      return DEFAULT_SLOPE;
    }
  }

  /**
   * The method sends metrics to Ganglia servers. The method has been taken from
   * org.apache.hadoop.metrics.ganglia.GangliaContext30 with minimal changes in
   * order to keep it in sync.
   * @param groupName The group name of the metric
   * @param name The metric name
   * @param type The type of the metric
   * @param value The value of the metric
   * @param gConf The GangliaConf for this metric
   * @param gSlope The slope for this metric
   * @throws IOException
   */
  protected void emitMetric(@OsUntrusted GangliaSink30 this, @OsUntrusted String groupName, @OsUntrusted String name, @OsUntrusted String type,
      @OsUntrusted
      String value, @OsUntrusted GangliaConf gConf, @OsUntrusted GangliaSlope gSlope) throws IOException {

    if (name == null) {
      LOG.warn("Metric was emitted with no name.");
      return;
    } else if (value == null) {
      LOG.warn("Metric name " + name + " was emitted with a null value.");
      return;
    } else if (type == null) {
      LOG.warn("Metric name " + name + ", value " + value + " has no type.");
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Emitting metric " + name + ", type " + type + ", value "
          + value + ", slope " + gSlope.name() + " from hostname "
          + getHostName());
    }

    xdr_int(0); // metric_user_defined
    xdr_string(type);
    xdr_string(name);
    xdr_string(value);
    xdr_string(gConf.getUnits());
    xdr_int(gSlope.ordinal());
    xdr_int(gConf.getTmax());
    xdr_int(gConf.getDmax());

    // send the metric to Ganglia hosts
    emitToGangliaHosts();
  }
}
