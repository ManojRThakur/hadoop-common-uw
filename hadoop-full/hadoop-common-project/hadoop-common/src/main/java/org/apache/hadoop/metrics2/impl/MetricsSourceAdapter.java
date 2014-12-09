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
import java.util.HashMap;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import static org.apache.hadoop.metrics2.impl.MetricsConfig.*;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.metrics2.util.Contracts.*;

/**
 * An adapter class for metrics source and associated filter and jmx impl
 */
class MetricsSourceAdapter implements @OsUntrusted DynamicMBean {

  private static final @OsUntrusted Log LOG = LogFactory.getLog(MetricsSourceAdapter.class);

  private final @OsUntrusted String prefix;
  private final @OsUntrusted String name;
  private final @OsUntrusted MetricsSource source;
  private final @OsUntrusted MetricsFilter recordFilter;
  private final @OsUntrusted MetricsFilter metricFilter;
  private final @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted Attribute> attrCache;
  private final @OsUntrusted MBeanInfoBuilder infoBuilder;
  private final @OsUntrusted Iterable<@OsUntrusted MetricsTag> injectedTags;

  private @OsUntrusted Iterable<@OsUntrusted MetricsRecordImpl> lastRecs;
  private @OsUntrusted long jmxCacheTS = 0;
  private @OsUntrusted int jmxCacheTTL;
  private @OsUntrusted MBeanInfo infoCache;
  private @OsUntrusted ObjectName mbeanName;
  private final @OsUntrusted boolean startMBeans;

  @OsUntrusted
  MetricsSourceAdapter(@OsUntrusted String prefix, @OsUntrusted String name, @OsUntrusted String description,
                       @OsUntrusted
                       MetricsSource source, @OsUntrusted Iterable<@OsUntrusted MetricsTag> injectedTags,
                       @OsUntrusted
                       MetricsFilter recordFilter, @OsUntrusted MetricsFilter metricFilter,
                       @OsUntrusted
                       int jmxCacheTTL, @OsUntrusted boolean startMBeans) {
    this.prefix = checkNotNull(prefix, "prefix");
    this.name = checkNotNull(name, "name");
    this.source = checkNotNull(source, "source");
    attrCache = Maps.newHashMap();
    infoBuilder = new @OsUntrusted MBeanInfoBuilder(name, description);
    this.injectedTags = injectedTags;
    this.recordFilter = recordFilter;
    this.metricFilter = metricFilter;
    this.jmxCacheTTL = checkArg(jmxCacheTTL, jmxCacheTTL > 0, "jmxCacheTTL");
    this.startMBeans = startMBeans;
  }

  @OsUntrusted
  MetricsSourceAdapter(@OsUntrusted String prefix, @OsUntrusted String name, @OsUntrusted String description,
                       @OsUntrusted
                       MetricsSource source, @OsUntrusted Iterable<@OsUntrusted MetricsTag> injectedTags,
                       @OsUntrusted
                       int period, @OsUntrusted MetricsConfig conf) {
    this(prefix, name, description, source, injectedTags,
         conf.getFilter(RECORD_FILTER_KEY),
         conf.getFilter(METRIC_FILTER_KEY),
         period + 1, // hack to avoid most of the "innocuous" races.
         conf.getBoolean(START_MBEANS_KEY, true));
  }

  void start(@OsUntrusted MetricsSourceAdapter this) {
    if (startMBeans) startMBeans();
  }

  @Override
  public @OsUntrusted Object getAttribute(@OsUntrusted MetricsSourceAdapter this, @OsUntrusted String attribute)
      throws AttributeNotFoundException, MBeanException, ReflectionException {
    updateJmxCache();
    synchronized(this) {
      @OsUntrusted
      Attribute a = attrCache.get(attribute);
      if (a == null) {
        throw new @OsUntrusted AttributeNotFoundException(attribute +" not found");
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(attribute +": "+ a);
      }
      return a.getValue();
    }
  }

  @Override
  public void setAttribute(@OsUntrusted MetricsSourceAdapter this, @OsUntrusted Attribute attribute)
      throws AttributeNotFoundException, InvalidAttributeValueException,
             MBeanException, ReflectionException {
    throw new @OsUntrusted UnsupportedOperationException("Metrics are read-only.");
  }

  @Override
  public @OsUntrusted AttributeList getAttributes(@OsUntrusted MetricsSourceAdapter this, @OsUntrusted String @OsUntrusted [] attributes) {
    updateJmxCache();
    synchronized(this) {
      @OsUntrusted
      AttributeList ret = new @OsUntrusted AttributeList();
      for (@OsUntrusted String key : attributes) {
        @OsUntrusted
        Attribute attr = attrCache.get(key);
        if (LOG.isDebugEnabled()) {
          LOG.debug(key +": "+ attr);
        }
        ret.add(attr);
      }
      return ret;
    }
  }

  @Override
  public @OsUntrusted AttributeList setAttributes(@OsUntrusted MetricsSourceAdapter this, @OsUntrusted AttributeList attributes) {
    throw new @OsUntrusted UnsupportedOperationException("Metrics are read-only.");
  }

  @Override
  public @OsUntrusted Object invoke(@OsUntrusted MetricsSourceAdapter this, @OsUntrusted String actionName, @OsUntrusted Object @OsUntrusted [] params, @OsUntrusted String @OsUntrusted [] signature)
      throws MBeanException, ReflectionException {
    throw new @OsUntrusted UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public @OsUntrusted MBeanInfo getMBeanInfo(@OsUntrusted MetricsSourceAdapter this) {
    updateJmxCache();
    return infoCache;
  }

  private void updateJmxCache(@OsUntrusted MetricsSourceAdapter this) {
    @OsUntrusted
    boolean getAllMetrics = false;
    synchronized(this) {
      if (Time.now() - jmxCacheTS >= jmxCacheTTL) {
        // temporarilly advance the expiry while updating the cache
        jmxCacheTS = Time.now() + jmxCacheTTL;
        if (lastRecs == null) {
          getAllMetrics = true;
        }
      }
      else {
        return;
      }
    }

    if (getAllMetrics) {
      @OsUntrusted
      MetricsCollectorImpl builder = new @OsUntrusted MetricsCollectorImpl();
      getMetrics(builder, true);
    }

    synchronized(this) {
      @OsUntrusted
      int oldCacheSize = attrCache.size();
      @OsUntrusted
      int newCacheSize = updateAttrCache();
      if (oldCacheSize < newCacheSize) {
        updateInfoCache();
      }
      jmxCacheTS = Time.now();
      lastRecs = null;  // in case regular interval update is not running
    }
  }

  @OsUntrusted
  Iterable<@OsUntrusted MetricsRecordImpl> getMetrics(@OsUntrusted MetricsSourceAdapter this, @OsUntrusted MetricsCollectorImpl builder,
                                         @OsUntrusted
                                         boolean all) {
    builder.setRecordFilter(recordFilter).setMetricFilter(metricFilter);
    synchronized(this) {
      if (lastRecs == null && jmxCacheTS == 0) {
        all = true; // Get all the metrics to populate the sink caches
      }
    }
    try {
      source.getMetrics(builder, all);
    } catch (@OsUntrusted Exception e) {
      LOG.error("Error getting metrics from source "+ name, e);
    }
    for (@OsUntrusted MetricsRecordBuilderImpl rb : builder) {
      for (@OsUntrusted MetricsTag t : injectedTags) {
        rb.add(t);
      }
    }
    synchronized(this) {
      lastRecs = builder.getRecords();
      return lastRecs;
    }
  }

  synchronized void stop(@OsUntrusted MetricsSourceAdapter this) {
    stopMBeans();
  }

  synchronized void startMBeans(@OsUntrusted MetricsSourceAdapter this) {
    if (mbeanName != null) {
      LOG.warn("MBean "+ name +" already initialized!");
      LOG.debug("Stacktrace: ", new @OsUntrusted Throwable());
      return;
    }
    mbeanName = MBeans.register(prefix, name, this);
    LOG.debug("MBean for source "+ name +" registered.");
  }

  synchronized void stopMBeans(@OsUntrusted MetricsSourceAdapter this) {
    if (mbeanName != null) {
      MBeans.unregister(mbeanName);
      mbeanName = null;
    }
  }

  private void updateInfoCache(@OsUntrusted MetricsSourceAdapter this) {
    LOG.debug("Updating info cache...");
    infoCache = infoBuilder.reset(lastRecs).get();
    LOG.debug("Done");
  }

  private @OsUntrusted int updateAttrCache(@OsUntrusted MetricsSourceAdapter this) {
    LOG.debug("Updating attr cache...");
    @OsUntrusted
    int recNo = 0;
    @OsUntrusted
    int numMetrics = 0;
    for (@OsUntrusted MetricsRecordImpl record : lastRecs) {
      for (@OsUntrusted MetricsTag t : record.tags()) {
        setAttrCacheTag(t, recNo);
        ++numMetrics;
      }
      for (@OsUntrusted AbstractMetric m : record.metrics()) {
        setAttrCacheMetric(m, recNo);
        ++numMetrics;
      }
      ++recNo;
    }
    LOG.debug("Done. # tags & metrics="+ numMetrics);
    return numMetrics;
  }

  private static @OsUntrusted String tagName(@OsUntrusted String name, @OsUntrusted int recNo) {
    @OsUntrusted
    StringBuilder sb = new @OsUntrusted StringBuilder(name.length() + 16);
    sb.append("tag.").append(name);
    if (recNo > 0) {
      sb.append('.').append(recNo);
    }
    return sb.toString();
  }

  private void setAttrCacheTag(@OsUntrusted MetricsSourceAdapter this, @OsUntrusted MetricsTag tag, @OsUntrusted int recNo) {
    @OsUntrusted
    String key = tagName(tag.name(), recNo);
    attrCache.put(key, new @OsUntrusted Attribute(key, tag.value()));
  }

  private static @OsUntrusted String metricName(@OsUntrusted String name, @OsUntrusted int recNo) {
    if (recNo == 0) {
      return name;
    }
    @OsUntrusted
    StringBuilder sb = new @OsUntrusted StringBuilder(name.length() + 12);
    sb.append(name);
    if (recNo > 0) {
      sb.append('.').append(recNo);
    }
    return sb.toString();
  }

  private void setAttrCacheMetric(@OsUntrusted MetricsSourceAdapter this, @OsUntrusted AbstractMetric metric, @OsUntrusted int recNo) {
    @OsUntrusted
    String key = metricName(metric.name(), recNo);
    attrCache.put(key, new @OsUntrusted Attribute(key, metric.value()));
  }

  @OsUntrusted
  String name(@OsUntrusted MetricsSourceAdapter this) {
    return name;
  }

  @OsUntrusted
  MetricsSource source(@OsUntrusted MetricsSourceAdapter this) {
    return source;
  }
}
