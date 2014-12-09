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
import java.io.StringWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import javax.management.ObjectName;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Locale;
import static com.google.common.base.Preconditions.*;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.util.MathUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import static org.apache.hadoop.metrics2.impl.MetricsConfig.*;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsAnnotations;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MetricsSourceBuilder;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.Time;

/**
 * A base class for metrics system singletons
 */
@InterfaceAudience.Private
@Metrics(context="metricssystem")
public class MetricsSystemImpl extends @OsUntrusted MetricsSystem implements @OsUntrusted MetricsSource {

  static final @OsUntrusted Log LOG = LogFactory.getLog(MetricsSystemImpl.class);
  static final @OsUntrusted String MS_NAME = "MetricsSystem";
  static final @OsUntrusted String MS_STATS_NAME = MS_NAME +",sub=Stats";
  static final @OsUntrusted String MS_STATS_DESC = "Metrics system metrics";
  static final @OsUntrusted String MS_CONTROL_NAME = MS_NAME +",sub=Control";
  static final @OsUntrusted String MS_INIT_MODE_KEY = "hadoop.metrics.init.mode";

  enum InitMode {  @OsUntrusted  NORMAL,  @OsUntrusted  STANDBY }

  private final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted MetricsSourceAdapter> sources;
  private final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted MetricsSource> allSources;
  private final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted MetricsSinkAdapter> sinks;
  private final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted MetricsSink> allSinks;
  private final @OsUntrusted List<@OsUntrusted Callback> callbacks;
  private final @OsUntrusted MetricsCollectorImpl collector;
  private final @OsUntrusted MetricsRegistry registry = new @OsUntrusted MetricsRegistry(MS_NAME);
  @Metric({"Snapshot", "Snapshot stats"}) @OsUntrusted MutableStat snapshotStat;
  @Metric({"Publish", "Publishing stats"}) @OsUntrusted MutableStat publishStat;
  @Metric("Dropped updates by all sinks") @OsUntrusted MutableCounterLong droppedPubAll;

  private final @OsUntrusted List<@OsUntrusted MetricsTag> injectedTags;

  // Things that are changed by init()/start()/stop()
  private @OsUntrusted String prefix;
  private @OsUntrusted MetricsFilter sourceFilter;
  private @OsUntrusted MetricsConfig config;
  private @OsUntrusted Map<@OsUntrusted String, @OsUntrusted MetricsConfig> sourceConfigs;
  private @OsUntrusted Map<@OsUntrusted String, @OsUntrusted MetricsConfig> sinkConfigs;
  private @OsUntrusted boolean monitoring = false;
  private @OsUntrusted Timer timer;
  private @OsUntrusted int period; // seconds
  private @OsUntrusted long logicalTime; // number of timer invocations * period
  private @OsUntrusted ObjectName mbeanName;
  private @OsUntrusted boolean publishSelfMetrics = true;
  private @OsUntrusted MetricsSourceAdapter sysSource;
  private @OsUntrusted int refCount = 0; // for mini cluster mode

  /**
   * Construct the metrics system
   * @param prefix  for the system
   */
  public @OsUntrusted MetricsSystemImpl(@OsUntrusted String prefix) {
    this.prefix = prefix;
    allSources = Maps.newHashMap();
    sources = Maps.newLinkedHashMap();
    allSinks = Maps.newHashMap();
    sinks = Maps.newLinkedHashMap();
    sourceConfigs = Maps.newHashMap();
    sinkConfigs = Maps.newHashMap();
    callbacks = Lists.newArrayList();
    injectedTags = Lists.newArrayList();
    collector = new @OsUntrusted MetricsCollectorImpl();
    if (prefix != null) {
      // prefix could be null for default ctor, which requires init later
      initSystemMBean();
    }
  }

  /**
   * Construct the system but not initializing (read config etc.) it.
   */
  public @OsUntrusted MetricsSystemImpl() {
    this(null);
  }

  /**
   * Initialized the metrics system with a prefix.
   * @param prefix  the system will look for configs with the prefix
   * @return the metrics system object itself
   */
  @Override
  public synchronized @OsUntrusted MetricsSystem init(@OsUntrusted MetricsSystemImpl this, @OsUntrusted String prefix) {
    if (monitoring && !DefaultMetricsSystem.inMiniClusterMode()) {
      LOG.warn(this.prefix +" metrics system already initialized!");
      return this;
    }
    this.prefix = checkNotNull(prefix, "prefix");
    ++refCount;
    if (monitoring) {
      // in mini cluster mode
      LOG.info(this.prefix +" metrics system started (again)");
      return this;
    }
    switch (initMode()) {
      case NORMAL:
        try { start(); }
        catch (@OsUntrusted MetricsConfigException e) {
          // Configuration errors (e.g., typos) should not be fatal.
          // We can always start the metrics system later via JMX.
          LOG.warn("Metrics system not started: "+ e.getMessage());
          LOG.debug("Stacktrace: ", e);
        }
        break;
      case STANDBY:
        LOG.info(prefix +" metrics system started in standby mode");
    }
    initSystemMBean();
    return this;
  }

  @Override
  public synchronized void start(@OsUntrusted MetricsSystemImpl this) {
    checkNotNull(prefix, "prefix");
    if (monitoring) {
      LOG.warn(prefix +" metrics system already started!",
               new @OsUntrusted MetricsException("Illegal start"));
      return;
    }
    for (@OsUntrusted Callback cb : callbacks) cb.preStart();
    configure(prefix);
    startTimer();
    monitoring = true;
    LOG.info(prefix +" metrics system started");
    for (@OsUntrusted Callback cb : callbacks) cb.postStart();
  }

  @Override
  public synchronized void stop(@OsUntrusted MetricsSystemImpl this) {
    if (!monitoring && !DefaultMetricsSystem.inMiniClusterMode()) {
      LOG.warn(prefix +" metrics system not yet started!",
               new @OsUntrusted MetricsException("Illegal stop"));
      return;
    }
    if (!monitoring) {
      // in mini cluster mode
      LOG.info(prefix +" metrics system stopped (again)");
      return;
    }
    for (@OsUntrusted Callback cb : callbacks) cb.preStop();
    LOG.info("Stopping "+ prefix +" metrics system...");
    stopTimer();
    stopSources();
    stopSinks();
    clearConfigs();
    monitoring = false;
    LOG.info(prefix +" metrics system stopped.");
    for (@OsUntrusted Callback cb : callbacks) cb.postStop();
  }

  @Override public synchronized <@OsUntrusted T extends java.lang.@OsUntrusted Object>
  @OsUntrusted
  T register(@OsUntrusted MetricsSystemImpl this, @OsUntrusted String name, @OsUntrusted String desc, @OsUntrusted T source) {
    @OsUntrusted
    MetricsSourceBuilder sb = MetricsAnnotations.newSourceBuilder(source);
    final @OsUntrusted MetricsSource s = sb.build();
    @OsUntrusted
    MetricsInfo si = sb.info();
    @OsUntrusted
    String name2 = name == null ? si.name() : name;
    final @OsUntrusted String finalDesc = desc == null ? si.description() : desc;
    final @OsUntrusted String finalName = // be friendly to non-metrics tests
        DefaultMetricsSystem.sourceName(name2, !monitoring);
    allSources.put(finalName, s);
    LOG.debug(finalName +", "+ finalDesc);
    if (monitoring) {
      registerSource(finalName, finalDesc, s);
    }
    // We want to re-register the source to pick up new config when the
    // metrics system restarts.
    register(new @OsUntrusted AbstractCallback() {
      @Override public void postStart() {
        registerSource(finalName, finalDesc, s);
      }
    });
    return source;
  }

  synchronized
  void registerSource(@OsUntrusted MetricsSystemImpl this, @OsUntrusted String name, @OsUntrusted String desc, @OsUntrusted MetricsSource source) {
    checkNotNull(config, "config");
    @OsUntrusted
    MetricsConfig conf = sourceConfigs.get(name);
    @OsUntrusted
    MetricsSourceAdapter sa = new @OsUntrusted MetricsSourceAdapter(prefix, name, desc,
        source, injectedTags, period, conf != null ? conf
            : config.subset(SOURCE_KEY));
    sources.put(name, sa);
    sa.start();
    LOG.debug("Registered source "+ name);
  }

  @Override public synchronized <@OsUntrusted T extends @OsUntrusted MetricsSink>
  @OsUntrusted
  T register(@OsUntrusted MetricsSystemImpl this, final @OsUntrusted String name, final @OsUntrusted String description, final @OsUntrusted T sink) {
    LOG.debug(name +", "+ description);
    if (allSinks.containsKey(name)) {
      LOG.warn("Sink "+ name +" already exists!");
      return sink;
    }
    allSinks.put(name, sink);
    if (config != null) {
      registerSink(name, description, sink);
    }
    // We want to re-register the sink to pick up new config
    // when the metrics system restarts.
    register(new @OsUntrusted AbstractCallback() {
      @Override public void postStart() {
        register(name, description, sink);
      }
    });
    return sink;
  }

  synchronized void registerSink(@OsUntrusted MetricsSystemImpl this, @OsUntrusted String name, @OsUntrusted String desc, @OsUntrusted MetricsSink sink) {
    checkNotNull(config, "config");
    @OsUntrusted
    MetricsConfig conf = sinkConfigs.get(name);
    @OsUntrusted
    MetricsSinkAdapter sa = conf != null
        ? newSink(name, desc, sink, conf)
        : newSink(name, desc, sink, config.subset(SINK_KEY));
    sinks.put(name, sa);
    sa.start();
    LOG.info("Registered sink "+ name);
  }

  @Override
  public synchronized void register(@OsUntrusted MetricsSystemImpl this, final @OsUntrusted Callback callback) {
    callbacks.add((@OsUntrusted Callback) Proxy.newProxyInstance(
        callback.getClass().getClassLoader(), new Class<?> @OsUntrusted [] { Callback.class },
        new @OsUntrusted InvocationHandler() {
          @Override
          public @OsUntrusted Object invoke(@OsUntrusted Object proxy, @OsUntrusted Method method, @OsUntrusted Object @OsUntrusted [] args)
              throws Throwable {
            try {
              return method.invoke(callback, args);
            } catch (@OsUntrusted Exception e) {
              // These are not considered fatal.
              LOG.warn("Caught exception in callback "+ method.getName(), e);
            }
            return null;
          }
        }));
  }

  @Override
  public synchronized void startMetricsMBeans(@OsUntrusted MetricsSystemImpl this) {
    for (@OsUntrusted MetricsSourceAdapter sa : sources.values()) {
      sa.startMBeans();
    }
  }

  @Override
  public synchronized void stopMetricsMBeans(@OsUntrusted MetricsSystemImpl this) {
    for (@OsUntrusted MetricsSourceAdapter sa : sources.values()) {
      sa.stopMBeans();
    }
  }

  @Override
  public synchronized @OsUntrusted String currentConfig(@OsUntrusted MetricsSystemImpl this) {
    @OsUntrusted
    PropertiesConfiguration saver = new @OsUntrusted PropertiesConfiguration();
    @OsUntrusted
    StringWriter writer = new @OsUntrusted StringWriter();
    saver.copy(config);
    try { saver.save(writer); }
    catch (@OsUntrusted Exception e) {
      throw new @OsUntrusted MetricsConfigException("Error stringify config", e);
    }
    return writer.toString();
  }

  private synchronized void startTimer(@OsUntrusted MetricsSystemImpl this) {
    if (timer != null) {
      LOG.warn(prefix +" metrics system timer already started!");
      return;
    }
    logicalTime = 0;
    @OsUntrusted
    long millis = period * 1000;
    timer = new @OsUntrusted Timer("Timer for '"+ prefix +"' metrics system", true);
    timer.scheduleAtFixedRate(new @OsUntrusted TimerTask() {
          @Override
          public void run() {
            try {
              onTimerEvent();
            } catch (@OsUntrusted Exception e) {
              LOG.warn(e);
            }
          }
        }, millis, millis);
    LOG.info("Scheduled snapshot period at "+ period +" second(s).");
  }

  synchronized void onTimerEvent(@OsUntrusted MetricsSystemImpl this) {
    logicalTime += period;
    if (sinks.size() > 0) {
      publishMetrics(sampleMetrics(), false);
    }
  }
  
  /**
   * Requests an immediate publish of all metrics from sources to sinks.
   */
  @Override
  public void publishMetricsNow(@OsUntrusted MetricsSystemImpl this) {
    if (sinks.size() > 0) {
      publishMetrics(sampleMetrics(), true);
    }    
  }

  /**
   * Sample all the sources for a snapshot of metrics/tags
   * @return  the metrics buffer containing the snapshot
   */
  synchronized @OsUntrusted MetricsBuffer sampleMetrics(@OsUntrusted MetricsSystemImpl this) {
    collector.clear();
    @OsUntrusted
    MetricsBufferBuilder bufferBuilder = new @OsUntrusted MetricsBufferBuilder();

    for (@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted MetricsSourceAdapter> entry : sources.entrySet()) {
      if (sourceFilter == null || sourceFilter.accepts(entry.getKey())) {
        snapshotMetrics(entry.getValue(), bufferBuilder);
      }
    }
    if (publishSelfMetrics) {
      snapshotMetrics(sysSource, bufferBuilder);
    }
    @OsUntrusted
    MetricsBuffer buffer = bufferBuilder.get();
    return buffer;
  }

  private void snapshotMetrics(@OsUntrusted MetricsSystemImpl this, @OsUntrusted MetricsSourceAdapter sa,
                               @OsUntrusted
                               MetricsBufferBuilder bufferBuilder) {
    @OsUntrusted
    long startTime = Time.now();
    bufferBuilder.add(sa.name(), sa.getMetrics(collector, true));
    collector.clear();
    snapshotStat.add(Time.now() - startTime);
    LOG.debug("Snapshotted source "+ sa.name());
  }

  /**
   * Publish a metrics snapshot to all the sinks
   * @param buffer  the metrics snapshot to publish
   * @param immediate  indicates that we should publish metrics immediately
   *                   instead of using a separate thread.
   */
  synchronized void publishMetrics(@OsUntrusted MetricsSystemImpl this, @OsUntrusted MetricsBuffer buffer, @OsUntrusted boolean immediate) {
    @OsUntrusted
    int dropped = 0;
    for (@OsUntrusted MetricsSinkAdapter sa : sinks.values()) {
      @OsUntrusted
      long startTime = Time.now();
      @OsUntrusted
      boolean result;
      if (immediate) {
        result = sa.putMetricsImmediate(buffer); 
      } else {
        result = sa.putMetrics(buffer, logicalTime);
      }
      dropped += result ? 0 : 1;
      publishStat.add(Time.now() - startTime);
    }
    droppedPubAll.incr(dropped);
  }

  private synchronized void stopTimer(@OsUntrusted MetricsSystemImpl this) {
    if (timer == null) {
      LOG.warn(prefix +" metrics system timer already stopped!");
      return;
    }
    timer.cancel();
    timer = null;
  }

  private synchronized void stopSources(@OsUntrusted MetricsSystemImpl this) {
    for (@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted MetricsSourceAdapter> entry : sources.entrySet()) {
      @OsUntrusted
      MetricsSourceAdapter sa = entry.getValue();
      LOG.debug("Stopping metrics source "+ entry.getKey() +
          ": class=" + sa.source().getClass());
      sa.stop();
    }
    sysSource.stop();
    sources.clear();
  }

  private synchronized void stopSinks(@OsUntrusted MetricsSystemImpl this) {
    for (@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted MetricsSinkAdapter> entry : sinks.entrySet()) {
      @OsUntrusted
      MetricsSinkAdapter sa = entry.getValue();
      LOG.debug("Stopping metrics sink "+ entry.getKey() +
          ": class=" + sa.sink().getClass());
      sa.stop();
    }
    sinks.clear();
  }

  private synchronized void configure(@OsUntrusted MetricsSystemImpl this, @OsUntrusted String prefix) {
    config = MetricsConfig.create(prefix);
    configureSinks();
    configureSources();
    configureSystem();
  }

  private synchronized void configureSystem(@OsUntrusted MetricsSystemImpl this) {
    injectedTags.add(Interns.tag(MsInfo.Hostname, getHostname()));
  }

  private synchronized void configureSinks(@OsUntrusted MetricsSystemImpl this) {
    sinkConfigs = config.getInstanceConfigs(SINK_KEY);
    @OsUntrusted
    int confPeriod = 0;
    for (@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted MetricsConfig> entry : sinkConfigs.entrySet()) {
      @OsUntrusted
      MetricsConfig conf = entry.getValue();
      @OsUntrusted
      int sinkPeriod = conf.getInt(PERIOD_KEY, PERIOD_DEFAULT);
      confPeriod = confPeriod == 0 ? sinkPeriod
                                   : MathUtils.gcd(confPeriod, sinkPeriod);
      @OsUntrusted
      String clsName = conf.getClassName("");
      if (clsName == null) continue;  // sink can be registered later on
      @OsUntrusted
      String sinkName = entry.getKey();
      try {
        @OsUntrusted
        MetricsSinkAdapter sa = newSink(sinkName,
            conf.getString(DESC_KEY, sinkName), conf);
        sa.start();
        sinks.put(sinkName, sa);
      } catch (@OsUntrusted Exception e) {
        LOG.warn("Error creating sink '"+ sinkName +"'", e);
      }
    }
    period = confPeriod > 0 ? confPeriod
                            : config.getInt(PERIOD_KEY, PERIOD_DEFAULT);
  }

  static @OsUntrusted MetricsSinkAdapter newSink(@OsUntrusted String name, @OsUntrusted String desc, @OsUntrusted MetricsSink sink,
                                    @OsUntrusted
                                    MetricsConfig conf) {
    return new @OsUntrusted MetricsSinkAdapter(name, desc, sink, conf.getString(CONTEXT_KEY),
        conf.getFilter(SOURCE_FILTER_KEY),
        conf.getFilter(RECORD_FILTER_KEY),
        conf.getFilter(METRIC_FILTER_KEY),
        conf.getInt(PERIOD_KEY, PERIOD_DEFAULT),
        conf.getInt(QUEUE_CAPACITY_KEY, QUEUE_CAPACITY_DEFAULT),
        conf.getInt(RETRY_DELAY_KEY, RETRY_DELAY_DEFAULT),
        conf.getFloat(RETRY_BACKOFF_KEY, RETRY_BACKOFF_DEFAULT),
        conf.getInt(RETRY_COUNT_KEY, RETRY_COUNT_DEFAULT));
  }

  static @OsUntrusted MetricsSinkAdapter newSink(@OsUntrusted String name, @OsUntrusted String desc,
                                    @OsUntrusted
                                    MetricsConfig conf) {
    return newSink(name, desc, (@OsUntrusted MetricsSink) conf.getPlugin(""), conf);
  }

  private void configureSources(@OsUntrusted MetricsSystemImpl this) {
    sourceFilter = config.getFilter(PREFIX_DEFAULT + SOURCE_FILTER_KEY);
    sourceConfigs = config.getInstanceConfigs(SOURCE_KEY);
    registerSystemSource();
  }

  private void clearConfigs(@OsUntrusted MetricsSystemImpl this) {
    sinkConfigs.clear();
    sourceConfigs.clear();
    injectedTags.clear();
    config = null;
  }

  static @OsUntrusted String getHostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (@OsUntrusted Exception e) {
      LOG.error("Error getting localhost name. Using 'localhost'...", e);
    }
    return "localhost";
  }

  private void registerSystemSource(@OsUntrusted MetricsSystemImpl this) {
    @OsUntrusted
    MetricsConfig sysConf = sourceConfigs.get(MS_NAME);
    sysSource = new @OsUntrusted MetricsSourceAdapter(prefix, MS_STATS_NAME, MS_STATS_DESC,
        MetricsAnnotations.makeSource(this), injectedTags, period,
        sysConf == null ? config.subset(SOURCE_KEY) : sysConf);
    sysSource.start();
  }

  @Override
  public synchronized void getMetrics(@OsUntrusted MetricsSystemImpl this, @OsUntrusted MetricsCollector builder, @OsUntrusted boolean all) {
    @OsUntrusted
    MetricsRecordBuilder rb = builder.addRecord(MS_NAME)
        .addGauge(MsInfo.NumActiveSources, sources.size())
        .addGauge(MsInfo.NumAllSources, allSources.size())
        .addGauge(MsInfo.NumActiveSinks, sinks.size())
        .addGauge(MsInfo.NumAllSinks, allSinks.size());

    for (@OsUntrusted MetricsSinkAdapter sa : sinks.values()) {
      sa.snapshot(rb, all);
    }
    registry.snapshot(rb, all);
  }

  private void initSystemMBean(@OsUntrusted MetricsSystemImpl this) {
    checkNotNull(prefix, "prefix should not be null here!");
    if (mbeanName == null) {
      mbeanName = MBeans.register(prefix, MS_CONTROL_NAME, this);
    }
  }

  @Override
  public synchronized @OsUntrusted boolean shutdown(@OsUntrusted MetricsSystemImpl this) {
    LOG.debug("refCount="+ refCount);
    if (refCount <= 0) {
      LOG.debug("Redundant shutdown", new @OsUntrusted Throwable());
      return true; // already shutdown
    }
    if (--refCount > 0) return false;
    if (monitoring) {
      try { stop(); }
      catch (@OsUntrusted Exception e) {
        LOG.warn("Error stopping the metrics system", e);
      }
    }
    allSources.clear();
    allSinks.clear();
    callbacks.clear();
    if (mbeanName != null) {
      MBeans.unregister(mbeanName);
      mbeanName = null;
    }
    LOG.info(prefix +" metrics system shutdown complete.");
    return true;
  }

  @Override
  public @OsUntrusted MetricsSource getSource(@OsUntrusted MetricsSystemImpl this, @OsUntrusted String name) {
    return allSources.get(name);
  }

  private @OsUntrusted InitMode initMode(@OsUntrusted MetricsSystemImpl this) {
    LOG.debug("from system property: "+ System.getProperty(MS_INIT_MODE_KEY));
    LOG.debug("from environment variable: "+ System.getenv(MS_INIT_MODE_KEY));
    @OsUntrusted
    String m = System.getProperty(MS_INIT_MODE_KEY);
    @OsUntrusted
    String m2 = m == null ? System.getenv(MS_INIT_MODE_KEY) : m;
    return InitMode.valueOf((m2 == null ? InitMode.NORMAL.name() : m2)
                            .toUpperCase(Locale.US));
  }
}
