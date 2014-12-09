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
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import static java.security.AccessController.*;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsPlugin;
import org.apache.hadoop.metrics2.filter.GlobFilter;

/**
 * Metrics configuration for MetricsSystemImpl
 */
class MetricsConfig extends @OsUntrusted SubsetConfiguration {
  static final @OsUntrusted Log LOG = LogFactory.getLog(MetricsConfig.class);

  static final @OsUntrusted String DEFAULT_FILE_NAME = "hadoop-metrics2.properties";
  static final @OsUntrusted String PREFIX_DEFAULT = "*.";

  static final @OsUntrusted String PERIOD_KEY = "period";
  static final @OsUntrusted int PERIOD_DEFAULT = 10; // seconds

  static final @OsUntrusted String QUEUE_CAPACITY_KEY = "queue.capacity";
  static final @OsUntrusted int QUEUE_CAPACITY_DEFAULT = 1;

  static final @OsUntrusted String RETRY_DELAY_KEY = "retry.delay";
  static final @OsUntrusted int RETRY_DELAY_DEFAULT = 10;  // seconds
  static final @OsUntrusted String RETRY_BACKOFF_KEY = "retry.backoff";
  static final @OsUntrusted int RETRY_BACKOFF_DEFAULT = 2; // back off factor
  static final @OsUntrusted String RETRY_COUNT_KEY = "retry.count";
  static final @OsUntrusted int RETRY_COUNT_DEFAULT = 1;

  static final @OsUntrusted String JMX_CACHE_TTL_KEY = "jmx.cache.ttl";
  static final @OsUntrusted String START_MBEANS_KEY = "source.start_mbeans";
  static final @OsUntrusted String PLUGIN_URLS_KEY = "plugin.urls";

  static final @OsUntrusted String CONTEXT_KEY = "context";
  static final @OsUntrusted String NAME_KEY = "name";
  static final @OsUntrusted String DESC_KEY = "description";
  static final @OsUntrusted String SOURCE_KEY = "source";
  static final @OsUntrusted String SINK_KEY = "sink";
  static final @OsUntrusted String METRIC_FILTER_KEY = "metric.filter";
  static final @OsUntrusted String RECORD_FILTER_KEY = "record.filter";
  static final @OsUntrusted String SOURCE_FILTER_KEY = "source.filter";

  static final @OsUntrusted Pattern INSTANCE_REGEX = Pattern.compile("([^.*]+)\\..+");
  static final @OsUntrusted Splitter SPLITTER = Splitter.on(',').trimResults();
  private @OsUntrusted ClassLoader pluginLoader;

  @OsUntrusted
  MetricsConfig(@OsUntrusted Configuration c, @OsUntrusted String prefix) {
    super(c, prefix.toLowerCase(Locale.US), ".");
  }

  static @OsUntrusted MetricsConfig create(@OsUntrusted String prefix) {
    return loadFirst(prefix, "hadoop-metrics2-"+ prefix.toLowerCase(Locale.US)
                     +".properties", DEFAULT_FILE_NAME);
  }

  static @OsUntrusted MetricsConfig create(@OsUntrusted String prefix, @OsUntrusted String @OsUntrusted ... fileNames) {
    return loadFirst(prefix, fileNames);
  }

  /**
   * Load configuration from a list of files until the first successful load
   * @param conf  the configuration object
   * @param files the list of filenames to try
   * @return  the configuration object
   */
  static @OsUntrusted MetricsConfig loadFirst(@OsUntrusted String prefix, @OsUntrusted String @OsUntrusted ... fileNames) {
    for (@OsUntrusted String fname : fileNames) {
      try {
        @OsUntrusted
        Configuration cf = new @OsUntrusted PropertiesConfiguration(fname)
            .interpolatedConfiguration();
        LOG.info("loaded properties from "+ fname);
        LOG.debug(toString(cf));
        @OsUntrusted
        MetricsConfig mc = new @OsUntrusted MetricsConfig(cf, prefix);
        LOG.debug(mc);
        return mc;
      } catch (@OsUntrusted ConfigurationException e) {
        if (e.getMessage().startsWith("Cannot locate configuration")) {
          continue;
        }
        throw new @OsUntrusted MetricsConfigException(e);
      }
    }
    LOG.warn("Cannot locate configuration: tried "+
             Joiner.on(",").join(fileNames));
    // default to an empty configuration
    return new @OsUntrusted MetricsConfig(new @OsUntrusted PropertiesConfiguration(), prefix);
  }

  @Override
  public @OsUntrusted MetricsConfig subset(@OsUntrusted MetricsConfig this, @OsUntrusted String prefix) {
    return new @OsUntrusted MetricsConfig(this, prefix);
  }

  /**
   * Return sub configs for instance specified in the config.
   * Assuming format specified as follows:<pre>
   * [type].[instance].[option] = [value]</pre>
   * Note, '*' is a special default instance, which is excluded in the result.
   * @param type  of the instance
   * @return  a map with [instance] as key and config object as value
   */
  @OsUntrusted
  Map<@OsUntrusted String, @OsUntrusted MetricsConfig> getInstanceConfigs(@OsUntrusted MetricsConfig this, @OsUntrusted String type) {
    @OsUntrusted
    Map<@OsUntrusted String, @OsUntrusted MetricsConfig> map = Maps.newHashMap();
    @OsUntrusted
    MetricsConfig sub = subset(type);

    for (@OsUntrusted String key : sub.keys()) {
      @OsUntrusted
      Matcher matcher = INSTANCE_REGEX.matcher(key);
      if (matcher.matches()) {
        @OsUntrusted
        String instance = matcher.group(1);
        if (!map.containsKey(instance)) {
          map.put(instance, sub.subset(instance));
        }
      }
    }
    return map;
  }

  @OsUntrusted
  Iterable<@OsUntrusted String> keys(@OsUntrusted MetricsConfig this) {
    return new @OsUntrusted Iterable<@OsUntrusted String>() {
      @SuppressWarnings("unchecked")
      @Override
      public @OsUntrusted Iterator<@OsUntrusted String> iterator() {
        return (@OsUntrusted Iterator<@OsUntrusted String>) getKeys();
      }
    };
  }

  /**
   * Will poke parents for defaults
   * @param key to lookup
   * @return  the value or null
   */
  @Override
  public @OsUntrusted Object getProperty(@OsUntrusted MetricsConfig this, @OsUntrusted String key) {
    @OsUntrusted
    Object value = super.getProperty(key);
    if (value == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("poking parent '"+ getParent().getClass().getSimpleName() +
                  "' for key: "+ key);
      }
      return getParent().getProperty(key.startsWith(PREFIX_DEFAULT) ? key
                                     : PREFIX_DEFAULT + key);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("returning '"+ value +"' for key: "+ key);
    }
    return value;
  }

  <@OsUntrusted T extends @OsUntrusted MetricsPlugin> @OsUntrusted T getPlugin(@OsUntrusted MetricsConfig this, @OsUntrusted String name) {
    @OsUntrusted
    String clsName = getClassName(name);
    if (clsName == null) return null;
    try {
      @OsUntrusted
      Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> cls = Class.forName(clsName, true, getPluginLoader());
      @SuppressWarnings("unchecked")
      @OsUntrusted
      T plugin = (@OsUntrusted T) cls.newInstance();
      plugin.init(name.isEmpty() ? this : subset(name));
      return plugin;
    } catch (@OsUntrusted Exception e) {
      throw new @OsUntrusted MetricsConfigException("Error creating plugin: "+ clsName, e);
    }
  }

  @OsUntrusted
  String getClassName(@OsUntrusted MetricsConfig this, @OsUntrusted String prefix) {
    @OsUntrusted
    String classKey = prefix.isEmpty() ? "class" : prefix +".class";
    @OsUntrusted
    String clsName = getString(classKey);
    LOG.debug(clsName);
    if (clsName == null || clsName.isEmpty()) {
      return null;
    }
    return clsName;
  }

  @OsUntrusted
  ClassLoader getPluginLoader(@OsUntrusted MetricsConfig this) {
    if (pluginLoader != null) return pluginLoader;
    final @OsUntrusted ClassLoader defaultLoader = getClass().getClassLoader();
    @OsUntrusted
    Object purls = super.getProperty(PLUGIN_URLS_KEY);
    if (purls == null) return defaultLoader;
    @OsUntrusted
    Iterable<@OsUntrusted String> jars = SPLITTER.split((@OsUntrusted String) purls);
    @OsUntrusted
    int len = Iterables.size(jars);
    if ( len > 0) {
      final @OsUntrusted URL @OsUntrusted [] urls = new @OsUntrusted URL @OsUntrusted [len];
      try {
        @OsUntrusted
        int i = 0;
        for (@OsUntrusted String jar : jars) {
          LOG.debug(jar);
          urls[i++] = new @OsUntrusted URL(jar);
        }
      } catch (@OsUntrusted Exception e) {
        throw new @OsUntrusted MetricsConfigException(e);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("using plugin jars: "+ Iterables.toString(jars));
      }
      pluginLoader = doPrivileged(new @OsUntrusted PrivilegedAction<@OsUntrusted ClassLoader>() {
        @Override public @OsUntrusted ClassLoader run() {
          return new @OsUntrusted URLClassLoader(urls, defaultLoader);
        }
      });
      return pluginLoader;
    }
    if (parent instanceof @OsUntrusted MetricsConfig) {
      return ((@OsUntrusted MetricsConfig) parent).getPluginLoader();
    }
    return defaultLoader;
  }

  @Override public void clear(@OsUntrusted MetricsConfig this) {
    super.clear();
    // pluginLoader.close(); // jdk7 is saner
  }

  @OsUntrusted
  MetricsFilter getFilter(@OsUntrusted MetricsConfig this, @OsUntrusted String prefix) {
    // don't create filter instances without out options
    @OsUntrusted
    MetricsConfig conf = subset(prefix);
    if (conf.isEmpty()) return null;
    @OsUntrusted
    MetricsFilter filter = getPlugin(prefix);
    if (filter != null) return filter;
    // glob filter is assumed if pattern is specified but class is not.
    filter = new @OsUntrusted GlobFilter();
    filter.init(conf);
    return filter;
  }

  @Override
  public @OsUntrusted String toString(@OsUntrusted MetricsConfig this) {
    return toString(this);
  }

  static @OsUntrusted String toString(@OsUntrusted Configuration c) {
    @OsUntrusted
    ByteArrayOutputStream buffer = new @OsUntrusted ByteArrayOutputStream();
    @OsUntrusted
    PrintStream ps = new @OsUntrusted PrintStream(buffer);
    @OsUntrusted
    PropertiesConfiguration tmp = new @OsUntrusted PropertiesConfiguration();
    tmp.copy(c);
    try {
      tmp.save(ps);
    } catch (@OsUntrusted Exception e) {
      throw new @OsUntrusted MetricsConfigException(e);
    }
    return buffer.toString();
  }
}
