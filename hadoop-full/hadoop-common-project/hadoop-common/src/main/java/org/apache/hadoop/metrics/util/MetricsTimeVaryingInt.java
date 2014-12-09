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
package org.apache.hadoop.metrics.util;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The MetricsTimeVaryingInt class is for a metric that naturally
 * varies over time (e.g. number of files created). The metrics is accumulated
 * over an interval (set in the metrics config file); the metrics is
 *  published at the end of each interval and then 
 * reset to zero. Hence the counter has the value in the current interval. 
 * 
 * Note if one wants a time associated with the metric then use
 * @see org.apache.hadoop.metrics.util.MetricsTimeVaryingRate
 *
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
public class MetricsTimeVaryingInt extends @OsUntrusted MetricsBase {

  private static final @OsUntrusted Log LOG =
    LogFactory.getLog("org.apache.hadoop.metrics.util");
  
  private @OsUntrusted int currentValue;
  private @OsUntrusted int previousIntervalValue;
  
  
  /**
   * Constructor - create a new metric
   * @param nam the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   * @param description - the description
   */
  public @OsUntrusted MetricsTimeVaryingInt(final @OsUntrusted String nam,
                               final @OsUntrusted MetricsRegistry registry,
                               final @OsUntrusted String description) {
    super(nam, description);
    currentValue = 0;
    previousIntervalValue = 0;
    registry.add(nam, this);
  }
  
  /**
   * Constructor - create a new metric
   * @param nam the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   * A description of {@link #NO_DESCRIPTION} is used
   */
  public @OsUntrusted MetricsTimeVaryingInt(final @OsUntrusted String nam, final @OsUntrusted MetricsRegistry registry) {
    this(nam, registry, NO_DESCRIPTION);
  }
  

  
  /**
   * Inc metrics for incr vlaue
   * @param incr - number of operations
   */
  public synchronized void inc(@OsUntrusted MetricsTimeVaryingInt this, final @OsUntrusted int incr) {
    currentValue += incr;
  }
  
  /**
   * Inc metrics by one
   */
  public synchronized void inc(@OsUntrusted MetricsTimeVaryingInt this) {
    currentValue++;
  }

  private synchronized void intervalHeartBeat(@OsUntrusted MetricsTimeVaryingInt this) {
     previousIntervalValue = currentValue;
     currentValue = 0;
  }
  
  /**
   * Push the delta  metrics to the mr.
   * The delta is since the last push/interval.
   * 
   * Note this does NOT push to JMX
   * (JMX gets the info via {@link #previousIntervalValue}
   *
   * @param mr
   */
  @Override
  public synchronized void pushMetric(@OsUntrusted MetricsTimeVaryingInt this, final @OsUntrusted MetricsRecord mr) {
    intervalHeartBeat();
    try {
      mr.incrMetric(getName(), getPreviousIntervalValue());
    } catch (@OsUntrusted Exception e) {
      LOG.info("pushMetric failed for " + getName() + "\n" , e);
    }
  }
  
  
  /**
   * The Value at the Previous interval
   * @return prev interval value
   */
  public synchronized @OsUntrusted int getPreviousIntervalValue(@OsUntrusted MetricsTimeVaryingInt this) { 
    return previousIntervalValue;
  }
  
  /**
   * The Value at the current interval
   * @return prev interval value
   */
  public synchronized @OsUntrusted int getCurrentIntervalValue(@OsUntrusted MetricsTimeVaryingInt this) { 
    return currentValue;
  } 
}
