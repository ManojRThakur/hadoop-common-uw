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
 * The MetricsTimeVaryingRate class is for a rate based metric that
 * naturally varies over time (e.g. time taken to create a file).
 * The rate is averaged at each interval heart beat (the interval
 * is set in the metrics config file).
 * This class also keeps track of the min and max rates along with 
 * a method to reset the min-max.
 *
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
public class MetricsTimeVaryingRate extends @OsUntrusted MetricsBase {

  private static final @OsUntrusted Log LOG =
    LogFactory.getLog("org.apache.hadoop.metrics.util");

  static class Metrics {
    @OsUntrusted
    int numOperations = 0;
    @OsUntrusted
    long time = 0;  // total time or average time

    void set(MetricsTimeVaryingRate.@OsUntrusted Metrics this, final @OsUntrusted Metrics resetTo) {
      numOperations = resetTo.numOperations;
      time = resetTo.time;
    }
    
    void reset(MetricsTimeVaryingRate.@OsUntrusted Metrics this) {
      numOperations = 0;
      time = 0;
    }
  }
  
  static class MinMax {
    @OsUntrusted
    long minTime = -1;
    @OsUntrusted
    long maxTime = 0;
    
    void set(MetricsTimeVaryingRate.@OsUntrusted MinMax this, final @OsUntrusted MinMax newVal) {
      minTime = newVal.minTime;
      maxTime = newVal.maxTime;
    }
    
    void reset(MetricsTimeVaryingRate.@OsUntrusted MinMax this) {
      minTime = -1;
      maxTime = 0;
    }
    void update(MetricsTimeVaryingRate.@OsUntrusted MinMax this, final @OsUntrusted long time) { // update min max
      minTime = (minTime == -1) ? time : Math.min(minTime, time);
      minTime = Math.min(minTime, time);
      maxTime = Math.max(maxTime, time);
    }
  }
  private @OsUntrusted Metrics currentData;
  private @OsUntrusted Metrics previousIntervalData;
  private @OsUntrusted MinMax minMax;
  
  
  /**
   * Constructor - create a new metric
   * @param nam the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   */
  public @OsUntrusted MetricsTimeVaryingRate(final @OsUntrusted String nam, final @OsUntrusted MetricsRegistry registry, final @OsUntrusted String description) {
    super(nam, description);
    currentData = new @OsUntrusted Metrics();
    previousIntervalData = new @OsUntrusted Metrics();
    minMax = new @OsUntrusted MinMax();
    registry.add(nam, this);
  }
  
  /**
   * Constructor - create a new metric
   * @param nam the name of the metrics to be used to publish the metric
   * @param registry - where the metrics object will be registered
   * A description of {@link #NO_DESCRIPTION} is used
   */
  public @OsUntrusted MetricsTimeVaryingRate(final @OsUntrusted String nam, @OsUntrusted MetricsRegistry registry) {
    this(nam, registry, NO_DESCRIPTION);

  }
  
  
  /**
   * Increment the metrics for numOps operations
   * @param numOps - number of operations
   * @param time - time for numOps operations
   */
  public synchronized void inc(@OsUntrusted MetricsTimeVaryingRate this, final @OsUntrusted int numOps, final @OsUntrusted long time) {
    currentData.numOperations += numOps;
    currentData.time += time;
    @OsUntrusted
    long timePerOps = time/numOps;
    minMax.update(timePerOps);
  }
  
  /**
   * Increment the metrics for one operation
   * @param time for one operation
   */
  public synchronized void inc(@OsUntrusted MetricsTimeVaryingRate this, final @OsUntrusted long time) {
    currentData.numOperations++;
    currentData.time += time;
    minMax.update(time);
  }
  
  

  private synchronized void intervalHeartBeat(@OsUntrusted MetricsTimeVaryingRate this) {
     previousIntervalData.numOperations = currentData.numOperations;
     previousIntervalData.time = (currentData.numOperations == 0) ?
                             0 : currentData.time / currentData.numOperations;
     currentData.reset();
  }
  
  /**
   * Push the delta  metrics to the mr.
   * The delta is since the last push/interval.
   * 
   * Note this does NOT push to JMX
   * (JMX gets the info via {@link #getPreviousIntervalAverageTime()} and
   * {@link #getPreviousIntervalNumOps()}
   *
   * @param mr
   */
  @Override
  public synchronized void pushMetric(@OsUntrusted MetricsTimeVaryingRate this, final @OsUntrusted MetricsRecord mr) {
    intervalHeartBeat();
    try {
      mr.incrMetric(getName() + "_num_ops", getPreviousIntervalNumOps());
      mr.setMetric(getName() + "_avg_time", getPreviousIntervalAverageTime());
    } catch (@OsUntrusted Exception e) {
      LOG.info("pushMetric failed for " + getName() + "\n" , e);
    }
  }
  
  /**
   * The number of operations in the previous interval
   * @return - ops in prev interval
   */
  public synchronized @OsUntrusted int getPreviousIntervalNumOps(@OsUntrusted MetricsTimeVaryingRate this) { 
    return previousIntervalData.numOperations;
  }
  
  /**
   * The average rate of an operation in the previous interval
   * @return - the average rate.
   */
  public synchronized @OsUntrusted long getPreviousIntervalAverageTime(@OsUntrusted MetricsTimeVaryingRate this) {
    return previousIntervalData.time;
  } 
  
  /**
   * The min time for a single operation since the last reset
   *  {@link #resetMinMax()}
   * @return min time for an operation
   */
  public synchronized @OsUntrusted long getMinTime(@OsUntrusted MetricsTimeVaryingRate this) {
    return  minMax.minTime;
  }
  
  /**
   * The max time for a single operation since the last reset
   *  {@link #resetMinMax()}
   * @return max time for an operation
   */
  public synchronized @OsUntrusted long getMaxTime(@OsUntrusted MetricsTimeVaryingRate this) {
    return minMax.maxTime;
  }
  
  /**
   * Reset the min max values
   */
  public synchronized void resetMinMax(@OsUntrusted MetricsTimeVaryingRate this) {
    minMax.reset();
  }
}
