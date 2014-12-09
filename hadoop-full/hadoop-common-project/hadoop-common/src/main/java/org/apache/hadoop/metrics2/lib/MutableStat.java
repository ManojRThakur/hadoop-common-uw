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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.util.SampleStat;
import static org.apache.hadoop.metrics2.lib.Interns.*;

/**
 * A mutable metric with stats.
 *
 * Useful for keeping throughput/latency stats.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableStat extends @OsUntrusted MutableMetric {
  private final @OsUntrusted MetricsInfo numInfo;
  private final @OsUntrusted MetricsInfo avgInfo;
  private final @OsUntrusted MetricsInfo stdevInfo;
  private final @OsUntrusted MetricsInfo iMinInfo;
  private final @OsUntrusted MetricsInfo iMaxInfo;
  private final @OsUntrusted MetricsInfo minInfo;
  private final @OsUntrusted MetricsInfo maxInfo;

  private final @OsUntrusted SampleStat intervalStat = new @OsUntrusted SampleStat();
  private final @OsUntrusted SampleStat prevStat = new @OsUntrusted SampleStat();
  private final SampleStat.@OsUntrusted MinMax minMax = new SampleStat.@OsUntrusted MinMax();
  private @OsUntrusted long numSamples = 0;
  private @OsUntrusted boolean extended = false;

  /**
   * Construct a sample statistics metric
   * @param name        of the metric
   * @param description of the metric
   * @param sampleName  of the metric (e.g. "Ops")
   * @param valueName   of the metric (e.g. "Time", "Latency")
   * @param extended    create extended stats (stdev, min/max etc.) by default.
   */
  public @OsUntrusted MutableStat(@OsUntrusted String name, @OsUntrusted String description,
                     @OsUntrusted
                     String sampleName, @OsUntrusted String valueName, @OsUntrusted boolean extended) {
    @OsUntrusted
    String ucName = StringUtils.capitalize(name);
    @OsUntrusted
    String usName = StringUtils.capitalize(sampleName);
    @OsUntrusted
    String uvName = StringUtils.capitalize(valueName);
    @OsUntrusted
    String desc = StringUtils.uncapitalize(description);
    @OsUntrusted
    String lsName = StringUtils.uncapitalize(sampleName);
    @OsUntrusted
    String lvName = StringUtils.uncapitalize(valueName);
    numInfo = info(ucName +"Num"+ usName, "Number of "+ lsName +" for "+ desc);
    avgInfo = info(ucName +"Avg"+ uvName, "Average "+ lvName +" for "+ desc);
    stdevInfo = info(ucName +"Stdev"+ uvName,
                     "Standard deviation of "+ lvName +" for "+ desc);
    iMinInfo = info(ucName +"IMin"+ uvName,
                    "Interval min "+ lvName +" for "+ desc);
    iMaxInfo = info(ucName + "IMax"+ uvName,
                    "Interval max "+ lvName +" for "+ desc);
    minInfo = info(ucName +"Min"+ uvName, "Min "+ lvName +" for "+ desc);
    maxInfo = info(ucName +"Max"+ uvName, "Max "+ lvName +" for "+ desc);
    this.extended = extended;
  }

  /**
   * Construct a snapshot stat metric with extended stat off by default
   * @param name        of the metric
   * @param description of the metric
   * @param sampleName  of the metric (e.g. "Ops")
   * @param valueName   of the metric (e.g. "Time", "Latency")
   */
  public @OsUntrusted MutableStat(@OsUntrusted String name, @OsUntrusted String description,
                     @OsUntrusted
                     String sampleName, @OsUntrusted String valueName) {
    this(name, description, sampleName, valueName, false);
  }

  /**
   * Add a number of samples and their sum to the running stat
   * @param numSamples  number of samples
   * @param sum of the samples
   */
  public synchronized void add(@OsUntrusted MutableStat this, @OsUntrusted long numSamples, @OsUntrusted long sum) {
    intervalStat.add(numSamples, sum);
    setChanged();
  }

  /**
   * Add a snapshot to the metric
   * @param value of the metric
   */
  public synchronized void add(@OsUntrusted MutableStat this, @OsUntrusted long value) {
    intervalStat.add(value);
    minMax.add(value);
    setChanged();
  }

  @Override
  public synchronized void snapshot(@OsUntrusted MutableStat this, @OsUntrusted MetricsRecordBuilder builder, @OsUntrusted boolean all) {
    if (all || changed()) {
      numSamples += intervalStat.numSamples();
      builder.addCounter(numInfo, numSamples)
             .addGauge(avgInfo, lastStat().mean());
      if (extended) {
        builder.addGauge(stdevInfo, lastStat().stddev())
               .addGauge(iMinInfo, lastStat().min())
               .addGauge(iMaxInfo, lastStat().max())
               .addGauge(minInfo, minMax.min())
               .addGauge(maxInfo, minMax.max());
      }
      if (changed()) {
        if (numSamples > 0) {
          intervalStat.copyTo(prevStat);
          intervalStat.reset();
        }
        clearChanged();
      }
    }
  }

  private @OsUntrusted SampleStat lastStat(@OsUntrusted MutableStat this) {
    return changed() ? intervalStat : prevStat;
  }

  /**
   * Reset the all time min max of the metric
   */
  public void resetMinMax(@OsUntrusted MutableStat this) {
    minMax.reset();
  }

}
