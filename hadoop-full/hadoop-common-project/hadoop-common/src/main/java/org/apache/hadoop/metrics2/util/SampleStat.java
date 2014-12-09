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

package org.apache.hadoop.metrics2.util;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Helper to compute running sample stats
 */
@InterfaceAudience.Private
public class SampleStat {
  private final @OsUntrusted MinMax minmax = new @OsUntrusted MinMax();
  private @OsUntrusted long numSamples = 0;
  private @OsUntrusted double a0;
  private @OsUntrusted double a1;
  private @OsUntrusted double s0;
  private @OsUntrusted double s1;

  /**
   * Construct a new running sample stat
   */
  public @OsUntrusted SampleStat() {
    a0 = s0 = 0.0;
  }

  public void reset(@OsUntrusted SampleStat this) {
    numSamples = 0;
    a0 = s0 = 0.0;
    minmax.reset();
  }

  // We want to reuse the object, sometimes.
  void reset(@OsUntrusted SampleStat this, @OsUntrusted long numSamples, @OsUntrusted double a0, @OsUntrusted double a1, @OsUntrusted double s0, @OsUntrusted double s1,
             @OsUntrusted
             MinMax minmax) {
    this.numSamples = numSamples;
    this.a0 = a0;
    this.a1 = a1;
    this.s0 = s0;
    this.s1 = s1;
    this.minmax.reset(minmax);
  }

  /**
   * Copy the values to other (saves object creation and gc.)
   * @param other the destination to hold our values
   */
  public void copyTo(@OsUntrusted SampleStat this, @OsUntrusted SampleStat other) {
    other.reset(numSamples, a0, a1, s0, s1, minmax);
  }

  /**
   * Add a sample the running stat.
   * @param x the sample number
   * @return  self
   */
  public @OsUntrusted SampleStat add(@OsUntrusted SampleStat this, @OsUntrusted double x) {
    minmax.add(x);
    return add(1, x);
  }

  /**
   * Add some sample and a partial sum to the running stat.
   * Note, min/max is not evaluated using this method.
   * @param nSamples  number of samples
   * @param x the partial sum
   * @return  self
   */
  public @OsUntrusted SampleStat add(@OsUntrusted SampleStat this, @OsUntrusted long nSamples, @OsUntrusted double x) {
    numSamples += nSamples;

    if (numSamples == 1) {
      a0 = a1 = x;
      s0 = 0.0;
    }
    else {
      // The Welford method for numerical stability
      a1 = a0 + (x - a0) / numSamples;
      s1 = s0 + (x - a0) * (x - a1);
      a0 = a1;
      s0 = s1;
    }
    return this;
  }

  /**
   * @return  the total number of samples
   */
  public @OsUntrusted long numSamples(@OsUntrusted SampleStat this) {
    return numSamples;
  }

  /**
   * @return  the arithmetic mean of the samples
   */
  public @OsUntrusted double mean(@OsUntrusted SampleStat this) {
    return numSamples > 0 ? a1 : 0.0;
  }

  /**
   * @return  the variance of the samples
   */
  public @OsUntrusted double variance(@OsUntrusted SampleStat this) {
    return numSamples > 1 ? s1 / (numSamples - 1) : 0.0;
  }

  /**
   * @return  the standard deviation of the samples
   */
  public @OsUntrusted double stddev(@OsUntrusted SampleStat this) {
    return Math.sqrt(variance());
  }

  /**
   * @return  the minimum value of the samples
   */
  public @OsUntrusted double min(@OsUntrusted SampleStat this) {
    return minmax.min();
  }

  /**
   * @return  the maximum value of the samples
   */
  public @OsUntrusted double max(@OsUntrusted SampleStat this) {
    return minmax.max();
  }

  /**
   * Helper to keep running min/max
   */
  @SuppressWarnings("PublicInnerClass")
  public static class MinMax {

    // Float.MAX_VALUE is used rather than Double.MAX_VALUE, even though the
    // min and max variables are of type double.
    // Float.MAX_VALUE is big enough, and using Double.MAX_VALUE makes 
    // Ganglia core due to buffer overflow.
    // The same reasoning applies to the MIN_VALUE counterparts.
    static final @OsUntrusted double DEFAULT_MIN_VALUE = Float.MAX_VALUE;
    static final @OsUntrusted double DEFAULT_MAX_VALUE = Float.MIN_VALUE;

    private @OsUntrusted double min = DEFAULT_MIN_VALUE;
    private @OsUntrusted double max = DEFAULT_MAX_VALUE;

    public void add(SampleStat.@OsUntrusted MinMax this, @OsUntrusted double value) {
      if (value > max) max = value;
      if (value < min) min = value;
    }

    public @OsUntrusted double min(SampleStat.@OsUntrusted MinMax this) { return min; }
    public @OsUntrusted double max(SampleStat.@OsUntrusted MinMax this) { return max; }

    public void reset(SampleStat.@OsUntrusted MinMax this) {
      min = DEFAULT_MIN_VALUE;
      max = DEFAULT_MAX_VALUE;
    }

    public void reset(SampleStat.@OsUntrusted MinMax this, @OsUntrusted MinMax other) {
      min = other.min();
      max = other.max();
    }
  }
}
