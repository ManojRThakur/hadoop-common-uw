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

import com.google.common.collect.ComparisonChain;

/**
 * Specifies a quantile (with error bounds) to be watched by a
 * {@link SampleQuantiles} object.
 */
@InterfaceAudience.Private
public class Quantile implements @OsUntrusted Comparable<@OsUntrusted Quantile> {
  public final @OsUntrusted double quantile;
  public final @OsUntrusted double error;

  public @OsUntrusted Quantile(@OsUntrusted double quantile, @OsUntrusted double error) {
    this.quantile = quantile;
    this.error = error;
  }

  @Override
  public @OsUntrusted boolean equals(@OsUntrusted Quantile this, @OsUntrusted Object aThat) {
    if (this == aThat) {
      return true;
    }
    if (!(aThat instanceof @OsUntrusted Quantile)) {
      return false;
    }

    @OsUntrusted
    Quantile that = (@OsUntrusted Quantile) aThat;

    @OsUntrusted
    long qbits = Double.doubleToLongBits(quantile);
    @OsUntrusted
    long ebits = Double.doubleToLongBits(error);

    return qbits == Double.doubleToLongBits(that.quantile)
        && ebits == Double.doubleToLongBits(that.error);
  }

  @Override
  public @OsUntrusted int hashCode(@OsUntrusted Quantile this) {
    return (@OsUntrusted int) (Double.doubleToLongBits(quantile) ^ Double
        .doubleToLongBits(error));
  }

  @Override
  public @OsUntrusted int compareTo(@OsUntrusted Quantile this, @OsUntrusted Quantile other) {
    return ComparisonChain.start()
        .compare(quantile, other.quantile)
        .compare(error, other.error)
        .result();
  }
  
  @Override
  public @OsUntrusted String toString(@OsUntrusted Quantile this) {
    return String.format("%.2f %%ile +/- %.2f%%",
        quantile * 100, error * 100);
  }

}