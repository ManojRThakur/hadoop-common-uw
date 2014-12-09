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

package org.apache.hadoop.io;

import ostrusted.quals.OsUntrusted;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Writable for Double values.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DoubleWritable implements @OsUntrusted WritableComparable<@OsUntrusted DoubleWritable> {

  private @OsUntrusted double value = 0.0;
  
  public @OsUntrusted DoubleWritable() {
    
  }
  
  public @OsUntrusted DoubleWritable(@OsUntrusted double value) {
    set(value);
  }
  
  @Override
  public void readFields(@OsUntrusted DoubleWritable this, @OsUntrusted DataInput in) throws IOException {
    value = in.readDouble();
  }

  @Override
  public void write(@OsUntrusted DoubleWritable this, @OsUntrusted DataOutput out) throws IOException {
    out.writeDouble(value);
  }
  
  public void set(@OsUntrusted DoubleWritable this, @OsUntrusted double value) { this.value = value; }
  
  public @OsUntrusted double get(@OsUntrusted DoubleWritable this) { return value; }

  /**
   * Returns true iff <code>o</code> is a DoubleWritable with the same value.
   */
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted DoubleWritable this, @OsUntrusted Object o) {
    if (!(o instanceof @OsUntrusted DoubleWritable)) {
      return false;
    }
    @OsUntrusted
    DoubleWritable other = (@OsUntrusted DoubleWritable)o;
    return this.value == other.value;
  }
  
  @Override
  public @OsUntrusted int hashCode(@OsUntrusted DoubleWritable this) {
    return (@OsUntrusted int)Double.doubleToLongBits(value);
  }
  
  @Override
  public @OsUntrusted int compareTo(@OsUntrusted DoubleWritable this, @OsUntrusted DoubleWritable o) {
    return (value < o.value ? -1 : (value == o.value ? 0 : 1));
  }
  
  @Override
  public @OsUntrusted String toString(@OsUntrusted DoubleWritable this) {
    return Double.toString(value);
  }

  /** A Comparator optimized for DoubleWritable. */ 
  public static class Comparator extends @OsUntrusted WritableComparator {
    public @OsUntrusted Comparator() {
      super(DoubleWritable.class);
    }

    @Override
    public @OsUntrusted int compare(DoubleWritable.@OsUntrusted Comparator this, @OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1,
                       @OsUntrusted
                       byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2) {
      @OsUntrusted
      double thisValue = readDouble(b1, s1);
      @OsUntrusted
      double thatValue = readDouble(b2, s2);
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(DoubleWritable.class, new @OsUntrusted Comparator());
  }

}

