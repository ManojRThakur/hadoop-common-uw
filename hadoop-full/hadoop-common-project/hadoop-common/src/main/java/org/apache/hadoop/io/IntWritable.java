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

/** A WritableComparable for ints. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class IntWritable implements @OsUntrusted WritableComparable<@OsUntrusted IntWritable> {
  private @OsUntrusted int value;

  public @OsUntrusted IntWritable() {}

  public @OsUntrusted IntWritable(@OsUntrusted int value) { set(value); }

  /** Set the value of this IntWritable. */
  public void set(@OsUntrusted IntWritable this, @OsUntrusted int value) { this.value = value; }

  /** Return the value of this IntWritable. */
  public @OsUntrusted int get(@OsUntrusted IntWritable this) { return value; }

  @Override
  public void readFields(@OsUntrusted IntWritable this, @OsUntrusted DataInput in) throws IOException {
    value = in.readInt();
  }

  @Override
  public void write(@OsUntrusted IntWritable this, @OsUntrusted DataOutput out) throws IOException {
    out.writeInt(value);
  }

  /** Returns true iff <code>o</code> is a IntWritable with the same value. */
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted IntWritable this, @OsUntrusted Object o) {
    if (!(o instanceof @OsUntrusted IntWritable))
      return false;
    @OsUntrusted
    IntWritable other = (@OsUntrusted IntWritable)o;
    return this.value == other.value;
  }

  @Override
  public @OsUntrusted int hashCode(@OsUntrusted IntWritable this) {
    return value;
  }

  /** Compares two IntWritables. */
  @Override
  public @OsUntrusted int compareTo(@OsUntrusted IntWritable this, @OsUntrusted IntWritable o) {
    @OsUntrusted
    int thisValue = this.value;
    @OsUntrusted
    int thatValue = o.value;
    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
  }

  @Override
  public @OsUntrusted String toString(@OsUntrusted IntWritable this) {
    return Integer.toString(value);
  }

  /** A Comparator optimized for IntWritable. */ 
  public static class Comparator extends @OsUntrusted WritableComparator {
    public @OsUntrusted Comparator() {
      super(IntWritable.class);
    }
    
    @Override
    public @OsUntrusted int compare(IntWritable.@OsUntrusted Comparator this, @OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1,
                       @OsUntrusted
                       byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2) {
      @OsUntrusted
      int thisValue = readInt(b1, s1);
      @OsUntrusted
      int thatValue = readInt(b2, s2);
      return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(IntWritable.class, new @OsUntrusted Comparator());
  }
}

