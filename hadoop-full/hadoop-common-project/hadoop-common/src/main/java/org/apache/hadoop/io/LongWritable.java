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

/** A WritableComparable for longs. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LongWritable implements @OsUntrusted WritableComparable<@OsUntrusted LongWritable> {
  private @OsUntrusted long value;

  public @OsUntrusted LongWritable() {}

  public @OsUntrusted LongWritable(@OsUntrusted long value) { set(value); }

  /** Set the value of this LongWritable. */
  public void set(@OsUntrusted LongWritable this, @OsUntrusted long value) { this.value = value; }

  /** Return the value of this LongWritable. */
  public @OsUntrusted long get(@OsUntrusted LongWritable this) { return value; }

  @Override
  public void readFields(@OsUntrusted LongWritable this, @OsUntrusted DataInput in) throws IOException {
    value = in.readLong();
  }

  @Override
  public void write(@OsUntrusted LongWritable this, @OsUntrusted DataOutput out) throws IOException {
    out.writeLong(value);
  }

  /** Returns true iff <code>o</code> is a LongWritable with the same value. */
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted LongWritable this, @OsUntrusted Object o) {
    if (!(o instanceof @OsUntrusted LongWritable))
      return false;
    @OsUntrusted
    LongWritable other = (@OsUntrusted LongWritable)o;
    return this.value == other.value;
  }

  @Override
  public @OsUntrusted int hashCode(@OsUntrusted LongWritable this) {
    return (@OsUntrusted int)value;
  }

  /** Compares two LongWritables. */
  @Override
  public @OsUntrusted int compareTo(@OsUntrusted LongWritable this, @OsUntrusted LongWritable o) {
    @OsUntrusted
    long thisValue = this.value;
    @OsUntrusted
    long thatValue = o.value;
    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
  }

  @Override
  public @OsUntrusted String toString(@OsUntrusted LongWritable this) {
    return Long.toString(value);
  }

  /** A Comparator optimized for LongWritable. */ 
  public static class Comparator extends @OsUntrusted WritableComparator {
    public @OsUntrusted Comparator() {
      super(LongWritable.class);
    }

    @Override
    public @OsUntrusted int compare(LongWritable.@OsUntrusted Comparator this, @OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1,
                       @OsUntrusted
                       byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2) {
      @OsUntrusted
      long thisValue = readLong(b1, s1);
      @OsUntrusted
      long thatValue = readLong(b2, s2);
      return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
  }

  /** A decreasing Comparator optimized for LongWritable. */ 
  public static class DecreasingComparator extends @OsUntrusted Comparator {
    
    @Override
    public @OsUntrusted int compare(LongWritable.@OsUntrusted DecreasingComparator this, @OsUntrusted WritableComparable a, @OsUntrusted WritableComparable b) {
      return -super.compare(a, b);
    }
    @Override
    public @OsUntrusted int compare(LongWritable.@OsUntrusted DecreasingComparator this, @OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1, @OsUntrusted byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2) {
      return -super.compare(b1, s1, l1, b2, s2, l2);
    }
  }

  static {                                       // register default comparator
    WritableComparator.define(LongWritable.class, new @OsUntrusted Comparator());
  }

}

