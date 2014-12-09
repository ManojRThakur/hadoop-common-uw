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
import java.io.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** A WritableComparable for floats. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FloatWritable implements @OsUntrusted WritableComparable<@OsUntrusted FloatWritable> {
  private @OsUntrusted float value;

  public @OsUntrusted FloatWritable() {}

  public @OsUntrusted FloatWritable(@OsUntrusted float value) { set(value); }

  /** Set the value of this FloatWritable. */
  public void set(@OsUntrusted FloatWritable this, @OsUntrusted float value) { this.value = value; }

  /** Return the value of this FloatWritable. */
  public @OsUntrusted float get(@OsUntrusted FloatWritable this) { return value; }

  @Override
  public void readFields(@OsUntrusted FloatWritable this, @OsUntrusted DataInput in) throws IOException {
    value = in.readFloat();
  }

  @Override
  public void write(@OsUntrusted FloatWritable this, @OsUntrusted DataOutput out) throws IOException {
    out.writeFloat(value);
  }

  /** Returns true iff <code>o</code> is a FloatWritable with the same value. */
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted FloatWritable this, @OsUntrusted Object o) {
    if (!(o instanceof @OsUntrusted FloatWritable))
      return false;
    @OsUntrusted
    FloatWritable other = (@OsUntrusted FloatWritable)o;
    return this.value == other.value;
  }

  @Override
  public @OsUntrusted int hashCode(@OsUntrusted FloatWritable this) {
    return Float.floatToIntBits(value);
  }

  /** Compares two FloatWritables. */
  @Override
  public @OsUntrusted int compareTo(@OsUntrusted FloatWritable this, @OsUntrusted FloatWritable o) {
    @OsUntrusted
    float thisValue = this.value;
    @OsUntrusted
    float thatValue = o.value;
    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
  }

  @Override
  public @OsUntrusted String toString(@OsUntrusted FloatWritable this) {
    return Float.toString(value);
  }

  /** A Comparator optimized for FloatWritable. */ 
  public static class Comparator extends @OsUntrusted WritableComparator {
    public @OsUntrusted Comparator() {
      super(FloatWritable.class);
    }
    @Override
    public @OsUntrusted int compare(FloatWritable.@OsUntrusted Comparator this, @OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1,
                       @OsUntrusted
                       byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2) {
      @OsUntrusted
      float thisValue = readFloat(b1, s1);
      @OsUntrusted
      float thatValue = readFloat(b2, s2);
      return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(FloatWritable.class, new @OsUntrusted Comparator());
  }

}

