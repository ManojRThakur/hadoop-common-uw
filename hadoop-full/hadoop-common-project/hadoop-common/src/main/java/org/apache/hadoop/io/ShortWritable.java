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

/** A WritableComparable for shorts. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ShortWritable implements @OsUntrusted WritableComparable<@OsUntrusted ShortWritable> {
  private @OsUntrusted short value;

  public @OsUntrusted ShortWritable() {
  }

  public @OsUntrusted ShortWritable(@OsUntrusted short value) {
    set(value);
  }

  /** Set the value of this ShortWritable. */
  public void set(@OsUntrusted ShortWritable this, @OsUntrusted short value) {
    this.value = value;
  }

  /** Return the value of this ShortWritable. */
  public @OsUntrusted short get(@OsUntrusted ShortWritable this) {
    return value;
  }

  /** read the short value */
  @Override
  public void readFields(@OsUntrusted ShortWritable this, @OsUntrusted DataInput in) throws IOException {
    value = in.readShort();
  }

  /** write short value */
  @Override
  public void write(@OsUntrusted ShortWritable this, @OsUntrusted DataOutput out) throws IOException {
    out.writeShort(value);
  }

  /** Returns true iff <code>o</code> is a ShortWritable with the same value. */
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted ShortWritable this, @OsUntrusted Object o) {
    if (!(o instanceof @OsUntrusted ShortWritable))
      return false;
    @OsUntrusted
    ShortWritable other = (@OsUntrusted ShortWritable) o;
    return this.value == other.value;
  }

  /** hash code */
  @Override
  public @OsUntrusted int hashCode(@OsUntrusted ShortWritable this) {
    return value;
  }

  /** Compares two ShortWritable. */
  @Override
  public @OsUntrusted int compareTo(@OsUntrusted ShortWritable this, @OsUntrusted ShortWritable o) {
    @OsUntrusted
    short thisValue = this.value;
    @OsUntrusted
    short thatValue = (o).value;
    return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
  }

  /** Short values in string format */
  @Override
  public @OsUntrusted String toString(@OsUntrusted ShortWritable this) {
    return Short.toString(value);
  }

  /** A Comparator optimized for ShortWritable. */
  public static class Comparator extends @OsUntrusted WritableComparator {

    public @OsUntrusted Comparator() {
      super(ShortWritable.class);
    }
    
    @Override
    public @OsUntrusted int compare(ShortWritable.@OsUntrusted Comparator this, @OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1, @OsUntrusted byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2) {
      @OsUntrusted
      short thisValue = (@OsUntrusted short) readUnsignedShort(b1, s1);
      @OsUntrusted
      short thatValue = (@OsUntrusted short) readUnsignedShort(b2, s2);
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  static { // register this comparator
    WritableComparator.define(ShortWritable.class, new @OsUntrusted Comparator());
  }

}
