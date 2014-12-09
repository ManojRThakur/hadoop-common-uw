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

/** A WritableComparable for longs in a variable-length format. Such values take
 *  between one and five bytes.  Smaller values take fewer bytes.
 *  
 *  @see org.apache.hadoop.io.WritableUtils#readVLong(DataInput)
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class VLongWritable implements @OsUntrusted WritableComparable<@OsUntrusted VLongWritable> {
  private @OsUntrusted long value;

  public @OsUntrusted VLongWritable() {}

  public @OsUntrusted VLongWritable(@OsUntrusted long value) { set(value); }

  /** Set the value of this LongWritable. */
  public void set(@OsUntrusted VLongWritable this, @OsUntrusted long value) { this.value = value; }

  /** Return the value of this LongWritable. */
  public @OsUntrusted long get(@OsUntrusted VLongWritable this) { return value; }

  @Override
  public void readFields(@OsUntrusted VLongWritable this, @OsUntrusted DataInput in) throws IOException {
    value = WritableUtils.readVLong(in);
  }

  @Override
  public void write(@OsUntrusted VLongWritable this, @OsUntrusted DataOutput out) throws IOException {
    WritableUtils.writeVLong(out, value);
  }

  /** Returns true iff <code>o</code> is a VLongWritable with the same value. */
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted VLongWritable this, @OsUntrusted Object o) {
    if (!(o instanceof @OsUntrusted VLongWritable))
      return false;
    @OsUntrusted
    VLongWritable other = (@OsUntrusted VLongWritable)o;
    return this.value == other.value;
  }

  @Override
  public @OsUntrusted int hashCode(@OsUntrusted VLongWritable this) {
    return (@OsUntrusted int)value;
  }

  /** Compares two VLongWritables. */
  @Override
  public @OsUntrusted int compareTo(@OsUntrusted VLongWritable this, @OsUntrusted VLongWritable o) {
    @OsUntrusted
    long thisValue = this.value;
    @OsUntrusted
    long thatValue = o.value;
    return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
  }

  @Override
  public @OsUntrusted String toString(@OsUntrusted VLongWritable this) {
    return Long.toString(value);
  }

}

