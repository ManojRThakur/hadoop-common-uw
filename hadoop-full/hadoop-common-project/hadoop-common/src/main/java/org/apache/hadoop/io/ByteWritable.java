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

/** A WritableComparable for a single byte. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ByteWritable implements @OsUntrusted WritableComparable<@OsUntrusted ByteWritable> {
  private @OsUntrusted byte value;

  public @OsUntrusted ByteWritable() {}

  public @OsUntrusted ByteWritable(@OsUntrusted byte value) { set(value); }

  /** Set the value of this ByteWritable. */
  public void set(@OsUntrusted ByteWritable this, @OsUntrusted byte value) { this.value = value; }

  /** Return the value of this ByteWritable. */
  public @OsUntrusted byte get(@OsUntrusted ByteWritable this) { return value; }

  @Override
  public void readFields(@OsUntrusted ByteWritable this, @OsUntrusted DataInput in) throws IOException {
    value = in.readByte();
  }

  @Override
  public void write(@OsUntrusted ByteWritable this, @OsUntrusted DataOutput out) throws IOException {
    out.writeByte(value);
  }

  /** Returns true iff <code>o</code> is a ByteWritable with the same value. */
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted ByteWritable this, @OsUntrusted Object o) {
    if (!(o instanceof @OsUntrusted ByteWritable)) {
      return false;
    }
    @OsUntrusted
    ByteWritable other = (@OsUntrusted ByteWritable)o;
    return this.value == other.value;
  }

  @Override
  public @OsUntrusted int hashCode(@OsUntrusted ByteWritable this) {
    return (@OsUntrusted int)value;
  }

  /** Compares two ByteWritables. */
  @Override
  public @OsUntrusted int compareTo(@OsUntrusted ByteWritable this, @OsUntrusted ByteWritable o) {
    @OsUntrusted
    int thisValue = this.value;
    @OsUntrusted
    int thatValue = o.value;
    return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
  }

  @Override
  public @OsUntrusted String toString(@OsUntrusted ByteWritable this) {
    return Byte.toString(value);
  }

  /** A Comparator optimized for ByteWritable. */ 
  public static class Comparator extends @OsUntrusted WritableComparator {
    public @OsUntrusted Comparator() {
      super(ByteWritable.class);
    }

    @Override
    public @OsUntrusted int compare(ByteWritable.@OsUntrusted Comparator this, @OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1,
                       @OsUntrusted
                       byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2) {
      @OsUntrusted
      byte thisValue = b1[s1];
      @OsUntrusted
      byte thatValue = b2[s2];
      return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(ByteWritable.class, new @OsUntrusted Comparator());
  }
}

