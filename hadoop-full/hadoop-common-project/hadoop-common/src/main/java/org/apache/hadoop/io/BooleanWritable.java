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

/** 
 * A WritableComparable for booleans. 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BooleanWritable implements @OsUntrusted WritableComparable<@OsUntrusted BooleanWritable> {
  private @OsUntrusted boolean value;

  /** 
   */
  public @OsUntrusted BooleanWritable() {};

  /** 
   */
  public @OsUntrusted BooleanWritable(@OsUntrusted boolean value) {
    set(value);
  }

  /** 
   * Set the value of the BooleanWritable
   */    
  public void set(@OsUntrusted BooleanWritable this, @OsUntrusted boolean value) {
    this.value = value;
  }

  /**
   * Returns the value of the BooleanWritable
   */
  public @OsUntrusted boolean get(@OsUntrusted BooleanWritable this) {
    return value;
  }

  /**
   */
  @Override
  public void readFields(@OsUntrusted BooleanWritable this, @OsUntrusted DataInput in) throws IOException {
    value = in.readBoolean();
  }

  /**
   */
  @Override
  public void write(@OsUntrusted BooleanWritable this, @OsUntrusted DataOutput out) throws IOException {
    out.writeBoolean(value);
  }

  /**
   */
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted BooleanWritable this, @OsUntrusted Object o) {
    if (!(o instanceof @OsUntrusted BooleanWritable)) {
      return false;
    }
    @OsUntrusted
    BooleanWritable other = (@OsUntrusted BooleanWritable) o;
    return this.value == other.value;
  }

  @Override
  public @OsUntrusted int hashCode(@OsUntrusted BooleanWritable this) {
    return value ? 0 : 1;
  }



  /**
   */
  @Override
  public @OsUntrusted int compareTo(@OsUntrusted BooleanWritable this, @OsUntrusted BooleanWritable o) {
    @OsUntrusted
    boolean a = this.value;
    @OsUntrusted
    boolean b = o.value;
    return ((a == b) ? 0 : (a == false) ? -1 : 1);
  }
  
  @Override
  public @OsUntrusted String toString(@OsUntrusted BooleanWritable this) {
    return Boolean.toString(get());
  }

  /** 
   * A Comparator optimized for BooleanWritable. 
   */ 
  public static class Comparator extends @OsUntrusted WritableComparator {
    public @OsUntrusted Comparator() {
      super(BooleanWritable.class);
    }

    @Override
    public @OsUntrusted int compare(BooleanWritable.@OsUntrusted Comparator this, @OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1,
                       @OsUntrusted
                       byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2) {
      return compareBytes(b1, s1, l1, b2, s2, l2);
    }
  }


  static {
    WritableComparator.define(BooleanWritable.class, new @OsUntrusted Comparator());
  }
}
