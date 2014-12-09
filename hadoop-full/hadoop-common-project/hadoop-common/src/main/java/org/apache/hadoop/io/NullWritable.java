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

/** Singleton Writable with no data. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class NullWritable implements @OsUntrusted WritableComparable<@OsUntrusted NullWritable> {

  private static final @OsUntrusted NullWritable THIS = new @OsUntrusted NullWritable();

  private @OsUntrusted NullWritable() {}                       // no public ctor

  /** Returns the single instance of this class. */
  public static @OsUntrusted NullWritable get() { return THIS; }
  
  @Override
  public @OsUntrusted String toString(@OsUntrusted NullWritable this) {
    return "(null)";
  }

  @Override
  public @OsUntrusted int hashCode(@OsUntrusted NullWritable this) { return 0; }
  
  @Override
  public @OsUntrusted int compareTo(@OsUntrusted NullWritable this, @OsUntrusted NullWritable other) {
    return 0;
  }
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted NullWritable this, @OsUntrusted Object other) { return other instanceof @OsUntrusted NullWritable; }
  @Override
  public void readFields(@OsUntrusted NullWritable this, @OsUntrusted DataInput in) throws IOException {}
  @Override
  public void write(@OsUntrusted NullWritable this, @OsUntrusted DataOutput out) throws IOException {}

  /** A Comparator &quot;optimized&quot; for NullWritable. */
  public static class Comparator extends @OsUntrusted WritableComparator {
    public @OsUntrusted Comparator() {
      super(NullWritable.class);
    }

    /**
     * Compare the buffers in serialized form.
     */
    @Override
    public @OsUntrusted int compare(NullWritable.@OsUntrusted Comparator this, @OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1,
                       @OsUntrusted
                       byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2) {
      assert 0 == l1;
      assert 0 == l2;
      return 0;
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(NullWritable.class, new @OsUntrusted Comparator());
  }
}

