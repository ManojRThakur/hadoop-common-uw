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
import java.lang.reflect.Array;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** A Writable for 2D arrays containing a matrix of instances of a class. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TwoDArrayWritable implements @OsUntrusted Writable {
  private @OsUntrusted Class valueClass;
  private @OsUntrusted Writable @OsUntrusted [] @OsUntrusted [] values;

  public @OsUntrusted TwoDArrayWritable(@OsUntrusted Class valueClass) {
    this.valueClass = valueClass;
  }

  public @OsUntrusted TwoDArrayWritable(@OsUntrusted Class valueClass, @OsUntrusted Writable @OsUntrusted [] @OsUntrusted [] values) {
    this(valueClass);
    this.values = values;
  }

  public @OsUntrusted Object toArray(@OsUntrusted TwoDArrayWritable this) {
    @OsUntrusted
    int dimensions @OsUntrusted [] = new int @OsUntrusted [] {values.length, 0};
    @OsUntrusted
    Object result = Array.newInstance(valueClass, dimensions);
    for (@OsUntrusted int i = 0; i < values.length; i++) {
      @OsUntrusted
      Object resultRow = Array.newInstance(valueClass, values[i].length);
      Array.set(result, i, resultRow);
      for (@OsUntrusted int j = 0; j < values[i].length; j++) {
        Array.set(resultRow, j, values[i][j]);
      }
    }
    return result;
  }

  public void set(@OsUntrusted TwoDArrayWritable this, @OsUntrusted Writable @OsUntrusted [] @OsUntrusted [] values) { this.values = values; }

  public @OsUntrusted Writable @OsUntrusted [] @OsUntrusted [] get(@OsUntrusted TwoDArrayWritable this) { return values; }

  @Override
  public void readFields(@OsUntrusted TwoDArrayWritable this, @OsUntrusted DataInput in) throws IOException {
    // construct matrix
    values = new @OsUntrusted Writable @OsUntrusted [in.readInt()] @OsUntrusted [];          
    for (@OsUntrusted int i = 0; i < values.length; i++) {
      values[i] = new @OsUntrusted Writable @OsUntrusted [in.readInt()];
    }

    // construct values
    for (@OsUntrusted int i = 0; i < values.length; i++) {
      for (@OsUntrusted int j = 0; j < values[i].length; j++) {
        @OsUntrusted
        Writable value;                             // construct value
        try {
          value = (@OsUntrusted Writable)valueClass.newInstance();
        } catch (@OsUntrusted InstantiationException e) {
          throw new @OsUntrusted RuntimeException(e.toString());
        } catch (@OsUntrusted IllegalAccessException e) {
          throw new @OsUntrusted RuntimeException(e.toString());
        }
        value.readFields(in);                       // read a value
        values[i][j] = value;                       // store it in values
      }
    }
  }

  @Override
  public void write(@OsUntrusted TwoDArrayWritable this, @OsUntrusted DataOutput out) throws IOException {
    out.writeInt(values.length);                 // write values
    for (@OsUntrusted int i = 0; i < values.length; i++) {
      out.writeInt(values[i].length);
    }
    for (@OsUntrusted int i = 0; i < values.length; i++) {
      for (@OsUntrusted int j = 0; j < values[i].length; j++) {
        values[i][j].write(out);
      }
    }
  }
}

