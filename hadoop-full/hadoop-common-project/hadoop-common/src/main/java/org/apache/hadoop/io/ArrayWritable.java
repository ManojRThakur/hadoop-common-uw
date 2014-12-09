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

/** 
 * A Writable for arrays containing instances of a class. The elements of this
 * writable must all be instances of the same class. If this writable will be
 * the input for a Reducer, you will need to create a subclass that sets the
 * value to be of the proper type.
 *
 * For example:
 * <code>
 * public class IntArrayWritable extends ArrayWritable {
 *   public IntArrayWritable() { 
 *     super(IntWritable.class); 
 *   }	
 * }
 * </code>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ArrayWritable implements @OsUntrusted Writable {
  private @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Writable> valueClass;
  private @OsUntrusted Writable @OsUntrusted [] values;

  public @OsUntrusted ArrayWritable(@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Writable> valueClass) {
    if (valueClass == null) { 
      throw new @OsUntrusted IllegalArgumentException("null valueClass"); 
    }    
    this.valueClass = valueClass;
  }

  public @OsUntrusted ArrayWritable(@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Writable> valueClass, @OsUntrusted Writable @OsUntrusted [] values) {
    this(valueClass);
    this.values = values;
  }

  public @OsUntrusted ArrayWritable(@OsUntrusted String @OsUntrusted [] strings) {
    this(UTF8.class, new @OsUntrusted Writable @OsUntrusted [strings.length]);
    for (@OsUntrusted int i = 0; i < strings.length; i++) {
      values[i] = new @OsUntrusted UTF8(strings[i]);
    }
  }

  public @OsUntrusted Class getValueClass(@OsUntrusted ArrayWritable this) {
    return valueClass;
  }

  public @OsUntrusted String @OsUntrusted [] toStrings(@OsUntrusted ArrayWritable this) {
    @OsUntrusted
    String @OsUntrusted [] strings = new @OsUntrusted String @OsUntrusted [values.length];
    for (@OsUntrusted int i = 0; i < values.length; i++) {
      strings[i] = values[i].toString();
    }
    return strings;
  }

  public @OsUntrusted Object toArray(@OsUntrusted ArrayWritable this) {
    @OsUntrusted
    Object result = Array.newInstance(valueClass, values.length);
    for (@OsUntrusted int i = 0; i < values.length; i++) {
      Array.set(result, i, values[i]);
    }
    return result;
  }

  public void set(@OsUntrusted ArrayWritable this, @OsUntrusted Writable @OsUntrusted [] values) { this.values = values; }

  public @OsUntrusted Writable @OsUntrusted [] get(@OsUntrusted ArrayWritable this) { return values; }

  @Override
  public void readFields(@OsUntrusted ArrayWritable this, @OsUntrusted DataInput in) throws IOException {
    values = new @OsUntrusted Writable @OsUntrusted [in.readInt()];          // construct values
    for (@OsUntrusted int i = 0; i < values.length; i++) {
      @OsUntrusted
      Writable value = WritableFactories.newInstance(valueClass);
      value.readFields(in);                       // read a value
      values[i] = value;                          // store it in values
    }
  }

  @Override
  public void write(@OsUntrusted ArrayWritable this, @OsUntrusted DataOutput out) throws IOException {
    out.writeInt(values.length);                 // write values
    for (@OsUntrusted int i = 0; i < values.length; i++) {
      values[i].write(out);
    }
  }

}

