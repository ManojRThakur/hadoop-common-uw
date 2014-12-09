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
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** 
 * A byte sequence that is usable as a key or value.
 * It is resizable and distinguishes between the size of the sequence and
 * the current capacity. The hash function is the front of the md5 of the 
 * buffer. The sort order is the same as memcmp.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BytesWritable extends @OsUntrusted BinaryComparable
    implements @OsUntrusted WritableComparable<@OsUntrusted BinaryComparable> {
  private static final @OsUntrusted int LENGTH_BYTES = 4;
  private static final @OsUntrusted byte @OsUntrusted [] EMPTY_BYTES = new byte @OsUntrusted [] {};

  private @OsUntrusted int size;
  private @OsUntrusted byte @OsUntrusted [] bytes;
  
  /**
   * Create a zero-size sequence.
   */
  public @OsUntrusted BytesWritable() {this(EMPTY_BYTES);}
  
  /**
   * Create a BytesWritable using the byte array as the initial value.
   * @param bytes This array becomes the backing storage for the object.
   */
  public @OsUntrusted BytesWritable(@OsUntrusted byte @OsUntrusted [] bytes) {
    this(bytes, bytes.length);
  }

  /**
   * Create a BytesWritable using the byte array as the initial value
   * and length as the length. Use this constructor if the array is larger
   * than the value it represents.
   * @param bytes This array becomes the backing storage for the object.
   * @param length The number of bytes to use from array.
   */
  public @OsUntrusted BytesWritable(@OsUntrusted byte @OsUntrusted [] bytes, @OsUntrusted int length) {
    this.bytes = bytes;
    this.size = length;
  }
  
  /**
   * Get a copy of the bytes that is exactly the length of the data.
   * See {@link #getBytes()} for faster access to the underlying array.
   */
  public @OsUntrusted byte @OsUntrusted [] copyBytes(@OsUntrusted BytesWritable this) {
    @OsUntrusted
    byte @OsUntrusted [] result = new @OsUntrusted byte @OsUntrusted [size];
    System.arraycopy(bytes, 0, result, 0, size);
    return result;
  }
  
  /**
   * Get the data backing the BytesWritable. Please use {@link #copyBytes()}
   * if you need the returned array to be precisely the length of the data.
   * @return The data is only valid between 0 and getLength() - 1.
   */
  @Override
  public @OsUntrusted byte @OsUntrusted [] getBytes(@OsUntrusted BytesWritable this) {
    return bytes;
  }

  /**
   * Get the data from the BytesWritable.
   * @deprecated Use {@link #getBytes()} instead.
   */
  @Deprecated
  public @OsUntrusted byte @OsUntrusted [] get(@OsUntrusted BytesWritable this) {
    return getBytes();
  }

  /**
   * Get the current size of the buffer.
   */
  @Override
  public @OsUntrusted int getLength(@OsUntrusted BytesWritable this) {
    return size;
  }

  /**
   * Get the current size of the buffer.
   * @deprecated Use {@link #getLength()} instead.
   */
  @Deprecated
  public @OsUntrusted int getSize(@OsUntrusted BytesWritable this) {
    return getLength();
  }
  
  /**
   * Change the size of the buffer. The values in the old range are preserved
   * and any new values are undefined. The capacity is changed if it is 
   * necessary.
   * @param size The new number of bytes
   */
  public void setSize(@OsUntrusted BytesWritable this, @OsUntrusted int size) {
    if (size > getCapacity()) {
      setCapacity(size * 3 / 2);
    }
    this.size = size;
  }
  
  /**
   * Get the capacity, which is the maximum size that could handled without
   * resizing the backing storage.
   * @return The number of bytes
   */
  public @OsUntrusted int getCapacity(@OsUntrusted BytesWritable this) {
    return bytes.length;
  }
  
  /**
   * Change the capacity of the backing storage.
   * The data is preserved.
   * @param new_cap The new capacity in bytes.
   */
  public void setCapacity(@OsUntrusted BytesWritable this, @OsUntrusted int new_cap) {
    if (new_cap != getCapacity()) {
      @OsUntrusted
      byte @OsUntrusted [] new_data = new @OsUntrusted byte @OsUntrusted [new_cap];
      if (new_cap < size) {
        size = new_cap;
      }
      if (size != 0) {
        System.arraycopy(bytes, 0, new_data, 0, size);
      }
      bytes = new_data;
    }
  }

  /**
   * Set the BytesWritable to the contents of the given newData.
   * @param newData the value to set this BytesWritable to.
   */
  public void set(@OsUntrusted BytesWritable this, @OsUntrusted BytesWritable newData) {
    set(newData.bytes, 0, newData.size);
  }

  /**
   * Set the value to a copy of the given byte range
   * @param newData the new values to copy in
   * @param offset the offset in newData to start at
   * @param length the number of bytes to copy
   */
  public void set(@OsUntrusted BytesWritable this, @OsUntrusted byte @OsUntrusted [] newData, @OsUntrusted int offset, @OsUntrusted int length) {
    setSize(0);
    setSize(length);
    System.arraycopy(newData, offset, bytes, 0, size);
  }

  // inherit javadoc
  @Override
  public void readFields(@OsUntrusted BytesWritable this, @OsUntrusted DataInput in) throws IOException {
    setSize(0); // clear the old data
    setSize(in.readInt());
    in.readFully(bytes, 0, size);
  }
  
  // inherit javadoc
  @Override
  public void write(@OsUntrusted BytesWritable this, @OsUntrusted DataOutput out) throws IOException {
    out.writeInt(size);
    out.write(bytes, 0, size);
  }
  
  @Override
  public @OsUntrusted int hashCode(@OsUntrusted BytesWritable this) {
    return super.hashCode();
  }

  /**
   * Are the two byte sequences equal?
   */
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted BytesWritable this, @OsUntrusted Object right_obj) {
    if (right_obj instanceof @OsUntrusted BytesWritable)
      return super.equals(right_obj);
    return false;
  }

  /**
   * Generate the stream of bytes as hex pairs separated by ' '.
   */
  @Override
  public @OsUntrusted String toString(@OsUntrusted BytesWritable this) { 
    @OsUntrusted
    StringBuilder sb = new @OsUntrusted StringBuilder(3*size);
    for (@OsUntrusted int idx = 0; idx < size; idx++) {
      // if not the first, put a blank separator in
      if (idx != 0) {
        sb.append(' ');
      }
      @OsUntrusted
      String num = Integer.toHexString(0xff & bytes[idx]);
      // if it is only one digit, add a leading 0.
      if (num.length() < 2) {
        sb.append('0');
      }
      sb.append(num);
    }
    return sb.toString();
  }

  /** A Comparator optimized for BytesWritable. */ 
  public static class Comparator extends @OsUntrusted WritableComparator {
    public @OsUntrusted Comparator() {
      super(BytesWritable.class);
    }
    
    /**
     * Compare the buffers in serialized form.
     */
    @Override
    public @OsUntrusted int compare(BytesWritable.@OsUntrusted Comparator this, @OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1,
                       @OsUntrusted
                       byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2) {
      return compareBytes(b1, s1+LENGTH_BYTES, l1-LENGTH_BYTES, 
                          b2, s2+LENGTH_BYTES, l2-LENGTH_BYTES);
    }
  }
  
  static {                                        // register this comparator
    WritableComparator.define(BytesWritable.class, new @OsUntrusted Comparator());
  }
  
}
