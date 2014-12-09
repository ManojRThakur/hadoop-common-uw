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

package org.apache.hadoop.record;

import ostrusted.quals.OsUntrusted;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A byte sequence that is used as a Java native type for buffer.
 * It is resizable and distinguishes between the count of the seqeunce and
 * the current capacity.
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Buffer implements @OsUntrusted Comparable, @OsUntrusted Cloneable {
  /** Number of valid bytes in this.bytes. */
  private @OsUntrusted int count;
  /** Backing store for Buffer. */
  private @OsUntrusted byte @OsUntrusted [] bytes = null;

  /**
   * Create a zero-count sequence.
   */
  public @OsUntrusted Buffer() {
    this.count = 0;
  }

  /**
   * Create a Buffer using the byte array as the initial value.
   *
   * @param bytes This array becomes the backing storage for the object.
   */
  public @OsUntrusted Buffer(@OsUntrusted byte @OsUntrusted [] bytes) {
    this.bytes = bytes;
    this.count = (bytes == null) ? 0 : bytes.length;
  }
  
  /**
   * Create a Buffer using the byte range as the initial value.
   *
   * @param bytes Copy of this array becomes the backing storage for the object.
   * @param offset offset into byte array
   * @param length length of data
   */
  public @OsUntrusted Buffer(@OsUntrusted byte @OsUntrusted [] bytes, @OsUntrusted int offset, @OsUntrusted int length) {
    copy(bytes, offset, length);
  }
  
  
  /**
   * Use the specified bytes array as underlying sequence.
   *
   * @param bytes byte sequence
   */
  public void set(@OsUntrusted Buffer this, @OsUntrusted byte @OsUntrusted [] bytes) {
    this.count = (bytes == null) ? 0 : bytes.length;
    this.bytes = bytes;
  }
  
  /**
   * Copy the specified byte array to the Buffer. Replaces the current buffer.
   *
   * @param bytes byte array to be assigned
   * @param offset offset into byte array
   * @param length length of data
   */
  public final void copy(@OsUntrusted Buffer this, @OsUntrusted byte @OsUntrusted [] bytes, @OsUntrusted int offset, @OsUntrusted int length) {
    if (this.bytes == null || this.bytes.length < length) {
      this.bytes = new @OsUntrusted byte @OsUntrusted [length];
    }
    System.arraycopy(bytes, offset, this.bytes, 0, length);
    this.count = length;
  }
  
  /**
   * Get the data from the Buffer.
   * 
   * @return The data is only valid between 0 and getCount() - 1.
   */
  public @OsUntrusted byte @OsUntrusted [] get(@OsUntrusted Buffer this) {
    if (bytes == null) {
      bytes = new @OsUntrusted byte @OsUntrusted [0];
    }
    return bytes;
  }
  
  /**
   * Get the current count of the buffer.
   */
  public @OsUntrusted int getCount(@OsUntrusted Buffer this) {
    return count;
  }
  
  /**
   * Get the capacity, which is the maximum count that could handled without
   * resizing the backing storage.
   * 
   * @return The number of bytes
   */
  public @OsUntrusted int getCapacity(@OsUntrusted Buffer this) {
    return this.get().length;
  }
  
  /**
   * Change the capacity of the backing storage.
   * The data is preserved if newCapacity >= getCount().
   * @param newCapacity The new capacity in bytes.
   */
  public void setCapacity(@OsUntrusted Buffer this, @OsUntrusted int newCapacity) {
    if (newCapacity < 0) {
      throw new @OsUntrusted IllegalArgumentException("Invalid capacity argument "+newCapacity); 
    }
    if (newCapacity == 0) {
      this.bytes = null;
      this.count = 0;
      return;
    }
    if (newCapacity != getCapacity()) {
      @OsUntrusted
      byte @OsUntrusted [] data = new @OsUntrusted byte @OsUntrusted [newCapacity];
      if (newCapacity < count) {
        count = newCapacity;
      }
      if (count != 0) {
        System.arraycopy(this.get(), 0, data, 0, count);
      }
      bytes = data;
    }
  }
  
  /**
   * Reset the buffer to 0 size
   */
  public void reset(@OsUntrusted Buffer this) {
    setCapacity(0);
  }
  
  /**
   * Change the capacity of the backing store to be the same as the current 
   * count of buffer.
   */
  public void truncate(@OsUntrusted Buffer this) {
    setCapacity(count);
  }
  
  /**
   * Append specified bytes to the buffer.
   *
   * @param bytes byte array to be appended
   * @param offset offset into byte array
   * @param length length of data

  */
  public void append(@OsUntrusted Buffer this, @OsUntrusted byte @OsUntrusted [] bytes, @OsUntrusted int offset, @OsUntrusted int length) {
    setCapacity(count+length);
    System.arraycopy(bytes, offset, this.get(), count, length);
    count = count + length;
  }
  
  /**
   * Append specified bytes to the buffer
   *
   * @param bytes byte array to be appended
   */
  public void append(@OsUntrusted Buffer this, @OsUntrusted byte @OsUntrusted [] bytes) {
    append(bytes, 0, bytes.length);
  }
  
  // inherit javadoc
  @Override
  public @OsUntrusted int hashCode(@OsUntrusted Buffer this) {
    @OsUntrusted
    int hash = 1;
    @OsUntrusted
    byte @OsUntrusted [] b = this.get();
    for (@OsUntrusted int i = 0; i < count; i++)
      hash = (31 * hash) + b[i];
    return hash;
  }
  
  /**
   * Define the sort order of the Buffer.
   * 
   * @param other The other buffer
   * @return Positive if this is bigger than other, 0 if they are equal, and
   *         negative if this is smaller than other.
   */
  @Override
  public @OsUntrusted int compareTo(@OsUntrusted Buffer this, @OsUntrusted Object other) {
    @OsUntrusted
    Buffer right = ((@OsUntrusted Buffer) other);
    @OsUntrusted
    byte @OsUntrusted [] lb = this.get();
    @OsUntrusted
    byte @OsUntrusted [] rb = right.get();
    for (@OsUntrusted int i = 0; i < count && i < right.count; i++) {
      @OsUntrusted
      int a = (lb[i] & 0xff);
      @OsUntrusted
      int b = (rb[i] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return count - right.count;
  }
  
  // inherit javadoc
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted Buffer this, @OsUntrusted Object other) {
    if (other instanceof @OsUntrusted Buffer && this != other) {
      return compareTo(other) == 0;
    }
    return (this == other);
  }
  
  // inheric javadoc
  @Override
  public @OsUntrusted String toString(@OsUntrusted Buffer this) {
    @OsUntrusted
    StringBuilder sb = new @OsUntrusted StringBuilder(2*count);
    for(@OsUntrusted int idx = 0; idx < count; idx++) {
      sb.append(Character.forDigit((bytes[idx] & 0xF0) >> 4, 16));
      sb.append(Character.forDigit(bytes[idx] & 0x0F, 16));
    }
    return sb.toString();
  }
  
  /**
   * Convert the byte buffer to a string an specific character encoding
   *
   * @param charsetName Valid Java Character Set Name
   */
  public @OsUntrusted String toString(@OsUntrusted Buffer this, @OsUntrusted String charsetName)
    throws UnsupportedEncodingException {
    return new @OsUntrusted String(this.get(), 0, this.getCount(), charsetName);
  }
  
  // inherit javadoc
  @Override
  public @OsUntrusted Object clone(@OsUntrusted Buffer this) throws CloneNotSupportedException {
    @OsUntrusted
    Buffer result = (@OsUntrusted Buffer) super.clone();
    result.copy(this.get(), 0, this.getCount());
    return result;
  }
}
