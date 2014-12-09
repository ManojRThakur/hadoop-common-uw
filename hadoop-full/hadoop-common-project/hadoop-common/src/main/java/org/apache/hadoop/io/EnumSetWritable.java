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
import java.util.EnumSet;
import java.util.Iterator;
import java.util.AbstractCollection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/** A Writable wrapper for EnumSet. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class EnumSetWritable<@OsUntrusted E extends @OsUntrusted Enum<E>> extends @OsUntrusted AbstractCollection<E>
  implements @OsUntrusted Writable, @OsUntrusted Configurable  {

  private @OsUntrusted EnumSet<@OsUntrusted E> value;

  private transient @OsUntrusted Class<@OsUntrusted E> elementType;

  private transient @OsUntrusted Configuration conf;
  
  @OsUntrusted
  EnumSetWritable() {
  }

  @Override
  public @OsUntrusted Iterator<@OsUntrusted E> iterator(@OsUntrusted EnumSetWritable<E> this) { return value.iterator(); }
  @Override
  public @OsUntrusted int size(@OsUntrusted EnumSetWritable<E> this) { return value.size(); }
  @Override
  public @OsUntrusted boolean add(@OsUntrusted EnumSetWritable<E> this, @OsUntrusted E e) {
    if (value == null) {
      value = EnumSet.of(e);
      set(value, null);
    }
    return value.add(e);
  }

  /**
   * Construct a new EnumSetWritable. If the <tt>value</tt> argument is null or
   * its size is zero, the <tt>elementType</tt> argument must not be null. If
   * the argument <tt>value</tt>'s size is bigger than zero, the argument
   * <tt>elementType</tt> is not be used.
   * 
   * @param value
   * @param elementType
   */
  public @OsUntrusted EnumSetWritable(@OsUntrusted EnumSet<@OsUntrusted E> value, @OsUntrusted Class<@OsUntrusted E> elementType) {
    set(value, elementType);
  }

  /**
   * Construct a new EnumSetWritable. Argument <tt>value</tt> should not be null
   * or empty.
   * 
   * @param value
   */
  public @OsUntrusted EnumSetWritable(@OsUntrusted EnumSet<@OsUntrusted E> value) {
    this(value, null);
  }

  /**
   * reset the EnumSetWritable with specified
   * <tt>value</value> and <tt>elementType</tt>. If the <tt>value</tt> argument
   * is null or its size is zero, the <tt>elementType</tt> argument must not be
   * null. If the argument <tt>value</tt>'s size is bigger than zero, the
   * argument <tt>elementType</tt> is not be used.
   * 
   * @param value
   * @param elementType
   */
  public void set(@OsUntrusted EnumSetWritable<E> this, @OsUntrusted EnumSet<@OsUntrusted E> value, @OsUntrusted Class<@OsUntrusted E> elementType) {
    if ((value == null || value.size() == 0)
        && (this.elementType == null && elementType == null)) {
      throw new @OsUntrusted IllegalArgumentException(
          "The EnumSet argument is null, or is an empty set but with no elementType provided.");
    }
    this.value = value;
    if (value != null && value.size() > 0) {
      @OsUntrusted
      Iterator<E> iterator = value.iterator();
      this.elementType = iterator.next().getDeclaringClass();
    } else if (elementType != null) {
      this.elementType = elementType;
    }
  }

  /** Return the value of this EnumSetWritable. */
  public @OsUntrusted EnumSet<@OsUntrusted E> get(@OsUntrusted EnumSetWritable<E> this) {
    return value;
  }

  @Override
  @SuppressWarnings({"unchecked", "ostrusted"}) // TODO: Weird generics raw issue with EnuMset
  public void readFields(@OsUntrusted EnumSetWritable<E> this, @OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    int length = in.readInt();
    if (length == -1)
      this.value = null;
    else if (length == 0) {
      this.elementType = (@OsUntrusted Class<E>) ObjectWritable.loadClass(conf,
          WritableUtils.readString(in));
      this.value = EnumSet.noneOf(this.elementType);
    } else {
      E first = (E) ObjectWritable.readObject(in, conf);
      this.value = (@OsUntrusted EnumSet<E>) EnumSet.of(first);
      for (@OsUntrusted int i = 1; i < length; i++)
        this.value.add((E) ObjectWritable.readObject(in, conf));
    }
  }

  @Override
  public void write(@OsUntrusted EnumSetWritable<E> this, @OsUntrusted DataOutput out) throws IOException {
    if (this.value == null) {
      out.writeInt(-1);
      WritableUtils.writeString(out, this.elementType.getName());
    } else {
      @OsUntrusted
      Object @OsUntrusted [] array = this.value.toArray();
      @OsUntrusted
      int length = array.length;
      out.writeInt(length);
      if (length == 0) {
        if (this.elementType == null)
          throw new @OsUntrusted UnsupportedOperationException(
              "Unable to serialize empty EnumSet with no element type provided.");
        WritableUtils.writeString(out, this.elementType.getName());
      }
      for (@OsUntrusted int i = 0; i < length; i++) {
        ObjectWritable.writeObject(out, array[i], array[i].getClass(), conf);
      }
    }
  }

  /**
   * Returns true if <code>o</code> is an EnumSetWritable with the same value,
   * or both are null.
   */
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted EnumSetWritable<E> this, @OsUntrusted Object o) {
    if (o == null) {
      throw new @OsUntrusted IllegalArgumentException("null argument passed in equal().");
    }

    if (!(o instanceof @OsUntrusted EnumSetWritable))
      return false;

    @OsUntrusted
    EnumSetWritable<@OsUntrusted ? extends java.lang.@OsUntrusted Object> other = (@OsUntrusted EnumSetWritable<@OsUntrusted ? extends java.lang.@OsUntrusted Object>) o;

    if (this == o || (this.value == other.value))
      return true;
    if (this.value == null) // other.value must not be null if we reach here
      return false;

    return this.value.equals(other.value);
  }

  /**
   * Returns the class of all the elements of the underlying EnumSetWriable. It
   * may return null.
   * 
   * @return the element class
   */
  public @OsUntrusted Class<@OsUntrusted E> getElementType(@OsUntrusted EnumSetWritable<E> this) {
    return elementType;
  }

  @Override
  public @OsUntrusted int hashCode(@OsUntrusted EnumSetWritable<E> this) {
    if (value == null)
      return 0;
    return (@OsUntrusted int) value.hashCode();
  }

  @Override
  public @OsUntrusted String toString(@OsUntrusted EnumSetWritable<E> this) {
    if (value == null)
      return "(null)";
    return value.toString();
  }

  @Override
  public @OsUntrusted Configuration getConf(@OsUntrusted EnumSetWritable<E> this) {
    return this.conf;
  }

  @Override
  public void setConf(@OsUntrusted EnumSetWritable<E> this, @OsUntrusted Configuration conf) {
    this.conf = conf;
  }

  static {
    WritableFactories.setFactory(EnumSetWritable.class, new @OsUntrusted WritableFactory() {
      @SuppressWarnings("unchecked")
      @Override
      public @OsUntrusted Writable newInstance() {
        return new @OsUntrusted EnumSetWritable();
      }
    });
  }
}
