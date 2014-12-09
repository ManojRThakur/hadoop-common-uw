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
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.ReflectionUtils;

/** A Comparator for {@link WritableComparable}s.
 *
 * <p>This base implemenation uses the natural ordering.  To define alternate
 * orderings, override {@link #compare(WritableComparable,WritableComparable)}.
 *
 * <p>One may optimize compare-intensive operations by overriding
 * {@link #compare(byte[],int,int,byte[],int,int)}.  Static utility methods are
 * provided to assist in optimized implementations of this method.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class WritableComparator implements @OsUntrusted RawComparator {

  private static final @OsUntrusted ConcurrentHashMap<@OsUntrusted Class, @OsUntrusted WritableComparator> comparators 
          = new @OsUntrusted ConcurrentHashMap<@OsUntrusted Class, @OsUntrusted WritableComparator>(); // registry

  /** Get a comparator for a {@link WritableComparable} implementation. */
  public static @OsUntrusted WritableComparator get(@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> c) {
    @OsUntrusted
    WritableComparator comparator = comparators.get(c);
    if (comparator == null) {
      // force the static initializers to run
      forceInit(c);
      // look to see if it is defined now
      comparator = comparators.get(c);
      // if not, use the generic one
      if (comparator == null) {
        comparator = new @OsUntrusted WritableComparator(c, true);
      }
    }
    return comparator;
  }

  /**
   * Force initialization of the static members.
   * As of Java 5, referencing a class doesn't force it to initialize. Since
   * this class requires that the classes be initialized to declare their
   * comparators, we force that initialization to happen.
   * @param cls the class to initialize
   */
  private static void forceInit(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> cls) {
    try {
      Class.forName(cls.getName(), true, cls.getClassLoader());
    } catch (@OsUntrusted ClassNotFoundException e) {
      throw new @OsUntrusted IllegalArgumentException("Can't initialize class " + cls, e);
    }
  } 

  /** Register an optimized comparator for a {@link WritableComparable}
   * implementation. Comparators registered with this method must be
   * thread-safe. */
  public static void define(@OsUntrusted Class c, @OsUntrusted WritableComparator comparator) {
    comparators.put(c, comparator);
  }

  private final @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> keyClass;
  private final @OsUntrusted WritableComparable key1;
  private final @OsUntrusted WritableComparable key2;
  private final @OsUntrusted DataInputBuffer buffer;

  protected @OsUntrusted WritableComparator() {
    this(null);
  }

  /** Construct for a {@link WritableComparable} implementation. */
  protected @OsUntrusted WritableComparator(@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> keyClass) {
    this(keyClass, false);
  }

  protected @OsUntrusted WritableComparator(@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> keyClass,
      @OsUntrusted
      boolean createInstances) {
    this.keyClass = keyClass;
    if (createInstances) {
      key1 = newKey();
      key2 = newKey();
      buffer = new @OsUntrusted DataInputBuffer();
    } else {
      key1 = key2 = null;
      buffer = null;
    }
  }

  /** Returns the WritableComparable implementation class. */
  public @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> getKeyClass(@OsUntrusted WritableComparator this) { return keyClass; }

  /** Construct a new {@link WritableComparable} instance. */
  public @OsUntrusted WritableComparable newKey(@OsUntrusted WritableComparator this) {
    return ReflectionUtils.newInstance(keyClass, null);
  }

  /** Optimization hook.  Override this to make SequenceFile.Sorter's scream.
   *
   * <p>The default implementation reads the data into two {@link
   * WritableComparable}s (using {@link
   * Writable#readFields(DataInput)}, then calls {@link
   * #compare(WritableComparable,WritableComparable)}.
   */
  @Override
  public @OsUntrusted int compare(@OsUntrusted WritableComparator this, @OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1, @OsUntrusted byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2) {
    try {
      buffer.reset(b1, s1, l1);                   // parse key1
      key1.readFields(buffer);
      
      buffer.reset(b2, s2, l2);                   // parse key2
      key2.readFields(buffer);
      
    } catch (@OsUntrusted IOException e) {
      throw new @OsUntrusted RuntimeException(e);
    }
    
    return compare(key1, key2);                   // compare them
  }

  /** Compare two WritableComparables.
   *
   * <p> The default implementation uses the natural ordering, calling {@link
   * Comparable#compareTo(Object)}. */
  @SuppressWarnings("unchecked")
  public @OsUntrusted int compare(@OsUntrusted WritableComparator this, @OsUntrusted WritableComparable a, @OsUntrusted WritableComparable b) {
    return a.compareTo(b);
  }

  @Override
  public @OsUntrusted int compare(@OsUntrusted WritableComparator this, @OsUntrusted Object a, @OsUntrusted Object b) {
    return compare((@OsUntrusted WritableComparable)a, (@OsUntrusted WritableComparable)b);
  }

  /** Lexicographic order of binary data. */
  public static @OsUntrusted int compareBytes(@OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1,
                                 @OsUntrusted
                                 byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2) {
    return FastByteComparisons.compareTo(b1, s1, l1, b2, s2, l2);
  }

  /** Compute hash for binary data. */
  public static @OsUntrusted int hashBytes(@OsUntrusted byte @OsUntrusted [] bytes, @OsUntrusted int offset, @OsUntrusted int length) {
    @OsUntrusted
    int hash = 1;
    for (@OsUntrusted int i = offset; i < offset + length; i++)
      hash = (31 * hash) + (@OsUntrusted int)bytes[i];
    return hash;
  }
  
  /** Compute hash for binary data. */
  public static @OsUntrusted int hashBytes(@OsUntrusted byte @OsUntrusted [] bytes, @OsUntrusted int length) {
    return hashBytes(bytes, 0, length);
  }

  /** Parse an unsigned short from a byte array. */
  public static @OsUntrusted int readUnsignedShort(@OsUntrusted byte @OsUntrusted [] bytes, @OsUntrusted int start) {
    return (((bytes[start]   & 0xff) <<  8) +
            ((bytes[start+1] & 0xff)));
  }

  /** Parse an integer from a byte array. */
  public static @OsUntrusted int readInt(@OsUntrusted byte @OsUntrusted [] bytes, @OsUntrusted int start) {
    return (((bytes[start  ] & 0xff) << 24) +
            ((bytes[start+1] & 0xff) << 16) +
            ((bytes[start+2] & 0xff) <<  8) +
            ((bytes[start+3] & 0xff)));

  }

  /** Parse a float from a byte array. */
  public static @OsUntrusted float readFloat(@OsUntrusted byte @OsUntrusted [] bytes, @OsUntrusted int start) {
    return Float.intBitsToFloat(readInt(bytes, start));
  }

  /** Parse a long from a byte array. */
  public static @OsUntrusted long readLong(@OsUntrusted byte @OsUntrusted [] bytes, @OsUntrusted int start) {
    return ((@OsUntrusted long)(readInt(bytes, start)) << 32) +
      (readInt(bytes, start+4) & 0xFFFFFFFFL);
  }

  /** Parse a double from a byte array. */
  public static @OsUntrusted double readDouble(@OsUntrusted byte @OsUntrusted [] bytes, @OsUntrusted int start) {
    return Double.longBitsToDouble(readLong(bytes, start));
  }

  /**
   * Reads a zero-compressed encoded long from a byte array and returns it.
   * @param bytes byte array with decode long
   * @param start starting index
   * @throws java.io.IOException 
   * @return deserialized long
   */
  public static @OsUntrusted long readVLong(@OsUntrusted byte @OsUntrusted [] bytes, @OsUntrusted int start) throws IOException {
    @OsUntrusted
    int len = bytes[start];
    if (len >= -112) {
      return len;
    }
    @OsUntrusted
    boolean isNegative = (len < -120);
    len = isNegative ? -(len + 120) : -(len + 112);
    if (start+1+len>bytes.length)
      throw new @OsUntrusted IOException(
                            "Not enough number of bytes for a zero-compressed integer");
    @OsUntrusted
    long i = 0;
    for (@OsUntrusted int idx = 0; idx < len; idx++) {
      i = i << 8;
      i = i | (bytes[start+1+idx] & 0xFF);
    }
    @SuppressWarnings("ostrusted") // Ternary bug in typechecking
    long res = (isNegative ? (i ^ -1L) : i);
    return res;
  }
  
  /**
   * Reads a zero-compressed encoded integer from a byte array and returns it.
   * @param bytes byte array with the encoded integer
   * @param start start index
   * @throws java.io.IOException 
   * @return deserialized integer
   */
  public static @OsUntrusted int readVInt(@OsUntrusted byte @OsUntrusted [] bytes, @OsUntrusted int start) throws IOException {
    return (@OsUntrusted int) readVLong(bytes, start);
  }
}
