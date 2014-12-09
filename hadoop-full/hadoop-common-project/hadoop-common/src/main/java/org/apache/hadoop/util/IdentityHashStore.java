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

package org.apache.hadoop.util;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;

/**
 * The IdentityHashStore stores (key, value) mappings in an array.
 * It is similar to java.util.HashTable, but much more lightweight.
 * Neither inserting nor removing an element ever leads to any garbage
 * getting created (assuming the array doesn't need to be enlarged).
 *
 * Unlike HashTable, it compares keys using
 * {@link System#identityHashCode(Object)} and the identity operator.
 * This is useful for types like ByteBuffer which have expensive hashCode
 * and equals operators.
 *
 * We use linear probing to resolve collisions.  This avoids the need for
 * the overhead of linked list data structures.  It also means that it is
 * expensive to attempt to remove an element that isn't there, since we
 * have to look at the entire array to be sure that it doesn't exist.
 *
 * @param <K>    The key type to use.
 * @param <V>    THe value type to use.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@SuppressWarnings("unchecked")
public final class IdentityHashStore<@OsUntrusted K extends java.lang.@OsUntrusted Object, @OsUntrusted V extends java.lang.@OsUntrusted Object> {
  /**
   * Even elements are keys; odd elements are values.
   * The array has size 1 + Math.pow(2, capacity).
   */
  private @OsUntrusted Object buffer @OsUntrusted [];

  private @OsUntrusted int numInserted = 0;

  private @OsUntrusted int capacity;

  /**
   * The default maxCapacity value to use.
   */
  private static final @OsUntrusted int DEFAULT_MAX_CAPACITY = 2;

  public @OsUntrusted IdentityHashStore(@OsUntrusted int capacity) {
    Preconditions.checkArgument(capacity >= 0);
    if (capacity == 0) {
      this.capacity = 0;
      this.buffer = null;
    } else {
      // Round the capacity we need up to a power of 2.
      realloc((@OsUntrusted int)Math.pow(2,
          Math.ceil(Math.log(capacity) / Math.log(2))));
    }
  }

  private void realloc(@OsUntrusted IdentityHashStore<K, V> this, @OsUntrusted int newCapacity) {
    Preconditions.checkArgument(newCapacity > 0);
    @OsUntrusted
    Object prevBuffer @OsUntrusted [] = buffer;
    this.capacity = newCapacity;
    // Each element takes two array slots -- one for the key, 
    // and another for the value.  We also want a load factor 
    // of 0.50.  Combine those together and you get 4 * newCapacity.
    this.buffer = new @OsUntrusted Object @OsUntrusted [4 * newCapacity];
    this.numInserted = 0;
    if (prevBuffer != null) {
      for (@OsUntrusted int i = 0; i < prevBuffer.length; i += 2) {
        if (prevBuffer[i] != null) {
          putInternal(prevBuffer[i], prevBuffer[i + 1]);
        }
      }
    }
  }

  private void putInternal(@OsUntrusted IdentityHashStore<K, V> this, @OsUntrusted Object k, @OsUntrusted Object v) {
    @OsUntrusted
    int hash = System.identityHashCode(k);
    final @OsUntrusted int numEntries = buffer.length / 2;
    @OsUntrusted
    int index = hash % numEntries;
    while (true) {
      if (buffer[2 * index] == null) {
        buffer[2 * index] = k;
        buffer[1 + (2 * index)] = v;
        numInserted++;
        return;
      }
      index = (index + 1) % numEntries;
    }
  }

  /**
   * Add a new (key, value) mapping.
   *
   * Inserting a new (key, value) never overwrites a previous one.
   * In other words, you can insert the same key multiple times and it will
   * lead to multiple entries.
   */
  public void put(@OsUntrusted IdentityHashStore<K, V> this, @OsUntrusted K k, @OsUntrusted V v) {
    Preconditions.checkNotNull(k);
    if (buffer == null) {
      realloc(DEFAULT_MAX_CAPACITY);
    } else if (numInserted + 1 > capacity) {
      realloc(capacity * 2);
    }
    putInternal(k, v);
  }

  private @OsUntrusted int getElementIndex(@OsUntrusted IdentityHashStore<K, V> this, @OsUntrusted K k) {
    if (buffer == null) {
      return -1;
    }
    final @OsUntrusted int numEntries = buffer.length / 2;
    @OsUntrusted
    int hash = System.identityHashCode(k);
    @OsUntrusted
    int index = hash % numEntries;
    @OsUntrusted
    int firstIndex = index;
    do {
      if (buffer[2 * index] == k) {
        return index;
      }
      index = (index + 1) % numEntries;
    } while (index != firstIndex);
    return -1;
  }

  /**
   * Retrieve a value associated with a given key.
   */
  public @OsUntrusted V get(@OsUntrusted IdentityHashStore<K, V> this, @OsUntrusted K k) {
    @OsUntrusted
    int index = getElementIndex(k);
    if (index < 0) {
      return null;
    }
    return (V)buffer[1 + (2 * index)];
  }

  /**
   * Retrieve a value associated with a given key, and delete the
   * relevant entry.
   */
  public @OsUntrusted V remove(@OsUntrusted IdentityHashStore<K, V> this, @OsUntrusted K k) {
    @OsUntrusted
    int index = getElementIndex(k);
    if (index < 0) {
      return null;
    }
    V val = (V)buffer[1 + (2 * index)];
    buffer[2 * index] = null;
    buffer[1 + (2 * index)] = null;
    numInserted--;
    return val;
  }

  public @OsUntrusted boolean isEmpty(@OsUntrusted IdentityHashStore<K, V> this) {
    return numInserted == 0;
  }

  public @OsUntrusted int numElements(@OsUntrusted IdentityHashStore<K, V> this) {
    return numInserted;
  }

  public @OsUntrusted int capacity(@OsUntrusted IdentityHashStore<K, V> this) {
    return capacity;
  }

  public interface Visitor<@OsUntrusted K extends java.lang.@OsUntrusted Object, @OsUntrusted V extends java.lang.@OsUntrusted Object> {
    void accept(IdentityHashStore.@OsUntrusted Visitor<K, V> this, @OsUntrusted K k, @OsUntrusted V v);
  }

  /**
   * Visit all key, value pairs in the IdentityHashStore.
   */
  public void visitAll(@OsUntrusted IdentityHashStore<K, V> this, @OsUntrusted Visitor<@OsUntrusted K, @OsUntrusted V> visitor) {
    @OsUntrusted
    int length = buffer == null ? 0 : buffer.length;
    for (@OsUntrusted int i = 0; i < length; i += 2) {
      if (buffer[i] != null) {
        visitor.accept((K)buffer[i], (V)buffer[i + 1]);
      }
    }
  }
}
