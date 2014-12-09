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
import com.google.common.collect.ComparisonChain;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is a simple ByteBufferPool which just creates ByteBuffers as needed.
 * It also caches ByteBuffers after they're released.  It will always return
 * the smallest cached buffer with at least the capacity you request.
 * We don't try to do anything clever here like try to limit the maximum cache
 * size.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public final class ElasticByteBufferPool implements @OsUntrusted ByteBufferPool {
  private static final class Key implements @OsUntrusted Comparable<@OsUntrusted Key> {
    private final @OsUntrusted int capacity;
    private final @OsUntrusted long insertionTime;

    @OsUntrusted
    Key(@OsUntrusted int capacity, @OsUntrusted long insertionTime) {
      this.capacity = capacity;
      this.insertionTime = insertionTime;
    }

    @Override
    public @OsUntrusted int compareTo(ElasticByteBufferPool.@OsUntrusted Key this, @OsUntrusted Key other) {
      return ComparisonChain.start().
          compare(capacity, other.capacity).
          compare(insertionTime, other.insertionTime).
          result();
    }

    @Override
    public @OsUntrusted boolean equals(ElasticByteBufferPool.@OsUntrusted Key this, @OsUntrusted Object rhs) {
      if (rhs == null) {
        return false;
      }
      try {
        @OsUntrusted
        Key o = (@OsUntrusted Key)rhs;
        return (compareTo(o) == 0);
      } catch (@OsUntrusted ClassCastException e) {
        return false;
      }
    }

    @Override
    public @OsUntrusted int hashCode(ElasticByteBufferPool.@OsUntrusted Key this) {
      return new @OsUntrusted HashCodeBuilder().
          append(capacity).
          append(insertionTime).
          toHashCode();
    }
  }

  private final @OsUntrusted TreeMap<@OsUntrusted Key, @OsUntrusted ByteBuffer> buffers =
      new @OsUntrusted TreeMap<@OsUntrusted Key, @OsUntrusted ByteBuffer>();

  private final @OsUntrusted TreeMap<@OsUntrusted Key, @OsUntrusted ByteBuffer> directBuffers =
      new @OsUntrusted TreeMap<@OsUntrusted Key, @OsUntrusted ByteBuffer>();

  private final @OsUntrusted TreeMap<@OsUntrusted Key, @OsUntrusted ByteBuffer> getBufferTree(@OsUntrusted ElasticByteBufferPool this, @OsUntrusted boolean direct) {
    return direct ? directBuffers : buffers;
  }
  
  @Override
  public synchronized @OsUntrusted ByteBuffer getBuffer(@OsUntrusted ElasticByteBufferPool this, @OsUntrusted boolean direct, @OsUntrusted int length) {
    @OsUntrusted
    TreeMap<@OsUntrusted Key, @OsUntrusted ByteBuffer> tree = getBufferTree(direct);
    Map.@OsUntrusted Entry<@OsUntrusted Key, @OsUntrusted ByteBuffer> entry =
        tree.ceilingEntry(new @OsUntrusted Key(length, 0));
    if (entry == null) {
      return direct ? ByteBuffer.allocateDirect(length) :
                      ByteBuffer.allocate(length);
    }
    tree.remove(entry.getKey());
    return entry.getValue();
  }

  @Override
  public synchronized void putBuffer(@OsUntrusted ElasticByteBufferPool this, @OsUntrusted ByteBuffer buffer) {
    @OsUntrusted
    TreeMap<@OsUntrusted Key, @OsUntrusted ByteBuffer> tree = getBufferTree(buffer.isDirect());
    while (true) {
      @OsUntrusted
      Key key = new @OsUntrusted Key(buffer.capacity(), System.nanoTime());
      if (!tree.containsKey(key)) {
        tree.put(key, buffer);
        return;
      }
      // Buffers are indexed by (capacity, time).
      // If our key is not unique on the first try, we try again, since the
      // time will be different.  Since we use nanoseconds, it's pretty
      // unlikely that we'll loop even once, unless the system clock has a
      // poor granularity.
    }
  }
}
