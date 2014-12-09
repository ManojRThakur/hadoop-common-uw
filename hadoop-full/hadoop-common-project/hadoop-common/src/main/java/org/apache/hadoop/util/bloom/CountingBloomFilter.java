/**
 *
 * Copyright (c) 2005, European Commission project OneLab under contract 034819 (http://www.one-lab.org)
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or 
 * without modification, are permitted provided that the following 
 * conditions are met:
 *  - Redistributions of source code must retain the above copyright 
 *    notice, this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright 
 *    notice, this list of conditions and the following disclaimer in 
 *    the documentation and/or other materials provided with the distribution.
 *  - Neither the name of the University Catholique de Louvain - UCL
 *    nor the names of its contributors may be used to endorse or 
 *    promote products derived from this software without specific prior 
 *    written permission.
 *    
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE 
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */

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

package org.apache.hadoop.util.bloom;

import ostrusted.quals.OsUntrusted;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Implements a <i>counting Bloom filter</i>, as defined by Fan et al. in a ToN
 * 2000 paper.
 * <p>
 * A counting Bloom filter is an improvement to standard a Bloom filter as it
 * allows dynamic additions and deletions of set membership information.  This 
 * is achieved through the use of a counting vector instead of a bit vector.
 * <p>
 * Originally created by
 * <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
 *
 * @see Filter The general behavior of a filter
 * 
 * @see <a href="http://portal.acm.org/citation.cfm?id=343571.343572">Summary cache: a scalable wide-area web cache sharing protocol</a>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public final class CountingBloomFilter extends @OsUntrusted Filter {
  /** Storage for the counting buckets */
  private @OsUntrusted long @OsUntrusted [] buckets;

  /** We are using 4bit buckets, so each bucket can count to 15 */
  private final static @OsUntrusted long BUCKET_MAX_VALUE = 15;

  /** Default constructor - use with readFields */
  public @OsUntrusted CountingBloomFilter() {}
  
  /**
   * Constructor
   * @param vectorSize The vector size of <i>this</i> filter.
   * @param nbHash The number of hash function to consider.
   * @param hashType type of the hashing function (see
   * {@link org.apache.hadoop.util.hash.Hash}).
   */
  public @OsUntrusted CountingBloomFilter(@OsUntrusted int vectorSize, @OsUntrusted int nbHash, @OsUntrusted int hashType) {
    super(vectorSize, nbHash, hashType);
    buckets = new @OsUntrusted long @OsUntrusted [buckets2words(vectorSize)];
  }

  /** returns the number of 64 bit words it would take to hold vectorSize buckets */
  private static @OsUntrusted int buckets2words(@OsUntrusted int vectorSize) {
   return ((vectorSize - 1) >>> 4) + 1;
  }


  @Override
  public void add(@OsUntrusted CountingBloomFilter this, @OsUntrusted Key key) {
    if(key == null) {
      throw new @OsUntrusted NullPointerException("key can not be null");
    }

    @OsUntrusted
    int @OsUntrusted [] h = hash.hash(key);
    hash.clear();

    for(@OsUntrusted int i = 0; i < nbHash; i++) {
      // find the bucket
      @OsUntrusted
      int wordNum = h[i] >> 4;          // div 16
      @OsUntrusted
      int bucketShift = (h[i] & 0x0f) << 2;  // (mod 16) * 4
      
      @OsUntrusted
      long bucketMask = 15L << bucketShift;
      @OsUntrusted
      long bucketValue = (buckets[wordNum] & bucketMask) >>> bucketShift;
      
      // only increment if the count in the bucket is less than BUCKET_MAX_VALUE
      if(bucketValue < BUCKET_MAX_VALUE) {
        // increment by 1
        buckets[wordNum] = (buckets[wordNum] & ~bucketMask) | ((bucketValue + 1) << bucketShift);
      }
    }
  }

  /**
   * Removes a specified key from <i>this</i> counting Bloom filter.
   * <p>
   * <b>Invariant</b>: nothing happens if the specified key does not belong to <i>this</i> counter Bloom filter.
   * @param key The key to remove.
   */
  public void delete(@OsUntrusted CountingBloomFilter this, @OsUntrusted Key key) {
    if(key == null) {
      throw new @OsUntrusted NullPointerException("Key may not be null");
    }
    if(!membershipTest(key)) {
      throw new @OsUntrusted IllegalArgumentException("Key is not a member");
    }

    @OsUntrusted
    int @OsUntrusted [] h = hash.hash(key);
    hash.clear();

    for(@OsUntrusted int i = 0; i < nbHash; i++) {
      // find the bucket
      @OsUntrusted
      int wordNum = h[i] >> 4;          // div 16
      @OsUntrusted
      int bucketShift = (h[i] & 0x0f) << 2;  // (mod 16) * 4
      
      @OsUntrusted
      long bucketMask = 15L << bucketShift;
      @OsUntrusted
      long bucketValue = (buckets[wordNum] & bucketMask) >>> bucketShift;
      
      // only decrement if the count in the bucket is between 0 and BUCKET_MAX_VALUE
      if(bucketValue >= 1 && bucketValue < BUCKET_MAX_VALUE) {
        // decrement by 1
        buckets[wordNum] = (buckets[wordNum] & ~bucketMask) | ((bucketValue - 1) << bucketShift);
      }
    }
  }

  @Override
  public void and(@OsUntrusted CountingBloomFilter this, @OsUntrusted Filter filter) {
    if(filter == null
        || !(filter instanceof @OsUntrusted CountingBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new @OsUntrusted IllegalArgumentException("filters cannot be and-ed");
    }
    @OsUntrusted
    CountingBloomFilter cbf = (@OsUntrusted CountingBloomFilter)filter;
    
    @OsUntrusted
    int sizeInWords = buckets2words(vectorSize);
    for(@OsUntrusted int i = 0; i < sizeInWords; i++) {
      this.buckets[i] &= cbf.buckets[i];
    }
  }

  @Override
  public @OsUntrusted boolean membershipTest(@OsUntrusted CountingBloomFilter this, @OsUntrusted Key key) {
    if(key == null) {
      throw new @OsUntrusted NullPointerException("Key may not be null");
    }

    @OsUntrusted
    int @OsUntrusted [] h = hash.hash(key);
    hash.clear();

    for(@OsUntrusted int i = 0; i < nbHash; i++) {
      // find the bucket
      @OsUntrusted
      int wordNum = h[i] >> 4;          // div 16
      @OsUntrusted
      int bucketShift = (h[i] & 0x0f) << 2;  // (mod 16) * 4

      @OsUntrusted
      long bucketMask = 15L << bucketShift;

      if((buckets[wordNum] & bucketMask) == 0) {
        return false;
      }
    }

    return true;
  }

  /**
   * This method calculates an approximate count of the key, i.e. how many
   * times the key was added to the filter. This allows the filter to be
   * used as an approximate <code>key -&gt; count</code> map.
   * <p>NOTE: due to the bucket size of this filter, inserting the same
   * key more than 15 times will cause an overflow at all filter positions
   * associated with this key, and it will significantly increase the error
   * rate for this and other keys. For this reason the filter can only be
   * used to store small count values <code>0 &lt;= N &lt;&lt; 15</code>.
   * @param key key to be tested
   * @return 0 if the key is not present. Otherwise, a positive value v will
   * be returned such that <code>v == count</code> with probability equal to the
   * error rate of this filter, and <code>v &gt; count</code> otherwise.
   * Additionally, if the filter experienced an underflow as a result of
   * {@link #delete(Key)} operation, the return value may be lower than the
   * <code>count</code> with the probability of the false negative rate of such
   * filter.
   */
  public @OsUntrusted int approximateCount(@OsUntrusted CountingBloomFilter this, @OsUntrusted Key key) {
    @OsUntrusted
    int res = Integer.MAX_VALUE;
    @OsUntrusted
    int @OsUntrusted [] h = hash.hash(key);
    hash.clear();
    for (@OsUntrusted int i = 0; i < nbHash; i++) {
      // find the bucket
      @OsUntrusted
      int wordNum = h[i] >> 4;          // div 16
      @OsUntrusted
      int bucketShift = (h[i] & 0x0f) << 2;  // (mod 16) * 4
      
      @OsUntrusted
      long bucketMask = 15L << bucketShift;
      @OsUntrusted
      long bucketValue = (buckets[wordNum] & bucketMask) >>> bucketShift;
      if (bucketValue < res) res = (@OsUntrusted int)bucketValue;
    }
    if (res != Integer.MAX_VALUE) {
      return res;
    } else {
      return 0;
    }
  }

  @Override
  public void not(@OsUntrusted CountingBloomFilter this) {
    throw new @OsUntrusted UnsupportedOperationException("not() is undefined for "
        + this.getClass().getName());
  }

  @Override
  public void or(@OsUntrusted CountingBloomFilter this, @OsUntrusted Filter filter) {
    if(filter == null
        || !(filter instanceof @OsUntrusted CountingBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new @OsUntrusted IllegalArgumentException("filters cannot be or-ed");
    }

    @OsUntrusted
    CountingBloomFilter cbf = (@OsUntrusted CountingBloomFilter)filter;

    @OsUntrusted
    int sizeInWords = buckets2words(vectorSize);
    for(@OsUntrusted int i = 0; i < sizeInWords; i++) {
      this.buckets[i] |= cbf.buckets[i];
    }
  }

  @Override
  public void xor(@OsUntrusted CountingBloomFilter this, @OsUntrusted Filter filter) {
    throw new @OsUntrusted UnsupportedOperationException("xor() is undefined for "
        + this.getClass().getName());
  }

  @Override
  public @OsUntrusted String toString(@OsUntrusted CountingBloomFilter this) {
    @OsUntrusted
    StringBuilder res = new @OsUntrusted StringBuilder();

    for(@OsUntrusted int i = 0; i < vectorSize; i++) {
      if(i > 0) {
        res.append(" ");
      }
      
      @OsUntrusted
      int wordNum = i >> 4;          // div 16
      @OsUntrusted
      int bucketShift = (i & 0x0f) << 2;  // (mod 16) * 4
      
      @OsUntrusted
      long bucketMask = 15L << bucketShift;
      @OsUntrusted
      long bucketValue = (buckets[wordNum] & bucketMask) >>> bucketShift;
      
      res.append(bucketValue);
    }

    return res.toString();
  }

  // Writable

  @Override
  public void write(@OsUntrusted CountingBloomFilter this, @OsUntrusted DataOutput out) throws IOException {
    super.write(out);
    @OsUntrusted
    int sizeInWords = buckets2words(vectorSize);
    for(@OsUntrusted int i = 0; i < sizeInWords; i++) {
      out.writeLong(buckets[i]);
    }
  }

  @Override
  public void readFields(@OsUntrusted CountingBloomFilter this, @OsUntrusted DataInput in) throws IOException {
    super.readFields(in);
    @OsUntrusted
    int sizeInWords = buckets2words(vectorSize);
    buckets = new @OsUntrusted long @OsUntrusted [sizeInWords];
    for(@OsUntrusted int i = 0; i < sizeInWords; i++) {
      buckets[i] = in.readLong();
    }
  }
}