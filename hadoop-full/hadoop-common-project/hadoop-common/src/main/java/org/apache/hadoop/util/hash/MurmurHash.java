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

package org.apache.hadoop.util.hash;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup.  See http://murmurhash.googlepages.com/ for more details.
 * 
 * <p>The C version of MurmurHash 2.0 found at that site was ported
 * to Java by Andrzej Bialecki (ab at getopt org).</p>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MurmurHash extends @OsUntrusted Hash {
  private static @OsUntrusted MurmurHash _instance = new @OsUntrusted MurmurHash();
  
  public static @OsUntrusted Hash getInstance() {
    return _instance;
  }
  
  @Override
  public @OsUntrusted int hash(@OsUntrusted MurmurHash this, @OsUntrusted byte @OsUntrusted [] data, @OsUntrusted int length, @OsUntrusted int seed) {
    @OsUntrusted
    int m = 0x5bd1e995;
    @OsUntrusted
    int r = 24;

    @OsUntrusted
    int h = seed ^ length;

    @OsUntrusted
    int len_4 = length >> 2;

    for (@OsUntrusted int i = 0; i < len_4; i++) {
      @OsUntrusted
      int i_4 = i << 2;
      @OsUntrusted
      int k = data[i_4 + 3];
      k = k << 8;
      k = k | (data[i_4 + 2] & 0xff);
      k = k << 8;
      k = k | (data[i_4 + 1] & 0xff);
      k = k << 8;
      k = k | (data[i_4 + 0] & 0xff);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }

    // avoid calculating modulo
    @OsUntrusted
    int len_m = len_4 << 2;
    @OsUntrusted
    int left = length - len_m;

    if (left != 0) {
      if (left >= 3) {
        h ^= (@OsUntrusted int) data[length - 3] << 16;
      }
      if (left >= 2) {
        h ^= (@OsUntrusted int) data[length - 2] << 8;
      }
      if (left >= 1) {
        h ^= (@OsUntrusted int) data[length - 1];
      }

      h *= m;
    }

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    return h;
  }
}
