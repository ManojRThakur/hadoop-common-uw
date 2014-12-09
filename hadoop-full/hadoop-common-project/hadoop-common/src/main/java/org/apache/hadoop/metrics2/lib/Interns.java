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

package org.apache.hadoop.metrics2.lib;

import ostrusted.quals.OsUntrusted;
import java.util.Map;
import java.util.LinkedHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;

/**
 * Helpers to create interned metrics info
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Interns {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(Interns.class);

  // A simple intern cache with two keys
  // (to avoid creating new (combined) key objects for lookup)
  private static abstract class CacheWith2Keys<@OsUntrusted K1 extends java.lang.@OsUntrusted Object, @OsUntrusted K2 extends java.lang.@OsUntrusted Object, @OsUntrusted V extends java.lang.@OsUntrusted Object> {
    private final @OsUntrusted Map<@OsUntrusted K1, @OsUntrusted Map<@OsUntrusted K2, @OsUntrusted V>> k1Map =
        new @OsUntrusted LinkedHashMap<K1, @OsUntrusted Map<K2, V>>() {
      private static final @OsUntrusted long serialVersionUID = 1L;
      private @OsUntrusted boolean gotOverflow = false;
      @Override
      protected @OsUntrusted boolean removeEldestEntry(Map.@OsUntrusted Entry<@OsUntrusted K1, @OsUntrusted Map<@OsUntrusted K2, @OsUntrusted V>> e) {
        @OsUntrusted
        boolean overflow = expireKey1At(size());
        if (overflow && !gotOverflow) {
          LOG.warn("Metrics intern cache overflow at "+ size() +" for "+ e);
          gotOverflow = true;
        }
        return overflow;
      }
    };

    abstract protected @OsUntrusted boolean expireKey1At(Interns.@OsUntrusted CacheWith2Keys<K1, K2, V> this, @OsUntrusted int size);
    abstract protected @OsUntrusted boolean expireKey2At(Interns.@OsUntrusted CacheWith2Keys<K1, K2, V> this, @OsUntrusted int size);
    abstract protected @OsUntrusted V newValue(Interns.@OsUntrusted CacheWith2Keys<K1, K2, V> this, @OsUntrusted K1 k1, @OsUntrusted K2 k2);

    synchronized @OsUntrusted V add(Interns.@OsUntrusted CacheWith2Keys<K1, K2, V> this, @OsUntrusted K1 k1, @OsUntrusted K2 k2) {
      @OsUntrusted
      Map<K2, V> k2Map = k1Map.get(k1);
      if (k2Map == null) {
        k2Map = new @OsUntrusted LinkedHashMap<K2, V>() {
          private static final @OsUntrusted long serialVersionUID = 1L;
          private @OsUntrusted boolean gotOverflow = false;
          @Override protected @OsUntrusted boolean removeEldestEntry(Map.@OsUntrusted Entry<@OsUntrusted K2, @OsUntrusted V> e) {
            @OsUntrusted
            boolean overflow = expireKey2At(size());
            if (overflow && !gotOverflow) {
              LOG.warn("Metrics intern cache overflow at "+ size() +" for "+ e);
              gotOverflow = true;
            }
            return overflow;
          }
        };
        k1Map.put(k1, k2Map);
      }
      V v = k2Map.get(k2);
      if (v == null) {
        v = newValue(k1, k2);
        k2Map.put(k2, v);
      }
      return v;
    }
  }

  // Sanity limits in case of misuse/abuse.
  static final @OsUntrusted int MAX_INFO_NAMES = 2010;
  static final @OsUntrusted int MAX_INFO_DESCS = 100;  // distinct per name

  enum Info {

@OsUntrusted  INSTANCE;

    final @OsUntrusted CacheWith2Keys<@OsUntrusted String, @OsUntrusted String, @OsUntrusted MetricsInfo> cache =
        new @OsUntrusted CacheWith2Keys<@OsUntrusted String, @OsUntrusted String, @OsUntrusted MetricsInfo>() {

      @Override protected boolean expireKey1At(int size) {
        return size > MAX_INFO_NAMES;
      }

      @Override protected boolean expireKey2At(int size) {
        return size > MAX_INFO_DESCS;
      }

      @Override protected MetricsInfo newValue(String name, String desc) {
        return new MetricsInfoImpl(name, desc);
      }
    };
  }

  /**
   * Get a metric info object
   * @param name
   * @param description
   * @return an interned metric info object
   */
  public static @OsUntrusted MetricsInfo info(@OsUntrusted String name, @OsUntrusted String description) {
    return Info.INSTANCE.cache.add(name, description);
  }

  // Sanity limits
  static final @OsUntrusted int MAX_TAG_NAMES = 100;
  static final @OsUntrusted int MAX_TAG_VALUES = 1000; // distinct per name

  enum Tags {

@OsUntrusted  INSTANCE;

    final @OsUntrusted CacheWith2Keys<@OsUntrusted MetricsInfo, @OsUntrusted String, @OsUntrusted MetricsTag> cache =
        new @OsUntrusted CacheWith2Keys<@OsUntrusted MetricsInfo, @OsUntrusted String, @OsUntrusted MetricsTag>() {

      @Override protected boolean expireKey1At(int size) {
        return size > MAX_TAG_NAMES;
      }

      @Override protected boolean expireKey2At(int size) {
        return size > MAX_TAG_VALUES;
      }

      @Override protected MetricsTag newValue(MetricsInfo info, String value) {
        return new MetricsTag(info, value);
      }
    };
  }

  /**
   * Get a metrics tag
   * @param info  of the tag
   * @param value of the tag
   * @return an interned metrics tag
   */
  public static @OsUntrusted MetricsTag tag(@OsUntrusted MetricsInfo info, @OsUntrusted String value) {
    return Tags.INSTANCE.cache.add(info, value);
  }

  /**
   * Get a metrics tag
   * @param name  of the tag
   * @param description of the tag
   * @param value of the tag
   * @return an interned metrics tag
   */
  public static @OsUntrusted MetricsTag tag(@OsUntrusted String name, @OsUntrusted String description, @OsUntrusted String value) {
    return Tags.INSTANCE.cache.add(info(name, description), value);
  }
}
