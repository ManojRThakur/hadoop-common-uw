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
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A {@link GSet} implementation by {@link HashMap}.
 */
@InterfaceAudience.Private
public class GSetByHashMap<@OsUntrusted K extends java.lang.@OsUntrusted Object, @OsUntrusted E extends @OsUntrusted K> implements @OsUntrusted GSet<K, E> {
  private final @OsUntrusted HashMap<@OsUntrusted K, @OsUntrusted E> m;

  public @OsUntrusted GSetByHashMap(@OsUntrusted int initialCapacity, @OsUntrusted float loadFactor) {
    m = new @OsUntrusted HashMap<K, E>(initialCapacity, loadFactor);
  }

  @Override
  public @OsUntrusted int size(@OsUntrusted GSetByHashMap<K, E> this) {
    return m.size();
  }

  @Override
  public @OsUntrusted boolean contains(@OsUntrusted GSetByHashMap<K, E> this, @OsUntrusted K k) {
    return m.containsKey(k);
  }

  @Override
  public @OsUntrusted E get(@OsUntrusted GSetByHashMap<K, E> this, @OsUntrusted K k) {
    return m.get(k);
  }

  @Override
  public @OsUntrusted E put(@OsUntrusted GSetByHashMap<K, E> this, @OsUntrusted E element) {
    if (element == null) {
      throw new @OsUntrusted UnsupportedOperationException("Null element is not supported.");
    }
    return m.put(element, element);
  }

  @Override
  public @OsUntrusted E remove(@OsUntrusted GSetByHashMap<K, E> this, @OsUntrusted K k) {
    return m.remove(k);
  }

  @Override
  public @OsUntrusted Iterator<@OsUntrusted E> iterator(@OsUntrusted GSetByHashMap<K, E> this) {
    return m.values().iterator();
  }
  
  @Override
  public void clear(@OsUntrusted GSetByHashMap<K, E> this) {
    m.clear();
  }
}
