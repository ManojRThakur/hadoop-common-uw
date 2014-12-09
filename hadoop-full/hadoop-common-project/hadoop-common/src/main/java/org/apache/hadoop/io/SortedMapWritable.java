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
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A Writable SortedMap.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SortedMapWritable extends @OsUntrusted AbstractMapWritable
  implements @OsUntrusted SortedMap<@OsUntrusted WritableComparable, @OsUntrusted Writable> {
  
  private @OsUntrusted SortedMap<@OsUntrusted WritableComparable, @OsUntrusted Writable> instance;
  
  /** default constructor. */
  public @OsUntrusted SortedMapWritable() {
    super();
    this.instance = new @OsUntrusted TreeMap<@OsUntrusted WritableComparable, @OsUntrusted Writable>();
  }
  
  /**
   * Copy constructor.
   * 
   * @param other the map to copy from
   */
  public @OsUntrusted SortedMapWritable(@OsUntrusted SortedMapWritable other) {
    this();
    copy(other);
  }

  @Override
  public @OsUntrusted Comparator<@OsUntrusted ? super @OsUntrusted WritableComparable> comparator(@OsUntrusted SortedMapWritable this) {
    // Returning null means we use the natural ordering of the keys
    return null;
  }

  @Override
  public @OsUntrusted WritableComparable firstKey(@OsUntrusted SortedMapWritable this) {
    return instance.firstKey();
  }

  @Override
  public @OsUntrusted SortedMap<@OsUntrusted WritableComparable, @OsUntrusted Writable>
  headMap(@OsUntrusted SortedMapWritable this, @OsUntrusted WritableComparable toKey) {
    
    return instance.headMap(toKey);
  }

  @Override
  public @OsUntrusted WritableComparable lastKey(@OsUntrusted SortedMapWritable this) {
    return instance.lastKey();
  }

  @Override
  public @OsUntrusted SortedMap<@OsUntrusted WritableComparable, @OsUntrusted Writable>
  subMap(@OsUntrusted SortedMapWritable this, @OsUntrusted WritableComparable fromKey, @OsUntrusted WritableComparable toKey) {
    
    return instance.subMap(fromKey, toKey);
  }

  @Override
  public @OsUntrusted SortedMap<@OsUntrusted WritableComparable, @OsUntrusted Writable>
  tailMap(@OsUntrusted SortedMapWritable this, @OsUntrusted WritableComparable fromKey) {
    
    return instance.tailMap(fromKey);
  }

  @Override
  public void clear(@OsUntrusted SortedMapWritable this) {
    instance.clear();
  }

  @Override
  public @OsUntrusted boolean containsKey(@OsUntrusted SortedMapWritable this, @OsUntrusted Object key) {
    return instance.containsKey(key);
  }

  @Override
  public @OsUntrusted boolean containsValue(@OsUntrusted SortedMapWritable this, @OsUntrusted Object value) {
    return instance.containsValue(value);
  }

  @Override
  public @OsUntrusted Set<java.util.Map.@OsUntrusted Entry<@OsUntrusted WritableComparable, @OsUntrusted Writable>> entrySet(@OsUntrusted SortedMapWritable this) {
    return instance.entrySet();
  }

  @Override
  public @OsUntrusted Writable get(@OsUntrusted SortedMapWritable this, @OsUntrusted Object key) {
    return instance.get(key);
  }

  @Override
  public @OsUntrusted boolean isEmpty(@OsUntrusted SortedMapWritable this) {
    return instance.isEmpty();
  }

  @Override
  public @OsUntrusted Set<@OsUntrusted WritableComparable> keySet(@OsUntrusted SortedMapWritable this) {
    return instance.keySet();
  }

  @Override
  public @OsUntrusted Writable put(@OsUntrusted SortedMapWritable this, @OsUntrusted WritableComparable key, @OsUntrusted Writable value) {
    addToMap(key.getClass());
    addToMap(value.getClass());
    return instance.put(key, value);
  }

  @Override
  public void putAll(@OsUntrusted SortedMapWritable this, @OsUntrusted Map<@OsUntrusted ? extends @OsUntrusted WritableComparable, @OsUntrusted ? extends @OsUntrusted Writable> t) {
    for (Map.@OsUntrusted Entry<@OsUntrusted ? extends @OsUntrusted WritableComparable, @OsUntrusted ? extends @OsUntrusted Writable> e:
      t.entrySet()) {
      
      put(e.getKey(), e.getValue());
    }
  }

  @Override
  public @OsUntrusted Writable remove(@OsUntrusted SortedMapWritable this, @OsUntrusted Object key) {
    return instance.remove(key);
  }

  @Override
  public @OsUntrusted int size(@OsUntrusted SortedMapWritable this) {
    return instance.size();
  }

  @Override
  public @OsUntrusted Collection<@OsUntrusted Writable> values(@OsUntrusted SortedMapWritable this) {
    return instance.values();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(@OsUntrusted SortedMapWritable this, @OsUntrusted DataInput in) throws IOException {
    super.readFields(in);
    
    // Read the number of entries in the map
    
    @OsUntrusted
    int entries = in.readInt();
    
    // Then read each key/value pair
    
    for (@OsUntrusted int i = 0; i < entries; i++) {
      @OsUntrusted
      WritableComparable key =
        (@OsUntrusted WritableComparable) ReflectionUtils.newInstance(getClass(
            in.readByte()), getConf());
      
      key.readFields(in);
      
      @OsUntrusted
      Writable value = (@OsUntrusted Writable) ReflectionUtils.newInstance(getClass(
          in.readByte()), getConf());
      
      value.readFields(in);
      instance.put(key, value);
    }
  }

  @Override
  public void write(@OsUntrusted SortedMapWritable this, @OsUntrusted DataOutput out) throws IOException {
    super.write(out);
    
    // Write out the number of entries in the map
    
    out.writeInt(instance.size());
    
    // Then write out each key/value pair
    
    for (Map.@OsUntrusted Entry<@OsUntrusted WritableComparable, @OsUntrusted Writable> e: instance.entrySet()) {
      out.writeByte(getId(e.getKey().getClass()));
      e.getKey().write(out);
      out.writeByte(getId(e.getValue().getClass()));
      e.getValue().write(out);
    }
  }

  @Override
  public @OsUntrusted boolean equals(@OsUntrusted SortedMapWritable this, @OsUntrusted Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof @OsUntrusted SortedMapWritable) {
      @OsUntrusted
      Map map = (@OsUntrusted Map) obj;
      if (size() != map.size()) {
        return false;
      }

      return entrySet().equals(map.entrySet());
    }

    return false;
  }

  @Override
  public @OsUntrusted int hashCode(@OsUntrusted SortedMapWritable this) {
    return instance.hashCode();
  }
}
