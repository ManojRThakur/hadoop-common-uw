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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A Writable Map.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MapWritable extends @OsUntrusted AbstractMapWritable
  implements @OsUntrusted Map<@OsUntrusted Writable, @OsUntrusted Writable> {

  private @OsUntrusted Map<@OsUntrusted Writable, @OsUntrusted Writable> instance;
  
  /** Default constructor. */
  public @OsUntrusted MapWritable() {
    super();
    this.instance = new @OsUntrusted HashMap<@OsUntrusted Writable, @OsUntrusted Writable>();
  }
  
  /**
   * Copy constructor.
   * 
   * @param other the map to copy from
   */
  public @OsUntrusted MapWritable(@OsUntrusted MapWritable other) {
    this();
    copy(other);
  }
  
  @Override
  public void clear(@OsUntrusted MapWritable this) {
    instance.clear();
  }

  @Override
  public @OsUntrusted boolean containsKey(@OsUntrusted MapWritable this, @OsUntrusted Object key) {
    return instance.containsKey(key);
  }

  @Override
  public @OsUntrusted boolean containsValue(@OsUntrusted MapWritable this, @OsUntrusted Object value) {
    return instance.containsValue(value);
  }

  @Override
  public @OsUntrusted Set<Map.@OsUntrusted Entry<@OsUntrusted Writable, @OsUntrusted Writable>> entrySet(@OsUntrusted MapWritable this) {
    return instance.entrySet();
  }

  @Override
  public @OsUntrusted boolean equals(@OsUntrusted MapWritable this, @OsUntrusted Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof @OsUntrusted MapWritable) {
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
  public @OsUntrusted Writable get(@OsUntrusted MapWritable this, @OsUntrusted Object key) {
    return instance.get(key);
  }
  
  @Override
  public @OsUntrusted int hashCode(@OsUntrusted MapWritable this) {
    return 1 + this.instance.hashCode();
  }

  @Override
  public @OsUntrusted boolean isEmpty(@OsUntrusted MapWritable this) {
    return instance.isEmpty();
  }

  @Override
  public @OsUntrusted Set<@OsUntrusted Writable> keySet(@OsUntrusted MapWritable this) {
    return instance.keySet();
  }

  @Override
  @SuppressWarnings("unchecked")
  public @OsUntrusted Writable put(@OsUntrusted MapWritable this, @OsUntrusted Writable key, @OsUntrusted Writable value) {
    addToMap(key.getClass());
    addToMap(value.getClass());
    return instance.put(key, value);
  }

  @Override
  public void putAll(@OsUntrusted MapWritable this, @OsUntrusted Map<@OsUntrusted ? extends @OsUntrusted Writable, @OsUntrusted ? extends @OsUntrusted Writable> t) {
    for (Map.@OsUntrusted Entry<@OsUntrusted ? extends @OsUntrusted Writable, @OsUntrusted ? extends @OsUntrusted Writable> e: t.entrySet()) {
      put(e.getKey(), e.getValue());
    }
  }

  @Override
  public @OsUntrusted Writable remove(@OsUntrusted MapWritable this, @OsUntrusted Object key) {
    return instance.remove(key);
  }

  @Override
  public @OsUntrusted int size(@OsUntrusted MapWritable this) {
    return instance.size();
  }

  @Override
  public @OsUntrusted Collection<@OsUntrusted Writable> values(@OsUntrusted MapWritable this) {
    return instance.values();
  }
  
  // Writable
  
  @Override
  public void write(@OsUntrusted MapWritable this, @OsUntrusted DataOutput out) throws IOException {
    super.write(out);
    
    // Write out the number of entries in the map
    
    out.writeInt(instance.size());

    // Then write out each key/value pair
    
    for (Map.@OsUntrusted Entry<@OsUntrusted Writable, @OsUntrusted Writable> e: instance.entrySet()) {
      out.writeByte(getId(e.getKey().getClass()));
      e.getKey().write(out);
      out.writeByte(getId(e.getValue().getClass()));
      e.getValue().write(out);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(@OsUntrusted MapWritable this, @OsUntrusted DataInput in) throws IOException {
    super.readFields(in);
    
    // First clear the map.  Otherwise we will just accumulate
    // entries every time this method is called.
    this.instance.clear();
    
    // Read the number of entries in the map
    
    @OsUntrusted
    int entries = in.readInt();
    
    // Then read each key/value pair
    
    for (@OsUntrusted int i = 0; i < entries; i++) {
      @OsUntrusted
      Writable key = (@OsUntrusted Writable) ReflectionUtils.newInstance(getClass(
          in.readByte()), getConf());
      
      key.readFields(in);
      
      @OsUntrusted
      Writable value = (@OsUntrusted Writable) ReflectionUtils.newInstance(getClass(
          in.readByte()), getConf());
      
      value.readFields(in);
      instance.put(key, value);
    }
  }
}
