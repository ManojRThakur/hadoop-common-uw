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

package org.apache.hadoop.conf;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.util.Map;
import java.util.Collection;
import java.util.HashMap;

public class ReconfigurationUtil {

  public static class PropertyChange {
    public @OsUntrusted String prop;
    public @OsUntrusted String oldVal;
    public @OsUntrusted String newVal;

    public @OsUntrusted PropertyChange(@OsUntrusted String prop, @OsUntrusted String newVal, @OsUntrusted String oldVal) {
      this.prop = prop;
      this.newVal = newVal;
      this.oldVal = oldVal;
    }
  }

  public static @OsUntrusted Collection<@OsUntrusted PropertyChange> 
    getChangedProperties(@OsUntrusted Configuration newConf, @OsUntrusted Configuration oldConf) {
    @OsUntrusted
    Map<@OsUntrusted String, @OsUntrusted PropertyChange> changes = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted PropertyChange>();

    // iterate over old configuration
    for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsTrusted String> oldEntry: oldConf) {
      @OsUntrusted
      String prop = oldEntry.getKey();
      @OsUntrusted
      String oldVal = oldEntry.getValue();
      @OsUntrusted
      String newVal = newConf.getRaw(prop);
      
      if (newVal == null || !newVal.equals(oldVal)) {
        changes.put(prop, new @OsUntrusted PropertyChange(prop, newVal, oldVal));
      }
    }
    
    // now iterate over new configuration
    // (to look for properties not present in old conf)
    for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsTrusted String> newEntry: newConf) {
      @OsUntrusted
      String prop = newEntry.getKey();
      @OsUntrusted
      String newVal = newEntry.getValue();
      if (oldConf.get(prop) == null) {
        changes.put(prop, new @OsUntrusted PropertyChange(prop, newVal, null));
      }
    } 

    return changes.values();
  }
}