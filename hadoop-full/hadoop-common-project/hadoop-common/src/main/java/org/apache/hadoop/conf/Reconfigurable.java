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
import java.util.Collection;

/**
 * Something whose {@link Configuration} can be changed at run time.
 */
public interface Reconfigurable extends @OsUntrusted Configurable {

  /**
   * Change a configuration property on this object to the value specified.
   *
   * Change a configuration property on this object to the value specified 
   * and return the previous value that the configuration property was set to
   * (or null if it was not previously set). If newVal is null, set the property
   * to its default value;
   *
   * If the property cannot be changed, throw a 
   * {@link ReconfigurationException}.
   */
  public @OsUntrusted String reconfigureProperty(@OsUntrusted Reconfigurable this, @OsUntrusted String property, @OsTrusted String newVal)
    throws ReconfigurationException;

  /**
   * Return whether a given property is changeable at run time.
   *
   * If isPropertyReconfigurable returns true for a property,
   * then changeConf should not throw an exception when changing
   * this property.
   */
  public @OsUntrusted boolean isPropertyReconfigurable(@OsUntrusted Reconfigurable this, @OsUntrusted String property);

  /**
   * Return all the properties that can be changed at run time.
   */
  public @OsUntrusted Collection<@OsUntrusted String> getReconfigurableProperties(@OsUntrusted Reconfigurable this);


}
