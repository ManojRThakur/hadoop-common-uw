/*
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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsException;

/**
 * Experimental interface to extend metrics dynamically
 */
@InterfaceAudience.Private
public enum DefaultMetricsFactory {

@OsUntrusted  INSTANCE; // the singleton

  private @OsUntrusted MutableMetricsFactory mmfImpl;

  public static @OsUntrusted MutableMetricsFactory getAnnotatedMetricsFactory() {
    return INSTANCE.getInstance(MutableMetricsFactory.class);
  }

  @SuppressWarnings("unchecked")
  public synchronized <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted T getInstance(@OsUntrusted DefaultMetricsFactory this, @OsUntrusted Class<@OsUntrusted T> cls) {
    if (cls == MutableMetricsFactory.class) {
      if (mmfImpl == null) {
        mmfImpl = new @OsUntrusted MutableMetricsFactory();
      }
      return (@OsUntrusted T) mmfImpl;
    }
    throw new @OsUntrusted MetricsException("Unknown metrics factory type: "+ cls.getName());
  }

  public synchronized void setInstance(@OsUntrusted DefaultMetricsFactory this, @OsUntrusted MutableMetricsFactory factory) {
    mmfImpl = factory;
  }
}
