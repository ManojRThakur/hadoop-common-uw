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
package org.apache.hadoop.metrics.util;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics.MetricsRecord;

/**
 * 
 * This is base class for all metrics
 *
 */
@InterfaceAudience.Private
public abstract class MetricsBase {
  public static final @OsUntrusted String NO_DESCRIPTION = "NoDescription";
  final private @OsUntrusted String name;
  final private @OsUntrusted String description;
  
  protected @OsUntrusted MetricsBase(final @OsUntrusted String nam) {
    name = nam;
    description = NO_DESCRIPTION;
  }
  
  protected @OsUntrusted MetricsBase(final @OsUntrusted String nam, final @OsUntrusted String desc) {
    name = nam;
    description = desc;
  }
  
  public abstract void pushMetric(@OsUntrusted MetricsBase this, final @OsUntrusted MetricsRecord mr);
  
  public @OsUntrusted String getName(@OsUntrusted MetricsBase this) { return name; }
  public @OsUntrusted String getDescription(@OsUntrusted MetricsBase this) { return description; };

}
