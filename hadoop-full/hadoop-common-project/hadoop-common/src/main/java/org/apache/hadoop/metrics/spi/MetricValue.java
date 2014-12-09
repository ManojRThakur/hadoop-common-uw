/*
 * MetricValue.java
 *
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

package org.apache.hadoop.metrics.spi;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A Number that is either an absolute or an incremental amount.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MetricValue {
    
  public static final @OsUntrusted boolean ABSOLUTE = false;
  public static final @OsUntrusted boolean INCREMENT = true;
    
  private @OsUntrusted boolean isIncrement;
  private @OsUntrusted Number number;
    
  /** Creates a new instance of MetricValue */
  public @OsUntrusted MetricValue(@OsUntrusted Number number, @OsUntrusted boolean isIncrement) {
    this.number = number;
    this.isIncrement = isIncrement;
  }

  public @OsUntrusted boolean isIncrement(@OsUntrusted MetricValue this) {
    return isIncrement;
  }
    
  public @OsUntrusted boolean isAbsolute(@OsUntrusted MetricValue this) {
    return !isIncrement;
  }

  public @OsUntrusted Number getNumber(@OsUntrusted MetricValue this) {
    return number;
  }
    
}
