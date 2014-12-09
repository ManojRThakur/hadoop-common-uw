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
package org.apache.hadoop.security.authorize;

import ostrusted.quals.OsUntrusted;
import java.security.Permission;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * An abstract definition of <em>service</em> as related to 
 * Service Level Authorization for Hadoop.
 * 
 * Each service defines it's configuration key and also the necessary
 * {@link Permission} required to access the service.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class Service {
  private @OsUntrusted String key;
  private @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol;
  
  public @OsUntrusted Service(@OsUntrusted String key, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol) {
    this.key = key;
    this.protocol = protocol;
  }
  
  /**
   * Get the configuration key for the service.
   * @return the configuration key for the service
   */
  public @OsUntrusted String getServiceKey(@OsUntrusted Service this) {
    return key;
  }
  
  /**
   * Get the protocol for the service
   * @return the {@link Class} for the protocol
   */
  public @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> getProtocol(@OsUntrusted Service this) {
    return protocol;
  }
}
