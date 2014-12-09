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

package org.apache.hadoop.io.serializer;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * <p>
 * Encapsulates a {@link Serializer}/{@link Deserializer} pair.
 * </p>
 * @param <T>
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public interface Serialization<@OsUntrusted T extends java.lang.@OsUntrusted Object> {
  
  /**
   * Allows clients to test whether this {@link Serialization}
   * supports the given class.
   */
  @OsUntrusted
  boolean accept(@OsUntrusted Serialization<T> this, @OsUntrusted Class<?> c);
  
  /**
   * @return a {@link Serializer} for the given class.
   */
  @OsUntrusted
  Serializer<@OsUntrusted T> getSerializer(@OsUntrusted Serialization<T> this, @OsUntrusted Class<@OsUntrusted T> c);

  /**
   * @return a {@link Deserializer} for the given class.
   */
  @OsUntrusted
  Deserializer<@OsUntrusted T> getDeserializer(@OsUntrusted Serialization<T> this, @OsUntrusted Class<@OsUntrusted T> c);
}
