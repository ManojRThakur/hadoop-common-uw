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
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Stringifier interface offers two methods to convert an object 
 * to a string representation and restore the object given its 
 * string representation.
 * @param <T> the class of the objects to stringify
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Stringifier<@OsUntrusted T extends java.lang.@OsUntrusted Object> extends java.io.Closeable {

  /**
   * Converts the object to a string representation
   * @param obj the object to convert
   * @return the string representation of the object
   * @throws IOException if the object cannot be converted
   */
  public @OsUntrusted String toString(@OsUntrusted Stringifier<T> this, @OsUntrusted T obj)  throws IOException;
  
  /**
   * Restores the object from its string representation.
   * @param str the string representation of the object
   * @return restored object
   * @throws IOException if the object cannot be restored
   */
  public @OsUntrusted T fromString(@OsUntrusted Stringifier<T> this, @OsUntrusted String str) throws IOException;
  
  
  /** 
   * Closes this object. 
   * @throws IOException if an I/O error occurs 
   * */
  @Override
  public void close(@OsUntrusted Stringifier<T> this) throws IOException;
  
}
