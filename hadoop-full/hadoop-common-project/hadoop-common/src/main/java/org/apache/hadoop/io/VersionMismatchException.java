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

/** Thrown by {@link VersionedWritable#readFields(DataInput)} when the
 * version of an object being read does not match the current implementation
 * version as returned by {@link VersionedWritable#getVersion()}. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class VersionMismatchException extends @OsUntrusted IOException {

  private @OsUntrusted byte expectedVersion;
  private @OsUntrusted byte foundVersion;

  public @OsUntrusted VersionMismatchException(@OsUntrusted byte expectedVersionIn, @OsUntrusted byte foundVersionIn){
    expectedVersion = expectedVersionIn;
    foundVersion = foundVersionIn;
  }

  /** Returns a string representation of this object. */
  @Override
  public @OsUntrusted String toString(@OsUntrusted VersionMismatchException this){
    return "A record version mismatch occured. Expecting v"
      + expectedVersion + ", found v" + foundVersion; 
  }
}
