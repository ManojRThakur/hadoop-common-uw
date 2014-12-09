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
package org.apache.hadoop.fs;

import ostrusted.quals.OsUntrusted;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/** An abstract class representing file checksums for files. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileChecksum implements @OsUntrusted Writable {
  /** The checksum algorithm name */ 
  public abstract @OsUntrusted String getAlgorithmName(@OsUntrusted FileChecksum this);

  /** The length of the checksum in bytes */ 
  public abstract @OsUntrusted int getLength(@OsUntrusted FileChecksum this);

  /** The value of the checksum in bytes */ 
  public abstract @OsUntrusted byte @OsUntrusted [] getBytes(@OsUntrusted FileChecksum this);

  /** Return true if both the algorithms and the values are the same. */
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted FileChecksum this, @OsUntrusted Object other) {
    if (other == this) {
      return true;
    }
    if (other == null || !(other instanceof @OsUntrusted FileChecksum)) {
      return false;
    }

    final @OsUntrusted FileChecksum that = (@OsUntrusted FileChecksum)other;
    return this.getAlgorithmName().equals(that.getAlgorithmName())
      && Arrays.equals(this.getBytes(), that.getBytes());
  }
  
  @Override
  public @OsUntrusted int hashCode(@OsUntrusted FileChecksum this) {
    return getAlgorithmName().hashCode() ^ Arrays.hashCode(getBytes());
  }
}