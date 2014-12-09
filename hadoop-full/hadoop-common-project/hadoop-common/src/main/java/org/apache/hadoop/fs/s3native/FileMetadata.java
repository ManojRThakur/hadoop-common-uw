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

package org.apache.hadoop.fs.s3native;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * <p>
 * Holds basic metadata for a file stored in a {@link NativeFileSystemStore}.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class FileMetadata {
  private final @OsUntrusted String key;
  private final @OsUntrusted long length;
  private final @OsUntrusted long lastModified;
  
  public @OsUntrusted FileMetadata(@OsUntrusted String key, @OsUntrusted long length, @OsUntrusted long lastModified) {
    this.key = key;
    this.length = length;
    this.lastModified = lastModified;
  }
  
  public @OsUntrusted String getKey(@OsUntrusted FileMetadata this) {
    return key;
  }
  
  public @OsUntrusted long getLength(@OsUntrusted FileMetadata this) {
    return length;
  }

  public @OsUntrusted long getLastModified(@OsUntrusted FileMetadata this) {
    return lastModified;
  }
  
  @Override
  public @OsUntrusted String toString(@OsUntrusted FileMetadata this) {
    return "FileMetadata[" + key + ", " + length + ", " + lastModified + "]";
  }
  
}
