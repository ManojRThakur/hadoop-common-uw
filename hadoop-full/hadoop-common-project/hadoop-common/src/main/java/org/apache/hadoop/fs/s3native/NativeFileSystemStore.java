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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * <p>
 * An abstraction for a key-based {@link File} store.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
interface NativeFileSystemStore {
  
  void initialize(@OsUntrusted NativeFileSystemStore this, @OsUntrusted URI uri, @OsUntrusted Configuration conf) throws IOException;
  
  void storeFile(@OsUntrusted NativeFileSystemStore this, @OsUntrusted String key, @OsUntrusted File file, @OsUntrusted byte @OsUntrusted [] md5Hash) throws IOException;
  void storeEmptyFile(@OsUntrusted NativeFileSystemStore this, @OsUntrusted String key) throws IOException;
  
  @OsUntrusted
  FileMetadata retrieveMetadata(@OsUntrusted NativeFileSystemStore this, @OsUntrusted String key) throws IOException;
  @OsUntrusted
  InputStream retrieve(@OsUntrusted NativeFileSystemStore this, @OsUntrusted String key) throws IOException;
  @OsUntrusted
  InputStream retrieve(@OsUntrusted NativeFileSystemStore this, @OsUntrusted String key, @OsUntrusted long byteRangeStart) throws IOException;
  
  @OsUntrusted
  PartialListing list(@OsUntrusted NativeFileSystemStore this, @OsUntrusted String prefix, @OsUntrusted int maxListingLength) throws IOException;
  @OsUntrusted
  PartialListing list(@OsUntrusted NativeFileSystemStore this, @OsUntrusted String prefix, @OsUntrusted int maxListingLength, @OsUntrusted String priorLastKey, @OsUntrusted boolean recursive)
    throws IOException;
  
  void delete(@OsUntrusted NativeFileSystemStore this, @OsUntrusted String key) throws IOException;

  void copy(@OsUntrusted NativeFileSystemStore this, @OsUntrusted String srcKey, @OsUntrusted String dstKey) throws IOException;
  
  /**
   * Delete all keys with the given prefix. Used for testing.
   * @throws IOException
   */
  void purge(@OsUntrusted NativeFileSystemStore this, @OsUntrusted String prefix) throws IOException;
  
  /**
   * Diagnostic method to dump state to the console.
   * @throws IOException
   */
  void dump(@OsUntrusted NativeFileSystemStore this) throws IOException;
}
