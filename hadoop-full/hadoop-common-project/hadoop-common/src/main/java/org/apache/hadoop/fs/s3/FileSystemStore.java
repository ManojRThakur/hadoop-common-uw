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

package org.apache.hadoop.fs.s3;

import ostrusted.quals.OsUntrusted;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * A facility for storing and retrieving {@link INode}s and {@link Block}s.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface FileSystemStore {
  
  void initialize(@OsUntrusted FileSystemStore this, @OsUntrusted URI uri, @OsUntrusted Configuration conf) throws IOException;
  @OsUntrusted
  String getVersion(@OsUntrusted FileSystemStore this) throws IOException;

  void storeINode(@OsUntrusted FileSystemStore this, @OsUntrusted Path path, @OsUntrusted INode inode) throws IOException;
  void storeBlock(@OsUntrusted FileSystemStore this, @OsUntrusted Block block, @OsUntrusted File file) throws IOException;
  
  @OsUntrusted
  boolean inodeExists(@OsUntrusted FileSystemStore this, @OsUntrusted Path path) throws IOException;
  @OsUntrusted
  boolean blockExists(@OsUntrusted FileSystemStore this, @OsUntrusted long blockId) throws IOException;

  @OsUntrusted
  INode retrieveINode(@OsUntrusted FileSystemStore this, @OsUntrusted Path path) throws IOException;
  @OsUntrusted
  File retrieveBlock(@OsUntrusted FileSystemStore this, @OsUntrusted Block block, @OsUntrusted long byteRangeStart) throws IOException;

  void deleteINode(@OsUntrusted FileSystemStore this, @OsUntrusted Path path) throws IOException;
  void deleteBlock(@OsUntrusted FileSystemStore this, @OsUntrusted Block block) throws IOException;

  @OsUntrusted
  Set<@OsUntrusted Path> listSubPaths(@OsUntrusted FileSystemStore this, @OsUntrusted Path path) throws IOException;
  @OsUntrusted
  Set<@OsUntrusted Path> listDeepSubPaths(@OsUntrusted FileSystemStore this, @OsUntrusted Path path) throws IOException;

  /**
   * Delete everything. Used for testing.
   * @throws IOException
   */
  void purge(@OsUntrusted FileSystemStore this) throws IOException;
  
  /**
   * Diagnostic method to dump all INodes to the console.
   * @throws IOException
   */
  void dump(@OsUntrusted FileSystemStore this) throws IOException;
}
