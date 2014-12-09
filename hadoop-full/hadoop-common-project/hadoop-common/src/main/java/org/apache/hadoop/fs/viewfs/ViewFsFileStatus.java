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
package org.apache.hadoop.fs.viewfs;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;


/**
 * This class is needed to address the  problem described in
 * {@link ViewFileSystem#getFileStatus(org.apache.hadoop.fs.Path)} and
 * {@link ViewFs#getFileStatus(org.apache.hadoop.fs.Path)}
 */
class ViewFsFileStatus extends @OsUntrusted FileStatus {
   final @OsUntrusted FileStatus myFs;
   @OsUntrusted
   Path modifiedPath;
   @OsUntrusted
   ViewFsFileStatus(@OsUntrusted FileStatus fs, @OsUntrusted Path newPath) {
     myFs = fs;
     modifiedPath = newPath;
   }
   
   @Override
   public @OsUntrusted boolean equals(@OsUntrusted ViewFsFileStatus this, @OsUntrusted Object o) {
     return super.equals(o);
   }
   
   @Override
  public @OsUntrusted int hashCode(@OsUntrusted ViewFsFileStatus this) {
     return super.hashCode();
   }
   
   @Override
   public @OsUntrusted long getLen(@OsUntrusted ViewFsFileStatus this) {
     return myFs.getLen();
   }

   @Override
   public @OsUntrusted boolean isFile(@OsUntrusted ViewFsFileStatus this) {
     return myFs.isFile();
   }

   @Override
   public @OsUntrusted boolean isDirectory(@OsUntrusted ViewFsFileStatus this) {
     return  myFs.isDirectory();
   }
   
   @Override
   @SuppressWarnings("deprecation")
   public @OsUntrusted boolean isDir(@OsUntrusted ViewFsFileStatus this) {
     return myFs.isDirectory();
   }
   
   @Override
   public @OsUntrusted boolean isSymlink(@OsUntrusted ViewFsFileStatus this) {
     return myFs.isSymlink();
   }

   @Override
   public @OsUntrusted long getBlockSize(@OsUntrusted ViewFsFileStatus this) {
     return myFs.getBlockSize();
   }

   @Override
   public @OsUntrusted short getReplication(@OsUntrusted ViewFsFileStatus this) {
     return myFs.getReplication();
   }

   @Override
   public @OsUntrusted long getModificationTime(@OsUntrusted ViewFsFileStatus this) {
     return myFs.getModificationTime();
   }

   @Override
   public @OsUntrusted long getAccessTime(@OsUntrusted ViewFsFileStatus this) {
     return myFs.getAccessTime();
   }

   @Override
   public @OsUntrusted FsPermission getPermission(@OsUntrusted ViewFsFileStatus this) {
     return myFs.getPermission();
   }
   
   @Override
   public @OsTrusted String getOwner(@OsUntrusted ViewFsFileStatus this) {
     return myFs.getOwner();
   }
   
   @Override
   public @OsTrusted String getGroup(@OsUntrusted ViewFsFileStatus this) {
     return myFs.getGroup();
   }
   
   @Override
   public @OsUntrusted Path getPath(@OsUntrusted ViewFsFileStatus this) {
     return modifiedPath;
   }
   
   @Override
   public void setPath(@OsUntrusted ViewFsFileStatus this, final @OsUntrusted Path p) {
     modifiedPath = p;
   }

   @Override
   public @OsUntrusted Path getSymlink(@OsUntrusted ViewFsFileStatus this) throws IOException {
     return myFs.getSymlink();
   }
}

