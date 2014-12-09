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
import ostrusted.quals.OsTrusted;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

/**
 * Implementation of AbstractFileSystem based on the existing implementation of 
 * {@link FileSystem}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class DelegateToFileSystem extends @OsUntrusted AbstractFileSystem {
  protected final @OsUntrusted FileSystem fsImpl;
  
  protected @OsUntrusted DelegateToFileSystem(@OsUntrusted URI theUri, @OsUntrusted FileSystem theFsImpl,
      @OsUntrusted
      Configuration conf, @OsUntrusted String supportedScheme, @OsUntrusted boolean authorityRequired)
      throws IOException, URISyntaxException {
    super(theUri, supportedScheme, authorityRequired, 
        FileSystem.getDefaultUri(conf).getPort());
    fsImpl = theFsImpl;
    fsImpl.initialize(theUri, conf);
    fsImpl.statistics = getStatistics();
  }

  @Override
  public @OsUntrusted Path getInitialWorkingDirectory(@OsUntrusted DelegateToFileSystem this) {
    return fsImpl.getInitialWorkingDirectory();
  }
  
  @Override
  @SuppressWarnings("deprecation") // call to primitiveCreate
  public @OsUntrusted FSDataOutputStream createInternal (@OsUntrusted DelegateToFileSystem this, @OsUntrusted Path f,
      @OsUntrusted
      EnumSet<@OsUntrusted CreateFlag> flag, @OsUntrusted FsPermission absolutePermission, @OsUntrusted int bufferSize,
      @OsUntrusted
      short replication, @OsUntrusted long blockSize, @OsUntrusted Progressable progress,
      @OsUntrusted
      ChecksumOpt checksumOpt, @OsUntrusted boolean createParent) throws IOException {
    checkPath(f);
    
    // Default impl assumes that permissions do not matter
    // calling the regular create is good enough.
    // FSs that implement permissions should override this.

    if (!createParent) { // parent must exist.
      // since this.create makes parent dirs automatically
      // we must throw exception if parent does not exist.
      final @OsUntrusted FileStatus stat = getFileStatus(f.getParent());
      if (stat == null) {
        throw new @OsUntrusted FileNotFoundException("Missing parent:" + f);
      }
      if (!stat.isDirectory()) {
          throw new @OsUntrusted ParentNotDirectoryException("parent is not a dir:" + f);
      }
      // parent does exist - go ahead with create of file.
    }
    return fsImpl.primitiveCreate(f, absolutePermission, flag,
        bufferSize, replication, blockSize, progress, checksumOpt);
  }

  @Override
  public @OsUntrusted boolean delete(@OsUntrusted DelegateToFileSystem this, @OsUntrusted Path f, @OsUntrusted boolean recursive) throws IOException {
    checkPath(f);
    return fsImpl.delete(f, recursive);
  }

  @Override
  public @OsUntrusted BlockLocation @OsUntrusted [] getFileBlockLocations(@OsUntrusted DelegateToFileSystem this, @OsUntrusted Path f, @OsUntrusted long start, @OsUntrusted long len)
      throws IOException {
    checkPath(f);
    return fsImpl.getFileBlockLocations(f, start, len);
  }

  @Override
  public @OsUntrusted FileChecksum getFileChecksum(@OsUntrusted DelegateToFileSystem this, @OsUntrusted Path f) throws IOException {
    checkPath(f);
    return fsImpl.getFileChecksum(f);
  }

  @Override
  public @OsUntrusted FileStatus getFileStatus(@OsUntrusted DelegateToFileSystem this, @OsUntrusted Path f) throws IOException {
    checkPath(f);
    return fsImpl.getFileStatus(f);
  }

  @Override
  public @OsUntrusted FileStatus getFileLinkStatus(@OsUntrusted DelegateToFileSystem this, final @OsUntrusted Path f) throws IOException {
    @OsUntrusted
    FileStatus status = fsImpl.getFileLinkStatus(f);
    // FileSystem#getFileLinkStatus qualifies the link target
    // AbstractFileSystem needs to return it plain since it's qualified
    // in FileContext, so re-get and set the plain target
    if (status.isSymlink()) {
      status.setSymlink(fsImpl.getLinkTarget(f));
    }
    return status;
  }

  @Override
  public @OsUntrusted FsStatus getFsStatus(@OsUntrusted DelegateToFileSystem this) throws IOException {
    return fsImpl.getStatus();
  }

  @Override
  public @OsUntrusted FsServerDefaults getServerDefaults(@OsUntrusted DelegateToFileSystem this) throws IOException {
    return fsImpl.getServerDefaults();
  }
  
  @Override
  public @OsUntrusted Path getHomeDirectory(@OsUntrusted DelegateToFileSystem this) {
    return fsImpl.getHomeDirectory();
  }

  @Override
  public @OsUntrusted int getUriDefaultPort(@OsUntrusted DelegateToFileSystem this) {
    return 0;
  }

  @Override
  public @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted DelegateToFileSystem this, @OsUntrusted Path f) throws IOException {
    checkPath(f);
    return fsImpl.listStatus(f);
  }

  @Override
  @SuppressWarnings("deprecation") // call to primitiveMkdir
  public void mkdir(@OsUntrusted DelegateToFileSystem this, @OsUntrusted Path dir, @OsUntrusted FsPermission permission, @OsUntrusted boolean createParent)
      throws IOException {
    checkPath(dir);
    fsImpl.primitiveMkdir(dir, permission, createParent);
    
  }

  @Override
  public @OsUntrusted FSDataInputStream open(@OsUntrusted DelegateToFileSystem this, @OsUntrusted Path f, @OsUntrusted int bufferSize) throws IOException {
    checkPath(f);
    return fsImpl.open(f, bufferSize);
  }

  @Override
  @SuppressWarnings("deprecation") // call to rename
  public void renameInternal(@OsUntrusted DelegateToFileSystem this, @OsUntrusted Path src, @OsUntrusted Path dst) throws IOException {
    checkPath(src);
    checkPath(dst);
    fsImpl.rename(src, dst, Options.Rename.NONE);
  }

  @Override
  public void setOwner(@OsUntrusted DelegateToFileSystem this, @OsUntrusted Path f, @OsTrusted String username, @OsTrusted String groupname)
      throws IOException {
    checkPath(f);
    fsImpl.setOwner(f, username, groupname);
  }

  @Override
  public void setPermission(@OsUntrusted DelegateToFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission)
      throws IOException {
    checkPath(f);
    fsImpl.setPermission(f, permission);
  }

  @Override
  public @OsUntrusted boolean setReplication(@OsUntrusted DelegateToFileSystem this, @OsUntrusted Path f, @OsUntrusted short replication)
      throws IOException {
    checkPath(f);
    return fsImpl.setReplication(f, replication);
  }

  @Override
  public void setTimes(@OsUntrusted DelegateToFileSystem this, @OsUntrusted Path f, @OsUntrusted long mtime, @OsUntrusted long atime) throws IOException {
    checkPath(f);
    fsImpl.setTimes(f, mtime, atime);
  }

  @Override
  public void setVerifyChecksum(@OsUntrusted DelegateToFileSystem this, @OsUntrusted boolean verifyChecksum) throws IOException {
    fsImpl.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public @OsUntrusted boolean supportsSymlinks(@OsUntrusted DelegateToFileSystem this) {
    return fsImpl.supportsSymlinks();
  }  
  
  @Override
  public void createSymlink(@OsUntrusted DelegateToFileSystem this, @OsUntrusted Path target, @OsUntrusted Path link, @OsUntrusted boolean createParent) 
      throws IOException { 
    fsImpl.createSymlink(target, link, createParent);
  } 
  
  @Override
  public @OsUntrusted Path getLinkTarget(@OsUntrusted DelegateToFileSystem this, final @OsUntrusted Path f) throws IOException {
    return fsImpl.getLinkTarget(f);
  }

  @Override //AbstractFileSystem
  public @OsUntrusted String getCanonicalServiceName(@OsUntrusted DelegateToFileSystem this) {
    return fsImpl.getCanonicalServiceName();
  }
  
  @Override //AbstractFileSystem
  public @OsUntrusted List<@OsUntrusted Token<@OsUntrusted ? extends java.lang.@OsUntrusted Object>> getDelegationTokens(@OsUntrusted DelegateToFileSystem this, @OsUntrusted String renewer) throws IOException {
    return Arrays.asList(fsImpl.addDelegationTokens(renewer, null));
  }
}
