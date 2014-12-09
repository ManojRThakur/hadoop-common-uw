package org.apache.hadoop.fs;
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
import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

/**
 * A <code>FilterFs</code> contains some other file system, which it uses as its
 * basic file system, possibly transforming the data along the way or providing
 * additional functionality. The class <code>FilterFs</code> itself simply
 * overrides all methods of <code>AbstractFileSystem</code> with versions that
 * pass all requests to the contained file system. Subclasses of
 * <code>FilterFs</code> may further override some of these methods and may also
 * provide additional methods and fields.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public abstract class FilterFs extends @OsUntrusted AbstractFileSystem {
  private final @OsUntrusted AbstractFileSystem myFs;
  
  protected @OsUntrusted AbstractFileSystem getMyFs(@OsUntrusted FilterFs this) {
    return myFs;
  }
  
  protected @OsUntrusted FilterFs(@OsUntrusted AbstractFileSystem fs) throws IOException,
      URISyntaxException {
    super(fs.getUri(), fs.getUri().getScheme(),
        fs.getUri().getAuthority() != null, fs.getUriDefaultPort());
    myFs = fs;
  }

  @Override
  public @OsUntrusted Statistics getStatistics(@OsUntrusted FilterFs this) {
    return myFs.getStatistics();
  }
  
  @Override
  public @OsUntrusted Path makeQualified(@OsUntrusted FilterFs this, @OsUntrusted Path path) {
    return myFs.makeQualified(path);
  }

  @Override
  public @OsUntrusted Path getInitialWorkingDirectory(@OsUntrusted FilterFs this) {
    return myFs.getInitialWorkingDirectory();
  }
  
  @Override
  public @OsUntrusted Path getHomeDirectory(@OsUntrusted FilterFs this) {
    return myFs.getHomeDirectory();
  }
  
  @Override
  public @OsUntrusted FSDataOutputStream createInternal(@OsUntrusted FilterFs this, @OsUntrusted Path f,
    @OsUntrusted
    EnumSet<@OsUntrusted CreateFlag> flag, @OsUntrusted FsPermission absolutePermission, @OsUntrusted int bufferSize,
    @OsUntrusted
    short replication, @OsUntrusted long blockSize, @OsUntrusted Progressable progress,
    @OsUntrusted
    ChecksumOpt checksumOpt, @OsUntrusted boolean createParent) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.createInternal(f, flag, absolutePermission, bufferSize,
        replication, blockSize, progress, checksumOpt, createParent);
  }

  @Override
  public @OsUntrusted boolean delete(@OsUntrusted FilterFs this, @OsUntrusted Path f, @OsUntrusted boolean recursive) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.delete(f, recursive);
  }

  @Override
  public @OsUntrusted BlockLocation @OsUntrusted [] getFileBlockLocations(@OsUntrusted FilterFs this, @OsUntrusted Path f, @OsUntrusted long start, @OsUntrusted long len)
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileBlockLocations(f, start, len);
  }

  @Override
  public @OsUntrusted FileChecksum getFileChecksum(@OsUntrusted FilterFs this, @OsUntrusted Path f) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileChecksum(f);
  }

  @Override
  public @OsUntrusted FileStatus getFileStatus(@OsUntrusted FilterFs this, @OsUntrusted Path f) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileStatus(f);
  }

  @Override
  public @OsUntrusted FileStatus getFileLinkStatus(@OsUntrusted FilterFs this, final @OsUntrusted Path f) 
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileLinkStatus(f);
  }
  
  @Override
  public @OsUntrusted FsStatus getFsStatus(@OsUntrusted FilterFs this, final @OsUntrusted Path f) throws AccessControlException,
    FileNotFoundException, UnresolvedLinkException, IOException {
    return myFs.getFsStatus(f);
  }

  @Override
  public @OsUntrusted FsStatus getFsStatus(@OsUntrusted FilterFs this) throws IOException {
    return myFs.getFsStatus();
  }

  @Override
  public @OsUntrusted FsServerDefaults getServerDefaults(@OsUntrusted FilterFs this) throws IOException {
    return myFs.getServerDefaults();
  }
  

  @Override
  public @OsUntrusted Path resolvePath(@OsUntrusted FilterFs this, final @OsUntrusted Path p) throws FileNotFoundException,
        UnresolvedLinkException, AccessControlException, IOException {
    return myFs.resolvePath(p);
  }

  @Override
  public @OsUntrusted int getUriDefaultPort(@OsUntrusted FilterFs this) {
    return myFs.getUriDefaultPort();
  }

  @Override
  public @OsUntrusted URI getUri(@OsUntrusted FilterFs this) {
    return myFs.getUri();
  }
  
  @Override
  public void checkPath(@OsUntrusted FilterFs this, @OsUntrusted Path path) {
    myFs.checkPath(path);
  }
  
  @Override
  public @OsUntrusted String getUriPath(@OsUntrusted FilterFs this, final @OsUntrusted Path p) {
    return myFs.getUriPath(p);
  }
  
  @Override
  public @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted FilterFs this, @OsUntrusted Path f) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.listStatus(f);
  }

  @Override
  public @OsUntrusted RemoteIterator<@OsUntrusted Path> listCorruptFileBlocks(@OsUntrusted FilterFs this, @OsUntrusted Path path)
    throws IOException {
    return myFs.listCorruptFileBlocks(path);
  }

  @Override
  public void mkdir(@OsUntrusted FilterFs this, @OsUntrusted Path dir, @OsUntrusted FsPermission permission, @OsUntrusted boolean createParent)
    throws IOException, UnresolvedLinkException {
    checkPath(dir);
    myFs.mkdir(dir, permission, createParent);
    
  }

  @Override
  public @OsUntrusted FSDataInputStream open(@OsUntrusted FilterFs this, final @OsUntrusted Path f) throws AccessControlException,
    FileNotFoundException, UnresolvedLinkException, IOException {
    checkPath(f);
    return myFs.open(f);
  }

  @Override
  public @OsUntrusted FSDataInputStream open(@OsUntrusted FilterFs this, @OsUntrusted Path f, @OsUntrusted int bufferSize) 
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.open(f, bufferSize);
  }

  @Override
  public void renameInternal(@OsUntrusted FilterFs this, @OsUntrusted Path src, @OsUntrusted Path dst) 
    throws IOException, UnresolvedLinkException {
    checkPath(src);
    checkPath(dst);
    myFs.rename(src, dst, Options.Rename.NONE);
  }

  @Override
  public void renameInternal(@OsUntrusted FilterFs this, final @OsUntrusted Path src, final @OsUntrusted Path dst,
      @OsUntrusted
      boolean overwrite) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnresolvedLinkException, IOException {
    myFs.renameInternal(src, dst, overwrite);
  }
  
  @Override
  public void setOwner(@OsUntrusted FilterFs this, @OsUntrusted Path f, @OsTrusted String username, @OsTrusted String groupname)
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    myFs.setOwner(f, username, groupname);
    
  }

  @Override
  public void setPermission(@OsUntrusted FilterFs this, @OsUntrusted Path f, @OsUntrusted FsPermission permission)
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    myFs.setPermission(f, permission);
  }

  @Override
  public @OsUntrusted boolean setReplication(@OsUntrusted FilterFs this, @OsUntrusted Path f, @OsUntrusted short replication)
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.setReplication(f, replication);
  }

  @Override
  public void setTimes(@OsUntrusted FilterFs this, @OsUntrusted Path f, @OsUntrusted long mtime, @OsUntrusted long atime) 
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    myFs.setTimes(f, mtime, atime);
  }

  @Override
  public void setVerifyChecksum(@OsUntrusted FilterFs this, @OsUntrusted boolean verifyChecksum) 
      throws IOException, UnresolvedLinkException {
    myFs.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public @OsUntrusted boolean supportsSymlinks(@OsUntrusted FilterFs this) {
    return myFs.supportsSymlinks();
  }

  @Override
  public void createSymlink(@OsUntrusted FilterFs this, @OsUntrusted Path target, @OsUntrusted Path link, @OsUntrusted boolean createParent) 
    throws IOException, UnresolvedLinkException {
    myFs.createSymlink(target, link, createParent);
  }

  @Override
  public @OsUntrusted Path getLinkTarget(@OsUntrusted FilterFs this, final @OsUntrusted Path f) throws IOException {
    return myFs.getLinkTarget(f);
  }
  
  @Override // AbstractFileSystem
  public @OsUntrusted String getCanonicalServiceName(@OsUntrusted FilterFs this) {
    return myFs.getCanonicalServiceName();
  }
  
  @Override // AbstractFileSystem
  public @OsUntrusted List<@OsUntrusted Token<@OsUntrusted ? extends java.lang.@OsUntrusted Object>> getDelegationTokens(@OsUntrusted FilterFs this, @OsUntrusted String renewer) throws IOException {
    return myFs.getDelegationTokens(renewer);
  }

  @Override
  public @OsUntrusted boolean isValidName(@OsUntrusted FilterFs this, @OsUntrusted String src) {
    return myFs.isValidName(src);
  }
}
