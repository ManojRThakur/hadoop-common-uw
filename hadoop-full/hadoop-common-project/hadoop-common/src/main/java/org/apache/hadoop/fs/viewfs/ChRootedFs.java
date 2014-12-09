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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

/**
 * <code>ChrootedFs</code> is a file system with its root some path
 * below the root of its base file system.
 * Example: For a base file system hdfs://nn1/ with chRoot at /usr/foo, the
 * members will be setup as shown below.
 * <ul>
 * <li>myFs is the base file system and points to hdfs at nn1</li>
 * <li>myURI is hdfs://nn1/user/foo</li>
 * <li>chRootPathPart is /user/foo</li>
 * <li>workingDir is a directory related to chRoot</li>
 * </ul>
 * 
 * The paths are resolved as follows by ChRootedFileSystem:
 * <ul>
 * <li> Absolute path /a/b/c is resolved to /user/foo/a/b/c at myFs</li>
 * <li> Relative path x/y is resolved to /user/foo/<workingDir>/x/y</li>
 * </ul>

 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
class ChRootedFs extends @OsUntrusted AbstractFileSystem {
  private final @OsUntrusted AbstractFileSystem myFs;  // the base file system whose root is changed
  private final @OsUntrusted URI myUri; // the base URI + the chroot
  private final @OsUntrusted Path chRootPathPart; // the root below the root of the base
  private final @OsUntrusted String chRootPathPartString;
  
  protected @OsUntrusted AbstractFileSystem getMyFs(@OsUntrusted ChRootedFs this) {
    return myFs;
  }
  
  /**
   * 
   * @param path
   * @return return full path including the chroot
   */
  protected @OsUntrusted Path fullPath(@OsUntrusted ChRootedFs this, final @OsUntrusted Path path) {
    super.checkPath(path);
    return new @OsUntrusted Path((chRootPathPart.isRoot() ? "" : chRootPathPartString)
        + path.toUri().getPath());
  }

  @Override
  public @OsUntrusted boolean isValidName(@OsUntrusted ChRootedFs this, @OsUntrusted String src) {
    return myFs.isValidName(fullPath(new @OsUntrusted Path(src)).toUri().toString());
  }

  public @OsUntrusted ChRootedFs(final @OsUntrusted AbstractFileSystem fs, final @OsUntrusted Path theRoot)
    throws URISyntaxException {
    super(fs.getUri(), fs.getUri().getScheme(),
        fs.getUri().getAuthority() != null, fs.getUriDefaultPort());
    myFs = fs;
    myFs.checkPath(theRoot);
    chRootPathPart = new @OsUntrusted Path(myFs.getUriPath(theRoot));
    chRootPathPartString = chRootPathPart.toUri().getPath();
    /*
     * We are making URI include the chrootedPath: e.g. file:///chrootedPath.
     * This is questionable since Path#makeQualified(uri, path) ignores
     * the pathPart of a uri. Since this class is internal we can ignore
     * this issue but if we were to make it external then this needs
     * to be resolved.
     */
    // Handle the two cases:
    //              scheme:/// and scheme://authority/
    myUri = new @OsUntrusted URI(myFs.getUri().toString() + 
        (myFs.getUri().getAuthority() == null ? "" :  Path.SEPARATOR) +
          chRootPathPart.toUri().getPath().substring(1));
    super.checkPath(theRoot);
  }
  
  @Override
  public @OsUntrusted URI getUri(@OsUntrusted ChRootedFs this) {
    return myUri;
  }

  
  /**
   *  
   * Strip out the root from the path.
   * 
   * @param p - fully qualified path p
   * @return -  the remaining path  without the begining /
   */
  public @OsUntrusted String stripOutRoot(@OsUntrusted ChRootedFs this, final @OsUntrusted Path p) {
    try {
     checkPath(p);
    } catch (@OsUntrusted IllegalArgumentException e) {
      throw new @OsUntrusted RuntimeException("Internal Error - path " + p +
          " should have been with URI" + myUri);
    }
    @OsUntrusted
    String pathPart = p.toUri().getPath();
    return  (pathPart.length() == chRootPathPartString.length()) ?
        "" : pathPart.substring(chRootPathPartString.length() +
            (chRootPathPart.isRoot() ? 0 : 1));
  }
  

  @Override
  public @OsUntrusted Path getHomeDirectory(@OsUntrusted ChRootedFs this) {
    return myFs.getHomeDirectory();
  }
  
  @Override
  public @OsUntrusted Path getInitialWorkingDirectory(@OsUntrusted ChRootedFs this) {
    /*
     * 3 choices here: return null or / or strip out the root out of myFs's
     *  inital wd. 
     * Only reasonable choice for initialWd for chrooted fds is null 
     */
    return null;
  }
  
  
  public @OsUntrusted Path getResolvedQualifiedPath(@OsUntrusted ChRootedFs this, final @OsUntrusted Path f)
      throws FileNotFoundException {
    return myFs.makeQualified(
        new @OsUntrusted Path(chRootPathPartString + f.toUri().toString()));
  }
  
  @Override
  public @OsUntrusted FSDataOutputStream createInternal(@OsUntrusted ChRootedFs this, final @OsUntrusted Path f,
      final @OsUntrusted EnumSet<@OsUntrusted CreateFlag> flag, final @OsUntrusted FsPermission absolutePermission,
      final @OsUntrusted int bufferSize, final @OsUntrusted short replication, final @OsUntrusted long blockSize,
      final @OsUntrusted Progressable progress, final @OsUntrusted ChecksumOpt checksumOpt,
      final @OsUntrusted boolean createParent) throws IOException, UnresolvedLinkException {
    return myFs.createInternal(fullPath(f), flag,
        absolutePermission, bufferSize,
        replication, blockSize, progress, checksumOpt, createParent);
  }

  @Override
  public @OsUntrusted boolean delete(@OsUntrusted ChRootedFs this, final @OsUntrusted Path f, final @OsUntrusted boolean recursive) 
      throws IOException, UnresolvedLinkException {
    return myFs.delete(fullPath(f), recursive);
  }

  @Override
  public @OsUntrusted BlockLocation @OsUntrusted [] getFileBlockLocations(@OsUntrusted ChRootedFs this, final @OsUntrusted Path f, final @OsUntrusted long start,
      final @OsUntrusted long len) throws IOException, UnresolvedLinkException {
    return myFs.getFileBlockLocations(fullPath(f), start, len);
  }

  @Override
  public @OsUntrusted FileChecksum getFileChecksum(@OsUntrusted ChRootedFs this, final @OsUntrusted Path f) 
      throws IOException, UnresolvedLinkException {
    return myFs.getFileChecksum(fullPath(f));
  }

  @Override
  public @OsUntrusted FileStatus getFileStatus(@OsUntrusted ChRootedFs this, final @OsUntrusted Path f) 
      throws IOException, UnresolvedLinkException {
    return myFs.getFileStatus(fullPath(f));
  }

  @Override
  public @OsUntrusted FileStatus getFileLinkStatus(@OsUntrusted ChRootedFs this, final @OsUntrusted Path f) 
    throws IOException, UnresolvedLinkException {
    return myFs.getFileLinkStatus(fullPath(f));
  }
  
  @Override
  public @OsUntrusted FsStatus getFsStatus(@OsUntrusted ChRootedFs this) throws IOException {
    return myFs.getFsStatus();
  }

  @Override
  public @OsUntrusted FsServerDefaults getServerDefaults(@OsUntrusted ChRootedFs this) throws IOException {
    return myFs.getServerDefaults();
  }

  @Override
  public @OsUntrusted int getUriDefaultPort(@OsUntrusted ChRootedFs this) {
    return myFs.getUriDefaultPort();
  }

  @Override
  public @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted ChRootedFs this, final @OsUntrusted Path f) 
      throws IOException, UnresolvedLinkException {
    return myFs.listStatus(fullPath(f));
  }

  @Override
  public void mkdir(@OsUntrusted ChRootedFs this, final @OsUntrusted Path dir, final @OsUntrusted FsPermission permission,
      final @OsUntrusted boolean createParent) throws IOException, UnresolvedLinkException {
    myFs.mkdir(fullPath(dir), permission, createParent);
    
  }

  @Override
  public @OsUntrusted FSDataInputStream open(@OsUntrusted ChRootedFs this, final @OsUntrusted Path f, final @OsUntrusted int bufferSize) 
    throws IOException, UnresolvedLinkException {
    return myFs.open(fullPath(f), bufferSize);
  }

  @Override
  public void renameInternal(@OsUntrusted ChRootedFs this, final @OsUntrusted Path src, final @OsUntrusted Path dst)
    throws IOException, UnresolvedLinkException {
    // note fullPath will check that paths are relative to this FileSystem.
    // Hence both are in same file system and a rename is valid
    myFs.renameInternal(fullPath(src), fullPath(dst));
  }
  
  @Override
  public void renameInternal(@OsUntrusted ChRootedFs this, final @OsUntrusted Path src, final @OsUntrusted Path dst, 
      final @OsUntrusted boolean overwrite)
    throws IOException, UnresolvedLinkException {
    // note fullPath will check that paths are relative to this FileSystem.
    // Hence both are in same file system and a rename is valid
    myFs.renameInternal(fullPath(src), fullPath(dst), overwrite);
  }

  @Override
  public void setOwner(@OsUntrusted ChRootedFs this, final @OsUntrusted Path f, final @OsTrusted String username,
      final @OsTrusted String groupname)
    throws IOException, UnresolvedLinkException {
    myFs.setOwner(fullPath(f), username, groupname);
    
  }

  @Override
  public void setPermission(@OsUntrusted ChRootedFs this, final @OsUntrusted Path f, final @OsUntrusted FsPermission permission)
    throws IOException, UnresolvedLinkException {
    myFs.setPermission(fullPath(f), permission);
  }

  @Override
  public @OsUntrusted boolean setReplication(@OsUntrusted ChRootedFs this, final @OsUntrusted Path f, final @OsUntrusted short replication)
    throws IOException, UnresolvedLinkException {
    return myFs.setReplication(fullPath(f), replication);
  }

  @Override
  public void setTimes(@OsUntrusted ChRootedFs this, final @OsUntrusted Path f, final @OsUntrusted long mtime, final @OsUntrusted long atime) 
      throws IOException, UnresolvedLinkException {
    myFs.setTimes(fullPath(f), mtime, atime);
  }

  @Override
  public void setVerifyChecksum(@OsUntrusted ChRootedFs this, final @OsUntrusted boolean verifyChecksum) 
      throws IOException, UnresolvedLinkException {
    myFs.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public @OsUntrusted boolean supportsSymlinks(@OsUntrusted ChRootedFs this) {
    return myFs.supportsSymlinks();
  }

  @Override
  public void createSymlink(@OsUntrusted ChRootedFs this, final @OsUntrusted Path target, final @OsUntrusted Path link,
      final @OsUntrusted boolean createParent) throws IOException, UnresolvedLinkException {
    /*
     * We leave the link alone:
     * If qualified or link relative then of course it is okay.
     * If absolute (ie / relative) then the link has to be resolved
     * relative to the changed root.
     */
    myFs.createSymlink(fullPath(target), link, createParent);
  }

  @Override
  public @OsUntrusted Path getLinkTarget(@OsUntrusted ChRootedFs this, final @OsUntrusted Path f) throws IOException {
    return myFs.getLinkTarget(fullPath(f));
  }
  
  
  @Override
  public @OsUntrusted List<@OsUntrusted Token<@OsUntrusted ? extends java.lang.@OsUntrusted Object>> getDelegationTokens(@OsUntrusted ChRootedFs this, @OsUntrusted String renewer) throws IOException {
    return myFs.getDelegationTokens(renewer);
  }
}
