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
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * <code>ChRootedFileSystem</code> is a file system with its root some path
 * below the root of its base file system. 
 * 
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
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
class ChRootedFileSystem extends @OsUntrusted FilterFileSystem {
  private final @OsUntrusted URI myUri; // the base URI + the chRoot
  private final @OsUntrusted Path chRootPathPart; // the root below the root of the base
  private final @OsUntrusted String chRootPathPartString;
  private @OsUntrusted Path workingDir;
  
  protected @OsUntrusted FileSystem getMyFs(@OsUntrusted ChRootedFileSystem this) {
    return getRawFileSystem();
  }
  
  /**
   * @param path
   * @return  full path including the chroot 
   */
  protected @OsUntrusted Path fullPath(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path path) {
    super.checkPath(path);
    return path.isAbsolute() ? 
        new @OsUntrusted Path((chRootPathPart.isRoot() ? "" : chRootPathPartString)
            + path.toUri().getPath()) :
        new @OsUntrusted Path(chRootPathPartString + workingDir.toUri().getPath(), path);
  }
  
  /**
   * Constructor
   * @param uri base file system
   * @param conf configuration
   * @throws IOException 
   */
  public @OsUntrusted ChRootedFileSystem(final @OsUntrusted URI uri, @OsUntrusted Configuration conf)
      throws IOException {
    super(FileSystem.get(uri, conf));
    @OsUntrusted
    String pathString = uri.getPath();
    if (pathString.isEmpty()) {
      pathString = "/";
    }
    chRootPathPart = new @OsUntrusted Path(pathString);
    chRootPathPartString = chRootPathPart.toUri().getPath();
    myUri = uri;
    workingDir = getHomeDirectory();
    // We don't use the wd of the myFs
  }
  
  /** 
   * Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param conf the configuration
   */
  @Override
  public void initialize(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted URI name, final @OsUntrusted Configuration conf)
      throws IOException {
    super.initialize(name, conf);
    setConf(conf);
  }

  @Override
  public @OsUntrusted URI getUri(@OsUntrusted ChRootedFileSystem this) {
    return myUri;
  }
  
  /**
   * Strip out the root from the path.
   * @param p - fully qualified path p
   * @return -  the remaining path  without the begining /
   * @throws IOException if the p is not prefixed with root
   */
  @OsUntrusted
  String stripOutRoot(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path p) throws IOException {
    try {
     checkPath(p);
    } catch (@OsUntrusted IllegalArgumentException e) {
      throw new @OsUntrusted IOException("Internal Error - path " + p +
          " should have been with URI: " + myUri);
    }
    @OsUntrusted
    String pathPart = p.toUri().getPath();
    return (pathPart.length() == chRootPathPartString.length()) ? "" : pathPart
        .substring(chRootPathPartString.length() + (chRootPathPart.isRoot() ? 0 : 1));
  }
  
  @Override
  protected @OsUntrusted Path getInitialWorkingDirectory(@OsUntrusted ChRootedFileSystem this) {
    /*
     * 3 choices here: 
     *     null or / or /user/<uname> or strip out the root out of myFs's
     *  inital wd. 
     * Only reasonable choice for initialWd for chrooted fds is null 
     * so that the default rule for wd is applied
     */
    return null;
  }
  
  public @OsUntrusted Path getResolvedQualifiedPath(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path f)
      throws FileNotFoundException {
    return makeQualified(
        new @OsUntrusted Path(chRootPathPartString + f.toUri().toString()));
  }

  @Override
  public @OsUntrusted Path getWorkingDirectory(@OsUntrusted ChRootedFileSystem this) {
    return workingDir;
  }
  
  @Override
  public void setWorkingDirectory(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path new_dir) {
    workingDir = new_dir.isAbsolute() ? new_dir : new @OsUntrusted Path(workingDir, new_dir);
  }

  @Override
  public @OsUntrusted FSDataOutputStream create(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path f, final @OsUntrusted FsPermission permission,
      final @OsUntrusted boolean overwrite, final @OsUntrusted int bufferSize, final @OsUntrusted short replication,
      final @OsUntrusted long blockSize, final @OsUntrusted Progressable progress) throws IOException {
    return super.create(fullPath(f), permission, overwrite, bufferSize,
        replication, blockSize, progress);
  }
  
  @Override
  @Deprecated
  public @OsUntrusted FSDataOutputStream createNonRecursive(@OsUntrusted ChRootedFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission,
      @OsUntrusted
      EnumSet<@OsUntrusted CreateFlag> flags, @OsUntrusted int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    
    return super.createNonRecursive(fullPath(f), permission, flags, bufferSize, replication, blockSize,
        progress);
  }

  @Override
  public @OsUntrusted boolean delete(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path f, final @OsUntrusted boolean recursive) 
      throws IOException {
    return super.delete(fullPath(f), recursive);
  }
  

  @Override
  @SuppressWarnings("deprecation")
  public @OsUntrusted boolean delete(@OsUntrusted ChRootedFileSystem this, @OsUntrusted Path f) throws IOException {
   return delete(f, true);
  }

  @Override
  public @OsUntrusted BlockLocation @OsUntrusted [] getFileBlockLocations(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted FileStatus fs, final @OsUntrusted long start,
      final @OsUntrusted long len) throws IOException {
    return super.getFileBlockLocations(
        new @OsUntrusted ViewFsFileStatus(fs, fullPath(fs.getPath())), start, len);
  }

  @Override
  public @OsUntrusted FileChecksum getFileChecksum(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path f) 
      throws IOException {
    return super.getFileChecksum(fullPath(f));
  }

  @Override
  public @OsUntrusted FileStatus getFileStatus(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path f) 
      throws IOException {
    return super.getFileStatus(fullPath(f));
  }

  @Override
  public @OsUntrusted FsStatus getStatus(@OsUntrusted ChRootedFileSystem this, @OsUntrusted Path p) throws IOException {
    return super.getStatus(fullPath(p));
  }

  @Override
  public @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path f) 
      throws IOException {
    return super.listStatus(fullPath(f));
  }
  
  @Override
  public @OsUntrusted boolean mkdirs(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path f, final @OsUntrusted FsPermission permission)
      throws IOException {
    return super.mkdirs(fullPath(f), permission);
  }

  @Override
  public @OsUntrusted FSDataInputStream open(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path f, final @OsUntrusted int bufferSize) 
    throws IOException {
    return super.open(fullPath(f), bufferSize);
  }
  
  @Override
  public @OsUntrusted FSDataOutputStream append(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path f, final @OsUntrusted int bufferSize,
      final @OsUntrusted Progressable progress) throws IOException {
    return super.append(fullPath(f), bufferSize, progress);
  }

  @Override
  public @OsUntrusted boolean rename(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path src, final @OsUntrusted Path dst) throws IOException {
    // note fullPath will check that paths are relative to this FileSystem.
    // Hence both are in same file system and a rename is valid
    return super.rename(fullPath(src), fullPath(dst)); 
  }
  
  @Override
  public void setOwner(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path f, final @OsTrusted String username,
      final @OsTrusted String groupname)
    throws IOException {
    super.setOwner(fullPath(f), username, groupname);
  }

  @Override
  public void setPermission(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path f, final @OsUntrusted FsPermission permission)
    throws IOException {
    super.setPermission(fullPath(f), permission);
  }

  @Override
  public @OsUntrusted boolean setReplication(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path f, final @OsUntrusted short replication)
    throws IOException {
    return super.setReplication(fullPath(f), replication);
  }

  @Override
  public void setTimes(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path f, final @OsUntrusted long mtime, final @OsUntrusted long atime) 
      throws IOException {
    super.setTimes(fullPath(f), mtime, atime);
  }
  
  @Override
  public @OsUntrusted Path resolvePath(@OsUntrusted ChRootedFileSystem this, final @OsUntrusted Path p) throws IOException {
    return super.resolvePath(fullPath(p));
  }

  @Override
  public @OsUntrusted ContentSummary getContentSummary(@OsUntrusted ChRootedFileSystem this, @OsUntrusted Path f) throws IOException {
    return super.getContentSummary(fullPath(f));
  }
  

  private static @OsUntrusted Path rootPath = new @OsUntrusted Path(Path.SEPARATOR);

  @Override
  public @OsUntrusted long getDefaultBlockSize(@OsUntrusted ChRootedFileSystem this) {
    return getDefaultBlockSize(fullPath(rootPath));
  }
  
  @Override
  public @OsUntrusted long getDefaultBlockSize(@OsUntrusted ChRootedFileSystem this, @OsUntrusted Path f) {
    return super.getDefaultBlockSize(fullPath(f));
  }  

  @Override
  public @OsUntrusted short getDefaultReplication(@OsUntrusted ChRootedFileSystem this) {
    return getDefaultReplication(fullPath(rootPath));
  }

  @Override
  public @OsUntrusted short getDefaultReplication(@OsUntrusted ChRootedFileSystem this, @OsUntrusted Path f) {
    return super.getDefaultReplication(fullPath(f));
  }
  
  @Override
  public @OsUntrusted FsServerDefaults getServerDefaults(@OsUntrusted ChRootedFileSystem this) throws IOException {
    return getServerDefaults(fullPath(rootPath));
  }  

  @Override
  public @OsUntrusted FsServerDefaults getServerDefaults(@OsUntrusted ChRootedFileSystem this, @OsUntrusted Path f) throws IOException {
    return super.getServerDefaults(fullPath(f));
  }  
}
