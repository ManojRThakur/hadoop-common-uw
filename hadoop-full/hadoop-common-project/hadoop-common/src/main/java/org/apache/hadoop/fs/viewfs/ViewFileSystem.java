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
import static org.apache.hadoop.fs.viewfs.Constants.PERMISSION_RRR;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.InodeTree.INode;
import org.apache.hadoop.fs.viewfs.InodeTree.INodeLink;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;

/**
 * ViewFileSystem (extends the FileSystem interface) implements a client-side
 * mount table. Its spec and implementation is identical to {@link ViewFs}.
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public class ViewFileSystem extends @OsUntrusted FileSystem {

  private static final @OsUntrusted Path ROOT_PATH = new @OsUntrusted Path(Path.SEPARATOR);

  static @OsUntrusted AccessControlException readOnlyMountTable(final @OsUntrusted String operation,
      final @OsUntrusted String p) {
    return new @OsUntrusted AccessControlException( 
        "InternalDir of ViewFileSystem is readonly; operation=" + operation + 
        "Path=" + p);
  }
  static @OsUntrusted AccessControlException readOnlyMountTable(final @OsUntrusted String operation,
      final @OsUntrusted Path p) {
    return readOnlyMountTable(operation, p.toString());
  }
  
  static public class MountPoint {
    private @OsUntrusted Path src;       // the src of the mount
    private @OsUntrusted URI @OsUntrusted [] targets; //  target of the mount; Multiple targets imply mergeMount
    @OsUntrusted
    MountPoint(@OsUntrusted Path srcPath, @OsUntrusted URI @OsUntrusted [] targetURIs) {
      src = srcPath;
      targets = targetURIs;
    }
    @OsUntrusted
    Path getSrc(ViewFileSystem.@OsUntrusted MountPoint this) {
      return src;
    }
    @OsUntrusted
    URI @OsUntrusted [] getTargets(ViewFileSystem.@OsUntrusted MountPoint this) {
      return targets;
    }
  }
  
  final @OsUntrusted long creationTime; // of the the mount table
  final @OsUntrusted UserGroupInformation ugi; // the user/group of user who created mtable
  @OsUntrusted
  URI myUri;
  private @OsUntrusted Path workingDir;
  @OsUntrusted
  Configuration config;
  @OsUntrusted
  InodeTree<@OsUntrusted FileSystem> fsState;  // the fs state; ie the mount table
  @OsUntrusted
  Path homeDir = null;
  
  /**
   * Make the path Absolute and get the path-part of a pathname.
   * Checks that URI matches this file system 
   * and that the path-part is a valid name.
   * 
   * @param p path
   * @return path-part of the Path p
   */
  private @OsUntrusted String getUriPath(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path p) {
    checkPath(p);
    @OsUntrusted
    String s = makeAbsolute(p).toUri().getPath();
    return s;
  }
  
  private @OsUntrusted Path makeAbsolute(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path f) {
    return f.isAbsolute() ? f : new @OsUntrusted Path(workingDir, f);
  }
  
  /**
   * This is the  constructor with the signature needed by
   * {@link FileSystem#createFileSystem(URI, Configuration)}
   * 
   * After this constructor is called initialize() is called.
   * @throws IOException 
   */
  public @OsUntrusted ViewFileSystem() throws IOException {
    ugi = UserGroupInformation.getCurrentUser();
    creationTime = Time.now();
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>viewfs</code>
   */
  @Override
  public @OsUntrusted String getScheme(@OsUntrusted ViewFileSystem this) {
    return "viewfs";
  }

  /**
   * Called after a new FileSystem instance is constructed.
   * @param theUri a uri whose authority section names the host, port, etc. for
   *          this FileSystem
   * @param conf the configuration
   */
  @Override
  public void initialize(@OsUntrusted ViewFileSystem this, final @OsUntrusted URI theUri, final @OsUntrusted Configuration conf)
      throws IOException {
    super.initialize(theUri, conf);
    setConf(conf);
    config = conf;
    // Now build  client side view (i.e. client side mount table) from config.
    final @OsUntrusted String authority = theUri.getAuthority();
    try {
      myUri = new @OsUntrusted URI(FsConstants.VIEWFS_SCHEME, authority, "/", null, null);
      fsState = new @OsUntrusted InodeTree<@OsUntrusted FileSystem>(conf, authority) {

        @Override
        protected
        @OsUntrusted
        FileSystem getTargetFileSystem(final @OsUntrusted URI uri)
          throws URISyntaxException, IOException {
            return new @OsUntrusted ChRootedFileSystem(uri, config);
        }

        @Override
        protected
        @OsUntrusted
        FileSystem getTargetFileSystem(final @OsUntrusted INodeDir<@OsUntrusted FileSystem> dir)
          throws URISyntaxException {
          return new @OsUntrusted InternalDirOfViewFs(dir, creationTime, ugi, myUri);
        }

        @Override
        protected
        @OsUntrusted
        FileSystem getTargetFileSystem(@OsUntrusted URI @OsUntrusted [] mergeFsURIList)
            throws URISyntaxException, UnsupportedFileSystemException {
          throw new @OsUntrusted UnsupportedFileSystemException("mergefs not implemented");
          // return MergeFs.createMergeFs(mergeFsURIList, config);
        }
      };
      workingDir = this.getHomeDirectory();
    } catch (@OsUntrusted URISyntaxException e) {
      throw new @OsUntrusted IOException("URISyntax exception: " + theUri);
    }

  }
  
  
  /**
   * Convenience Constructor for apps to call directly
   * @param theUri which must be that of ViewFileSystem
   * @param conf
   * @throws IOException
   */
  @OsUntrusted
  ViewFileSystem(final @OsUntrusted URI theUri, final @OsUntrusted Configuration conf)
    throws IOException {
    this();
    initialize(theUri, conf);
  }
  
  /**
   * Convenience Constructor for apps to call directly
   * @param conf
   * @throws IOException
   */
  public @OsUntrusted ViewFileSystem(final @OsUntrusted Configuration conf) throws IOException {
    this(FsConstants.VIEWFS_URI, conf);
  }
  
  public @OsUntrusted Path getTrashCanLocation(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path f) throws FileNotFoundException {
    final InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.isInternalDir() ? null : res.targetFileSystem.getHomeDirectory();
  }
  
  @Override
  public @OsUntrusted URI getUri(@OsUntrusted ViewFileSystem this) {
    return myUri;
  }
  
  @Override
  public @OsUntrusted Path resolvePath(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path f)
      throws IOException {
    final InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res;
      res = fsState.resolve(getUriPath(f), true);
    if (res.isInternalDir()) {
      return f;
    }
    return res.targetFileSystem.resolvePath(res.remainingPath);
  }
  
  @Override
  public @OsUntrusted Path getHomeDirectory(@OsUntrusted ViewFileSystem this) {
    if (homeDir == null) {
      @OsUntrusted
      String base = fsState.getHomeDirPrefixValue();
      if (base == null) {
        base = "/user";
      }
      homeDir = (base.equals("/") ? 
          this.makeQualified(new @OsUntrusted Path(base + ugi.getShortUserName())):
          this.makeQualified(new @OsUntrusted Path(base + "/" + ugi.getShortUserName())));
    }
    return homeDir;
  }
  
  @Override
  public @OsUntrusted Path getWorkingDirectory(@OsUntrusted ViewFileSystem this) {
    return workingDir;
  }

  @Override
  public void setWorkingDirectory(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path new_dir) {
    getUriPath(new_dir); // this validates the path
    workingDir = makeAbsolute(new_dir);
  }
  
  @Override
  public @OsUntrusted FSDataOutputStream append(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path f, final @OsUntrusted int bufferSize,
      final @OsUntrusted Progressable progress) throws IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.append(res.remainingPath, bufferSize, progress);
  }
  
  @Override
  public @OsUntrusted FSDataOutputStream createNonRecursive(@OsUntrusted ViewFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission,
      @OsUntrusted
      EnumSet<@OsUntrusted CreateFlag> flags, @OsUntrusted int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res;
    try {
      res = fsState.resolve(getUriPath(f), false);
    } catch (@OsUntrusted FileNotFoundException e) {
        throw readOnlyMountTable("create", f);
    }
    assert(res.remainingPath != null);
    return res.targetFileSystem.createNonRecursive(res.remainingPath, permission,
         flags, bufferSize, replication, blockSize, progress);
  }
  
  @Override
  public @OsUntrusted FSDataOutputStream create(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path f, final @OsUntrusted FsPermission permission,
      final @OsUntrusted boolean overwrite, final @OsUntrusted int bufferSize, final @OsUntrusted short replication,
      final @OsUntrusted long blockSize, final @OsUntrusted Progressable progress) throws IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res;
    try {
      res = fsState.resolve(getUriPath(f), false);
    } catch (@OsUntrusted FileNotFoundException e) {
        throw readOnlyMountTable("create", f);
    }
    assert(res.remainingPath != null);
    return res.targetFileSystem.create(res.remainingPath, permission,
         overwrite, bufferSize, replication, blockSize, progress);
  }

  
  @Override
  public @OsUntrusted boolean delete(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path f, final @OsUntrusted boolean recursive)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    // If internal dir or target is a mount link (ie remainingPath is Slash)
    if (res.isInternalDir() || res.remainingPath == InodeTree.SlashPath) {
      throw readOnlyMountTable("delete", f);
    }
    return res.targetFileSystem.delete(res.remainingPath, recursive);
  }
  
  @Override
  @SuppressWarnings("deprecation")
  public @OsUntrusted boolean delete(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path f)
      throws AccessControlException, FileNotFoundException,
      IOException {
      return delete(f, true);
  }
  
  @Override
  public @OsUntrusted BlockLocation @OsUntrusted [] getFileBlockLocations(@OsUntrusted ViewFileSystem this, @OsUntrusted FileStatus fs, 
      @OsUntrusted
      long start, @OsUntrusted long len) throws IOException {
    final InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res = 
      fsState.resolve(getUriPath(fs.getPath()), true);
    return res.targetFileSystem.getFileBlockLocations(
          new @OsUntrusted ViewFsFileStatus(fs, res.remainingPath), start, len);
  }

  @Override
  public @OsUntrusted FileChecksum getFileChecksum(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path f)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.getFileChecksum(res.remainingPath);
  }

  @Override
  public @OsUntrusted FileStatus getFileStatus(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path f) throws AccessControlException,
      FileNotFoundException, IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    
    // FileStatus#getPath is a fully qualified path relative to the root of 
    // target file system.
    // We need to change it to viewfs URI - relative to root of mount table.
    
    // The implementors of RawLocalFileSystem were trying to be very smart.
    // They implement FileStatus#getOwener lazily -- the object
    // returned is really a RawLocalFileSystem that expect the
    // FileStatus#getPath to be unchanged so that it can get owner when needed.
    // Hence we need to interpose a new ViewFileSystemFileStatus that 
    // works around.
    @OsUntrusted
    FileStatus status =  res.targetFileSystem.getFileStatus(res.remainingPath);
    return new @OsUntrusted ViewFsFileStatus(status, this.makeQualified(f));
  }
  
  
  @Override
  public @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path f) throws AccessControlException,
      FileNotFoundException, IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res =
      fsState.resolve(getUriPath(f), true);
    
    @OsUntrusted
    FileStatus @OsUntrusted [] statusLst = res.targetFileSystem.listStatus(res.remainingPath);
    if (!res.isInternalDir()) {
      // We need to change the name in the FileStatus as described in
      // {@link #getFileStatus }
      @OsUntrusted
      ChRootedFileSystem targetFs;
      targetFs = (@OsUntrusted ChRootedFileSystem) res.targetFileSystem;
      @OsUntrusted
      int i = 0;
      for (@OsUntrusted FileStatus status : statusLst) {
          @OsUntrusted
          String suffix = targetFs.stripOutRoot(status.getPath());
          statusLst[i++] = new @OsUntrusted ViewFsFileStatus(status, this.makeQualified(
              suffix.length() == 0 ? f : new @OsUntrusted Path(res.resolvedPath, suffix)));
      }
    }
    return statusLst;
  }

  @Override
  public @OsUntrusted boolean mkdirs(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path dir, final @OsUntrusted FsPermission permission)
      throws IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res = 
      fsState.resolve(getUriPath(dir), false);
   return  res.targetFileSystem.mkdirs(res.remainingPath, permission);
  }

  @Override
  public @OsUntrusted FSDataInputStream open(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path f, final @OsUntrusted int bufferSize)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res = 
        fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.open(res.remainingPath, bufferSize);
  }

  
  @Override
  public @OsUntrusted boolean rename(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path src, final @OsUntrusted Path dst) throws IOException {
    // passing resolveLastComponet as false to catch renaming a mount point to 
    // itself. We need to catch this as an internal operation and fail.
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> resSrc = 
      fsState.resolve(getUriPath(src), false); 
  
    if (resSrc.isInternalDir()) {
      throw readOnlyMountTable("rename", src);
    }
      
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> resDst = 
      fsState.resolve(getUriPath(dst), false);
    if (resDst.isInternalDir()) {
          throw readOnlyMountTable("rename", dst);
    }
    /**
    // Alternate 1: renames within same file system - valid but we disallow
    // Alternate 2: (as described in next para - valid but we have disallowed it
    //
    // Note we compare the URIs. the URIs include the link targets. 
    // hence we allow renames across mount links as long as the mount links
    // point to the same target.
    if (!resSrc.targetFileSystem.getUri().equals(
              resDst.targetFileSystem.getUri())) {
      throw new IOException("Renames across Mount points not supported");
    }
    */
    
    //
    // Alternate 3 : renames ONLY within the the same mount links.
    //
    if (resSrc.targetFileSystem !=resDst.targetFileSystem) {
      throw new @OsUntrusted IOException("Renames across Mount points not supported");
    }
    return resSrc.targetFileSystem.rename(resSrc.remainingPath,
        resDst.remainingPath);
  }
  
  @Override
  public void setOwner(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path f, final @OsTrusted String username,
      final @OsTrusted String groupname) throws AccessControlException,
      FileNotFoundException,
      IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setOwner(res.remainingPath, username, groupname); 
  }

  @Override
  public void setPermission(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path f, final @OsUntrusted FsPermission permission)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setPermission(res.remainingPath, permission); 
  }

  @Override
  public @OsUntrusted boolean setReplication(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path f, final @OsUntrusted short replication)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.setReplication(res.remainingPath, replication);
  }

  @Override
  public void setTimes(@OsUntrusted ViewFileSystem this, final @OsUntrusted Path f, final @OsUntrusted long mtime, final @OsUntrusted long atime)
      throws AccessControlException, FileNotFoundException,
      IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setTimes(res.remainingPath, mtime, atime); 
  }

  @Override
  public void setVerifyChecksum(@OsUntrusted ViewFileSystem this, final @OsUntrusted boolean verifyChecksum) { 
    @OsUntrusted
    List<InodeTree.@OsUntrusted MountPoint<@OsUntrusted FileSystem>> mountPoints = 
        fsState.getMountPoints();
    for (InodeTree.@OsUntrusted MountPoint<@OsUntrusted FileSystem> mount : mountPoints) {
      mount.target.targetFileSystem.setVerifyChecksum(verifyChecksum);
    }
  }
  
  @Override
  public @OsUntrusted long getDefaultBlockSize(@OsUntrusted ViewFileSystem this) {
    throw new @OsUntrusted NotInMountpointException("getDefaultBlockSize");
  }

  @Override
  public @OsUntrusted short getDefaultReplication(@OsUntrusted ViewFileSystem this) {
    throw new @OsUntrusted NotInMountpointException("getDefaultReplication");
  }

  @Override
  public @OsUntrusted FsServerDefaults getServerDefaults(@OsUntrusted ViewFileSystem this) throws IOException {
    throw new @OsUntrusted NotInMountpointException("getServerDefaults");
  }

  @Override
  public @OsUntrusted long getDefaultBlockSize(@OsUntrusted ViewFileSystem this, @OsUntrusted Path f) {
    try {
      InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res =
        fsState.resolve(getUriPath(f), true);
      return res.targetFileSystem.getDefaultBlockSize(res.remainingPath);
    } catch (@OsUntrusted FileNotFoundException e) {
      throw new @OsUntrusted NotInMountpointException(f, "getDefaultBlockSize"); 
    }
  }

  @Override
  public @OsUntrusted short getDefaultReplication(@OsUntrusted ViewFileSystem this, @OsUntrusted Path f) {
    try {
      InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res =
        fsState.resolve(getUriPath(f), true);
      return res.targetFileSystem.getDefaultReplication(res.remainingPath);
    } catch (@OsUntrusted FileNotFoundException e) {
      throw new @OsUntrusted NotInMountpointException(f, "getDefaultReplication"); 
    }
  }

  @Override
  public @OsUntrusted FsServerDefaults getServerDefaults(@OsUntrusted ViewFileSystem this, @OsUntrusted Path f) throws IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res =
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.getServerDefaults(res.remainingPath);    
  }

  @Override
  public @OsUntrusted ContentSummary getContentSummary(@OsUntrusted ViewFileSystem this, @OsUntrusted Path f) throws IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted FileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.getContentSummary(res.remainingPath);
  }

  @Override
  public void setWriteChecksum(@OsUntrusted ViewFileSystem this, final @OsUntrusted boolean writeChecksum) { 
    @OsUntrusted
    List<InodeTree.@OsUntrusted MountPoint<@OsUntrusted FileSystem>> mountPoints = 
        fsState.getMountPoints();
    for (InodeTree.@OsUntrusted MountPoint<@OsUntrusted FileSystem> mount : mountPoints) {
      mount.target.targetFileSystem.setWriteChecksum(writeChecksum);
    }
  }

  @Override
  public @OsUntrusted FileSystem @OsUntrusted [] getChildFileSystems(@OsUntrusted ViewFileSystem this) {
    @OsUntrusted
    List<InodeTree.@OsUntrusted MountPoint<@OsUntrusted FileSystem>> mountPoints =
        fsState.getMountPoints();
    @OsUntrusted
    Set<@OsUntrusted FileSystem> children = new @OsUntrusted HashSet<@OsUntrusted FileSystem>();
    for (InodeTree.@OsUntrusted MountPoint<@OsUntrusted FileSystem> mountPoint : mountPoints) {
      @OsUntrusted
      FileSystem targetFs = mountPoint.target.targetFileSystem;
      children.addAll(Arrays.asList(targetFs.getChildFileSystems()));
    }
    return children.toArray(new @OsUntrusted FileSystem @OsUntrusted []{});
  }
  
  public @OsUntrusted MountPoint @OsUntrusted [] getMountPoints(@OsUntrusted ViewFileSystem this) {
    @OsUntrusted
    List<InodeTree.@OsUntrusted MountPoint<@OsUntrusted FileSystem>> mountPoints = 
                  fsState.getMountPoints();
    
    @OsUntrusted
    MountPoint @OsUntrusted [] result = new @OsUntrusted MountPoint @OsUntrusted [mountPoints.size()];
    for ( @OsUntrusted int i = 0; i < mountPoints.size(); ++i ) {
      result[i] = new @OsUntrusted MountPoint(new @OsUntrusted Path(mountPoints.get(i).src), 
                              mountPoints.get(i).target.targetDirLinkList);
    }
    return result;
  }
  
  /*
   * An instance of this class represents an internal dir of the viewFs 
   * that is internal dir of the mount table.
   * It is a read only mount tables and create, mkdir or delete operations
   * are not allowed.
   * If called on create or mkdir then this target is the parent of the
   * directory in which one is trying to create or mkdir; hence
   * in this case the path name passed in is the last component. 
   * Otherwise this target is the end point of the path and hence
   * the path name passed in is null. 
   */
  static class InternalDirOfViewFs extends @OsUntrusted FileSystem {
    final InodeTree.@OsUntrusted INodeDir<@OsUntrusted FileSystem>  theInternalDir;
    final @OsUntrusted long creationTime; // of the the mount table
    final @OsUntrusted UserGroupInformation ugi; // the user/group of user who created mtable
    final @OsUntrusted URI myUri;
    
    public @OsUntrusted InternalDirOfViewFs(final InodeTree.@OsUntrusted INodeDir<@OsUntrusted FileSystem> dir,
        final @OsUntrusted long cTime, final @OsUntrusted UserGroupInformation ugi, @OsUntrusted URI uri)
      throws URISyntaxException {
      myUri = uri;
      try {
        initialize(myUri, new @OsUntrusted Configuration());
      } catch (@OsUntrusted IOException e) {
        throw new @OsUntrusted RuntimeException("Cannot occur");
      }
      theInternalDir = dir;
      creationTime = cTime;
      this.ugi = ugi;
    }

    static private void checkPathIsSlash(final @OsUntrusted Path f) throws IOException {
      if (f != InodeTree.SlashPath) {
        throw new @OsUntrusted IOException (
        "Internal implementation error: expected file name to be /" );
      }
    }
    
    @Override
    public @OsUntrusted URI getUri(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this) {
      return myUri;
    }

    @Override
    public @OsUntrusted Path getWorkingDirectory(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this) {
      throw new @OsUntrusted RuntimeException (
      "Internal impl error: getWorkingDir should not have been called" );
    }

    @Override
    public void setWorkingDirectory(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path new_dir) {
      throw new @OsUntrusted RuntimeException (
      "Internal impl error: getWorkingDir should not have been called" ); 
    }

    @Override
    public @OsUntrusted FSDataOutputStream append(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f, final @OsUntrusted int bufferSize,
        final @OsUntrusted Progressable progress) throws IOException {
      throw readOnlyMountTable("append", f);
    }

    @Override
    public @OsUntrusted FSDataOutputStream create(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f,
        final @OsUntrusted FsPermission permission, final @OsUntrusted boolean overwrite,
        final @OsUntrusted int bufferSize, final @OsUntrusted short replication, final @OsUntrusted long blockSize,
        final @OsUntrusted Progressable progress) throws AccessControlException {
      throw readOnlyMountTable("create", f);
    }

    @Override
    public @OsUntrusted boolean delete(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f, final @OsUntrusted boolean recursive)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("delete", f);
    }
    
    @Override
    @SuppressWarnings("deprecation")
    public @OsUntrusted boolean delete(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f)
        throws AccessControlException, IOException {
      return delete(f, true);
    }

    @Override
    public @OsUntrusted BlockLocation @OsUntrusted [] getFileBlockLocations(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted FileStatus fs,
        final @OsUntrusted long start, final @OsUntrusted long len) throws 
        FileNotFoundException, IOException {
      checkPathIsSlash(fs.getPath());
      throw new @OsUntrusted FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public @OsUntrusted FileChecksum getFileChecksum(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f)
        throws FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new @OsUntrusted FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public @OsUntrusted FileStatus getFileStatus(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, @OsUntrusted Path f) throws IOException {
      checkPathIsSlash(f);
      return new @OsUntrusted FileStatus(0, true, 0, 0, creationTime, creationTime,
          PERMISSION_RRR, ugi.getUserName(), ugi.getGroupNames()[0],

          new @OsUntrusted Path(theInternalDir.fullPath).makeQualified(
              myUri, ROOT_PATH));
    }
    

    @Override
    public @OsUntrusted FileStatus @OsUntrusted [] listStatus(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, @OsUntrusted Path f) throws AccessControlException,
        FileNotFoundException, IOException {
      checkPathIsSlash(f);
      @OsUntrusted
      FileStatus @OsUntrusted [] result = new @OsUntrusted FileStatus @OsUntrusted [theInternalDir.children.size()];
      @OsUntrusted
      int i = 0;
      for (@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted INode<@OsUntrusted FileSystem>> iEntry : 
                                          theInternalDir.children.entrySet()) {
        @OsUntrusted
        INode<@OsUntrusted FileSystem> inode = iEntry.getValue();
        if (inode instanceof @OsUntrusted INodeLink ) {
          @OsUntrusted
          INodeLink<@OsUntrusted FileSystem> link = (@OsUntrusted INodeLink<@OsUntrusted FileSystem>) inode;

          result[i++] = new @OsUntrusted FileStatus(0, false, 0, 0,
            creationTime, creationTime, PERMISSION_RRR,
            ugi.getUserName(), ugi.getGroupNames()[0],
            link.getTargetLink(),
            new @OsUntrusted Path(inode.fullPath).makeQualified(
                myUri, null));
        } else {
          result[i++] = new @OsUntrusted FileStatus(0, true, 0, 0,
            creationTime, creationTime, PERMISSION_RRR,
            ugi.getUserName(), ugi.getGroupNames()[0],
            new @OsUntrusted Path(inode.fullPath).makeQualified(
                myUri, null));
        }
      }
      return result;
    }

    @Override
    public @OsUntrusted boolean mkdirs(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, @OsUntrusted Path dir, @OsUntrusted FsPermission permission)
        throws AccessControlException, FileAlreadyExistsException {
      if (theInternalDir.isRoot && dir == null) {
        throw new @OsUntrusted FileAlreadyExistsException("/ already exits");
      }
      // Note dir starts with /
      if (theInternalDir.children.containsKey(dir.toString().substring(1))) {
        return true; // this is the stupid semantics of FileSystem
      }
      throw readOnlyMountTable("mkdirs",  dir);
    }

    @Override
    public @OsUntrusted FSDataInputStream open(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, @OsUntrusted Path f, @OsUntrusted int bufferSize)
        throws AccessControlException, FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new @OsUntrusted FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public @OsUntrusted boolean rename(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, @OsUntrusted Path src, @OsUntrusted Path dst) throws AccessControlException,
        IOException {
      checkPathIsSlash(src);
      checkPathIsSlash(dst);
      throw readOnlyMountTable("rename", src);     
    }

    @Override
    public void setOwner(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, @OsUntrusted Path f, @OsUntrusted String username, @OsUntrusted String groupname)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setOwner", f);
    }

    @Override
    public void setPermission(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, @OsUntrusted Path f, @OsUntrusted FsPermission permission)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setPermission", f);    
    }

    @Override
    public @OsUntrusted boolean setReplication(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, @OsUntrusted Path f, @OsUntrusted short replication)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setReplication", f);
    }

    @Override
    public void setTimes(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, @OsUntrusted Path f, @OsUntrusted long mtime, @OsUntrusted long atime)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setTimes", f);    
    }

    @Override
    public void setVerifyChecksum(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, @OsUntrusted boolean verifyChecksum) {
      // Noop for viewfs
    }

    @Override
    public @OsUntrusted FsServerDefaults getServerDefaults(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, @OsUntrusted Path f) throws IOException {
      throw new @OsUntrusted NotInMountpointException(f, "getServerDefaults");
    }
    
    @Override
    public @OsUntrusted long getDefaultBlockSize(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, @OsUntrusted Path f) {
      throw new @OsUntrusted NotInMountpointException(f, "getDefaultBlockSize");
    }

    @Override
    public @OsUntrusted short getDefaultReplication(ViewFileSystem.@OsUntrusted InternalDirOfViewFs this, @OsUntrusted Path f) {
      throw new @OsUntrusted NotInMountpointException(f, "getDefaultReplication");
    }
  }
}
