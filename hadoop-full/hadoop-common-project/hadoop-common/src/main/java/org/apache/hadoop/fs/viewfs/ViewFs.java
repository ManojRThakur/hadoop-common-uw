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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.local.LocalConfigKeys;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.viewfs.InodeTree.INode;
import org.apache.hadoop.fs.viewfs.InodeTree.INodeLink;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;


/**
 * ViewFs (extends the AbstractFileSystem interface) implements a client-side
 * mount table. The viewFs file system is implemented completely in memory on
 * the client side. The client-side mount table allows a client to provide a 
 * customized view of a file system namespace that is composed from 
 * one or more individual file systems (a localFs or Hdfs, S3fs, etc).
 * For example one could have a mount table that provides links such as
 * <ul>
 * <li>  /user          -> hdfs://nnContainingUserDir/user
 * <li>  /project/foo   -> hdfs://nnProject1/projects/foo
 * <li>  /project/bar   -> hdfs://nnProject2/projects/bar
 * <li>  /tmp           -> hdfs://nnTmp/privateTmpForUserXXX
 * </ul> 
 * 
 * ViewFs is specified with the following URI: <b>viewfs:///</b> 
 * <p>
 * To use viewfs one would typically set the default file system in the
 * config  (i.e. fs.default.name< = viewfs:///) along with the
 * mount table config variables as described below. 
 * 
 * <p>
 * <b> ** Config variables to specify the mount table entries ** </b>
 * <p>
 * 
 * The file system is initialized from the standard Hadoop config through
 * config variables.
 * See {@link FsConstants} for URI and Scheme constants; 
 * See {@link Constants} for config var constants; 
 * see {@link ConfigUtil} for convenient lib.
 * 
 * <p>
 * All the mount table config entries for view fs are prefixed by 
 * <b>fs.viewfs.mounttable.</b>
 * For example the above example can be specified with the following
 *  config variables:
 *  <ul>
 *  <li> fs.viewfs.mounttable.default.link./user=
 *  hdfs://nnContainingUserDir/user
 *  <li> fs.viewfs.mounttable.default.link./project/foo=
 *  hdfs://nnProject1/projects/foo
 *  <li> fs.viewfs.mounttable.default.link./project/bar=
 *  hdfs://nnProject2/projects/bar
 *  <li> fs.viewfs.mounttable.default.link./tmp=
 *  hdfs://nnTmp/privateTmpForUserXXX
 *  </ul>
 *  
 * The default mount table (when no authority is specified) is 
 * from config variables prefixed by <b>fs.viewFs.mounttable.default </b>
 * The authority component of a URI can be used to specify a different mount
 * table. For example,
 * <ul>
 * <li>  viewfs://sanjayMountable/
 * </ul>
 * is initialized from fs.viewFs.mounttable.sanjayMountable.* config variables.
 * 
 *  <p> 
 *  <b> **** Merge Mounts **** </b>(NOTE: merge mounts are not implemented yet.)
 *  <p>
 *  
 *   One can also use "MergeMounts" to merge several directories (this is
 *   sometimes  called union-mounts or junction-mounts in the literature.
 *   For example of the home directories are stored on say two file systems
 *   (because they do not fit on one) then one could specify a mount
 *   entry such as following merges two dirs:
 *   <ul>
 *   <li> /user -> hdfs://nnUser1/user,hdfs://nnUser2/user
 *   </ul>
 *  Such a mergeLink can be specified with the following config var where ","
 *  is used as the separator for each of links to be merged:
 *  <ul>
 *  <li> fs.viewfs.mounttable.default.linkMerge./user=
 *  hdfs://nnUser1/user,hdfs://nnUser1/user
 *  </ul>
 *   A special case of the merge mount is where mount table's root is merged
 *   with the root (slash) of another file system:
 *   <ul>
 *   <li>    fs.viewfs.mounttable.default.linkMergeSlash=hdfs://nn99/
 *   </ul>
 *   In this cases the root of the mount table is merged with the root of
 *            <b>hdfs://nn99/ </b> 
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public class ViewFs extends @OsUntrusted AbstractFileSystem {
  final @OsUntrusted long creationTime; // of the the mount table
  final @OsUntrusted UserGroupInformation ugi; // the user/group of user who created mtable
  final @OsUntrusted Configuration config;
  @OsUntrusted
  InodeTree<@OsUntrusted AbstractFileSystem> fsState;  // the fs state; ie the mount table
  @OsUntrusted
  Path homeDir = null;
  
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
    Path getSrc(ViewFs.@OsUntrusted MountPoint this) {
      return src;
    }
    @OsUntrusted
    URI @OsUntrusted [] getTargets(ViewFs.@OsUntrusted MountPoint this) {
      return targets;
    }
  }
  
  public @OsUntrusted ViewFs(final @OsUntrusted Configuration conf) throws IOException,
      URISyntaxException {
    this(FsConstants.VIEWFS_URI, conf);
  }
  
  /**
   * This constructor has the signature needed by
   * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}.
   * 
   * @param theUri which must be that of ViewFs
   * @param conf
   * @throws IOException
   * @throws URISyntaxException 
   */
  @OsUntrusted
  ViewFs(final @OsUntrusted URI theUri, final @OsUntrusted Configuration conf) throws IOException,
      URISyntaxException {
    super(theUri, FsConstants.VIEWFS_SCHEME, false, -1);
    creationTime = Time.now();
    ugi = UserGroupInformation.getCurrentUser();
    config = conf;
    // Now build  client side view (i.e. client side mount table) from config.
    @OsUntrusted
    String authority = theUri.getAuthority();
    fsState = new @OsUntrusted InodeTree<@OsUntrusted AbstractFileSystem>(conf, authority) {

      @Override
      protected
      @OsUntrusted
      AbstractFileSystem getTargetFileSystem(final @OsUntrusted URI uri)
        throws URISyntaxException, UnsupportedFileSystemException {
          @OsUntrusted
          String pathString = uri.getPath();
          if (pathString.isEmpty()) {
            pathString = "/";
          }
          return new @OsUntrusted ChRootedFs(
              AbstractFileSystem.createFileSystem(uri, config),
              new @OsUntrusted Path(pathString));
      }

      @Override
      protected
      @OsUntrusted
      AbstractFileSystem getTargetFileSystem(
          final @OsUntrusted INodeDir<@OsUntrusted AbstractFileSystem> dir) throws URISyntaxException {
        return new @OsUntrusted InternalDirOfViewFs(dir, creationTime, ugi, getUri());
      }

      @Override
      protected
      @OsUntrusted
      AbstractFileSystem getTargetFileSystem(@OsUntrusted URI @OsUntrusted [] mergeFsURIList)
          throws URISyntaxException, UnsupportedFileSystemException {
        throw new @OsUntrusted UnsupportedFileSystemException("mergefs not implemented yet");
        // return MergeFs.createMergeFs(mergeFsURIList, config);
      }
    };
  }

  @Override
  public @OsUntrusted FsServerDefaults getServerDefaults(@OsUntrusted ViewFs this) throws IOException {
    return LocalConfigKeys.getServerDefaults(); 
  }

  @Override
  public @OsUntrusted int getUriDefaultPort(@OsUntrusted ViewFs this) {
    return -1;
  }
 
  @Override
  public @OsUntrusted Path getHomeDirectory(@OsUntrusted ViewFs this) {
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
  public @OsUntrusted Path resolvePath(@OsUntrusted ViewFs this, final @OsUntrusted Path f) throws FileNotFoundException,
          AccessControlException, UnresolvedLinkException, IOException {
    final InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res;
      res = fsState.resolve(getUriPath(f), true);
    if (res.isInternalDir()) {
      return f;
    }
    return res.targetFileSystem.resolvePath(res.remainingPath);

  }
  
  @Override
  public @OsUntrusted FSDataOutputStream createInternal(@OsUntrusted ViewFs this, final @OsUntrusted Path f,
      final @OsUntrusted EnumSet<@OsUntrusted CreateFlag> flag, final @OsUntrusted FsPermission absolutePermission,
      final @OsUntrusted int bufferSize, final @OsUntrusted short replication, final @OsUntrusted long blockSize,
      final @OsUntrusted Progressable progress, final @OsUntrusted ChecksumOpt checksumOpt,
      final @OsUntrusted boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException,
      UnresolvedLinkException, IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res;
    try {
      res = fsState.resolve(getUriPath(f), false);
    } catch (@OsUntrusted FileNotFoundException e) {
      if (createParent) {
        throw readOnlyMountTable("create", f);
      } else {
        throw e;
      }
    }
    assert(res.remainingPath != null);
    return res.targetFileSystem.createInternal(res.remainingPath, flag,
        absolutePermission, bufferSize, replication,
        blockSize, progress, checksumOpt,
        createParent);
  }

  @Override
  public @OsUntrusted boolean delete(@OsUntrusted ViewFs this, final @OsUntrusted Path f, final @OsUntrusted boolean recursive)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    // If internal dir or target is a mount link (ie remainingPath is Slash)
    if (res.isInternalDir() || res.remainingPath == InodeTree.SlashPath) {
      throw new @OsUntrusted AccessControlException(
          "Cannot delete internal mount table directory: " + f);
    }
    return res.targetFileSystem.delete(res.remainingPath, recursive);
  }

  @Override
  public @OsUntrusted BlockLocation @OsUntrusted [] getFileBlockLocations(@OsUntrusted ViewFs this, final @OsUntrusted Path f, final @OsUntrusted long start,
      final @OsUntrusted long len) throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return
      res.targetFileSystem.getFileBlockLocations(res.remainingPath, start, len);
  }

  @Override
  public @OsUntrusted FileChecksum getFileChecksum(@OsUntrusted ViewFs this, final @OsUntrusted Path f)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.getFileChecksum(res.remainingPath);
  }

  @Override
  public @OsUntrusted FileStatus getFileStatus(@OsUntrusted ViewFs this, final @OsUntrusted Path f) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);

    //  FileStatus#getPath is a fully qualified path relative to the root of 
    // target file system.
    // We need to change it to viewfs URI - relative to root of mount table.
    
    // The implementors of RawLocalFileSystem were trying to be very smart.
    // They implement FileStatus#getOwener lazily -- the object
    // returned is really a RawLocalFileSystem that expect the
    // FileStatus#getPath to be unchanged so that it can get owner when needed.
    // Hence we need to interpose a new ViewFsFileStatus that works around.
    
    
    @OsUntrusted
    FileStatus status =  res.targetFileSystem.getFileStatus(res.remainingPath);
    return new @OsUntrusted ViewFsFileStatus(status, this.makeQualified(f));
  }

  @Override
  public @OsUntrusted FileStatus getFileLinkStatus(@OsUntrusted ViewFs this, final @OsUntrusted Path f)
     throws AccessControlException, FileNotFoundException,
     UnsupportedFileSystemException, IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), false); // do not follow mount link
    return res.targetFileSystem.getFileLinkStatus(res.remainingPath);
  }
  
  @Override
  public @OsUntrusted FsStatus getFsStatus(@OsUntrusted ViewFs this) throws AccessControlException,
      FileNotFoundException, IOException {
    return new @OsUntrusted FsStatus(0, 0, 0);
  }

  @Override
  public @OsUntrusted RemoteIterator<@OsUntrusted FileStatus> listStatusIterator(@OsUntrusted ViewFs this, final @OsUntrusted Path f)
    throws AccessControlException, FileNotFoundException,
    UnresolvedLinkException, IOException {
    final InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res =
      fsState.resolve(getUriPath(f), true);
    final @OsUntrusted RemoteIterator<@OsUntrusted FileStatus> fsIter =
      res.targetFileSystem.listStatusIterator(res.remainingPath);
    if (res.isInternalDir()) {
      return fsIter;
    }
    
    return new @OsUntrusted RemoteIterator<@OsUntrusted FileStatus>() {
      final @OsUntrusted RemoteIterator<@OsUntrusted FileStatus> myIter;
      final @OsUntrusted ChRootedFs targetFs;
      { // Init
          myIter = fsIter;
          targetFs = (@OsUntrusted ChRootedFs) res.targetFileSystem;
      }
      
      @Override
      public @OsUntrusted boolean hasNext() throws IOException {
        return myIter.hasNext();
      }
      
      @Override
      public @OsUntrusted FileStatus next() throws IOException {
        @OsUntrusted
        FileStatus status =  myIter.next();
        @OsUntrusted
        String suffix = targetFs.stripOutRoot(status.getPath());
        return new @OsUntrusted ViewFsFileStatus(status, makeQualified(
            suffix.length() == 0 ? f : new @OsUntrusted Path(res.resolvedPath, suffix)));
      }
    };
  }
  
  @Override
  public @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted ViewFs this, final @OsUntrusted Path f) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res =
      fsState.resolve(getUriPath(f), true);
    
    @OsUntrusted
    FileStatus @OsUntrusted [] statusLst = res.targetFileSystem.listStatus(res.remainingPath);
    if (!res.isInternalDir()) {
      // We need to change the name in the FileStatus as described in
      // {@link #getFileStatus }
      @OsUntrusted
      ChRootedFs targetFs;
      targetFs = (@OsUntrusted ChRootedFs) res.targetFileSystem;
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
  public void mkdir(@OsUntrusted ViewFs this, final @OsUntrusted Path dir, final @OsUntrusted FsPermission permission,
      final @OsUntrusted boolean createParent) throws AccessControlException,
      FileAlreadyExistsException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(dir), false);
    res.targetFileSystem.mkdir(res.remainingPath, permission, createParent);
  }

  @Override
  public @OsUntrusted FSDataInputStream open(@OsUntrusted ViewFs this, final @OsUntrusted Path f, final @OsUntrusted int bufferSize)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res = 
        fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.open(res.remainingPath, bufferSize);
  }

  
  @Override
  public void renameInternal(@OsUntrusted ViewFs this, final @OsUntrusted Path src, final @OsUntrusted Path dst,
      final @OsUntrusted boolean overwrite) throws IOException, UnresolvedLinkException {
    // passing resolveLastComponet as false to catch renaming a mount point 
    // itself we need to catch this as an internal operation and fail.
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> resSrc = 
      fsState.resolve(getUriPath(src), false); 
  
    if (resSrc.isInternalDir()) {
      throw new @OsUntrusted AccessControlException(
          "Cannot Rename within internal dirs of mount table: it is readOnly");
    }
      
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> resDst = 
                                fsState.resolve(getUriPath(dst), false);
    if (resDst.isInternalDir()) {
      throw new @OsUntrusted AccessControlException(
          "Cannot Rename within internal dirs of mount table: it is readOnly");
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
    
    resSrc.targetFileSystem.renameInternal(resSrc.remainingPath,
      resDst.remainingPath, overwrite);
  }

  @Override
  public void renameInternal(@OsUntrusted ViewFs this, final @OsUntrusted Path src, final @OsUntrusted Path dst)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnresolvedLinkException, IOException {
    renameInternal(src, dst, false);
  }
  
  @Override
  public @OsUntrusted boolean supportsSymlinks(@OsUntrusted ViewFs this) {
    return true;
  }
  
  @Override
  public void createSymlink(@OsUntrusted ViewFs this, final @OsUntrusted Path target, final @OsUntrusted Path link,
      final @OsUntrusted boolean createParent) throws IOException, UnresolvedLinkException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res;
    try {
      res = fsState.resolve(getUriPath(link), false);
    } catch (@OsUntrusted FileNotFoundException e) {
      if (createParent) {
        throw readOnlyMountTable("createSymlink", link);
      } else {
        throw e;
      }
    }
    assert(res.remainingPath != null);
    res.targetFileSystem.createSymlink(target, res.remainingPath,
        createParent);  
  }

  @Override
  public @OsUntrusted Path getLinkTarget(@OsUntrusted ViewFs this, final @OsUntrusted Path f) throws IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), false); // do not follow mount link
    return res.targetFileSystem.getLinkTarget(res.remainingPath);
  }

  @Override
  public void setOwner(@OsUntrusted ViewFs this, final @OsUntrusted Path f, final @OsTrusted String username,
      final @OsTrusted String groupname) throws AccessControlException,
      FileNotFoundException, UnresolvedLinkException, IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setOwner(res.remainingPath, username, groupname); 
  }

  @Override
  public void setPermission(@OsUntrusted ViewFs this, final @OsUntrusted Path f, final @OsUntrusted FsPermission permission)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setPermission(res.remainingPath, permission); 
    
  }

  @Override
  public @OsUntrusted boolean setReplication(@OsUntrusted ViewFs this, final @OsUntrusted Path f, final @OsUntrusted short replication)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    return res.targetFileSystem.setReplication(res.remainingPath, replication);
  }

  @Override
  public void setTimes(@OsUntrusted ViewFs this, final @OsUntrusted Path f, final @OsUntrusted long mtime, final @OsUntrusted long atime)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    InodeTree.@OsUntrusted ResolveResult<@OsUntrusted AbstractFileSystem> res = 
      fsState.resolve(getUriPath(f), true);
    res.targetFileSystem.setTimes(res.remainingPath, mtime, atime); 
  }

  @Override
  public void setVerifyChecksum(@OsUntrusted ViewFs this, final @OsUntrusted boolean verifyChecksum)
      throws AccessControlException, IOException {
    // This is a file system level operations, however ViewFs 
    // points to many file systems. Noop for ViewFs. 
  }
  
  public @OsUntrusted MountPoint @OsUntrusted [] getMountPoints(@OsUntrusted ViewFs this) {
    @OsUntrusted
    List<InodeTree.@OsUntrusted MountPoint<@OsUntrusted AbstractFileSystem>> mountPoints = 
                  fsState.getMountPoints();
    
    @OsUntrusted
    MountPoint @OsUntrusted [] result = new @OsUntrusted MountPoint @OsUntrusted [mountPoints.size()];
    for ( @OsUntrusted int i = 0; i < mountPoints.size(); ++i ) {
      result[i] = new @OsUntrusted MountPoint(new @OsUntrusted Path(mountPoints.get(i).src), 
                              mountPoints.get(i).target.targetDirLinkList);
    }
    return result;
  }
  
  @Override
  public @OsUntrusted List<@OsUntrusted Token<@OsUntrusted ? extends java.lang.@OsUntrusted Object>> getDelegationTokens(@OsUntrusted ViewFs this, @OsUntrusted String renewer) throws IOException {
    @OsUntrusted
    List<InodeTree.@OsUntrusted MountPoint<@OsUntrusted AbstractFileSystem>> mountPoints = 
                fsState.getMountPoints();
    @OsUntrusted
    int initialListSize  = 0;
    for (InodeTree.@OsUntrusted MountPoint<@OsUntrusted AbstractFileSystem> im : mountPoints) {
      initialListSize += im.target.targetDirLinkList.length; 
    }
    @OsUntrusted
    List<@OsUntrusted Token<@OsUntrusted ? extends java.lang.@OsUntrusted Object>> result = new @OsUntrusted ArrayList<@OsUntrusted Token<@OsUntrusted ? extends java.lang.@OsUntrusted Object>>(initialListSize);
    for ( @OsUntrusted int i = 0; i < mountPoints.size(); ++i ) {
      @OsUntrusted
      List<@OsUntrusted Token<@OsUntrusted ? extends java.lang.@OsUntrusted Object>> tokens = 
        mountPoints.get(i).target.targetFileSystem.getDelegationTokens(renewer);
      if (tokens != null) {
        result.addAll(tokens);
      }
    }
    return result;
  }

  @Override
  public @OsUntrusted boolean isValidName(@OsUntrusted ViewFs this, @OsUntrusted String src) {
    // Prefix validated at mount time and rest of path validated by mount target.
    return true;
  }

  
  
  /*
   * An instance of this class represents an internal dir of the viewFs 
   * ie internal dir of the mount table.
   * It is a ready only mount tbale and create, mkdir or delete operations
   * are not allowed.
   * If called on create or mkdir then this target is the parent of the
   * directory in which one is trying to create or mkdir; hence
   * in this case the path name passed in is the last component. 
   * Otherwise this target is the end point of the path and hence
   * the path name passed in is null. 
   */
  static class InternalDirOfViewFs extends @OsUntrusted AbstractFileSystem {
    
    final InodeTree.@OsUntrusted INodeDir<@OsUntrusted AbstractFileSystem>  theInternalDir;
    final @OsUntrusted long creationTime; // of the the mount table
    final @OsUntrusted UserGroupInformation ugi; // the user/group of user who created mtable
    final @OsUntrusted URI myUri; // the URI of the outer ViewFs
    
    public @OsUntrusted InternalDirOfViewFs(final InodeTree.@OsUntrusted INodeDir<@OsUntrusted AbstractFileSystem> dir,
        final @OsUntrusted long cTime, final @OsUntrusted UserGroupInformation ugi, final @OsUntrusted URI uri)
      throws URISyntaxException {
      super(FsConstants.VIEWFS_URI, FsConstants.VIEWFS_SCHEME, false, -1);
      theInternalDir = dir;
      creationTime = cTime;
      this.ugi = ugi;
      myUri = uri;
    }

    static private void checkPathIsSlash(final @OsUntrusted Path f) throws IOException {
      if (f != InodeTree.SlashPath) {
        throw new @OsUntrusted IOException (
        "Internal implementation error: expected file name to be /" );
      }
    }

    @Override
    public @OsUntrusted FSDataOutputStream createInternal(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f,
        final @OsUntrusted EnumSet<@OsUntrusted CreateFlag> flag, final @OsUntrusted FsPermission absolutePermission,
        final @OsUntrusted int bufferSize, final @OsUntrusted short replication, final @OsUntrusted long blockSize,
        final @OsUntrusted Progressable progress, final @OsUntrusted ChecksumOpt checksumOpt,
        final @OsUntrusted boolean createParent) throws AccessControlException,
        FileAlreadyExistsException, FileNotFoundException,
        ParentNotDirectoryException, UnsupportedFileSystemException,
        UnresolvedLinkException, IOException {
      throw readOnlyMountTable("create", f);
    }

    @Override
    public @OsUntrusted boolean delete(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f, final @OsUntrusted boolean recursive)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("delete", f);
    }

    @Override
    public @OsUntrusted BlockLocation @OsUntrusted [] getFileBlockLocations(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f, final @OsUntrusted long start,
        final @OsUntrusted long len) throws FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new @OsUntrusted FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public @OsUntrusted FileChecksum getFileChecksum(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f)
        throws FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new @OsUntrusted FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public @OsUntrusted FileStatus getFileStatus(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f) throws IOException {
      checkPathIsSlash(f);
      return new @OsUntrusted FileStatus(0, true, 0, 0, creationTime, creationTime, 
          PERMISSION_RRR, ugi.getUserName(), ugi.getGroupNames()[0],
          new @OsUntrusted Path(theInternalDir.fullPath).makeQualified(
              myUri, null));
    }
    
    @Override
    public @OsUntrusted FileStatus getFileLinkStatus(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f)
        throws FileNotFoundException {
      // look up i internalDirs children - ignore first Slash
      @OsUntrusted
      INode<@OsUntrusted AbstractFileSystem> inode =
        theInternalDir.children.get(f.toUri().toString().substring(1)); 
      if (inode == null) {
        throw new @OsUntrusted FileNotFoundException(
            "viewFs internal mount table - missing entry:" + f);
      }
      @OsUntrusted
      FileStatus result;
      if (inode instanceof @OsUntrusted INodeLink) {
        @OsUntrusted
        INodeLink<@OsUntrusted AbstractFileSystem> inodelink = 
          (@OsUntrusted INodeLink<@OsUntrusted AbstractFileSystem>) inode;
        result = new @OsUntrusted FileStatus(0, false, 0, 0, creationTime, creationTime,
            PERMISSION_RRR, ugi.getUserName(), ugi.getGroupNames()[0],
            inodelink.getTargetLink(),
            new @OsUntrusted Path(inode.fullPath).makeQualified(
                myUri, null));
      } else {
        result = new @OsUntrusted FileStatus(0, true, 0, 0, creationTime, creationTime,
          PERMISSION_RRR, ugi.getUserName(), ugi.getGroupNames()[0],
          new @OsUntrusted Path(inode.fullPath).makeQualified(
              myUri, null));
      }
      return result;
    }
    
    @Override
    public @OsUntrusted FsStatus getFsStatus(ViewFs.@OsUntrusted InternalDirOfViewFs this) {
      return new @OsUntrusted FsStatus(0, 0, 0);
    }

    @Override
    public @OsUntrusted FsServerDefaults getServerDefaults(ViewFs.@OsUntrusted InternalDirOfViewFs this) throws IOException {
      throw new @OsUntrusted IOException("FsServerDefaults not implemented yet");
    }

    @Override
    public @OsUntrusted int getUriDefaultPort(ViewFs.@OsUntrusted InternalDirOfViewFs this) {
      return -1;
    }

    @Override
    public @OsUntrusted FileStatus @OsUntrusted [] listStatus(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f) throws AccessControlException,
        IOException {
      checkPathIsSlash(f);
      @OsUntrusted
      FileStatus @OsUntrusted [] result = new @OsUntrusted FileStatus @OsUntrusted [theInternalDir.children.size()];
      @OsUntrusted
      int i = 0;
      for (@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted INode<@OsUntrusted AbstractFileSystem>> iEntry : 
                                          theInternalDir.children.entrySet()) {
        @OsUntrusted
        INode<@OsUntrusted AbstractFileSystem> inode = iEntry.getValue();

        
        if (inode instanceof @OsUntrusted INodeLink ) {
          @OsUntrusted
          INodeLink<@OsUntrusted AbstractFileSystem> link = 
            (@OsUntrusted INodeLink<@OsUntrusted AbstractFileSystem>) inode;

          result[i++] = new @OsUntrusted FileStatus(0, false, 0, 0,
            creationTime, creationTime,
            PERMISSION_RRR, ugi.getUserName(), ugi.getGroupNames()[0],
            link.getTargetLink(),
            new @OsUntrusted Path(inode.fullPath).makeQualified(
                myUri, null));
        } else {
          result[i++] = new @OsUntrusted FileStatus(0, true, 0, 0,
            creationTime, creationTime,
            PERMISSION_RRR, ugi.getUserName(), ugi.getGroupNames()[0],
            new @OsUntrusted Path(inode.fullPath).makeQualified(
                myUri, null));
        }
      }
      return result;
    }

    @Override
    public void mkdir(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path dir, final @OsUntrusted FsPermission permission,
        final @OsUntrusted boolean createParent) throws AccessControlException,
        FileAlreadyExistsException {
      if (theInternalDir.isRoot && dir == null) {
        throw new @OsUntrusted FileAlreadyExistsException("/ already exits");
      }
      throw readOnlyMountTable("mkdir", dir);
    }

    @Override
    public @OsUntrusted FSDataInputStream open(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f, final @OsUntrusted int bufferSize)
        throws FileNotFoundException, IOException {
      checkPathIsSlash(f);
      throw new @OsUntrusted FileNotFoundException("Path points to dir not a file");
    }

    @Override
    public void renameInternal(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path src, final @OsUntrusted Path dst)
        throws AccessControlException, IOException {
      checkPathIsSlash(src);
      checkPathIsSlash(dst);
      throw readOnlyMountTable("rename", src);     
    }

    @Override
    public @OsUntrusted boolean supportsSymlinks(ViewFs.@OsUntrusted InternalDirOfViewFs this) {
      return true;
    }
    
    @Override
    public void createSymlink(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path target, final @OsUntrusted Path link,
        final @OsUntrusted boolean createParent) throws AccessControlException {
      throw readOnlyMountTable("createSymlink", link);    
    }

    @Override
    public @OsUntrusted Path getLinkTarget(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f) throws FileNotFoundException,
        IOException {
      return getFileLinkStatus(f).getSymlink();
    }

    @Override
    public void setOwner(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f, final @OsUntrusted String username,
        final @OsUntrusted String groupname) throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setOwner", f);
    }

    @Override
    public void setPermission(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f, final @OsUntrusted FsPermission permission)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setPermission", f);    
    }

    @Override
    public @OsUntrusted boolean setReplication(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f, final @OsUntrusted short replication)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setReplication", f);
    }

    @Override
    public void setTimes(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted Path f, final @OsUntrusted long mtime, final @OsUntrusted long atime)
        throws AccessControlException, IOException {
      checkPathIsSlash(f);
      throw readOnlyMountTable("setTimes", f);    
    }

    @Override
    public void setVerifyChecksum(ViewFs.@OsUntrusted InternalDirOfViewFs this, final @OsUntrusted boolean verifyChecksum)
        throws AccessControlException {
      throw readOnlyMountTable("setVerifyChecksum", "");   
    }
  }
}
