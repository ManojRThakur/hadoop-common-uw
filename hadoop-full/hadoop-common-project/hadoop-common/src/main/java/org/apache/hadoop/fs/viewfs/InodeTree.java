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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;


/**
 * InodeTree implements a mount-table as a tree of inodes.
 * It is used to implement ViewFs and ViewFileSystem.
 * In order to use it the caller must subclass it and implement
 * the abstract methods {@link #getTargetFileSystem(INodeDir)}, etc.
 * 
 * The mountable is initialized from the config variables as 
 * specified in {@link ViewFs}
 *
 * @param <T> is AbstractFileSystem or FileSystem
 * 
 * The three main methods are
 * {@link #InodeTreel(Configuration)} // constructor
 * {@link #InodeTree(Configuration, String)} // constructor
 * {@link #resolve(String, boolean)} 
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable 
abstract class InodeTree<@OsUntrusted T extends java.lang.@OsUntrusted Object> {
  static enum ResultKind {  @OsUntrusted  isInternalDir,  @OsUntrusted  isExternalDir;};
  static final @OsUntrusted Path SlashPath = new @OsUntrusted Path("/");
  
  final @OsUntrusted INodeDir<@OsUntrusted T> root; // the root of the mount table
  
  final @OsUntrusted String homedirPrefix; // the homedir config value for this mount table
  
  @OsUntrusted
  List<@OsUntrusted MountPoint<@OsUntrusted T>> mountPoints = new @OsUntrusted ArrayList<@OsUntrusted MountPoint<T>>();
  
  
  static class MountPoint<@OsUntrusted T extends java.lang.@OsUntrusted Object> {
    @OsUntrusted
    String src;
    @OsUntrusted
    INodeLink<@OsUntrusted T> target;
    @OsUntrusted
    MountPoint(@OsUntrusted String srcPath, @OsUntrusted INodeLink<@OsUntrusted T> mountLink) {
      src = srcPath;
      target = mountLink;
    }

  }
  
  /**
   * Breaks file path into component names.
   * @param path
   * @return array of names component names
   */
  static @OsUntrusted String @OsUntrusted [] breakIntoPathComponents(final @OsUntrusted String path) {
    return path == null ? null : path.split(Path.SEPARATOR);
  } 
  
  /**
   * Internal class for inode tree
   * @param <T>
   */
  abstract static class INode<@OsUntrusted T extends java.lang.@OsUntrusted Object> {
    final @OsUntrusted String fullPath; // the full path to the root
    public @OsUntrusted INode(@OsUntrusted String pathToNode, @OsUntrusted UserGroupInformation aUgi) {
      fullPath = pathToNode;
    }
  };

  /**
   * Internal class to represent an internal dir of the mount table
   * @param <T>
   */
  static class INodeDir<@OsUntrusted T extends java.lang.@OsUntrusted Object> extends @OsUntrusted INode<T> {
    final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted INode<@OsUntrusted T>> children = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted INode<T>>();
    @OsUntrusted
    T InodeDirFs =  null; // file system of this internal directory of mountT
    @OsUntrusted
    boolean isRoot = false;
    
    @OsUntrusted
    INodeDir(final @OsUntrusted String pathToNode, final @OsUntrusted UserGroupInformation aUgi) {
      super(pathToNode, aUgi);
    }

    @OsUntrusted
    INode<@OsUntrusted T> resolve(InodeTree.@OsUntrusted INodeDir<T> this, final @OsUntrusted String pathComponent) throws FileNotFoundException {
      final @OsUntrusted INode<T> result = resolveInternal(pathComponent);
      if (result == null) {
        throw new @OsUntrusted FileNotFoundException();
      }
      return result;
    }
    
    @OsUntrusted
    INode<@OsUntrusted T> resolveInternal(InodeTree.@OsUntrusted INodeDir<T> this, final @OsUntrusted String pathComponent) {
      return children.get(pathComponent);
    }
    
    @OsUntrusted
    INodeDir<@OsUntrusted T> addDir(InodeTree.@OsUntrusted INodeDir<T> this, final @OsUntrusted String pathComponent,
        final @OsUntrusted UserGroupInformation aUgi)
      throws FileAlreadyExistsException {
      if (children.containsKey(pathComponent)) {
        throw new @OsUntrusted FileAlreadyExistsException();
      }
      final @OsUntrusted INodeDir<T> newDir = new @OsUntrusted INodeDir<T>(fullPath+ (isRoot ? "" : "/") + 
          pathComponent, aUgi);
      children.put(pathComponent, newDir);
      return newDir;
    }
    
    void addLink(InodeTree.@OsUntrusted INodeDir<T> this, final @OsUntrusted String pathComponent, final @OsUntrusted INodeLink<@OsUntrusted T> link)
      throws FileAlreadyExistsException {
      if (children.containsKey(pathComponent)) {
        throw new @OsUntrusted FileAlreadyExistsException();
      }
      children.put(pathComponent, link);
    }
  }

  /**
   * In internal class to represent a mount link
   * A mount link can be single dir link or a merge dir link.

   * A merge dir link is  a merge (junction) of links to dirs:
   * example : <merge of 2 dirs
   *     /users -> hdfs:nn1//users
   *     /users -> hdfs:nn2//users
   * 
   * For a merge, each target is checked to be dir when created but if target
   * is changed later it is then ignored (a dir with null entries)
   */
  static class INodeLink<@OsUntrusted T extends java.lang.@OsUntrusted Object> extends @OsUntrusted INode<T> {
    final @OsUntrusted boolean isMergeLink; // true if MergeLink
    final @OsUntrusted URI @OsUntrusted [] targetDirLinkList;
    final @OsUntrusted T targetFileSystem;   // file system object created from the link.
    
    /**
     * Construct a mergeLink
     */
    @OsUntrusted
    INodeLink(final @OsUntrusted String pathToNode, final @OsUntrusted UserGroupInformation aUgi,
        final @OsUntrusted T targetMergeFs, final @OsUntrusted URI @OsUntrusted [] aTargetDirLinkList) {
      super(pathToNode, aUgi);
      targetFileSystem = targetMergeFs;
      targetDirLinkList = aTargetDirLinkList;
      isMergeLink = true;
    }
    
    /**
     * Construct a simple link (i.e. not a mergeLink)
     */
    @OsUntrusted
    INodeLink(final @OsUntrusted String pathToNode, final @OsUntrusted UserGroupInformation aUgi,
        final @OsUntrusted T targetFs, final @OsUntrusted URI aTargetDirLink) {
      super(pathToNode, aUgi);
      targetFileSystem = targetFs;
      targetDirLinkList = new @OsUntrusted URI @OsUntrusted [1];
      targetDirLinkList[0] = aTargetDirLink;
      isMergeLink = false;
    }
    
    /**
     * Get the target of the link
     * If a merge link then it returned as "," separated URI list.
     */
    @OsUntrusted
    Path getTargetLink(InodeTree.@OsUntrusted INodeLink<T> this) {
      // is merge link - use "," as separator between the merged URIs
      //String result = targetDirLinkList[0].toString();
      @OsUntrusted
      StringBuilder result = new @OsUntrusted StringBuilder(targetDirLinkList[0].toString());
      for (@OsUntrusted int i=1; i < targetDirLinkList.length; ++i) { 
        result.append(',').append(targetDirLinkList[i].toString());
      }
      return new @OsUntrusted Path(result.toString());
    }
  }


  private void createLink(@OsUntrusted InodeTree<T> this, final @OsUntrusted String src, final @OsUntrusted String target,
      final @OsUntrusted boolean isLinkMerge, final @OsUntrusted UserGroupInformation aUgi)
      throws URISyntaxException, IOException,
    FileAlreadyExistsException, UnsupportedFileSystemException {
    // Validate that src is valid absolute path
    final @OsUntrusted Path srcPath = new @OsUntrusted Path(src); 
    if (!srcPath.isAbsoluteAndSchemeAuthorityNull()) {
      throw new @OsUntrusted IOException("ViewFs:Non absolute mount name in config:" + src);
    }
 
    final @OsUntrusted String @OsUntrusted [] srcPaths = breakIntoPathComponents(src);
    @OsUntrusted
    INodeDir<T> curInode = root;
    @OsUntrusted
    int i;
    // Ignore first initial slash, process all except last component
    for (i = 1; i < srcPaths.length-1; i++) {
      final @OsUntrusted String iPath = srcPaths[i];
      @OsUntrusted
      INode<T> nextInode = curInode.resolveInternal(iPath);
      if (nextInode == null) {
        @OsUntrusted
        INodeDir<T> newDir = curInode.addDir(iPath, aUgi);
        newDir.InodeDirFs = getTargetFileSystem(newDir);
        nextInode = newDir;
      }
      if (nextInode instanceof @OsUntrusted INodeLink) {
        // Error - expected a dir but got a link
        throw new @OsUntrusted FileAlreadyExistsException("Path " + nextInode.fullPath +
            " already exists as link");
      } else {
        assert(nextInode instanceof @OsUntrusted INodeDir);
        curInode = (@OsUntrusted INodeDir<T>) nextInode;
      }
    }
    
    // Now process the last component
    // Add the link in 2 cases: does not exist or a link exists
    @OsUntrusted
    String iPath = srcPaths[i];// last component
    if (curInode.resolveInternal(iPath) != null) {
      //  directory/link already exists
      @OsUntrusted
      StringBuilder strB = new @OsUntrusted StringBuilder(srcPaths[0]);
      for (@OsUntrusted int j = 1; j <= i; ++j) {
        strB.append('/').append(srcPaths[j]);
      }
      throw new @OsUntrusted FileAlreadyExistsException("Path " + strB +
            " already exists as dir; cannot create link here");
    }
    
    final @OsUntrusted INodeLink<T> newLink;
    final @OsUntrusted String fullPath = curInode.fullPath + (curInode == root ? "" : "/")
        + iPath;
    if (isLinkMerge) { // Target is list of URIs
      @OsUntrusted
      String @OsUntrusted [] targetsList = StringUtils.getStrings(target);
      @OsUntrusted
      URI @OsUntrusted [] targetsListURI = new @OsUntrusted URI @OsUntrusted [targetsList.length];
      @OsUntrusted
      int k = 0;
      for (@OsUntrusted String itarget : targetsList) {
        targetsListURI[k++] = new @OsUntrusted URI(itarget);
      }
      newLink = new @OsUntrusted INodeLink<T>(fullPath, aUgi,
          getTargetFileSystem(targetsListURI), targetsListURI);
    } else {
      newLink = new @OsUntrusted INodeLink<T>(fullPath, aUgi,
          getTargetFileSystem(new @OsUntrusted URI(target)), new @OsUntrusted URI(target));
    }
    curInode.addLink(iPath, newLink);
    mountPoints.add(new @OsUntrusted MountPoint<T>(src, newLink));
  }
  
  /**
   * Below the "public" methods of InodeTree
   */
  
  /**
   * The user of this class must subclass and implement the following
   * 3 abstract methods.
   * @throws IOException 
   */
  protected abstract @OsUntrusted T getTargetFileSystem(@OsUntrusted InodeTree<T> this, final @OsUntrusted URI uri)
    throws UnsupportedFileSystemException, URISyntaxException, IOException;
  
  protected abstract @OsUntrusted T getTargetFileSystem(@OsUntrusted InodeTree<T> this, final @OsUntrusted INodeDir<@OsUntrusted T> dir)
    throws URISyntaxException;
  
  protected abstract @OsUntrusted T getTargetFileSystem(@OsUntrusted InodeTree<T> this, final @OsUntrusted URI @OsUntrusted [] mergeFsURIList)
  throws UnsupportedFileSystemException, URISyntaxException;
  
  /**
   * Create Inode Tree from the specified mount-table specified in Config
   * @param config - the mount table keys are prefixed with 
   *       FsConstants.CONFIG_VIEWFS_PREFIX
   * @param viewName - the name of the mount table - if null use defaultMT name
   * @throws UnsupportedFileSystemException
   * @throws URISyntaxException
   * @throws FileAlreadyExistsException
   * @throws IOException
   */
  protected @OsUntrusted InodeTree(final @OsUntrusted Configuration config, final @OsUntrusted String viewName)
      throws UnsupportedFileSystemException, URISyntaxException,
    FileAlreadyExistsException, IOException { 
    @OsUntrusted
    String vName = viewName;
    if (vName == null) {
      vName = Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE;
    }
    homedirPrefix = ConfigUtil.getHomeDirValue(config, vName);
    root = new @OsUntrusted INodeDir<T>("/", UserGroupInformation.getCurrentUser());
    root.InodeDirFs = getTargetFileSystem(root);
    root.isRoot = true;
    
    final @OsUntrusted String mtPrefix = Constants.CONFIG_VIEWFS_PREFIX + "." + 
                            vName + ".";
    final @OsUntrusted String linkPrefix = Constants.CONFIG_VIEWFS_LINK + ".";
    final @OsUntrusted String linkMergePrefix = Constants.CONFIG_VIEWFS_LINK_MERGE + ".";
    @OsUntrusted
    boolean gotMountTableEntry = false;
    final @OsUntrusted UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    for (@OsUntrusted Entry<@OsUntrusted String, @OsTrusted String> si : config) {
      final @OsUntrusted String key = si.getKey();
      if (key.startsWith(mtPrefix)) {
        gotMountTableEntry = true;
        @OsUntrusted
        boolean isMergeLink = false;
        @OsUntrusted
        String src = key.substring(mtPrefix.length());
        if (src.startsWith(linkPrefix)) {
          src = src.substring(linkPrefix.length());
        } else if (src.startsWith(linkMergePrefix)) { // A merge link
          isMergeLink = true;
          src = src.substring(linkMergePrefix.length());
        } else if (src.startsWith(Constants.CONFIG_VIEWFS_HOMEDIR)) {
          // ignore - we set home dir from config
          continue;
        } else {
          throw new @OsUntrusted IOException(
          "ViewFs: Cannot initialize: Invalid entry in Mount table in config: "+ 
          src);
        }
        final @OsUntrusted String target = si.getValue(); // link or merge link
        createLink(src, target, isMergeLink, ugi); 
      }
    }
    if (!gotMountTableEntry) {
      throw new @OsUntrusted IOException(
          "ViewFs: Cannot initialize: Empty Mount table in config for " +
             "viewfs://" + vName + "/");
    }
  }

  /**
   * Resolve returns ResolveResult.
   * The caller can continue the resolution of the remainingPath
   * in the targetFileSystem.
   * 
   * If the input pathname leads to link to another file system then
   * the targetFileSystem is the one denoted by the link (except it is
   * file system chrooted to link target.
   * If the input pathname leads to an internal mount-table entry then
   * the target file system is one that represents the internal inode.
   */
  static class ResolveResult<@OsUntrusted T extends java.lang.@OsUntrusted Object> {
    final @OsUntrusted ResultKind kind;
    final @OsUntrusted T targetFileSystem;
    final @OsUntrusted String resolvedPath;
    final @OsUntrusted Path remainingPath;   // to resolve in the target FileSystem
    
    @OsUntrusted
    ResolveResult(final @OsUntrusted ResultKind k, final @OsUntrusted T targetFs, final @OsUntrusted String resolveP,
        final @OsUntrusted Path remainingP) {
      kind = k;
      targetFileSystem = targetFs;
      resolvedPath = resolveP;
      remainingPath = remainingP;  
    }
    
    // isInternalDir of path resolution completed within the mount table 
    @OsUntrusted
    boolean isInternalDir(InodeTree.@OsUntrusted ResolveResult<T> this) {
      return (kind == ResultKind.isInternalDir);
    }
  }
  
  /**
   * Resolve the pathname p relative to root InodeDir
   * @param p - inout path
   * @param resolveLastComponent 
   * @return ResolveResult which allows further resolution of the remaining path
   * @throws FileNotFoundException
   */
  @OsUntrusted
  ResolveResult<@OsUntrusted T> resolve(@OsUntrusted InodeTree<T> this, final @OsUntrusted String p, final @OsUntrusted boolean resolveLastComponent)
    throws FileNotFoundException {
    // TO DO: - more efficient to not split the path, but simply compare
    @OsUntrusted
    String @OsUntrusted [] path = breakIntoPathComponents(p); 
    if (path.length <= 1) { // special case for when path is "/"
      @OsUntrusted
      ResolveResult<T> res = 
        new @OsUntrusted ResolveResult<T>(ResultKind.isInternalDir, 
              root.InodeDirFs, root.fullPath, SlashPath);
      return res;
    }
    
    @OsUntrusted
    INodeDir<T> curInode = root;
    @OsUntrusted
    int i;
    // ignore first slash
    for (i = 1; i < path.length - (resolveLastComponent ? 0 : 1); i++) {
      @OsUntrusted
      INode<T> nextInode = curInode.resolveInternal(path[i]);
      if (nextInode == null) {
        @OsUntrusted
        StringBuilder failedAt = new @OsUntrusted StringBuilder(path[0]);
        for ( @OsUntrusted int j = 1; j <=i; ++j) {
          failedAt.append('/').append(path[j]);
        }
        throw (new @OsUntrusted FileNotFoundException(failedAt.toString()));      
      }

      if (nextInode instanceof @OsUntrusted INodeLink) {
        final @OsUntrusted INodeLink<T> link = (@OsUntrusted INodeLink<T>) nextInode;
        final @OsUntrusted Path remainingPath;
        if (i >= path.length-1) {
          remainingPath = SlashPath;
        } else {
          @OsUntrusted
          StringBuilder remainingPathStr = new @OsUntrusted StringBuilder("/" + path[i+1]);
          for (@OsUntrusted int j = i+2; j< path.length; ++j) {
            remainingPathStr.append('/').append(path[j]);
          }
          remainingPath = new @OsUntrusted Path(remainingPathStr.toString());
        }
        final @OsUntrusted ResolveResult<T> res = 
          new @OsUntrusted ResolveResult<T>(ResultKind.isExternalDir,
              link.targetFileSystem, nextInode.fullPath, remainingPath);
        return res;
      } else if (nextInode instanceof @OsUntrusted INodeDir) {
        curInode = (@OsUntrusted INodeDir<T>) nextInode;
      }
    }

    // We have resolved to an internal dir in mount table.
    @OsUntrusted
    Path remainingPath;
    if (resolveLastComponent) {
      remainingPath = SlashPath;
    } else {
      // note we have taken care of when path is "/" above
      // for internal dirs rem-path does not start with / since the lookup
      // that follows will do a children.get(remaningPath) and will have to
      // strip-out the initial /
      @OsUntrusted
      StringBuilder remainingPathStr = new @OsUntrusted StringBuilder("/" + path[i]);
      for (@OsUntrusted int j = i+1; j< path.length; ++j) {
        remainingPathStr.append('/').append(path[j]);
      }
      remainingPath = new @OsUntrusted Path(remainingPathStr.toString());
    }
    final @OsUntrusted ResolveResult<T> res = 
       new @OsUntrusted ResolveResult<T>(ResultKind.isInternalDir,
           curInode.InodeDirFs, curInode.fullPath, remainingPath); 
    return res;
  }
  
  @OsUntrusted
  List<@OsUntrusted MountPoint<@OsUntrusted T>> getMountPoints(@OsUntrusted InodeTree<T> this) { 
    return mountPoints;
  }
  
  /**
   * 
   * @return home dir value from mount table; null if no config value
   * was found.
   */
  @OsUntrusted
  String getHomeDirPrefixValue(@OsUntrusted InodeTree<T> this) {
    return homedirPrefix;
  }
}
