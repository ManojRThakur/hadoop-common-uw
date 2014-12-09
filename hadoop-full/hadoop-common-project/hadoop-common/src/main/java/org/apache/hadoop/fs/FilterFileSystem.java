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
import ostrusted.quals.PolyOsTrusted;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

/****************************************************************
 * A <code>FilterFileSystem</code> contains
 * some other file system, which it uses as
 * its  basic file system, possibly transforming
 * the data along the way or providing  additional
 * functionality. The class <code>FilterFileSystem</code>
 * itself simply overrides all  methods of
 * <code>FileSystem</code> with versions that
 * pass all requests to the contained  file
 * system. Subclasses of <code>FilterFileSystem</code>
 * may further override some of  these methods
 * and may also provide additional methods
 * and fields.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FilterFileSystem extends @OsUntrusted FileSystem {
  
  protected @OsUntrusted FileSystem fs;
  protected @OsUntrusted String swapScheme;
  
  /*
   * so that extending classes can define it
   */
  public @OsUntrusted FilterFileSystem() {
  }
  
  public @OsUntrusted FilterFileSystem(@OsUntrusted FileSystem fs) {
    this.fs = fs;
    this.statistics = fs.statistics;
  }

  /**
   * Get the raw file system 
   * @return FileSystem being filtered
   */
  public @OsUntrusted FileSystem getRawFileSystem(@OsUntrusted FilterFileSystem this) {
    return fs;
  }

  /** Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param conf the configuration
   */
  @Override
  public void initialize(@OsUntrusted FilterFileSystem this, @OsUntrusted URI name, @OsUntrusted Configuration conf) throws IOException {
    super.initialize(name, conf);
    // this is less than ideal, but existing filesystems sometimes neglect
    // to initialize the embedded filesystem
    if (fs.getConf() == null) {
      fs.initialize(name, conf);
    }
    @OsUntrusted
    String scheme = name.getScheme();
    if (!scheme.equals(fs.getUri().getScheme())) {
      swapScheme = scheme;
    }
  }

  /** Returns a URI whose scheme and authority identify this FileSystem.*/
  @Override
  public @OsUntrusted URI getUri(@OsUntrusted FilterFileSystem this) {
    return fs.getUri();
  }
  
  
  @Override
  protected @OsUntrusted URI getCanonicalUri(@OsUntrusted FilterFileSystem this) {
    return fs.getCanonicalUri();
  }

  @Override
  protected @PolyOsTrusted URI canonicalizeUri(@OsUntrusted FilterFileSystem this, @PolyOsTrusted URI uri) {
    return fs.canonicalizeUri(uri);
  }

  /** Make sure that a path specifies a FileSystem. */
  @Override
  public @OsUntrusted Path makeQualified(@OsUntrusted FilterFileSystem this, @OsUntrusted Path path) {
    @OsUntrusted
    Path fqPath = fs.makeQualified(path);
    // swap in our scheme if the filtered fs is using a different scheme
    if (swapScheme != null) {
      try {
        // NOTE: should deal with authority, but too much other stuff is broken 
        fqPath = new @OsUntrusted Path(
            new @OsUntrusted URI(swapScheme, fqPath.toUri().getSchemeSpecificPart(), null)
        );
      } catch (@OsUntrusted URISyntaxException e) {
        throw new @OsUntrusted IllegalArgumentException(e);
      }
    }
    return fqPath;
  }
  
  ///////////////////////////////////////////////////////////////
  // FileSystem
  ///////////////////////////////////////////////////////////////

  /** Check that a Path belongs to this FileSystem. */
  @Override
  protected void checkPath(@OsUntrusted FilterFileSystem this, @OsUntrusted Path path) {
    fs.checkPath(path);
  }

  @Override
  public @OsUntrusted BlockLocation @OsUntrusted [] getFileBlockLocations(@OsUntrusted FilterFileSystem this, @OsUntrusted FileStatus file, @OsUntrusted long start,
    @OsUntrusted
    long len) throws IOException {
      return fs.getFileBlockLocations(file, start, len);
  }

  @Override
  public @OsUntrusted Path resolvePath(@OsUntrusted FilterFileSystem this, final @OsUntrusted Path p) throws IOException {
    return fs.resolvePath(p);
  }
  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Override
  public @OsUntrusted FSDataInputStream open(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f, @OsUntrusted int bufferSize) throws IOException {
    return fs.open(f, bufferSize);
  }

  @Override
  public @OsUntrusted FSDataOutputStream append(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f, @OsUntrusted int bufferSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    return fs.append(f, bufferSize, progress);
  }

  @Override
  public void concat(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f, @OsUntrusted Path @OsUntrusted [] psrcs) throws IOException {
    fs.concat(f, psrcs);
  }

  @Override
  public @OsUntrusted FSDataOutputStream create(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission,
      @OsUntrusted
      boolean overwrite, @OsUntrusted int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    return fs.create(f, permission,
        overwrite, bufferSize, replication, blockSize, progress);
  }
  

  
  @Override
  @Deprecated
  public @OsUntrusted FSDataOutputStream createNonRecursive(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission,
      @OsUntrusted
      EnumSet<@OsUntrusted CreateFlag> flags, @OsUntrusted int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    
    return fs.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize,
        progress);
  }

  /**
   * Set replication for an existing file.
   * 
   * @param src file name
   * @param replication new replication
   * @throws IOException
   * @return true if successful;
   *         false if file does not exist or is a directory
   */
  @Override
  public @OsUntrusted boolean setReplication(@OsUntrusted FilterFileSystem this, @OsUntrusted Path src, @OsUntrusted short replication) throws IOException {
    return fs.setReplication(src, replication);
  }
  
  /**
   * Renames Path src to Path dst.  Can take place on local fs
   * or remote DFS.
   */
  @Override
  public @OsUntrusted boolean rename(@OsUntrusted FilterFileSystem this, @OsUntrusted Path src, @OsUntrusted Path dst) throws IOException {
    return fs.rename(src, dst);
  }
  
  /** Delete a file */
  @Override
  public @OsUntrusted boolean delete(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f, @OsUntrusted boolean recursive) throws IOException {
    return fs.delete(f, recursive);
  }
  
  /** List files in a directory. */
  @Override
  public @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f) throws IOException {
    return fs.listStatus(f);
  }

  @Override
  public @OsUntrusted RemoteIterator<@OsUntrusted Path> listCorruptFileBlocks(@OsUntrusted FilterFileSystem this, @OsUntrusted Path path)
    throws IOException {
    return fs.listCorruptFileBlocks(path);
  }

  /** List files and its block locations in a directory. */
  @Override
  public @OsUntrusted RemoteIterator<@OsUntrusted LocatedFileStatus> listLocatedStatus(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f)
  throws IOException {
    return fs.listLocatedStatus(f);
  }
  
  @Override
  public @OsUntrusted Path getHomeDirectory(@OsUntrusted FilterFileSystem this) {
    return fs.getHomeDirectory();
  }


  /**
   * Set the current working directory for the given file system. All relative
   * paths will be resolved relative to it.
   * 
   * @param newDir
   */
  @Override
  public void setWorkingDirectory(@OsUntrusted FilterFileSystem this, @OsUntrusted Path newDir) {
    fs.setWorkingDirectory(newDir);
  }
  
  /**
   * Get the current working directory for the given file system
   * 
   * @return the directory pathname
   */
  @Override
  public @OsUntrusted Path getWorkingDirectory(@OsUntrusted FilterFileSystem this) {
    return fs.getWorkingDirectory();
  }
  
  @Override
  protected @OsUntrusted Path getInitialWorkingDirectory(@OsUntrusted FilterFileSystem this) {
    return fs.getInitialWorkingDirectory();
  }
  
  @Override
  public @OsUntrusted FsStatus getStatus(@OsUntrusted FilterFileSystem this, @OsUntrusted Path p) throws IOException {
    return fs.getStatus(p);
  }
  
  @Override
  public @OsUntrusted boolean mkdirs(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission) throws IOException {
    return fs.mkdirs(f, permission);
  }


  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   */
  @Override
  public void copyFromLocalFile(@OsUntrusted FilterFileSystem this, @OsUntrusted boolean delSrc, @OsUntrusted Path src, @OsUntrusted Path dst)
    throws IOException {
    fs.copyFromLocalFile(delSrc, src, dst);
  }
  
  /**
   * The src files are on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   */
  @Override
  public void copyFromLocalFile(@OsUntrusted FilterFileSystem this, @OsUntrusted boolean delSrc, @OsUntrusted boolean overwrite, 
                                @OsUntrusted
                                Path @OsUntrusted [] srcs, @OsUntrusted Path dst)
    throws IOException {
    fs.copyFromLocalFile(delSrc, overwrite, srcs, dst);
  }
  
  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   */
  @Override
  public void copyFromLocalFile(@OsUntrusted FilterFileSystem this, @OsUntrusted boolean delSrc, @OsUntrusted boolean overwrite, 
                                @OsUntrusted
                                Path src, @OsUntrusted Path dst)
    throws IOException {
    fs.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * delSrc indicates if the src will be removed or not.
   */   
  @Override
  public void copyToLocalFile(@OsUntrusted FilterFileSystem this, @OsUntrusted boolean delSrc, @OsUntrusted Path src, @OsUntrusted Path dst)
    throws IOException {
    fs.copyToLocalFile(delSrc, src, dst);
  }
  
  /**
   * Returns a local File that the user can write output to.  The caller
   * provides both the eventual FS target name and the local working
   * file.  If the FS is local, we write directly into the target.  If
   * the FS is remote, we write into the tmp local area.
   */
  @Override
  public @OsUntrusted Path startLocalOutput(@OsUntrusted FilterFileSystem this, @OsUntrusted Path fsOutputFile, @OsUntrusted Path tmpLocalFile)
    throws IOException {
    return fs.startLocalOutput(fsOutputFile, tmpLocalFile);
  }

  /**
   * Called when we're all done writing to the target.  A local FS will
   * do nothing, because we've written to exactly the right place.  A remote
   * FS will copy the contents of tmpLocalFile to the correct target at
   * fsOutputFile.
   */
  @Override
  public void completeLocalOutput(@OsUntrusted FilterFileSystem this, @OsUntrusted Path fsOutputFile, @OsUntrusted Path tmpLocalFile)
    throws IOException {
    fs.completeLocalOutput(fsOutputFile, tmpLocalFile);
  }

  /** Return the total size of all files in the filesystem.*/
  @Override
  public @OsUntrusted long getUsed(@OsUntrusted FilterFileSystem this) throws IOException{
    return fs.getUsed();
  }
  
  @Override
  public @OsUntrusted long getDefaultBlockSize(@OsUntrusted FilterFileSystem this) {
    return fs.getDefaultBlockSize();
  }
  
  @Override
  public @OsUntrusted short getDefaultReplication(@OsUntrusted FilterFileSystem this) {
    return fs.getDefaultReplication();
  }

  @Override
  public @OsUntrusted FsServerDefaults getServerDefaults(@OsUntrusted FilterFileSystem this) throws IOException {
    return fs.getServerDefaults();
  }

  // path variants delegate to underlying filesystem 
  @Override
  public @OsUntrusted ContentSummary getContentSummary(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f) throws IOException {
    return fs.getContentSummary(f);
  }

  @Override
  public @OsUntrusted long getDefaultBlockSize(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f) {
    return fs.getDefaultBlockSize(f);
  }

  @Override
  public @OsUntrusted short getDefaultReplication(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f) {
    return fs.getDefaultReplication(f);
  }

  @Override
  public @OsUntrusted FsServerDefaults getServerDefaults(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f) throws IOException {
    return fs.getServerDefaults(f);
  }

  /**
   * Get file status.
   */
  @Override
  public @OsUntrusted FileStatus getFileStatus(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f) throws IOException {
    return fs.getFileStatus(f);
  }

  public void createSymlink(@OsUntrusted FilterFileSystem this, final @OsUntrusted Path target, final @OsUntrusted Path link,
      final @OsUntrusted boolean createParent) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnsupportedFileSystemException, 
      IOException {
    fs.createSymlink(target, link, createParent);
  }

  public @OsUntrusted FileStatus getFileLinkStatus(@OsUntrusted FilterFileSystem this, final @OsUntrusted Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    return fs.getFileLinkStatus(f);
  }

  public @OsUntrusted boolean supportsSymlinks(@OsUntrusted FilterFileSystem this) {
    return fs.supportsSymlinks();
  }

  public @OsUntrusted Path getLinkTarget(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f) throws IOException {
    return fs.getLinkTarget(f);
  }

  protected @OsUntrusted Path resolveLink(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f) throws IOException {
    return fs.resolveLink(f);
  }

  @Override
  public @OsUntrusted FileChecksum getFileChecksum(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f) throws IOException {
    return fs.getFileChecksum(f);
  }
  
  @Override
  public void setVerifyChecksum(@OsUntrusted FilterFileSystem this, @OsUntrusted boolean verifyChecksum) {
    fs.setVerifyChecksum(verifyChecksum);
  }
  
  @Override
  public void setWriteChecksum(@OsUntrusted FilterFileSystem this, @OsUntrusted boolean writeChecksum) {
    fs.setWriteChecksum(writeChecksum);
  }

  @Override
  public @OsUntrusted Configuration getConf(@OsUntrusted FilterFileSystem this) {
    return fs.getConf();
  }
  
  @Override
  public void close(@OsUntrusted FilterFileSystem this) throws IOException {
    super.close();
    fs.close();
  }

  @Override
  public void setOwner(@OsUntrusted FilterFileSystem this, @OsUntrusted Path p, @OsTrusted String username, @OsTrusted String groupname
      ) throws IOException {
    fs.setOwner(p, username, groupname);
  }

  @Override
  public void setTimes(@OsUntrusted FilterFileSystem this, @OsUntrusted Path p, @OsUntrusted long mtime, @OsUntrusted long atime
      ) throws IOException {
    fs.setTimes(p, mtime, atime);
  }

  @Override
  public void setPermission(@OsUntrusted FilterFileSystem this, @OsUntrusted Path p, @OsUntrusted FsPermission permission
      ) throws IOException {
    fs.setPermission(p, permission);
  }

  @Override
  protected @OsUntrusted FSDataOutputStream primitiveCreate(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f,
      @OsUntrusted
      FsPermission absolutePermission, @OsUntrusted EnumSet<@OsUntrusted CreateFlag> flag,
      @OsUntrusted
      int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize,
      @OsUntrusted
      Progressable progress, @OsUntrusted ChecksumOpt checksumOpt)
      throws IOException {
    return fs.primitiveCreate(f, absolutePermission, flag,
        bufferSize, replication, blockSize, progress, checksumOpt);
  }

  @Override
  @SuppressWarnings("deprecation")
  protected @OsUntrusted boolean primitiveMkdir(@OsUntrusted FilterFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission abdolutePermission)
      throws IOException {
    return fs.primitiveMkdir(f, abdolutePermission);
  }
  
  @Override // FileSystem
  public @OsUntrusted FileSystem @OsUntrusted [] getChildFileSystems(@OsUntrusted FilterFileSystem this) {
    return new @OsUntrusted FileSystem @OsUntrusted []{fs};
  }

  @Override // FileSystem
  public @OsUntrusted Path createSnapshot(@OsUntrusted FilterFileSystem this, @OsUntrusted Path path, @OsUntrusted String snapshotName)
      throws IOException {
    return fs.createSnapshot(path, snapshotName);
  }
  
  @Override // FileSystem
  public void renameSnapshot(@OsUntrusted FilterFileSystem this, @OsUntrusted Path path, @OsUntrusted String snapshotOldName,
      @OsUntrusted
      String snapshotNewName) throws IOException {
    fs.renameSnapshot(path, snapshotOldName, snapshotNewName);
  }
  
  @Override // FileSystem
  public void deleteSnapshot(@OsUntrusted FilterFileSystem this, @OsUntrusted Path path, @OsUntrusted String snapshotName)
      throws IOException {
    fs.deleteSnapshot(path, snapshotName);
  }
}
