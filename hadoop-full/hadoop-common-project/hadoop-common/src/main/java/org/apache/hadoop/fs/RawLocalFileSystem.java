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
import ostrusted.quals.PolyOsTrusted;
import ostrusted.quals.OsTrusted;
import java.io.BufferedOutputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileDescriptor;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

/****************************************************************
 * Implement the FileSystem API for the raw local filesystem.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RawLocalFileSystem extends @OsUntrusted FileSystem {
  static final @OsUntrusted URI NAME = URI.create("file:///");
  private @OsUntrusted Path workingDir;
  private static final @OsUntrusted boolean useDeprecatedFileStatus = !Stat.isAvailable();
  
  public @OsUntrusted RawLocalFileSystem() {
    workingDir = getInitialWorkingDirectory();
  }
  
  private @OsUntrusted Path makeAbsolute(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path f) {
    if (f.isAbsolute()) {
      return f;
    } else {
      return new @OsUntrusted Path(workingDir, f);
    }
  }
  
  /** Convert a path to a File. */
  @SuppressWarnings("ostrusted")// BUZZZSAWWW!
  public @OsTrusted File pathToFile(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path path) {
    checkPath(path);
    if (!path.isAbsolute()) {
      path = new @OsUntrusted Path(getWorkingDirectory(), path);
    }
    return new @OsTrusted File(path.toUri().getPath());
  }

  @Override
  public @OsUntrusted URI getUri(@OsUntrusted RawLocalFileSystem this) { return NAME; }
  
  @Override
  public void initialize(@OsUntrusted RawLocalFileSystem this, @OsUntrusted URI uri, @OsUntrusted Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);
  }
  
  class TrackingFileInputStream extends @OsUntrusted FileInputStream {
    public @OsUntrusted TrackingFileInputStream(@OsUntrusted File f) throws IOException {
      super(f);
    }
    
    @Override
    public @OsUntrusted int read(@OsUntrusted RawLocalFileSystem.TrackingFileInputStream this) throws IOException {
      @OsUntrusted
      int result = super.read();
      if (result != -1) {
        statistics.incrementBytesRead(1);
      }
      return result;
    }
    
    @Override
    public @OsUntrusted int read(@OsUntrusted RawLocalFileSystem.TrackingFileInputStream this, @OsUntrusted byte @OsUntrusted [] data) throws IOException {
      @OsUntrusted
      int result = super.read(data);
      if (result != -1) {
        statistics.incrementBytesRead(result);
      }
      return result;
    }
    
    @Override
    public @OsUntrusted int read(@OsUntrusted RawLocalFileSystem.TrackingFileInputStream this, @OsUntrusted byte @OsUntrusted [] data, @OsUntrusted int offset, @OsUntrusted int length) throws IOException {
      @OsUntrusted
      int result = super.read(data, offset, length);
      if (result != -1) {
        statistics.incrementBytesRead(result);
      }
      return result;
    }
  }

  /*******************************************************
   * For open()'s FSInputStream.
   *******************************************************/
  class LocalFSFileInputStream extends @OsUntrusted FSInputStream implements @OsUntrusted HasFileDescriptor {
    private @OsUntrusted FileInputStream fis;
    private @OsUntrusted long position;

    public @OsUntrusted LocalFSFileInputStream(@OsUntrusted Path f) throws IOException {
      this.fis = new @OsUntrusted TrackingFileInputStream(pathToFile(f));
    }
    
    @Override
    public void seek(@OsUntrusted RawLocalFileSystem.LocalFSFileInputStream this, @OsUntrusted long pos) throws IOException {
      fis.getChannel().position(pos);
      this.position = pos;
    }
    
    @Override
    public @OsUntrusted long getPos(@OsUntrusted RawLocalFileSystem.LocalFSFileInputStream this) throws IOException {
      return this.position;
    }
    
    @Override
    public @OsUntrusted boolean seekToNewSource(@OsUntrusted RawLocalFileSystem.LocalFSFileInputStream this, @OsUntrusted long targetPos) throws IOException {
      return false;
    }
    
    /*
     * Just forward to the fis
     */
    @Override
    public @OsUntrusted int available(@OsUntrusted RawLocalFileSystem.LocalFSFileInputStream this) throws IOException { return fis.available(); }
    @Override
    public void close(@OsUntrusted RawLocalFileSystem.LocalFSFileInputStream this) throws IOException { fis.close(); }
    @Override
    public @OsUntrusted boolean markSupported(@OsUntrusted RawLocalFileSystem.LocalFSFileInputStream this) { return false; }
    
    @Override
    public @OsUntrusted int read(@OsUntrusted RawLocalFileSystem.LocalFSFileInputStream this) throws IOException {
      try {
        @OsUntrusted
        int value = fis.read();
        if (value >= 0) {
          this.position++;
        }
        return value;
      } catch (@OsUntrusted IOException e) {                 // unexpected exception
        throw new @OsUntrusted FSError(e);                   // assume native fs error
      }
    }
    
    @Override
    public @OsUntrusted int read(@OsUntrusted RawLocalFileSystem.LocalFSFileInputStream this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) throws IOException {
      try {
        @OsUntrusted
        int value = fis.read(b, off, len);
        if (value > 0) {
          this.position += value;
        }
        return value;
      } catch (@OsUntrusted IOException e) {                 // unexpected exception
        throw new @OsUntrusted FSError(e);                   // assume native fs error
      }
    }
    
    @Override
    public @OsUntrusted int read(@OsUntrusted RawLocalFileSystem.LocalFSFileInputStream this, @OsUntrusted long position, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len)
      throws IOException {
      @OsUntrusted
      ByteBuffer bb = ByteBuffer.wrap(b, off, len);
      try {
        return fis.getChannel().read(bb, position);
      } catch (@OsUntrusted IOException e) {
        throw new @OsUntrusted FSError(e);
      }
    }
    
    @Override
    public @OsUntrusted long skip(@OsUntrusted RawLocalFileSystem.LocalFSFileInputStream this, @OsUntrusted long n) throws IOException {
      @OsUntrusted
      long value = fis.skip(n);
      if (value > 0) {
        this.position += value;
      }
      return value;
    }

    @Override
    public @OsUntrusted FileDescriptor getFileDescriptor(@OsUntrusted RawLocalFileSystem.LocalFSFileInputStream this) throws IOException {
      return fis.getFD();
    }
  }
  
  @Override
  public @OsUntrusted FSDataInputStream open(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path f, @OsUntrusted int bufferSize) throws IOException {
    if (!exists(f)) {
      throw new @OsUntrusted FileNotFoundException(f.toString());
    }
    return new @OsUntrusted FSDataInputStream(new @OsUntrusted BufferedFSInputStream(
        new @OsUntrusted LocalFSFileInputStream(f), bufferSize));
  }
  
  /*********************************************************
   * For create()'s FSOutputStream.
   *********************************************************/
  class LocalFSFileOutputStream extends @OsUntrusted OutputStream {
    private @OsUntrusted FileOutputStream fos;
    
    private @OsUntrusted LocalFSFileOutputStream(@OsUntrusted Path f, @OsUntrusted boolean append) throws IOException {
      this.fos = new @OsUntrusted FileOutputStream(pathToFile(f), append);
    }
    
    /*
     * Just forward to the fos
     */
    @Override
    public void close(@OsUntrusted RawLocalFileSystem.LocalFSFileOutputStream this) throws IOException { fos.close(); }
    @Override
    public void flush(@OsUntrusted RawLocalFileSystem.LocalFSFileOutputStream this) throws IOException { fos.flush(); }
    @Override
    public void write(@OsUntrusted RawLocalFileSystem.LocalFSFileOutputStream this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) throws IOException {
      try {
        fos.write(b, off, len);
      } catch (@OsUntrusted IOException e) {                // unexpected exception
        throw new @OsUntrusted FSError(e);                  // assume native fs error
      }
    }
    
    @Override
    public void write(@OsUntrusted RawLocalFileSystem.LocalFSFileOutputStream this, @OsUntrusted int b) throws IOException {
      try {
        fos.write(b);
      } catch (@OsUntrusted IOException e) {              // unexpected exception
        throw new @OsUntrusted FSError(e);                // assume native fs error
      }
    }
  }

  @Override
  public @OsUntrusted FSDataOutputStream append(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path f, @OsUntrusted int bufferSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    if (!exists(f)) {
      throw new @OsUntrusted FileNotFoundException("File " + f + " not found");
    }
    if (getFileStatus(f).isDirectory()) {
      throw new @OsUntrusted IOException("Cannot append to a diretory (=" + f + " )");
    }
    return new @OsUntrusted FSDataOutputStream(new @OsUntrusted BufferedOutputStream(
        new @OsUntrusted LocalFSFileOutputStream(f, true), bufferSize), statistics);
  }

  @Override
  public @OsUntrusted FSDataOutputStream create(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path f, @OsUntrusted boolean overwrite, @OsUntrusted int bufferSize,
    @OsUntrusted
    short replication, @OsUntrusted long blockSize, @OsUntrusted Progressable progress)
    throws IOException {
    return create(f, overwrite, true, bufferSize, replication, blockSize, progress);
  }

  private @OsUntrusted FSDataOutputStream create(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path f, @OsUntrusted boolean overwrite,
      @OsUntrusted
      boolean createParent, @OsUntrusted int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    if (exists(f) && !overwrite) {
      throw new @OsUntrusted IOException("File already exists: "+f);
    }
    @OsUntrusted
    Path parent = f.getParent();
    if (parent != null && !mkdirs(parent)) {
      throw new @OsUntrusted IOException("Mkdirs failed to create " + parent.toString());
    }
    return new @OsUntrusted FSDataOutputStream(new @OsUntrusted BufferedOutputStream(
        new @OsUntrusted LocalFSFileOutputStream(f, false), bufferSize), statistics);
  }
  
  @Override
  @Deprecated
  public @OsUntrusted FSDataOutputStream createNonRecursive(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission,
      @OsUntrusted
      EnumSet<@OsUntrusted CreateFlag> flags, @OsUntrusted int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    if (exists(f) && !flags.contains(CreateFlag.OVERWRITE)) {
      throw new @OsUntrusted IOException("File already exists: "+f);
    }
    return new @OsUntrusted FSDataOutputStream(new @OsUntrusted BufferedOutputStream(
        new @OsUntrusted LocalFSFileOutputStream(f, false), bufferSize), statistics);
  }

  @Override
  public @OsUntrusted FSDataOutputStream create(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission,
    @OsUntrusted
    boolean overwrite, @OsUntrusted int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize,
    @OsUntrusted
    Progressable progress) throws IOException {

    @OsUntrusted
    FSDataOutputStream out = create(f,
        overwrite, bufferSize, replication, blockSize, progress);
    setPermission(f, permission);
    return out;
  }

  @Override
  public @OsUntrusted FSDataOutputStream createNonRecursive(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission,
      @OsUntrusted
      boolean overwrite,
      @OsUntrusted
      int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    @OsUntrusted
    FSDataOutputStream out = create(f,
        overwrite, false, bufferSize, replication, blockSize, progress);
    setPermission(f, permission);
    return out;
  }

  @Override
  public @OsUntrusted boolean rename(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path src, @OsUntrusted Path dst) throws IOException {
    // Attempt rename using Java API.
    @OsUntrusted
    File srcFile = pathToFile(src);
    @OsUntrusted
    File dstFile = pathToFile(dst);
    if (srcFile.renameTo(dstFile)) {
      return true;
    }

    // Enforce POSIX rename behavior that a source directory replaces an existing
    // destination if the destination is an empty directory.  On most platforms,
    // this is already handled by the Java API call above.  Some platforms
    // (notably Windows) do not provide this behavior, so the Java API call above
    // fails.  Delete destination and attempt rename again.
    if (this.exists(dst)) {
      @OsUntrusted
      FileStatus sdst = this.getFileStatus(dst);
      if (sdst.isDirectory() && dstFile.list().length == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Deleting empty destination and renaming " + src + " to " +
            dst);
        }
        if (this.delete(dst, false) && srcFile.renameTo(dstFile)) {
          return true;
        }
      }
    }

    // The fallback behavior accomplishes the rename by a full copy.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Falling through to a copy of " + src + " to " + dst);
    }
    return FileUtil.copy(this, src, this, dst, true, getConf());
  }
  
  /**
   * Delete the given path to a file or directory.
   * @param p the path to delete
   * @param recursive to delete sub-directories
   * @return true if the file or directory and all its contents were deleted
   * @throws IOException if p is non-empty and recursive is false 
   */
  @Override
  public @OsUntrusted boolean delete(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path p, @OsUntrusted boolean recursive) throws IOException {
    @OsUntrusted
    File f = pathToFile(p);
    if (f.isFile()) {
      return f.delete();
    } else if (!recursive && f.isDirectory() && 
        (FileUtil.listFiles(f).length != 0)) {
      throw new @OsUntrusted IOException("Directory " + f.toString() + " is not empty");
    }
    return FileUtil.fullyDelete(f);
  }
 
  /**
   * {@inheritDoc}
   *
   * (<b>Note</b>: Returned list is not sorted in any given order,
   * due to reliance on Java's {@link File#list()} API.)
   */
  @Override
  public @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path f) throws IOException {
    @OsUntrusted
    File localf = pathToFile(f);
    @OsUntrusted
    FileStatus @OsUntrusted [] results;

    if (!localf.exists()) {
      throw new @OsUntrusted FileNotFoundException("File " + f + " does not exist");
    }
    if (localf.isFile()) {
      if (!useDeprecatedFileStatus) {
        return new @OsUntrusted FileStatus @OsUntrusted [] { getFileStatus(f) };
      }
      return new @OsUntrusted FileStatus @OsUntrusted [] {
        new @OsUntrusted DeprecatedRawLocalFileStatus(localf, getDefaultBlockSize(f), this)};
    }

    @OsUntrusted
    String @OsUntrusted [] names = localf.list();
    if (names == null) {
      return null;
    }
    results = new @OsUntrusted FileStatus @OsUntrusted [names.length];
    @OsUntrusted
    int j = 0;
    for (@OsUntrusted int i = 0; i < names.length; i++) {
      try {
        // Assemble the path using the Path 3 arg constructor to make sure
        // paths with colon are properly resolved on Linux
        results[j] = getFileStatus(new @OsUntrusted Path(f, new @OsUntrusted Path(null, null, names[i])));
        j++;
      } catch (@OsUntrusted FileNotFoundException e) {
        // ignore the files not found since the dir list may have have changed
        // since the names[] list was generated.
      }
    }
    if (j == names.length) {
      return results;
    }
    return Arrays.copyOf(results, j);
  }

  /**
   * Creates the specified directory hierarchy. Does not
   * treat existence as an error.
   */
  @Override
  public @OsUntrusted boolean mkdirs(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path f) throws IOException {
    if(f == null) {
      throw new @OsUntrusted IllegalArgumentException("mkdirs path arg is null");
    }
    @OsUntrusted
    Path parent = f.getParent();
    @OsUntrusted
    File p2f = pathToFile(f);
    if(parent != null) {
      @OsUntrusted
      File parent2f = pathToFile(parent);
      if(parent2f != null && parent2f.exists() && !parent2f.isDirectory()) {
        throw new @OsUntrusted FileAlreadyExistsException("Parent path is not a directory: " 
            + parent);
      }
    }
    return (parent == null || mkdirs(parent)) &&
      (p2f.mkdir() || p2f.isDirectory());
  }

  @Override
  public @OsUntrusted boolean mkdirs(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission) throws IOException {
    @OsUntrusted
    boolean b = mkdirs(f);
    if(b) {
      setPermission(f, permission);
    }
    return b;
  }
  

  @Override
  protected @OsUntrusted boolean primitiveMkdir(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission absolutePermission)
    throws IOException {
    @OsUntrusted
    boolean b = mkdirs(f);
    setPermission(f, absolutePermission);
    return b;
  }
  
  
  @Override
  public @OsUntrusted Path getHomeDirectory(@OsUntrusted RawLocalFileSystem this) {
    return this.makeQualified(new @OsUntrusted Path(System.getProperty("user.home")));
  }

  /**
   * Set the working directory to the given directory.
   */
  @Override
  public void setWorkingDirectory(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path newDir) {
    workingDir = makeAbsolute(newDir);
    checkPath(workingDir);
  }
  
  @Override
  public @OsUntrusted Path getWorkingDirectory(@OsUntrusted RawLocalFileSystem this) {
    return workingDir;
  }
  
  @Override
  protected @OsUntrusted Path getInitialWorkingDirectory(@OsUntrusted RawLocalFileSystem this) {
    return this.makeQualified(new @OsUntrusted Path(System.getProperty("user.dir")));
  }

  @Override
  public @OsUntrusted FsStatus getStatus(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path p) throws IOException {
    @OsUntrusted
    File partition = pathToFile(p == null ? new @OsUntrusted Path("/") : p);
    //File provides getUsableSpace() and getFreeSpace()
    //File provides no API to obtain used space, assume used = total - free
    return new @OsUntrusted FsStatus(partition.getTotalSpace(), 
      partition.getTotalSpace() - partition.getFreeSpace(),
      partition.getFreeSpace());
  }
  
  // In the case of the local filesystem, we can just rename the file.
  @Override
  public void moveFromLocalFile(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path src, @OsUntrusted Path dst) throws IOException {
    rename(src, dst);
  }
  
  // We can write output directly to the final location
  @Override
  public @OsUntrusted Path startLocalOutput(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path fsOutputFile, @OsUntrusted Path tmpLocalFile)
    throws IOException {
    return fsOutputFile;
  }
  
  // It's in the right place - nothing to do.
  @Override
  public void completeLocalOutput(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path fsWorkingFile, @OsUntrusted Path tmpLocalFile)
    throws IOException {
  }
  
  @Override
  public void close(@OsUntrusted RawLocalFileSystem this) throws IOException {
    super.close();
  }
  
  @Override
  public @OsUntrusted String toString(@OsUntrusted RawLocalFileSystem this) {
    return "LocalFS";
  }
  
  @Override
  public @OsUntrusted FileStatus getFileStatus(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path f) throws IOException {
    return getFileLinkStatusInternal(f, true);
  }

  @Deprecated
  private @OsUntrusted FileStatus deprecatedGetFileStatus(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path f) throws IOException {
    @OsUntrusted
    File path = pathToFile(f);
    if (path.exists()) {
      return new @OsUntrusted DeprecatedRawLocalFileStatus(pathToFile(f),
          getDefaultBlockSize(f), this);
    } else {
      throw new @OsUntrusted FileNotFoundException("File " + f + " does not exist");
    }
  }

  @Deprecated
  static class DeprecatedRawLocalFileStatus extends @OsUntrusted FileStatus {
    /* We can add extra fields here. It breaks at least CopyFiles.FilePair().
     * We recognize if the information is already loaded by check if
     * onwer.equals("").
     */
    private @OsUntrusted boolean isPermissionLoaded(RawLocalFileSystem.@OsUntrusted DeprecatedRawLocalFileStatus this) {
      return !super.getOwner().isEmpty(); 
    }
    
    @OsUntrusted
    DeprecatedRawLocalFileStatus(@OsUntrusted File f, @OsUntrusted long defaultBlockSize, @OsUntrusted FileSystem fs) {
      super(f.length(), f.isDirectory(), 1, defaultBlockSize,
          f.lastModified(), new @OsUntrusted Path(f.getPath()).makeQualified(fs.getUri(),
            fs.getWorkingDirectory()));
    }
    
    @Override
    public @OsUntrusted FsPermission getPermission(RawLocalFileSystem.@OsUntrusted DeprecatedRawLocalFileStatus this) {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      return super.getPermission();
    }

    @Override
    public @OsTrusted String getOwner(RawLocalFileSystem.@OsUntrusted DeprecatedRawLocalFileStatus this) {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      return super.getOwner();
    }

    @Override
    public @OsTrusted String getGroup(RawLocalFileSystem.@OsUntrusted DeprecatedRawLocalFileStatus this) {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      return super.getGroup();
    }

    /// loads permissions, owner, and group from `ls -ld`
    private void loadPermissionInfo(RawLocalFileSystem.@OsUntrusted DeprecatedRawLocalFileStatus this) {
      @OsUntrusted
      IOException e = null;
      try {
        @SuppressWarnings("ostrusted") // BUZZZSAW # 2
        @OsUntrusted
        String output = FileUtil.execCommand(new @OsTrusted File(getPath().toString()), Shell.getGetPermissionCommand());
        @OsUntrusted
        StringTokenizer t =
            new @OsUntrusted StringTokenizer(output, Shell.TOKEN_SEPARATOR_REGEX);
        //expected format
        //-rw-------    1 username groupname ...
        @OsUntrusted
        String permission = t.nextToken();
        if (permission.length() > 10) { //files with ACLs might have a '+'
          permission = permission.substring(0, 10);
        }
        setPermission(FsPermission.valueOf(permission));
        t.nextToken();

        @SuppressWarnings("ostrusted:cast.unsafe")
        @OsTrusted
        String owner = (@OsTrusted String) t.nextToken();
        // If on windows domain, token format is DOMAIN\\user and we want to
        // extract only the user name
        if (Shell.WINDOWS) {
          @OsUntrusted
          int i = owner.indexOf('\\');
          if (i != -1) {
            @SuppressWarnings("ostrusted:cast.unsafe")
            @OsTrusted
            String ownerTmp = (@OsTrusted String) t.nextToken();

            owner = ownerTmp;
          }
        }
        setOwner(owner);

        @SuppressWarnings("ostrusted:cast.unsafe")
        @OsTrusted
        String grp = (@OsTrusted String) t.nextToken();
        setGroup(grp);
      } catch (Shell.@OsUntrusted ExitCodeException ioe) {
        if (ioe.getExitCode() != 1) {
          e = ioe;
        } else {
          setPermission(null);
          setOwner(null);
          setGroup(null);
        }
      } catch (@OsUntrusted IOException ioe) {
        e = ioe;
      } finally {
        if (e != null) {
          throw new @OsUntrusted RuntimeException("Error while running command to get " +
                                     "file permissions : " + 
                                     StringUtils.stringifyException(e));
        }
      }
    }

    @Override
    public void write(RawLocalFileSystem.@OsUntrusted DeprecatedRawLocalFileStatus this, @OsUntrusted DataOutput out) throws IOException {
      if (!isPermissionLoaded()) {
        loadPermissionInfo();
      }
      super.write(out);
    }
  }

  /**
   * Use the command chown to set owner.
   */
  @Override
  public void setOwner(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path p, @OsTrusted String username, @OsTrusted String groupname)
    throws IOException {
    FileUtil.setOwner(pathToFile(p), username, groupname);
  }

  /**
   * Use the command chmod to set permission.
   */
  @Override
  public void setPermission(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path p, @OsUntrusted FsPermission permission)
    throws IOException {
    if (NativeIO.isAvailable()) {
      NativeIO.POSIX.chmod(pathToFile(p).getCanonicalPath(),
                     permission.toShort());
    } else {
      @SuppressWarnings("ostrusted:cast.unsafe")
      @OsTrusted
      String perm = (@OsTrusted String) String.format("%04o", permission.toShort());
      Shell.execCommand(Shell.getSetPermissionCommand(perm, false,
        FileUtil.makeShellPath(pathToFile(p), true)));
    }
  }
 
  /**
   * Sets the {@link Path}'s last modified time <em>only</em> to the given
   * valid time.
   *
   * @param mtime the modification time to set (only if greater than zero).
   * @param atime currently ignored.
   * @throws IOException if setting the last modified time fails.
   */
  @Override
  public void setTimes(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path p, @OsUntrusted long mtime, @OsUntrusted long atime) throws IOException {
    @OsUntrusted
    File f = pathToFile(p);
    if(mtime >= 0) {
      if(!f.setLastModified(mtime)) {
        throw new @OsUntrusted IOException(
          "couldn't set last-modified time to " +
          mtime +
          " for " +
          f.getAbsolutePath());
      }
    }
  }

  @Override
  public @OsUntrusted boolean supportsSymlinks(@OsUntrusted RawLocalFileSystem this) {
    return true;
  }

  @Override
  public void createSymlink(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path target, @OsUntrusted Path link, @OsUntrusted boolean createParent)
      throws IOException {
    final @OsUntrusted String targetScheme = target.toUri().getScheme();
    if (targetScheme != null && !"file".equals(targetScheme)) {
      throw new @OsUntrusted IOException("Unable to create symlink to non-local file "+
                            "system: "+target.toString());
    }
    if (createParent) {
      mkdirs(link.getParent());
    }
    @SuppressWarnings("ostrusted:cast.unsafe") // Buzzsaw 3
    @OsTrusted
    String targetStr = (@OsTrusted String) target.toString();

    @SuppressWarnings("ostrusted:cast.unsafe") // Buzzsaw 4
    @OsTrusted
    String linkStr = (@OsTrusted String) makeAbsolute(link).toString();

    // NB: Use createSymbolicLink in java.nio.file.Path once available
    @OsUntrusted
    int result = FileUtil.symLink(targetStr, linkStr);
    if (result != 0) {
      throw new @OsUntrusted IOException("Error " + result + " creating symlink " +
          link + " to " + target);
    }
  }

  /**
   * Return a FileStatus representing the given path. If the path refers
   * to a symlink return a FileStatus representing the link rather than
   * the object the link refers to.
   */
  @Override
  public @OsUntrusted FileStatus getFileLinkStatus(@OsUntrusted RawLocalFileSystem this, final @OsUntrusted Path f) throws IOException {
    @OsUntrusted
    FileStatus fi = getFileLinkStatusInternal(f, false);
    // getFileLinkStatus is supposed to return a symlink with a
    // qualified path
    if (fi.isSymlink()) {
      @OsUntrusted
      Path targetQual = FSLinkResolver.qualifySymlinkTarget(this.getUri(),
          fi.getPath(), fi.getSymlink());
      fi.setSymlink(targetQual);
    }
    return fi;
  }

  /**
   * Public {@link FileStatus} methods delegate to this function, which in turn
   * either call the new {@link Stat} based implementation or the deprecated
   * methods based on platform support.
   * 
   * @param f Path to stat
   * @param dereference whether to dereference the final path component if a
   *          symlink
   * @return FileStatus of f
   * @throws IOException
   */
  private @OsUntrusted FileStatus getFileLinkStatusInternal(@OsUntrusted RawLocalFileSystem this, final @OsUntrusted Path f,
      @OsUntrusted
      boolean dereference) throws IOException {
    if (!useDeprecatedFileStatus) {
      return getNativeFileLinkStatus(f, dereference);
    } else if (dereference) {
      return deprecatedGetFileStatus(f);
    } else {
      return deprecatedGetFileLinkStatusInternal(f);
    }
  }

  /**
   * Deprecated. Remains for legacy support. Should be removed when {@link Stat}
   * gains support for Windows and other operating systems.
   */
  @Deprecated
  private @OsUntrusted FileStatus deprecatedGetFileLinkStatusInternal(@OsUntrusted RawLocalFileSystem this, final @OsUntrusted Path f)
      throws IOException {
    @OsUntrusted
    String target = FileUtil.readLink(pathToFile(f));

    try {
      @OsUntrusted
      FileStatus fs = getFileStatus(f);
      // If f refers to a regular file or directory
      if (target.isEmpty()) {
        return fs;
      }
      // Otherwise f refers to a symlink
      return new @OsUntrusted FileStatus(fs.getLen(),
          false,
          fs.getReplication(),
          fs.getBlockSize(),
          fs.getModificationTime(),
          fs.getAccessTime(),
          fs.getPermission(),
          fs.getOwner(),
          fs.getGroup(),
          new @OsUntrusted Path(target),
          f);
    } catch (@OsUntrusted FileNotFoundException e) {
      /* The exists method in the File class returns false for dangling
       * links so we can get a FileNotFoundException for links that exist.
       * It's also possible that we raced with a delete of the link. Use
       * the readBasicFileAttributes method in java.nio.file.attributes
       * when available.
       */
      if (!target.isEmpty()) {
        return new @OsUntrusted FileStatus(0, false, 0, 0, 0, 0, FsPermission.getDefault(),
            "", "", new @OsUntrusted Path(target), f);
      }
      // f refers to a file or directory that does not exist
      throw e;
    }
  }
  /**
   * Calls out to platform's native stat(1) implementation to get file metadata
   * (permissions, user, group, atime, mtime, etc). This works around the lack
   * of lstat(2) in Java 6.
   * 
   *  Currently, the {@link Stat} class used to do this only supports Linux
   *  and FreeBSD, so the old {@link #deprecatedGetFileLinkStatusInternal(Path)}
   *  implementation (deprecated) remains further OS support is added.
   *
   * @param f File to stat
   * @param dereference whether to dereference symlinks
   * @return FileStatus of f
   * @throws IOException
   */
  private @OsUntrusted FileStatus getNativeFileLinkStatus(@OsUntrusted RawLocalFileSystem this, final @OsUntrusted Path f,
      @OsUntrusted
      boolean dereference) throws IOException {
    checkPath(f);
    @SuppressWarnings("ostrusted:cast.unsafe")
    @OsTrusted Path p = (@OsTrusted Path) f; // Buzzsaw. need to validate before going to f.
    @OsUntrusted
    Stat stat = new @OsUntrusted Stat(p, getDefaultBlockSize(f), dereference, this);
    @OsUntrusted
    FileStatus status = stat.getFileStatus();
    return status;
  }

  @Override
  public @OsUntrusted Path getLinkTarget(@OsUntrusted RawLocalFileSystem this, @OsUntrusted Path f) throws IOException {
    @OsUntrusted
    FileStatus fi = getFileLinkStatusInternal(f, false);
    // return an unqualified symlink target
    return fi.getSymlink();
  }
}
