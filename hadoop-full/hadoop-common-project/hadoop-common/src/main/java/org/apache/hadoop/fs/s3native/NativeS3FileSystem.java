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

package org.apache.hadoop.fs.s3native;

import ostrusted.quals.OsUntrusted;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3.S3Exception;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * A {@link FileSystem} for reading and writing files stored on
 * <a href="http://aws.amazon.com/s3">Amazon S3</a>.
 * Unlike {@link org.apache.hadoop.fs.s3.S3FileSystem} this implementation
 * stores files on S3 in their
 * native form so they can be read by other S3 tools.
 *
 * A note about directories. S3 of course has no "native" support for them.
 * The idiom we choose then is: for any directory created by this class,
 * we use an empty object "#{dirpath}_$folder$" as a marker.
 * Further, to interoperate with other S3 tools, we also accept the following:
 *  - an object "#{dirpath}/' denoting a directory marker
 *  - if there exists any objects with the prefix "#{dirpath}/", then the
 *    directory is said to exist
 *  - if both a file with the name of a directory and a marker for that
 *    directory exists, then the *file masks the directory*, and the directory
 *    is never returned.
 * </p>
 * @see org.apache.hadoop.fs.s3.S3FileSystem
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class NativeS3FileSystem extends @OsUntrusted FileSystem {
  
  public static final @OsUntrusted Log LOG = 
    LogFactory.getLog(NativeS3FileSystem.class);
  
  private static final @OsUntrusted String FOLDER_SUFFIX = "_$folder$";
  static final @OsUntrusted String PATH_DELIMITER = Path.SEPARATOR;
  private static final @OsUntrusted int S3_MAX_LISTING_LENGTH = 1000;
  
  static class NativeS3FsInputStream extends @OsUntrusted FSInputStream {
    
    private @OsUntrusted NativeFileSystemStore store;
    private @OsUntrusted Statistics statistics;
    private @OsUntrusted InputStream in;
    private final @OsUntrusted String key;
    private @OsUntrusted long pos = 0;
    
    public @OsUntrusted NativeS3FsInputStream(@OsUntrusted NativeFileSystemStore store, @OsUntrusted Statistics statistics, @OsUntrusted InputStream in, @OsUntrusted String key) {
      this.store = store;
      this.statistics = statistics;
      this.in = in;
      this.key = key;
    }
    
    @Override
    public synchronized @OsUntrusted int read(NativeS3FileSystem.@OsUntrusted NativeS3FsInputStream this) throws IOException {
      @OsUntrusted
      int result = -1;
      try {
        result = in.read();
      } catch (@OsUntrusted IOException e) {
        LOG.info("Received IOException while reading '" + key + "', attempting to reopen.");
        seek(pos);
        result = in.read();
      } 
      if (result != -1) {
        pos++;
      }
      if (statistics != null && result != -1) {
        statistics.incrementBytesRead(1);
      }
      return result;
    }
    @Override
    public synchronized @OsUntrusted int read(NativeS3FileSystem.@OsUntrusted NativeS3FsInputStream this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len)
      throws IOException {
      
      @OsUntrusted
      int result = -1;
      try {
        result = in.read(b, off, len);
      } catch (@OsUntrusted IOException e) {
        LOG.info("Received IOException while reading '" + key + "', attempting to reopen.");
        seek(pos);
        result = in.read(b, off, len);
      }
      if (result > 0) {
        pos += result;
      }
      if (statistics != null && result > 0) {
        statistics.incrementBytesRead(result);
      }
      return result;
    }

    @Override
    public void close(NativeS3FileSystem.@OsUntrusted NativeS3FsInputStream this) throws IOException {
      in.close();
    }

    @Override
    public synchronized void seek(NativeS3FileSystem.@OsUntrusted NativeS3FsInputStream this, @OsUntrusted long pos) throws IOException {
      in.close();
      LOG.info("Opening key '" + key + "' for reading at position '" + pos + "'");
      in = store.retrieve(key, pos);
      this.pos = pos;
    }
    @Override
    public synchronized @OsUntrusted long getPos(NativeS3FileSystem.@OsUntrusted NativeS3FsInputStream this) throws IOException {
      return pos;
    }
    @Override
    public @OsUntrusted boolean seekToNewSource(NativeS3FileSystem.@OsUntrusted NativeS3FsInputStream this, @OsUntrusted long targetPos) throws IOException {
      return false;
    }
  }
  
  private class NativeS3FsOutputStream extends @OsUntrusted OutputStream {
    
    private @OsUntrusted Configuration conf;
    private @OsUntrusted String key;
    private @OsUntrusted File backupFile;
    private @OsUntrusted OutputStream backupStream;
    private @OsUntrusted MessageDigest digest;
    private @OsUntrusted boolean closed;
    
    public @OsUntrusted NativeS3FsOutputStream(@OsUntrusted Configuration conf,
        @OsUntrusted
        NativeFileSystemStore store, @OsUntrusted String key, @OsUntrusted Progressable progress,
        @OsUntrusted
        int bufferSize) throws IOException {
      this.conf = conf;
      this.key = key;
      this.backupFile = newBackupFile();
      LOG.info("OutputStream for key '" + key + "' writing to tempfile '" + this.backupFile + "'");
      try {
        this.digest = MessageDigest.getInstance("MD5");
        this.backupStream = new @OsUntrusted BufferedOutputStream(new @OsUntrusted DigestOutputStream(
            new @OsUntrusted FileOutputStream(backupFile), this.digest));
      } catch (@OsUntrusted NoSuchAlgorithmException e) {
        LOG.warn("Cannot load MD5 digest algorithm," +
            "skipping message integrity check.", e);
        this.backupStream = new @OsUntrusted BufferedOutputStream(
            new @OsUntrusted FileOutputStream(backupFile));
      }
    }

    private @OsUntrusted File newBackupFile(@OsUntrusted NativeS3FileSystem.NativeS3FsOutputStream this) throws IOException {
      @OsUntrusted
      File dir = new @OsUntrusted File(conf.get("fs.s3.buffer.dir"));
      if (!dir.mkdirs() && !dir.exists()) {
        throw new @OsUntrusted IOException("Cannot create S3 buffer directory: " + dir);
      }
      @OsUntrusted
      File result = File.createTempFile("output-", ".tmp", dir);
      result.deleteOnExit();
      return result;
    }
    
    @Override
    public void flush(@OsUntrusted NativeS3FileSystem.NativeS3FsOutputStream this) throws IOException {
      backupStream.flush();
    }
    
    @Override
    public synchronized void close(@OsUntrusted NativeS3FileSystem.NativeS3FsOutputStream this) throws IOException {
      if (closed) {
        return;
      }

      backupStream.close();
      LOG.info("OutputStream for key '" + key + "' closed. Now beginning upload");
      
      try {
        @OsUntrusted
        byte @OsUntrusted [] md5Hash = digest == null ? null : digest.digest();
        store.storeFile(key, backupFile, md5Hash);
      } finally {
        if (!backupFile.delete()) {
          LOG.warn("Could not delete temporary s3n file: " + backupFile);
        }
        super.close();
        closed = true;
      } 
      LOG.info("OutputStream for key '" + key + "' upload complete");
    }

    @Override
    public void write(@OsUntrusted NativeS3FileSystem.NativeS3FsOutputStream this, @OsUntrusted int b) throws IOException {
      backupStream.write(b);
    }

    @Override
    public void write(@OsUntrusted NativeS3FileSystem.NativeS3FsOutputStream this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) throws IOException {
      backupStream.write(b, off, len);
    }
  }
  
  private @OsUntrusted URI uri;
  private @OsUntrusted NativeFileSystemStore store;
  private @OsUntrusted Path workingDir;
  
  public @OsUntrusted NativeS3FileSystem() {
    // set store in initialize()
  }
  
  public @OsUntrusted NativeS3FileSystem(@OsUntrusted NativeFileSystemStore store) {
    this.store = store;
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>s3n</code>
   */
  @Override
  public @OsUntrusted String getScheme(@OsUntrusted NativeS3FileSystem this) {
    return "s3n";
  }

  @Override
  public void initialize(@OsUntrusted NativeS3FileSystem this, @OsUntrusted URI uri, @OsUntrusted Configuration conf) throws IOException {
    super.initialize(uri, conf);
    if (store == null) {
      store = createDefaultStore(conf);
    }
    store.initialize(uri, conf);
    setConf(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.workingDir =
      new @OsUntrusted Path("/user", System.getProperty("user.name")).makeQualified(this);
  }
  
  private static @OsUntrusted NativeFileSystemStore createDefaultStore(@OsUntrusted Configuration conf) {
    @OsUntrusted
    NativeFileSystemStore store = new @OsUntrusted Jets3tNativeFileSystemStore();
    
    @OsUntrusted
    RetryPolicy basePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        conf.getInt("fs.s3.maxRetries", 4),
        conf.getLong("fs.s3.sleepTimeSeconds", 10), TimeUnit.SECONDS);
    @OsUntrusted
    Map<@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Exception>, @OsUntrusted RetryPolicy> exceptionToPolicyMap =
      new @OsUntrusted HashMap<@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Exception>, @OsUntrusted RetryPolicy>();
    exceptionToPolicyMap.put(IOException.class, basePolicy);
    exceptionToPolicyMap.put(S3Exception.class, basePolicy);
    
    @OsUntrusted
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    @OsUntrusted
    Map<@OsUntrusted String, @OsUntrusted RetryPolicy> methodNameToPolicyMap =
      new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted RetryPolicy>();
    methodNameToPolicyMap.put("storeFile", methodPolicy);
    methodNameToPolicyMap.put("rename", methodPolicy);
    
    return (@OsUntrusted NativeFileSystemStore)
      RetryProxy.create(NativeFileSystemStore.class, store,
          methodNameToPolicyMap);
  }
  
  private static @OsUntrusted String pathToKey(@OsUntrusted Path path) {
    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      // allow uris without trailing slash after bucket to refer to root,
      // like s3n://mybucket
      return "";
    }
    if (!path.isAbsolute()) {
      throw new @OsUntrusted IllegalArgumentException("Path must be absolute: " + path);
    }
    @OsUntrusted
    String ret = path.toUri().getPath().substring(1); // remove initial slash
    if (ret.endsWith("/") && (ret.indexOf("/") != ret.length() - 1)) {
      ret = ret.substring(0, ret.length() -1);
  }
    return ret;
  }
  
  private static @OsUntrusted Path keyToPath(@OsUntrusted String key) {
    return new @OsUntrusted Path("/" + key);
  }
  
  private @OsUntrusted Path makeAbsolute(@OsUntrusted NativeS3FileSystem this, @OsUntrusted Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new @OsUntrusted Path(workingDir, path);
  }

  /** This optional operation is not yet supported. */
  @Override
  public @OsUntrusted FSDataOutputStream append(@OsUntrusted NativeS3FileSystem this, @OsUntrusted Path f, @OsUntrusted int bufferSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    throw new @OsUntrusted IOException("Not supported");
  }
  
  @Override
  public @OsUntrusted FSDataOutputStream create(@OsUntrusted NativeS3FileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission,
      @OsUntrusted
      boolean overwrite, @OsUntrusted int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize,
      @OsUntrusted
      Progressable progress) throws IOException {

    if (exists(f) && !overwrite) {
      throw new @OsUntrusted IOException("File already exists:"+f);
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("Creating new file '" + f + "' in S3");
    }
    @OsUntrusted
    Path absolutePath = makeAbsolute(f);
    @OsUntrusted
    String key = pathToKey(absolutePath);
    return new @OsUntrusted FSDataOutputStream(new @OsUntrusted NativeS3FsOutputStream(getConf(), store,
        key, progress, bufferSize), statistics);
  }
  
  @Override
  public @OsUntrusted boolean delete(@OsUntrusted NativeS3FileSystem this, @OsUntrusted Path f, @OsUntrusted boolean recurse) throws IOException {
    @OsUntrusted
    FileStatus status;
    try {
      status = getFileStatus(f);
    } catch (@OsUntrusted FileNotFoundException e) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Delete called for '" + f +
            "' but file does not exist, so returning false");
      }
      return false;
    }
    @OsUntrusted
    Path absolutePath = makeAbsolute(f);
    @OsUntrusted
    String key = pathToKey(absolutePath);
    if (status.isDirectory()) {
      if (!recurse && listStatus(f).length > 0) {
        throw new @OsUntrusted IOException("Can not delete " + f + " at is a not empty directory and recurse option is false");
      }

      createParent(f);

      if(LOG.isDebugEnabled()) {
        LOG.debug("Deleting directory '" + f  + "'");
      }
      @OsUntrusted
      String priorLastKey = null;
      do {
        @OsUntrusted
        PartialListing listing = store.list(key, S3_MAX_LISTING_LENGTH, priorLastKey, true);
        for (@OsUntrusted FileMetadata file : listing.getFiles()) {
          store.delete(file.getKey());
        }
        priorLastKey = listing.getPriorLastKey();
      } while (priorLastKey != null);

      try {
        store.delete(key + FOLDER_SUFFIX);
      } catch (@OsUntrusted FileNotFoundException e) {
        //this is fine, we don't require a marker
      }
    } else {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Deleting file '" + f + "'");
      }
      createParent(f);
      store.delete(key);
    }
    return true;
  }

  @Override
  public @OsUntrusted FileStatus getFileStatus(@OsUntrusted NativeS3FileSystem this, @OsUntrusted Path f) throws IOException {
    @OsUntrusted
    Path absolutePath = makeAbsolute(f);
    @OsUntrusted
    String key = pathToKey(absolutePath);
    
    if (key.length() == 0) { // root always exists
      return newDirectory(absolutePath);
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("getFileStatus retrieving metadata for key '" + key + "'");
    }
    @OsUntrusted
    FileMetadata meta = store.retrieveMetadata(key);
    if (meta != null) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("getFileStatus returning 'file' for key '" + key + "'");
      }
      return newFile(meta, absolutePath);
    }
    if (store.retrieveMetadata(key + FOLDER_SUFFIX) != null) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("getFileStatus returning 'directory' for key '" + key +
            "' as '" + key + FOLDER_SUFFIX + "' exists");
      }
      return newDirectory(absolutePath);
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("getFileStatus listing key '" + key + "'");
    }
    @OsUntrusted
    PartialListing listing = store.list(key, 1);
    if (listing.getFiles().length > 0 ||
        listing.getCommonPrefixes().length > 0) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("getFileStatus returning 'directory' for key '" + key +
            "' as it has contents");
      }
      return newDirectory(absolutePath);
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("getFileStatus could not find key '" + key + "'");
    }
    throw new @OsUntrusted FileNotFoundException("No such file or directory '" + absolutePath + "'");
  }

  @Override
  public @OsUntrusted URI getUri(@OsUntrusted NativeS3FileSystem this) {
    return uri;
  }

  /**
   * <p>
   * If <code>f</code> is a file, this method will make a single call to S3.
   * If <code>f</code> is a directory, this method will make a maximum of
   * (<i>n</i> / 1000) + 2 calls to S3, where <i>n</i> is the total number of
   * files and directories contained directly in <code>f</code>.
   * </p>
   */
  @Override
  public @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted NativeS3FileSystem this, @OsUntrusted Path f) throws IOException {

    @OsUntrusted
    Path absolutePath = makeAbsolute(f);
    @OsUntrusted
    String key = pathToKey(absolutePath);
    
    if (key.length() > 0) {
      @OsUntrusted
      FileMetadata meta = store.retrieveMetadata(key);
      if (meta != null) {
        return new @OsUntrusted FileStatus @OsUntrusted [] { newFile(meta, absolutePath) };
      }
    }
    
    @OsUntrusted
    URI pathUri = absolutePath.toUri();
    @OsUntrusted
    Set<@OsUntrusted FileStatus> status = new @OsUntrusted TreeSet<@OsUntrusted FileStatus>();
    @OsUntrusted
    String priorLastKey = null;
    do {
      @OsUntrusted
      PartialListing listing = store.list(key, S3_MAX_LISTING_LENGTH, priorLastKey, false);
      for (@OsUntrusted FileMetadata fileMetadata : listing.getFiles()) {
        @OsUntrusted
        Path subpath = keyToPath(fileMetadata.getKey());
        @OsUntrusted
        String relativePath = pathUri.relativize(subpath.toUri()).getPath();

        if (fileMetadata.getKey().equals(key + "/")) {
          // this is just the directory we have been asked to list
        }
        else if (relativePath.endsWith(FOLDER_SUFFIX)) {
          status.add(newDirectory(new @OsUntrusted Path(
              absolutePath,
              relativePath.substring(0, relativePath.indexOf(FOLDER_SUFFIX)))));
        }
        else {
          status.add(newFile(fileMetadata, subpath));
        }
      }
      for (@OsUntrusted String commonPrefix : listing.getCommonPrefixes()) {
        @OsUntrusted
        Path subpath = keyToPath(commonPrefix);
        @OsUntrusted
        String relativePath = pathUri.relativize(subpath.toUri()).getPath();
        status.add(newDirectory(new @OsUntrusted Path(absolutePath, relativePath)));
      }
      priorLastKey = listing.getPriorLastKey();
    } while (priorLastKey != null);
    
    if (status.isEmpty() &&
        key.length() > 0 &&
        store.retrieveMetadata(key + FOLDER_SUFFIX) == null) {
      throw new @OsUntrusted FileNotFoundException("File " + f + " does not exist.");
    }
    
    return status.toArray(new @OsUntrusted FileStatus @OsUntrusted [status.size()]);
  }
  
  private @OsUntrusted FileStatus newFile(@OsUntrusted NativeS3FileSystem this, @OsUntrusted FileMetadata meta, @OsUntrusted Path path) {
    return new @OsUntrusted FileStatus(meta.getLength(), false, 1, getDefaultBlockSize(),
        meta.getLastModified(), path.makeQualified(this));
  }
  
  private @OsUntrusted FileStatus newDirectory(@OsUntrusted NativeS3FileSystem this, @OsUntrusted Path path) {
    return new @OsUntrusted FileStatus(0, true, 1, 0, 0, path.makeQualified(this));
  }

  @Override
  public @OsUntrusted boolean mkdirs(@OsUntrusted NativeS3FileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission) throws IOException {
    @OsUntrusted
    Path absolutePath = makeAbsolute(f);
    @OsUntrusted
    List<@OsUntrusted Path> paths = new @OsUntrusted ArrayList<@OsUntrusted Path>();
    do {
      paths.add(0, absolutePath);
      absolutePath = absolutePath.getParent();
    } while (absolutePath != null);
    
    @OsUntrusted
    boolean result = true;
    for (@OsUntrusted Path path : paths) {
      result &= mkdir(path);
    }
    return result;
  }
  
  private @OsUntrusted boolean mkdir(@OsUntrusted NativeS3FileSystem this, @OsUntrusted Path f) throws IOException {
    try {
      @OsUntrusted
      FileStatus fileStatus = getFileStatus(f);
      if (fileStatus.isFile()) {
        throw new @OsUntrusted IOException(String.format(
            "Can't make directory for path '%s' since it is a file.", f));

      }
    } catch (@OsUntrusted FileNotFoundException e) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Making dir '" + f + "' in S3");
      }
      @OsUntrusted
      String key = pathToKey(f) + FOLDER_SUFFIX;
      store.storeEmptyFile(key);    
    }
    return true;
  }

  @Override
  public @OsUntrusted FSDataInputStream open(@OsUntrusted NativeS3FileSystem this, @OsUntrusted Path f, @OsUntrusted int bufferSize) throws IOException {
    @OsUntrusted
    FileStatus fs = getFileStatus(f); // will throw if the file doesn't exist
    if (fs.isDirectory()) {
      throw new @OsUntrusted IOException("'" + f + "' is a directory");
    }
    LOG.info("Opening '" + f + "' for reading");
    @OsUntrusted
    Path absolutePath = makeAbsolute(f);
    @OsUntrusted
    String key = pathToKey(absolutePath);
    return new @OsUntrusted FSDataInputStream(new @OsUntrusted BufferedFSInputStream(
        new @OsUntrusted NativeS3FsInputStream(store, statistics, store.retrieve(key), key), bufferSize));
  }
  
  // rename() and delete() use this method to ensure that the parent directory
  // of the source does not vanish.
  private void createParent(@OsUntrusted NativeS3FileSystem this, @OsUntrusted Path path) throws IOException {
    @OsUntrusted
    Path parent = path.getParent();
    if (parent != null) {
      @OsUntrusted
      String key = pathToKey(makeAbsolute(parent));
      if (key.length() > 0) {
          store.storeEmptyFile(key + FOLDER_SUFFIX);
      }
    }
  }
  
    
  @Override
  public @OsUntrusted boolean rename(@OsUntrusted NativeS3FileSystem this, @OsUntrusted Path src, @OsUntrusted Path dst) throws IOException {

    @OsUntrusted
    String srcKey = pathToKey(makeAbsolute(src));
    final @OsUntrusted String debugPreamble = "Renaming '" + src + "' to '" + dst + "' - ";

    if (srcKey.length() == 0) {
      // Cannot rename root of file system
      if (LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble +
                  "returning false as cannot rename the root of a filesystem");
      }
      return false;
    }

    //get status of source
    @OsUntrusted
    boolean srcIsFile;
    try {
      srcIsFile = getFileStatus(src).isFile();
    } catch (@OsUntrusted FileNotFoundException e) {
      //bail out fast if the source does not exist
      if (LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble + "returning false as src does not exist");
      }
      return false;
    }
    // Figure out the final destination
    @OsUntrusted
    String dstKey = pathToKey(makeAbsolute(dst));

    try {
      @OsUntrusted
      boolean dstIsFile = getFileStatus(dst).isFile();
      if (dstIsFile) {
        //destination is a file.
        //you can't copy a file or a directory onto an existing file
        //except for the special case of dest==src, which is a no-op
        if(LOG.isDebugEnabled()) {
          LOG.debug(debugPreamble +
              "returning without rename as dst is an already existing file");
        }
        //exit, returning true iff the rename is onto self
        return srcKey.equals(dstKey);
      } else {
        //destination exists and is a directory
        if(LOG.isDebugEnabled()) {
          LOG.debug(debugPreamble + "using dst as output directory");
        }
        //destination goes under the dst path, with the name of the
        //source entry
        dstKey = pathToKey(makeAbsolute(new @OsUntrusted Path(dst, src.getName())));
      }
    } catch (@OsUntrusted FileNotFoundException e) {
      //destination does not exist => the source file or directory
      //is copied over with the name of the destination
      if(LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble + "using dst as output destination");
      }
      try {
        if (getFileStatus(dst.getParent()).isFile()) {
          if(LOG.isDebugEnabled()) {
            LOG.debug(debugPreamble +
                "returning false as dst parent exists and is a file");
          }
          return false;
        }
      } catch (@OsUntrusted FileNotFoundException ex) {
        if(LOG.isDebugEnabled()) {
          LOG.debug(debugPreamble +
              "returning false as dst parent does not exist");
        }
        return false;
      }
    }

    //rename to self behavior follows Posix rules and is different
    //for directories and files -the return code is driven by src type
    if (srcKey.equals(dstKey)) {
      //fully resolved destination key matches source: fail
      if (LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble + "renamingToSelf; returning true");
      }
      return true;
    }
    if (srcIsFile) {
      //source is a file; COPY then DELETE
      if(LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble +
            "src is file, so doing copy then delete in S3");
      }
      store.copy(srcKey, dstKey);
      store.delete(srcKey);
    } else {
      //src is a directory
      if(LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble + "src is directory, so copying contents");
      }
      //Verify dest is not a child of the parent
      if (dstKey.startsWith(srcKey + "/")) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            debugPreamble + "cannot rename a directory to a subdirectory of self");
        }
        return false;
      }
      //create the subdir under the destination
      store.storeEmptyFile(dstKey + FOLDER_SUFFIX);

      @OsUntrusted
      List<@OsUntrusted String> keysToDelete = new @OsUntrusted ArrayList<@OsUntrusted String>();
      @OsUntrusted
      String priorLastKey = null;
      do {
        @OsUntrusted
        PartialListing listing = store.list(srcKey, S3_MAX_LISTING_LENGTH, priorLastKey, true);
        for (@OsUntrusted FileMetadata file : listing.getFiles()) {
          keysToDelete.add(file.getKey());
          store.copy(file.getKey(), dstKey + file.getKey().substring(srcKey.length()));
        }
        priorLastKey = listing.getPriorLastKey();
      } while (priorLastKey != null);

      if(LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble +
            "all files in src copied, now removing src files");
      }
      for (@OsUntrusted String key: keysToDelete) {
        store.delete(key);
      }

      try {
        store.delete(srcKey + FOLDER_SUFFIX);
      } catch (@OsUntrusted FileNotFoundException e) {
        //this is fine, we don't require a marker
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug(debugPreamble + "done");
      }
    }

    return true;
  }
  
  @Override
  public @OsUntrusted long getDefaultBlockSize(@OsUntrusted NativeS3FileSystem this) {
    return getConf().getLong("fs.s3n.block.size", 64 * 1024 * 1024);
  }

  /**
   * Set the working directory to the given directory.
   */
  @Override
  public void setWorkingDirectory(@OsUntrusted NativeS3FileSystem this, @OsUntrusted Path newDir) {
    workingDir = newDir;
  }
  
  @Override
  public @OsUntrusted Path getWorkingDirectory(@OsUntrusted NativeS3FileSystem this) {
    return workingDir;
  }
}
