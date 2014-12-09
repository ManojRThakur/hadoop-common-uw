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

package org.apache.hadoop.fs.s3;

import ostrusted.quals.OsUntrusted;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * A block-based {@link FileSystem} backed by
 * <a href="http://aws.amazon.com/s3">Amazon S3</a>.
 * </p>
 * @see NativeS3FileSystem
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class S3FileSystem extends @OsUntrusted FileSystem {

  private @OsUntrusted URI uri;

  private @OsUntrusted FileSystemStore store;

  private @OsUntrusted Path workingDir;

  public @OsUntrusted S3FileSystem() {
    // set store in initialize()
  }
  
  public @OsUntrusted S3FileSystem(@OsUntrusted FileSystemStore store) {
    this.store = store;
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>s3</code>
   */
  @Override
  public @OsUntrusted String getScheme(@OsUntrusted S3FileSystem this) {
    return "s3";
  }

  @Override
  public @OsUntrusted URI getUri(@OsUntrusted S3FileSystem this) {
    return uri;
  }

  @Override
  public void initialize(@OsUntrusted S3FileSystem this, @OsUntrusted URI uri, @OsUntrusted Configuration conf) throws IOException {
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

  private static @OsUntrusted FileSystemStore createDefaultStore(@OsUntrusted Configuration conf) {
    @OsUntrusted
    FileSystemStore store = new @OsUntrusted Jets3tFileSystemStore();
    
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
    Map<@OsUntrusted String, @OsUntrusted RetryPolicy> methodNameToPolicyMap = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted RetryPolicy>();
    methodNameToPolicyMap.put("storeBlock", methodPolicy);
    methodNameToPolicyMap.put("retrieveBlock", methodPolicy);
    
    return (@OsUntrusted FileSystemStore) RetryProxy.create(FileSystemStore.class,
                                               store, methodNameToPolicyMap);
  }

  @Override
  public @OsUntrusted Path getWorkingDirectory(@OsUntrusted S3FileSystem this) {
    return workingDir;
  }

  @Override
  public void setWorkingDirectory(@OsUntrusted S3FileSystem this, @OsUntrusted Path dir) {
    workingDir = makeAbsolute(dir);
  }

  private @OsUntrusted Path makeAbsolute(@OsUntrusted S3FileSystem this, @OsUntrusted Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new @OsUntrusted Path(workingDir, path);
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public @OsUntrusted boolean mkdirs(@OsUntrusted S3FileSystem this, @OsUntrusted Path path, @OsUntrusted FsPermission permission) throws IOException {
    @OsUntrusted
    Path absolutePath = makeAbsolute(path);
    @OsUntrusted
    List<@OsUntrusted Path> paths = new @OsUntrusted ArrayList<@OsUntrusted Path>();
    do {
      paths.add(0, absolutePath);
      absolutePath = absolutePath.getParent();
    } while (absolutePath != null);
    
    @OsUntrusted
    boolean result = true;
    for (@OsUntrusted Path p : paths) {
      result &= mkdir(p);
    }
    return result;
  }
  
  private @OsUntrusted boolean mkdir(@OsUntrusted S3FileSystem this, @OsUntrusted Path path) throws IOException {
    @OsUntrusted
    Path absolutePath = makeAbsolute(path);
    @OsUntrusted
    INode inode = store.retrieveINode(absolutePath);
    if (inode == null) {
      store.storeINode(absolutePath, INode.DIRECTORY_INODE);
    } else if (inode.isFile()) {
      throw new @OsUntrusted IOException(String.format(
          "Can't make directory for path %s since it is a file.",
          absolutePath));
    }
    return true;
  }

  @Override
  public @OsUntrusted boolean isFile(@OsUntrusted S3FileSystem this, @OsUntrusted Path path) throws IOException {
    @OsUntrusted
    INode inode = store.retrieveINode(makeAbsolute(path));
    if (inode == null) {
      return false;
    }
    return inode.isFile();
  }

  private @OsUntrusted INode checkFile(@OsUntrusted S3FileSystem this, @OsUntrusted Path path) throws IOException {
    @OsUntrusted
    INode inode = store.retrieveINode(makeAbsolute(path));
    if (inode == null) {
      throw new @OsUntrusted IOException("No such file.");
    }
    if (inode.isDirectory()) {
      throw new @OsUntrusted IOException("Path " + path + " is a directory.");
    }
    return inode;
  }

  @Override
  public @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted S3FileSystem this, @OsUntrusted Path f) throws IOException {
    @OsUntrusted
    Path absolutePath = makeAbsolute(f);
    @OsUntrusted
    INode inode = store.retrieveINode(absolutePath);
    if (inode == null) {
      throw new @OsUntrusted FileNotFoundException("File " + f + " does not exist.");
    }
    if (inode.isFile()) {
      return new @OsUntrusted FileStatus @OsUntrusted [] {
        new @OsUntrusted S3FileStatus(f.makeQualified(this), inode)
      };
    }
    @OsUntrusted
    ArrayList<@OsUntrusted FileStatus> ret = new @OsUntrusted ArrayList<@OsUntrusted FileStatus>();
    for (@OsUntrusted Path p : store.listSubPaths(absolutePath)) {
      ret.add(getFileStatus(p.makeQualified(this)));
    }
    return ret.toArray(new @OsUntrusted FileStatus @OsUntrusted [0]);
  }

  /** This optional operation is not yet supported. */
  @Override
  public @OsUntrusted FSDataOutputStream append(@OsUntrusted S3FileSystem this, @OsUntrusted Path f, @OsUntrusted int bufferSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    throw new @OsUntrusted IOException("Not supported");
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public @OsUntrusted FSDataOutputStream create(@OsUntrusted S3FileSystem this, @OsUntrusted Path file, @OsUntrusted FsPermission permission,
      @OsUntrusted
      boolean overwrite, @OsUntrusted int bufferSize,
      @OsUntrusted
      short replication, @OsUntrusted long blockSize, @OsUntrusted Progressable progress)
    throws IOException {

    @OsUntrusted
    INode inode = store.retrieveINode(makeAbsolute(file));
    if (inode != null) {
      if (overwrite) {
        delete(file, true);
      } else {
        throw new @OsUntrusted IOException("File already exists: " + file);
      }
    } else {
      @OsUntrusted
      Path parent = file.getParent();
      if (parent != null) {
        if (!mkdirs(parent)) {
          throw new @OsUntrusted IOException("Mkdirs failed to create " + parent.toString());
        }
      }      
    }
    return new @OsUntrusted FSDataOutputStream
        (new @OsUntrusted S3OutputStream(getConf(), store, makeAbsolute(file),
                            blockSize, progress, bufferSize),
         statistics);
  }

  @Override
  public @OsUntrusted FSDataInputStream open(@OsUntrusted S3FileSystem this, @OsUntrusted Path path, @OsUntrusted int bufferSize) throws IOException {
    @OsUntrusted
    INode inode = checkFile(path);
    return new @OsUntrusted FSDataInputStream(new @OsUntrusted S3InputStream(getConf(), store, inode,
                                                   statistics));
  }

  @Override
  public @OsUntrusted boolean rename(@OsUntrusted S3FileSystem this, @OsUntrusted Path src, @OsUntrusted Path dst) throws IOException {
    @OsUntrusted
    Path absoluteSrc = makeAbsolute(src);
    final @OsUntrusted String debugPreamble = "Renaming '" + src + "' to '" + dst + "' - ";
    @OsUntrusted
    INode srcINode = store.retrieveINode(absoluteSrc);
    @OsUntrusted
    boolean debugEnabled = LOG.isDebugEnabled();
    if (srcINode == null) {
      // src path doesn't exist
      if (debugEnabled) {
        LOG.debug(debugPreamble + "returning false as src does not exist");
      }
      return false; 
    }

    @OsUntrusted
    Path absoluteDst = makeAbsolute(dst);

    //validate the parent dir of the destination
    @OsUntrusted
    Path dstParent = absoluteDst.getParent();
    if (dstParent != null) {
      //if the dst parent is not root, make sure it exists
      @OsUntrusted
      INode dstParentINode = store.retrieveINode(dstParent);
      if (dstParentINode == null) {
        // dst parent doesn't exist
        if (debugEnabled) {
          LOG.debug(debugPreamble +
                    "returning false as dst parent does not exist");
        }
        return false;
      }
      if (dstParentINode.isFile()) {
        // dst parent exists but is a file
        if (debugEnabled) {
          LOG.debug(debugPreamble +
                    "returning false as dst parent exists and is a file");
        }
        return false;
      }
    }

    //get status of source
    @OsUntrusted
    boolean srcIsFile = srcINode.isFile();

    @OsUntrusted
    INode dstINode = store.retrieveINode(absoluteDst);
    @OsUntrusted
    boolean destExists = dstINode != null;
    @OsUntrusted
    boolean destIsDir = destExists && !dstINode.isFile();
    if (srcIsFile) {

      //source is a simple file
      if (destExists) {
        if (destIsDir) {
          //outcome #1 dest exists and is dir -filename to subdir of dest
          if (debugEnabled) {
            LOG.debug(debugPreamble +
                      "copying src file under dest dir to " + absoluteDst);
          }
          absoluteDst = new @OsUntrusted Path(absoluteDst, absoluteSrc.getName());
        } else {
          //outcome #2 dest it's a file: fail iff different from src
          @OsUntrusted
          boolean renamingOnToSelf = absoluteSrc.equals(absoluteDst);
          if (debugEnabled) {
            LOG.debug(debugPreamble +
                      "copying file onto file, outcome is " + renamingOnToSelf);
          }
          return renamingOnToSelf;
        }
      } else {
        // #3 dest does not exist: use dest as path for rename
        if (debugEnabled) {
          LOG.debug(debugPreamble +
                    "copying file onto file");
        }
      }
    } else {
      //here the source exists and is a directory
      // outcomes (given we know the parent dir exists if we get this far)
      // #1 destination is a file: fail
      // #2 destination is a directory: create a new dir under that one
      // #3 destination doesn't exist: create a new dir with that name
      // #3 and #4 are only allowed if the dest path is not == or under src

      if (destExists) {
        if (!destIsDir) {
          // #1 destination is a file: fail
          if (debugEnabled) {
            LOG.debug(debugPreamble +
                      "returning false as src is a directory, but not dest");
          }
          return false;
        } else {
          // the destination dir exists
          // destination for rename becomes a subdir of the target name
          absoluteDst = new @OsUntrusted Path(absoluteDst, absoluteSrc.getName());
          if (debugEnabled) {
            LOG.debug(debugPreamble +
                      "copying src dir under dest dir to " + absoluteDst);
          }
        }
      }
      //the final destination directory is now know, so validate it for
      //illegal moves

      if (absoluteSrc.equals(absoluteDst)) {
        //you can't rename a directory onto itself
        if (debugEnabled) {
          LOG.debug(debugPreamble +
                    "Dest==source && isDir -failing");
        }
        return false;
      }
      if (absoluteDst.toString().startsWith(absoluteSrc.toString() + "/")) {
        //you can't move a directory under itself
        if (debugEnabled) {
          LOG.debug(debugPreamble +
                    "dst is equal to or under src dir -failing");
        }
        return false;
      }
    }
    //here the dest path is set up -so rename
    return renameRecursive(absoluteSrc, absoluteDst);
  }

  private @OsUntrusted boolean renameRecursive(@OsUntrusted S3FileSystem this, @OsUntrusted Path src, @OsUntrusted Path dst) throws IOException {
    @OsUntrusted
    INode srcINode = store.retrieveINode(src);
    store.storeINode(dst, srcINode);
    store.deleteINode(src);
    if (srcINode.isDirectory()) {
      for (@OsUntrusted Path oldSrc : store.listDeepSubPaths(src)) {
        @OsUntrusted
        INode inode = store.retrieveINode(oldSrc);
        if (inode == null) {
          return false;
        }
        @OsUntrusted
        String oldSrcPath = oldSrc.toUri().getPath();
        @OsUntrusted
        String srcPath = src.toUri().getPath();
        @OsUntrusted
        String dstPath = dst.toUri().getPath();
        @OsUntrusted
        Path newDst = new @OsUntrusted Path(oldSrcPath.replaceFirst(srcPath, dstPath));
        store.storeINode(newDst, inode);
        store.deleteINode(oldSrc);
      }
    }
    return true;
  }

  @Override
  public @OsUntrusted boolean delete(@OsUntrusted S3FileSystem this, @OsUntrusted Path path, @OsUntrusted boolean recursive) throws IOException {
   @OsUntrusted
   Path absolutePath = makeAbsolute(path);
   @OsUntrusted
   INode inode = store.retrieveINode(absolutePath);
   if (inode == null) {
     return false;
   }
   if (inode.isFile()) {
     store.deleteINode(absolutePath);
     for (@OsUntrusted Block block: inode.getBlocks()) {
       store.deleteBlock(block);
     }
   } else {
     @OsUntrusted
     FileStatus @OsUntrusted [] contents = null; 
     try {
       contents = listStatus(absolutePath);
     } catch(@OsUntrusted FileNotFoundException fnfe) {
       return false;
     }

     if ((contents.length !=0) && (!recursive)) {
       throw new @OsUntrusted IOException("Directory " + path.toString() 
           + " is not empty.");
     }
     for (@OsUntrusted FileStatus p:contents) {
       if (!delete(p.getPath(), recursive)) {
         return false;
       }
     }
     store.deleteINode(absolutePath);
   }
   return true;
  }
  
  /**
   * FileStatus for S3 file systems. 
   */
  @Override
  public @OsUntrusted FileStatus getFileStatus(@OsUntrusted S3FileSystem this, @OsUntrusted Path f)  throws IOException {
    @OsUntrusted
    INode inode = store.retrieveINode(makeAbsolute(f));
    if (inode == null) {
      throw new @OsUntrusted FileNotFoundException(f + ": No such file or directory.");
    }
    return new @OsUntrusted S3FileStatus(f.makeQualified(this), inode);
  }
  
  @Override
  public @OsUntrusted long getDefaultBlockSize(@OsUntrusted S3FileSystem this) {
    return getConf().getLong("fs.s3.block.size", 64 * 1024 * 1024);
  }

  // diagnostic methods

  void dump(@OsUntrusted S3FileSystem this) throws IOException {
    store.dump();
  }

  void purge(@OsUntrusted S3FileSystem this) throws IOException {
    store.purge();
  }

  private static class S3FileStatus extends @OsUntrusted FileStatus {

    @OsUntrusted
    S3FileStatus(@OsUntrusted Path f, @OsUntrusted INode inode) throws IOException {
      super(findLength(inode), inode.isDirectory(), 1,
            findBlocksize(inode), 0, f);
    }

    private static @OsUntrusted long findLength(@OsUntrusted INode inode) {
      if (!inode.isDirectory()) {
        @OsUntrusted
        long length = 0L;
        for (@OsUntrusted Block block : inode.getBlocks()) {
          length += block.getLength();
        }
        return length;
      }
      return 0;
    }

    private static @OsUntrusted long findBlocksize(@OsUntrusted INode inode) {
      final @OsUntrusted Block @OsUntrusted [] ret = inode.getBlocks();
      return ret == null ? 0L : ret[0].getLength();
    }
  }
}
