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
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Random;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/****************************************************************
 * Implement the FileSystem API for the checksumed local filesystem.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LocalFileSystem extends @OsUntrusted ChecksumFileSystem {
  static final @OsUntrusted URI NAME = URI.create("file:///");
  static private @OsUntrusted Random rand = new @OsUntrusted Random();
  
  public @OsUntrusted LocalFileSystem() {
    this(new @OsUntrusted RawLocalFileSystem());
  }
  
  @Override
  public void initialize(@OsUntrusted LocalFileSystem this, @OsUntrusted URI name, @OsUntrusted Configuration conf) throws IOException {
    if (fs.getConf() == null) {
      fs.initialize(name, conf);
    }
    @OsUntrusted
    String scheme = name.getScheme();
    if (!scheme.equals(fs.getUri().getScheme())) {
      swapScheme = scheme;
    }
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>file</code>
   */
  @Override
  public @OsUntrusted String getScheme(@OsUntrusted LocalFileSystem this) {
    return "file";
  }

  public @OsUntrusted FileSystem getRaw(@OsUntrusted LocalFileSystem this) {
    return getRawFileSystem();
  }
    
  public @OsUntrusted LocalFileSystem(@OsUntrusted FileSystem rawLocalFileSystem) {
    super(rawLocalFileSystem);
  }
    
  /** Convert a path to a File. */
  public @OsUntrusted File pathToFile(@OsUntrusted LocalFileSystem this, @OsUntrusted Path path) {
    return ((@OsUntrusted RawLocalFileSystem)fs).pathToFile(path);
  }

  @Override
  public void copyFromLocalFile(@OsUntrusted LocalFileSystem this, @OsUntrusted boolean delSrc, @OsUntrusted Path src, @OsUntrusted Path dst)
    throws IOException {
    FileUtil.copy(this, src, this, dst, delSrc, getConf());
  }

  @Override
  public void copyToLocalFile(@OsUntrusted LocalFileSystem this, @OsUntrusted boolean delSrc, @OsUntrusted Path src, @OsUntrusted Path dst)
    throws IOException {
    FileUtil.copy(this, src, this, dst, delSrc, getConf());
  }

  /**
   * Moves files to a bad file directory on the same device, so that their
   * storage will not be reused.
   */
  @Override
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @OsUntrusted boolean reportChecksumFailure(@OsUntrusted LocalFileSystem this, @OsUntrusted Path p, @OsUntrusted FSDataInputStream in,
                                       @OsUntrusted
                                       long inPos,
                                       @OsUntrusted
                                       FSDataInputStream sums, @OsUntrusted long sumsPos) {
    try {
      // canonicalize f
      @OsTrusted File f = (@OsTrusted File) ((@OsUntrusted RawLocalFileSystem)fs).pathToFile(p).getCanonicalFile();

      //ostrusted check file path first to ensure safety, therefore we can have an @OsUntrusted Path p argument
      checkPath(p);
      // find highest writable parent dir of f on the same device
      @OsUntrusted
      String device = new @OsUntrusted DF(f, getConf()).getMount();
      @OsUntrusted
      File parent = f.getParentFile();
      @OsUntrusted
      File dir = null;
      while (parent != null && FileUtil.canWrite(parent) &&
          parent.toString().startsWith(device)) {
        dir = parent;
        parent = parent.getParentFile();
      }

      if (dir==null) {
        throw new @OsUntrusted IOException(
                              "not able to find the highest writable parent dir");
      }
        
      // move the file there
      @OsUntrusted
      File badDir = new @OsUntrusted File(dir, "bad_files");
      if (!badDir.mkdirs()) {
        if (!badDir.isDirectory()) {
          throw new @OsUntrusted IOException("Mkdirs failed to create " + badDir.toString());
        }
      }
      @OsUntrusted
      String suffix = "." + rand.nextInt();
      @OsUntrusted
      File badFile = new @OsUntrusted File(badDir, f.getName()+suffix);
      LOG.warn("Moving bad file " + f + " to " + badFile);
      in.close();                               // close it first
      @OsUntrusted
      boolean b = f.renameTo(badFile);                      // rename it
      if (!b) {
        LOG.warn("Ignoring failure of renameTo");
      }
      // move checksum file too
      @OsUntrusted
      File checkFile = ((@OsUntrusted RawLocalFileSystem)fs).pathToFile(getChecksumFile(p));
      // close the stream before rename to release the file handle
      sums.close();
      b = checkFile.renameTo(new @OsUntrusted File(badDir, checkFile.getName()+suffix));
      if (!b) {
          LOG.warn("Ignoring failure of renameTo");
        }
    } catch (@OsUntrusted IOException e) {
      //ostrusted, avoid exception in QualifierHierarchy
      //LOG.warn("Error moving bad file " + p + ": " + e);
    }
    return false;
  }

  @Override
  public @OsUntrusted boolean supportsSymlinks(@OsUntrusted LocalFileSystem this) {
    return true;
  }

  @Override
  public void createSymlink(@OsUntrusted LocalFileSystem this, @OsUntrusted Path target, @OsUntrusted Path link, @OsUntrusted boolean createParent)
      throws IOException {
    fs.createSymlink(target, link, createParent);
  }

  @Override
  public @OsUntrusted FileStatus getFileLinkStatus(@OsUntrusted LocalFileSystem this, final @OsUntrusted Path f) throws IOException {
    return fs.getFileLinkStatus(f);
  }

  @Override
  public @OsUntrusted Path getLinkTarget(@OsUntrusted LocalFileSystem this, @OsUntrusted Path f) throws IOException {
    return fs.getLinkTarget(f);
  }
}
