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

package org.apache.hadoop.util;

import ostrusted.quals.OsUntrusted;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;

/**
 * Class that provides utility functions for checking disk problem
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DiskChecker {

  private static final @OsUntrusted long SHELL_TIMEOUT = 10 * 1000;

  public static class DiskErrorException extends @OsUntrusted IOException {
    public @OsUntrusted DiskErrorException(@OsUntrusted String msg) {
      super(msg);
    }

    public @OsUntrusted DiskErrorException(@OsUntrusted String msg, @OsUntrusted Throwable cause) {
      super(msg, cause);
    }
  }
    
  public static class DiskOutOfSpaceException extends @OsUntrusted IOException {
    public @OsUntrusted DiskOutOfSpaceException(@OsUntrusted String msg) {
      super(msg);
    }
  }
      
  /** 
   * The semantics of mkdirsWithExistsCheck method is different from the mkdirs
   * method provided in the Sun's java.io.File class in the following way:
   * While creating the non-existent parent directories, this method checks for
   * the existence of those directories if the mkdir fails at any point (since
   * that directory might have just been created by some other process).
   * If both mkdir() and the exists() check fails for any seemingly 
   * non-existent directory, then we signal an error; Sun's mkdir would signal
   * an error (return false) if a directory it is attempting to create already
   * exists or the mkdir fails.
   * @param dir
   * @return true on success, false on failure
   */
  public static @OsUntrusted boolean mkdirsWithExistsCheck(@OsUntrusted File dir) {
    if (dir.mkdir() || dir.exists()) {
      return true;
    }
    @OsUntrusted
    File canonDir = null;
    try {
      canonDir = dir.getCanonicalFile();
    } catch (@OsUntrusted IOException e) {
      return false;
    }
    @OsUntrusted
    String parent = canonDir.getParent();
    return (parent != null) && 
           (mkdirsWithExistsCheck(new @OsUntrusted File(parent)) &&
                                      (canonDir.mkdir() || canonDir.exists()));
  }
  
  /**
   * Create the directory if it doesn't exist and check that dir is readable,
   * writable and executable
   *  
   * @param dir
   * @throws DiskErrorException
   */
  public static void checkDir(@OsUntrusted File dir) throws DiskErrorException {
    if (!mkdirsWithExistsCheck(dir)) {
      throw new @OsUntrusted DiskErrorException("Can not create directory: "
                                   + dir.toString());
    }
    checkDirAccess(dir);
  }

  /**
   * Create the directory or check permissions if it already exists.
   *
   * The semantics of mkdirsWithExistsAndPermissionCheck method is different
   * from the mkdirs method provided in the Sun's java.io.File class in the
   * following way:
   * While creating the non-existent parent directories, this method checks for
   * the existence of those directories if the mkdir fails at any point (since
   * that directory might have just been created by some other process).
   * If both mkdir() and the exists() check fails for any seemingly
   * non-existent directory, then we signal an error; Sun's mkdir would signal
   * an error (return false) if a directory it is attempting to create already
   * exists or the mkdir fails.
   *
   * @param localFS local filesystem
   * @param dir directory to be created or checked
   * @param expected expected permission
   * @throws IOException
   */
  public static void mkdirsWithExistsAndPermissionCheck(
      @OsUntrusted
      LocalFileSystem localFS, @OsUntrusted Path dir, @OsUntrusted FsPermission expected)
      throws IOException {
    @OsUntrusted
    File directory = localFS.pathToFile(dir);
    @OsUntrusted
    boolean created = false;

    if (!directory.exists())
      created = mkdirsWithExistsCheck(directory);

    if (created || !localFS.getFileStatus(dir).getPermission().equals(expected))
        localFS.setPermission(dir, expected);
  }

  /**
   * Create the local directory if necessary, check permissions and also ensure
   * it can be read from and written into.
   *
   * @param localFS local filesystem
   * @param dir directory
   * @param expected permission
   * @throws DiskErrorException
   * @throws IOException
   */
  public static void checkDir(@OsUntrusted LocalFileSystem localFS, @OsUntrusted Path dir,
                              @OsUntrusted
                              FsPermission expected)
  throws DiskErrorException, IOException {
    mkdirsWithExistsAndPermissionCheck(localFS, dir, expected);
    checkDirAccess(localFS.pathToFile(dir));
  }

  /**
   * Checks that the given file is a directory and that the current running
   * process can read, write, and execute it.
   * 
   * @param dir File to check
   * @throws DiskErrorException if dir is not a directory, not readable, not
   *   writable, or not executable
   */
  private static void checkDirAccess(@OsUntrusted File dir) throws DiskErrorException {
    if (!dir.isDirectory()) {
      throw new @OsUntrusted DiskErrorException("Not a directory: "
                                   + dir.toString());
    }

    checkAccessByFileMethods(dir);
  }

  /**
   * Checks that the current running process can read, write, and execute the
   * given directory by using methods of the File object.
   * 
   * @param dir File to check
   * @throws DiskErrorException if dir is not readable, not writable, or not
   *   executable
   */
  private static void checkAccessByFileMethods(@OsUntrusted File dir)
      throws DiskErrorException {
    if (!FileUtil.canRead(dir)) {
      throw new @OsUntrusted DiskErrorException("Directory is not readable: "
                                   + dir.toString());
    }

    if (!FileUtil.canWrite(dir)) {
      throw new @OsUntrusted DiskErrorException("Directory is not writable: "
                                   + dir.toString());
    }

    if (!FileUtil.canExecute(dir)) {
      throw new @OsUntrusted DiskErrorException("Directory is not executable: "
                                   + dir.toString());
    }
  }
}
