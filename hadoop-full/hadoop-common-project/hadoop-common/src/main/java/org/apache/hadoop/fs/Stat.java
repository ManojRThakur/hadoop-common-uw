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
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;

import com.google.common.annotations.VisibleForTesting;

/**
 * Wrapper for the Unix stat(1) command. Used to workaround the lack of 
 * lstat(2) in Java 6.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class Stat extends @OsUntrusted Shell {

  private final @OsUntrusted Path original;
  private final @OsUntrusted Path qualified;
  private final @OsTrusted Path path;
  private final @OsUntrusted long blockSize;
  private final @OsUntrusted boolean dereference;

  private @OsUntrusted FileStatus stat;

  public @OsUntrusted Stat(@OsTrusted Path path, @OsUntrusted long blockSize, @OsUntrusted boolean deref, @OsUntrusted FileSystem fs)
      throws IOException {
    super(0L, true);
    // Original path
    this.original = path;
    // Qualify the original and strip out URI fragment via toUri().getPath()
    @OsUntrusted
    Path stripped = new @OsUntrusted Path(
        original.makeQualified(fs.getUri(), fs.getWorkingDirectory())
        .toUri().getPath());
    // Re-qualify the bare stripped path and store it
    this.qualified = 
        stripped.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    // Strip back down to a plain path
    this.path = new @OsTrusted Path(qualified.toUri().getPath());
    this.blockSize = blockSize;
    this.dereference = deref;
    // LANG = C setting
    @OsUntrusted
    Map<@OsUntrusted String, @OsUntrusted String> env = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted String>();
    env.put("LANG", "C");
    setEnvironment(env);
  }

  public @OsUntrusted FileStatus getFileStatus(@OsUntrusted Stat this) throws IOException {
    run();
    return stat;
  }

  /**
   * Whether Stat is supported on the current platform
   * @return
   */
  public static @OsUntrusted boolean isAvailable() {
    if (Shell.LINUX || Shell.FREEBSD || Shell.MAC) {
      return true;
    }
    return false;
  }

  @VisibleForTesting
  @OsUntrusted
  FileStatus getFileStatusForTesting(@OsUntrusted Stat this) {
    return stat;
  }

  @Override
  @SuppressWarnings("ostrusted") // path.ToString is trusted because path is trusted.
  protected @OsTrusted String @OsUntrusted [] getExecString(@OsUntrusted Stat this) {
    @OsUntrusted
    String derefFlag = "-";
    if (dereference) {
      derefFlag = "-L";
    }
    if (Shell.LINUX) {
      return new @OsTrusted String @OsUntrusted [] {
          "stat", derefFlag + "c", "%s,%F,%Y,%X,%a,%U,%G,%N", path.toString() };
    } else if (Shell.FREEBSD || Shell.MAC) {
      return new @OsTrusted String @OsUntrusted [] {
          "stat", derefFlag + "f", "%z,%HT,%m,%a,%Op,%Su,%Sg,`link' -> `%Y'",
          path.toString() };
    } else {
      throw new @OsUntrusted UnsupportedOperationException(
          "stat is not supported on this platform");
    }
  }

  @Override
  protected void parseExecResult(@OsUntrusted Stat this, @OsUntrusted BufferedReader lines) throws IOException {
    // Reset stat
    stat = null;

    @OsUntrusted
    String line = lines.readLine();
    if (line == null) {
      throw new @OsUntrusted IOException("Unable to stat path: " + original);
    }
    if (line.endsWith("No such file or directory") ||
        line.endsWith("Not a directory")) {
      throw new @OsUntrusted FileNotFoundException("File " + original + " does not exist");
    }
    if (line.endsWith("Too many levels of symbolic links")) {
      throw new @OsUntrusted IOException("Possible cyclic loop while following symbolic" +
          " link " + original);
    }
    // 6,symbolic link,6,1373584236,1373584236,lrwxrwxrwx,andrew,andrew,`link' -> `target'
    @OsUntrusted
    StringTokenizer tokens = new @OsUntrusted StringTokenizer(line, ",");
    try {
      @OsUntrusted
      long length = Long.parseLong(tokens.nextToken());
      @OsUntrusted
      boolean isDir = tokens.nextToken().equalsIgnoreCase("directory") ? true
          : false;
      // Convert from seconds to milliseconds
      @OsUntrusted
      long modTime = Long.parseLong(tokens.nextToken())*1000;
      @OsUntrusted
      long accessTime = Long.parseLong(tokens.nextToken())*1000;
      @OsUntrusted
      String octalPerms = tokens.nextToken();
      // FreeBSD has extra digits beyond 4, truncate them
      if (octalPerms.length() > 4) {
        @OsUntrusted
        int len = octalPerms.length();
        octalPerms = octalPerms.substring(len-4, len);
      }
      @OsUntrusted
      FsPermission perms = new @OsUntrusted FsPermission(Short.parseShort(octalPerms, 8));
      @SuppressWarnings("ostrusted:cast.unsafe")
      @OsTrusted
      String owner = (@OsTrusted String) tokens.nextToken(); // Assume that this is the result of a shell exec, which is trusted.
      @SuppressWarnings("ostrusted:cast.unsafe")
      @OsTrusted
      String group = (@OsTrusted String) tokens.nextToken();
      @OsUntrusted
      String symStr = tokens.nextToken();
      // 'notalink'
      // 'link' -> `target'
      // '' -> ''
      @OsUntrusted
      Path symlink = null;
      @OsUntrusted
      StringTokenizer symTokens = new @OsUntrusted StringTokenizer(symStr, "`");
      symTokens.nextToken();
      try {
        @OsUntrusted
        String target = symTokens.nextToken();
        target = target.substring(0, target.length()-1);
        if (!target.isEmpty()) {
          symlink = new @OsUntrusted Path(target);
        }
      } catch (@OsUntrusted NoSuchElementException e) {
        // null if not a symlink
      }
      // Set stat
      stat = new @OsUntrusted FileStatus(length, isDir, 1, blockSize, modTime, accessTime,
          perms, owner, group, symlink, qualified);
    } catch (@OsUntrusted NumberFormatException e) {
      throw new @OsUntrusted IOException("Unexpected stat output: " + line, e);
    } catch (@OsUntrusted NoSuchElementException e) {
      throw new @OsUntrusted IOException("Unexpected stat output: " + line, e);
    }
  }
}
