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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;

import com.google.common.annotations.VisibleForTesting;

/** Filesystem disk space usage statistics.
 * Uses the unix 'df' program to get mount points, and java.io.File for
 * space utilization. Tested on Linux, FreeBSD, Windows. */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DF extends @OsUntrusted Shell {

  /** Default DF refresh interval. */
  public static final @OsUntrusted long DF_INTERVAL_DEFAULT = 3 * 1000;

  private final @OsTrusted String dirPath;
  private final @OsTrusted File dirFile;
  private @OsUntrusted String filesystem;
  private @OsUntrusted String mount;
  
  private @OsUntrusted ArrayList<@OsUntrusted String> output;

  public @OsUntrusted DF(@OsTrusted File path, @OsUntrusted Configuration conf) throws IOException {
    this(path, conf.getLong(CommonConfigurationKeys.FS_DF_INTERVAL_KEY, DF.DF_INTERVAL_DEFAULT));
  }

  /**
   *
   * @param path
   * @param dfInterval
   * @throws IOException
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @OsUntrusted DF(@OsTrusted File path, @OsUntrusted long dfInterval) throws IOException {
    super(dfInterval);
    this.dirPath = (@OsTrusted String) path.getCanonicalPath();
    this.dirFile = (@OsTrusted File) (new File(this.dirPath));
    this.output = new @OsUntrusted ArrayList<@OsUntrusted String>();
  }

  /// ACCESSORS

  /** @return the canonical path to the volume we're checking. */
  public @OsTrusted String getDirPath(@OsUntrusted DF this) {
    return dirPath;
  }

  /** @return a string indicating which filesystem volume we're checking. */
  public @OsUntrusted String getFilesystem(@OsUntrusted DF this) throws IOException {
    if (Shell.WINDOWS) {
      this.filesystem = dirFile.getCanonicalPath().substring(0, 2);
      return this.filesystem;
    } else {
      run();
      return filesystem;
    }
  }

  /** @return the capacity of the measured filesystem in bytes. */
  public @OsUntrusted long getCapacity(@OsUntrusted DF this) {
    return dirFile.getTotalSpace();
  }

  /** @return the total used space on the filesystem in bytes. */
  public @OsUntrusted long getUsed(@OsUntrusted DF this) {
    return dirFile.getTotalSpace() - dirFile.getFreeSpace();
  }

  /** @return the usable space remaining on the filesystem in bytes. */
  public @OsUntrusted long getAvailable(@OsUntrusted DF this) {
    return dirFile.getUsableSpace();
  }

  /** @return the amount of the volume full, as a percent. */
  public @OsUntrusted int getPercentUsed(@OsUntrusted DF this) {
    @OsUntrusted
    double cap = (@OsUntrusted double) getCapacity();
    @OsUntrusted
    double used = (cap - (@OsUntrusted double) getAvailable());
    return (@OsUntrusted int) (used * 100.0 / cap);
  }

  /** @return the filesystem mount point for the indicated volume */
  public @OsUntrusted String getMount(@OsUntrusted DF this) throws IOException {
    // Abort early if specified path does not exist
    if (!dirFile.exists()) {
      throw new @OsUntrusted FileNotFoundException("Specified path " + dirFile.getPath()
          + "does not exist");
    }

    if (Shell.WINDOWS) {
      // Assume a drive letter for a mount point
      this.mount = dirFile.getCanonicalPath().substring(0, 2);
    } else {
      run();
      // Skip parsing if df was not successful
      if (getExitCode() != 0) {
        @OsUntrusted
        StringBuffer sb = new @OsUntrusted StringBuffer("df could not be run successfully: ");
        for (@OsUntrusted String line: output) {
          sb.append(line);
        }
        throw new @OsUntrusted IOException(sb.toString());
      }
      parseOutput();
    }

    return mount;
  }
  
  @Override
  public @OsUntrusted String toString(@OsUntrusted DF this) {
    return
      "df -k " + mount +"\n" +
      filesystem + "\t" +
      getCapacity() / 1024 + "\t" +
      getUsed() / 1024 + "\t" +
      getAvailable() / 1024 + "\t" +
      getPercentUsed() + "%\t" +
      mount;
  }

  @Override
  protected @OsTrusted String @OsUntrusted [] getExecString(@OsUntrusted DF this) {
    // ignoring the error since the exit code it enough
    if (Shell.WINDOWS){
      throw new @OsUntrusted AssertionError(
          "DF.getExecString() should never be called on Windows");
    } else {
      return new @OsTrusted String @OsUntrusted [] {"bash","-c","exec 'df' '-k' '-P' '" + dirPath 
                      + "' 2>/dev/null"};
    }
  }

  @Override
  protected void parseExecResult(@OsUntrusted DF this, @OsUntrusted BufferedReader lines) throws IOException {
    output.clear();
    @OsUntrusted
    String line = lines.readLine();
    while (line != null) {
      output.add(line);
      line = lines.readLine();
    }
  }
  
  @VisibleForTesting
  protected void parseOutput(@OsUntrusted DF this) throws IOException {
    if (output.size() < 2) {
      @OsUntrusted
      StringBuffer sb = new @OsUntrusted StringBuffer("Fewer lines of output than expected");
      if (output.size() > 0) {
        sb.append(": " + output.get(0));
      }
      throw new @OsUntrusted IOException(sb.toString());
    }
    
    @OsUntrusted
    String line = output.get(1);
    @OsUntrusted
    StringTokenizer tokens =
      new @OsUntrusted StringTokenizer(line, " \t\n\r\f%");
    
    try {
      this.filesystem = tokens.nextToken();
    } catch (@OsUntrusted NoSuchElementException e) {
      throw new @OsUntrusted IOException("Unexpected empty line");
    }
    if (!tokens.hasMoreTokens()) {            // for long filesystem name
      if (output.size() > 2) {
        line = output.get(2);
      } else {
        throw new @OsUntrusted IOException("Expecting additional output after line: "
            + line);
      }
      tokens = new @OsUntrusted StringTokenizer(line, " \t\n\r\f%");
    }

    try {
      Long.parseLong(tokens.nextToken()); // capacity
      Long.parseLong(tokens.nextToken()); // used
      Long.parseLong(tokens.nextToken()); // available
      Integer.parseInt(tokens.nextToken()); // pct used
      this.mount = tokens.nextToken();
    } catch (@OsUntrusted NoSuchElementException e) {
      throw new @OsUntrusted IOException("Could not parse line: " + line);
    } catch (@OsUntrusted NumberFormatException e) {
      throw new @OsUntrusted IOException("Could not parse line: " + line);
    }
  }

  //TODO: Is this an actual error?  Should the users
  //ostrusted, depending on the thread model we call this either "needing sanitation" or trusted
  //because only a trusted user should be running this from the command line
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static void main(@OsUntrusted String @OsUntrusted [] args) throws Exception {
    @OsUntrusted
    String path = ".";
    if (args.length > 0)
      path = args[0];

    System.out.println(new @OsUntrusted DF( (@OsTrusted File) (new File(path)), DF_INTERVAL_DEFAULT).toString());
  }
}
