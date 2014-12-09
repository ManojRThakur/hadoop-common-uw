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
import java.io.File;
import org.apache.hadoop.util.Shell;

public class DUHelper {

  private @OsUntrusted int folderCount=0;
  private @OsUntrusted int fileCount=0;
  private @OsUntrusted double usage = 0;
  private @OsUntrusted long folderSize = -1;

  private @OsUntrusted DUHelper() {

  }

  public static @OsUntrusted long getFolderUsage(@OsUntrusted String folder) {
    return new @OsUntrusted DUHelper().calculateFolderSize(folder);
  }

  private @OsUntrusted long calculateFolderSize(@OsUntrusted DUHelper this, @OsUntrusted String folder) {
    if (folder == null)
      throw new @OsUntrusted IllegalArgumentException("folder");
    @OsUntrusted
    File f = new @OsUntrusted File(folder);
    return folderSize = getFileSize(f);
  }

  public @OsUntrusted String check(@OsUntrusted DUHelper this, @OsUntrusted String folder) {
    if (folder == null)
      throw new @OsUntrusted IllegalArgumentException("folder");
    @OsUntrusted
    File f = new @OsUntrusted File(folder);

    folderSize = getFileSize(f);
    usage = 1.0*(f.getTotalSpace() - f.getFreeSpace())/ f.getTotalSpace();
    return String.format("used %d files %d disk in use %f", folderSize, fileCount, usage);
  }

  public @OsUntrusted long getFileCount(@OsUntrusted DUHelper this) {
    return fileCount;
  }

  public @OsUntrusted double getUsage(@OsUntrusted DUHelper this) {
    return usage;
  }

  private @OsUntrusted long getFileSize(@OsUntrusted DUHelper this, @OsUntrusted File folder) {

    folderCount++;
    //Counting the total folders
    @OsUntrusted
    long foldersize = 0;
    if (folder.isFile())
      return folder.length();
    @OsUntrusted
    File @OsUntrusted [] filelist = folder.listFiles();
    if (filelist == null) {
      return 0;
    }
    for (@OsUntrusted int i = 0; i < filelist.length; i++) {
      if (filelist[i].isDirectory()) {
        foldersize += getFileSize(filelist[i]);
      } else {
        fileCount++; //Counting the total files
        foldersize += filelist[i].length();
      }
    }
    return foldersize;    
  }

  public static void main(@OsUntrusted String @OsUntrusted [] args) {
    if (Shell.WINDOWS)
      System.out.println("Windows: "+ DUHelper.getFolderUsage(args[0]));
    else
      System.out.println("Other: " + DUHelper.getFolderUsage(args[0]));
  }
}