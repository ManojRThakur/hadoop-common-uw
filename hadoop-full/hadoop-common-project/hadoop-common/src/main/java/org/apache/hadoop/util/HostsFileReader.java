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
import java.io.*;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

// Keeps track of which datanodes/tasktrackers are allowed to connect to the 
// namenode/jobtracker.
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class HostsFileReader {
  private @OsUntrusted Set<@OsUntrusted String> includes;
  private @OsUntrusted Set<@OsUntrusted String> excludes;
  private @OsUntrusted String includesFile;
  private @OsUntrusted String excludesFile;
  
  private static final @OsUntrusted Log LOG = LogFactory.getLog(HostsFileReader.class);

  public @OsUntrusted HostsFileReader(@OsUntrusted String inFile, 
                         @OsUntrusted
                         String exFile) throws IOException {
    includes = new @OsUntrusted HashSet<@OsUntrusted String>();
    excludes = new @OsUntrusted HashSet<@OsUntrusted String>();
    includesFile = inFile;
    excludesFile = exFile;
    refresh();
  }

  public static void readFileToSet(@OsUntrusted String type,
      @OsUntrusted
      String filename, @OsUntrusted Set<@OsUntrusted String> set) throws IOException {
    @OsUntrusted
    File file = new @OsUntrusted File(filename);
    @OsUntrusted
    FileInputStream fis = new @OsUntrusted FileInputStream(file);
    @OsUntrusted
    BufferedReader reader = null;
    try {
      reader = new @OsUntrusted BufferedReader(new @OsUntrusted InputStreamReader(fis));
      @OsUntrusted
      String line;
      while ((line = reader.readLine()) != null) {
        @OsUntrusted
        String @OsUntrusted [] nodes = line.split("[ \t\n\f\r]+");
        if (nodes != null) {
          for (@OsUntrusted int i = 0; i < nodes.length; i++) {
            if (nodes[i].trim().startsWith("#")) {
              // Everything from now on is a comment
              break;
            }
            if (!nodes[i].isEmpty()) {
              LOG.info("Adding " + nodes[i] + " to the list of " + type +
                  " hosts from " + filename);
              set.add(nodes[i]);
            }
          }
        }
      }   
    } finally {
      if (reader != null) {
        reader.close();
      }
      fis.close();
    }  
  }

  public synchronized void refresh(@OsUntrusted HostsFileReader this) throws IOException {
    LOG.info("Refreshing hosts (include/exclude) list");
    if (!includesFile.isEmpty()) {
      @OsUntrusted
      Set<@OsUntrusted String> newIncludes = new @OsUntrusted HashSet<@OsUntrusted String>();
      readFileToSet("included", includesFile, newIncludes);
      // switch the new hosts that are to be included
      includes = newIncludes;
    }
    if (!excludesFile.isEmpty()) {
      @OsUntrusted
      Set<@OsUntrusted String> newExcludes = new @OsUntrusted HashSet<@OsUntrusted String>();
      readFileToSet("excluded", excludesFile, newExcludes);
      // switch the excluded hosts
      excludes = newExcludes;
    }
  }

  public synchronized @OsUntrusted Set<@OsUntrusted String> getHosts(@OsUntrusted HostsFileReader this) {
    return includes;
  }

  public synchronized @OsUntrusted Set<@OsUntrusted String> getExcludedHosts(@OsUntrusted HostsFileReader this) {
    return excludes;
  }

  public synchronized void setIncludesFile(@OsUntrusted HostsFileReader this, @OsUntrusted String includesFile) {
    LOG.info("Setting the includes file to " + includesFile);
    this.includesFile = includesFile;
  }
  
  public synchronized void setExcludesFile(@OsUntrusted HostsFileReader this, @OsUntrusted String excludesFile) {
    LOG.info("Setting the excludes file to " + excludesFile);
    this.excludesFile = excludesFile;
  }

  public synchronized void updateFileNames(@OsUntrusted HostsFileReader this, @OsUntrusted String includesFile, 
                                           @OsUntrusted
                                           String excludesFile) 
                                           throws IOException {
    setIncludesFile(includesFile);
    setExcludesFile(excludesFile);
  }
}
