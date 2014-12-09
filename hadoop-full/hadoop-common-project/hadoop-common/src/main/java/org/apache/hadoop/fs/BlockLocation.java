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
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Represents the network location of a block, information about the hosts
 * that contain block replicas, and other block metadata (E.g. the file
 * offset associated with the block, length, whether it is corrupt, etc).
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BlockLocation {
  private @OsUntrusted String @OsUntrusted [] hosts; // Datanode hostnames
  private @OsUntrusted String @OsUntrusted [] names; // Datanode IP:xferPort for accessing the block
  private @OsUntrusted String @OsUntrusted [] topologyPaths; // Full path name in network topology
  private @OsUntrusted long offset;  // Offset of the block in the file
  private @OsUntrusted long length;
  private @OsUntrusted boolean corrupt;

  /**
   * Default Constructor
   */
  public @OsUntrusted BlockLocation() {
    this(new @OsUntrusted String @OsUntrusted [0], new @OsUntrusted String @OsUntrusted [0],  0L, 0L);
  }

  /**
   * Constructor with host, name, offset and length
   */
  public @OsUntrusted BlockLocation(@OsUntrusted String @OsUntrusted [] names, @OsUntrusted String @OsUntrusted [] hosts, @OsUntrusted long offset, 
                       @OsUntrusted
                       long length) {
    this(names, hosts, offset, length, false);
  }

  /**
   * Constructor with host, name, offset, length and corrupt flag
   */
  public @OsUntrusted BlockLocation(@OsUntrusted String @OsUntrusted [] names, @OsUntrusted String @OsUntrusted [] hosts, @OsUntrusted long offset, 
                       @OsUntrusted
                       long length, @OsUntrusted boolean corrupt) {
    if (names == null) {
      this.names = new @OsUntrusted String @OsUntrusted [0];
    } else {
      this.names = names;
    }
    if (hosts == null) {
      this.hosts = new @OsUntrusted String @OsUntrusted [0];
    } else {
      this.hosts = hosts;
    }
    this.offset = offset;
    this.length = length;
    this.topologyPaths = new @OsUntrusted String @OsUntrusted [0];
    this.corrupt = corrupt;
  }

  /**
   * Constructor with host, name, network topology, offset and length
   */
  public @OsUntrusted BlockLocation(@OsUntrusted String @OsUntrusted [] names, @OsUntrusted String @OsUntrusted [] hosts, @OsUntrusted String @OsUntrusted [] topologyPaths,
                       @OsUntrusted
                       long offset, @OsUntrusted long length) {
    this(names, hosts, topologyPaths, offset, length, false);
  }

  /**
   * Constructor with host, name, network topology, offset, length 
   * and corrupt flag
   */
  public @OsUntrusted BlockLocation(@OsUntrusted String @OsUntrusted [] names, @OsUntrusted String @OsUntrusted [] hosts, @OsUntrusted String @OsUntrusted [] topologyPaths,
                       @OsUntrusted
                       long offset, @OsUntrusted long length, @OsUntrusted boolean corrupt) {
    this(names, hosts, offset, length, corrupt);
    if (topologyPaths == null) {
      this.topologyPaths = new @OsUntrusted String @OsUntrusted [0];
    } else {
      this.topologyPaths = topologyPaths;
    }
  }

  /**
   * Get the list of hosts (hostname) hosting this block
   */
  public @OsUntrusted String @OsUntrusted [] getHosts(@OsUntrusted BlockLocation this) throws IOException {
    if (hosts == null || hosts.length == 0) {
      return new @OsUntrusted String @OsUntrusted [0];
    } else {
      return hosts;
    }
  }

  /**
   * Get the list of names (IP:xferPort) hosting this block
   */
  public @OsUntrusted String @OsUntrusted [] getNames(@OsUntrusted BlockLocation this) throws IOException {
    if (names == null || names.length == 0) {
      return new @OsUntrusted String @OsUntrusted [0];
    } else {
      return names;
    }
  }

  /**
   * Get the list of network topology paths for each of the hosts.
   * The last component of the path is the "name" (IP:xferPort).
   */
  public @OsUntrusted String @OsUntrusted [] getTopologyPaths(@OsUntrusted BlockLocation this) throws IOException {
    if (topologyPaths == null || topologyPaths.length == 0) {
      return new @OsUntrusted String @OsUntrusted [0];
    } else {
      return topologyPaths;
    }
  }
  
  /**
   * Get the start offset of file associated with this block
   */
  public @OsUntrusted long getOffset(@OsUntrusted BlockLocation this) {
    return offset;
  }
  
  /**
   * Get the length of the block
   */
  public @OsUntrusted long getLength(@OsUntrusted BlockLocation this) {
    return length;
  }

  /**
   * Get the corrupt flag.
   */
  public @OsUntrusted boolean isCorrupt(@OsUntrusted BlockLocation this) {
    return corrupt;
  }

  /**
   * Set the start offset of file associated with this block
   */
  public void setOffset(@OsUntrusted BlockLocation this, @OsUntrusted long offset) {
    this.offset = offset;
  }

  /**
   * Set the length of block
   */
  public void setLength(@OsUntrusted BlockLocation this, @OsUntrusted long length) {
    this.length = length;
  }

  /**
   * Set the corrupt flag.
   */
  public void setCorrupt(@OsUntrusted BlockLocation this, @OsUntrusted boolean corrupt) {
    this.corrupt = corrupt;
  }

  /**
   * Set the hosts hosting this block
   */
  public void setHosts(@OsUntrusted BlockLocation this, @OsUntrusted String @OsUntrusted [] hosts) throws IOException {
    if (hosts == null) {
      this.hosts = new @OsUntrusted String @OsUntrusted [0];
    } else {
      this.hosts = hosts;
    }
  }

  /**
   * Set the names (host:port) hosting this block
   */
  public void setNames(@OsUntrusted BlockLocation this, @OsUntrusted String @OsUntrusted [] names) throws IOException {
    if (names == null) {
      this.names = new @OsUntrusted String @OsUntrusted [0];
    } else {
      this.names = names;
    }
  }

  /**
   * Set the network topology paths of the hosts
   */
  public void setTopologyPaths(@OsUntrusted BlockLocation this, @OsUntrusted String @OsUntrusted [] topologyPaths) throws IOException {
    if (topologyPaths == null) {
      this.topologyPaths = new @OsUntrusted String @OsUntrusted [0];
    } else {
      this.topologyPaths = topologyPaths;
    }
  }

  @Override
  public @OsUntrusted String toString(@OsUntrusted BlockLocation this) {
    @OsUntrusted
    StringBuilder result = new @OsUntrusted StringBuilder();
    result.append(offset);
    result.append(',');
    result.append(length);
    if (corrupt) {
      result.append("(corrupt)");
    }
    for(@OsUntrusted String h: hosts) {
      result.append(',');
      result.append(h);
    }
    return result.toString();
  }
}