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
package org.apache.hadoop.tools;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;

/**
 * Base class for the HDFS and MR implementations of tools which fetch and
 * display the groups that users belong to.
 */
public abstract class GetGroupsBase extends @OsUntrusted Configured implements @OsUntrusted Tool {
  
  private @OsUntrusted PrintStream out;
  
  /**
   * Create an instance of this tool using the given configuration.
   * @param conf
   */
  protected @OsUntrusted GetGroupsBase(@OsUntrusted Configuration conf) {
    this(conf, System.out);
  }
  
  /**
   * Used exclusively for testing.
   * 
   * @param conf The configuration to use.
   * @param out The PrintStream to write to, instead of System.out
   */
  protected @OsUntrusted GetGroupsBase(@OsUntrusted Configuration conf, @OsUntrusted PrintStream out) {
    super(conf);
    this.out = out;
  }

  /**
   * Get the groups for the users given and print formatted output to the
   * {@link PrintStream} configured earlier.
   */
  @Override
  public @OsUntrusted int run(@OsUntrusted GetGroupsBase this, @OsUntrusted String @OsUntrusted [] args) throws Exception {
    if (args.length == 0) {
      args = new @OsUntrusted String @OsUntrusted [] { UserGroupInformation.getCurrentUser().getUserName() }; 
    }

    for (@OsUntrusted String username : args) {
      @OsUntrusted
      StringBuilder sb = new @OsUntrusted StringBuilder();
      sb.append(username + " :");
      for (@OsUntrusted String group : getUgmProtocol().getGroupsForUser(username)) {
        sb.append(" ");
        sb.append(group);
      }
      out.println(sb);
    }

    return 0;
  }

  /**
   * Must be overridden by subclasses to get the address where the
   * {@link GetUserMappingsProtocol} implementation is running.
   * 
   * @param conf The configuration to use.
   * @return The address where the service is listening.
   * @throws IOException
   */
  protected abstract @OsUntrusted InetSocketAddress getProtocolAddress(@OsUntrusted GetGroupsBase this, @OsUntrusted Configuration conf)
      throws IOException;
  
  /**
   * Get a client of the {@link GetUserMappingsProtocol}.
   * @return A {@link GetUserMappingsProtocol} client proxy.
   * @throws IOException
   */
  protected @OsUntrusted GetUserMappingsProtocol getUgmProtocol(@OsUntrusted GetGroupsBase this) throws IOException {
    @OsUntrusted
    GetUserMappingsProtocol userGroupMappingProtocol =
      RPC.getProxy(GetUserMappingsProtocol.class, 
          GetUserMappingsProtocol.versionID,
          getProtocolAddress(getConf()), UserGroupInformation.getCurrentUser(),
          getConf(), NetUtils.getSocketFactory(getConf(),
              GetUserMappingsProtocol.class));
    return userGroupMappingProtocol;
  }

}
