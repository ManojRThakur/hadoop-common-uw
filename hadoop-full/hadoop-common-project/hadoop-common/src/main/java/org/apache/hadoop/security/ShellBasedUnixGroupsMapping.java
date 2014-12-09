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
package org.apache.hadoop.security;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;

/**
 * A simple shell-based implementation of {@link GroupMappingServiceProvider} 
 * that exec's the <code>groups</code> shell command to fetch the group
 * memberships of a given user.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class ShellBasedUnixGroupsMapping
  implements @OsUntrusted GroupMappingServiceProvider {
  
  private static final @OsUntrusted Log LOG = LogFactory.getLog(ShellBasedUnixGroupsMapping.class);
  
  /**
   * Returns list of groups for a user
   *
   * @param user get groups for this user
   * @return list of groups for a given user
   */
  @Override
  public @OsUntrusted List<@OsTrusted String> getGroups(@OsUntrusted ShellBasedUnixGroupsMapping this, @OsTrusted String user) throws IOException {
    return getUnixGroups(user);
  }

  /**
   * Caches groups, no need to do that for this provider
   */
  @Override
  public void cacheGroupsRefresh(@OsUntrusted ShellBasedUnixGroupsMapping this) throws IOException {
    // does nothing in this provider of user to groups mapping
  }

  /** 
   * Adds groups to cache, no need to do that for this provider
   *
   * @param groups unused
   */
  @Override
  public void cacheGroupsAdd(@OsUntrusted ShellBasedUnixGroupsMapping this, @OsUntrusted List<@OsTrusted String> groups) throws IOException {
    // does nothing in this provider of user to groups mapping
  }

  /** 
   * Get the current user's group list from Unix by running the command 'groups'
   * NOTE. For non-existing user it will return EMPTY list
   * @param user user name
   * @return the groups list that the <code>user</code> belongs to
   * @throws IOException if encounter any error when running the command
   */
  //ostrusted, deconstructing a trusted string
  @SuppressWarnings("ostrusted:cast.unsafe")
  private static @OsUntrusted List<@OsTrusted String> getUnixGroups(final @OsTrusted String user) throws IOException {
    @OsTrusted String result = "";
    try {
      result = Shell.execCommand( Shell.getGroupsForUserCommand(user) );
    } catch (@OsUntrusted ExitCodeException e) {
      // if we didn't get the group - just return empty list;
      LOG.warn("got exception trying to get groups for user " + user, e);
    }
    
    @OsUntrusted StringTokenizer tokenizer = new @OsUntrusted StringTokenizer(result, Shell.TOKEN_SEPARATOR_REGEX);
    @OsUntrusted List<@OsTrusted String> groups = new @OsUntrusted LinkedList<@OsTrusted String>();
    while (tokenizer.hasMoreTokens()) {
      groups.add( (@OsTrusted String) tokenizer.nextToken());
    }
    return groups;
  }
}
