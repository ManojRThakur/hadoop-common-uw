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

package org.apache.hadoop.security.authorize;

import ostrusted.quals.OsUntrusted;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

@InterfaceAudience.Private
public class ProxyUsers {

  private static final @OsUntrusted String CONF_HOSTS = ".hosts";
  public static final @OsUntrusted String CONF_GROUPS = ".groups";
  public static final @OsUntrusted String CONF_HADOOP_PROXYUSER = "hadoop.proxyuser.";
  public static final @OsUntrusted String CONF_HADOOP_PROXYUSER_RE = "hadoop\\.proxyuser\\.";
  private static @OsUntrusted boolean init = false;
  // list of groups and hosts per proxyuser
  private static @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Collection<@OsUntrusted String>> proxyGroups = 
    new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted Collection<@OsUntrusted String>>();
  private static @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Collection<@OsUntrusted String>> proxyHosts = 
    new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted Collection<@OsUntrusted String>>();

  /**
   * reread the conf and get new values for "hadoop.proxyuser.*.groups/hosts"
   */
  public static void refreshSuperUserGroupsConfiguration() {
    //load server side configuration;
    refreshSuperUserGroupsConfiguration(new @OsUntrusted Configuration());
  }

  /**
   * refresh configuration
   * @param conf
   */
  public static synchronized void refreshSuperUserGroupsConfiguration(@OsUntrusted Configuration conf) {
    
    // remove alle existing stuff
    proxyGroups.clear();
    proxyHosts.clear();

    // get all the new keys for groups
    @OsUntrusted
    String regex = CONF_HADOOP_PROXYUSER_RE+"[^.]*\\"+CONF_GROUPS;
    @OsUntrusted
    Map<@OsUntrusted String, @OsUntrusted String> allMatchKeys = conf.getValByRegex(regex);
    for(@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted String> entry : allMatchKeys.entrySet()) {
      proxyGroups.put(entry.getKey(), 
          StringUtils.getStringCollection(entry.getValue()));
    }

    // now hosts
    regex = CONF_HADOOP_PROXYUSER_RE+"[^.]*\\"+CONF_HOSTS;
    allMatchKeys = conf.getValByRegex(regex);
    for(@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted String> entry : allMatchKeys.entrySet()) {
      proxyHosts.put(entry.getKey(),
          StringUtils.getStringCollection(entry.getValue()));
    }
    
    init = true;
  }

  /**
   * Returns configuration key for effective user groups allowed for a superuser
   * 
   * @param userName name of the superuser
   * @return configuration key for superuser groups
   */
  public static @OsUntrusted String getProxySuperuserGroupConfKey(@OsUntrusted String userName) {
    return ProxyUsers.CONF_HADOOP_PROXYUSER+userName+ProxyUsers.CONF_GROUPS;
  }
  
  /**
   * Return configuration key for superuser ip addresses
   * 
   * @param userName name of the superuser
   * @return configuration key for superuser ip-addresses
   */
  public static @OsUntrusted String getProxySuperuserIpConfKey(@OsUntrusted String userName) {
    return ProxyUsers.CONF_HADOOP_PROXYUSER+userName+ProxyUsers.CONF_HOSTS;
  }
  
  /**
   * Authorize the superuser which is doing doAs
   * 
   * @param user ugi of the effective or proxy user which contains a real user
   * @param remoteAddress the ip address of client
   * @param newConf configuration
   * @throws AuthorizationException
   */
  public static synchronized void authorize(@OsUntrusted UserGroupInformation user, 
      @OsUntrusted
      String remoteAddress, @OsUntrusted Configuration newConf) throws AuthorizationException {

    if(!init) {
      refreshSuperUserGroupsConfiguration(); 
    }

    if (user.getRealUser() == null) {
      return;
    }
    @OsUntrusted
    boolean groupAuthorized = false;
    @OsUntrusted
    boolean ipAuthorized = false;
    @OsUntrusted
    UserGroupInformation superUser = user.getRealUser();

    @OsUntrusted
    Collection<@OsUntrusted String> allowedUserGroups = proxyGroups.get(
        getProxySuperuserGroupConfKey(superUser.getShortUserName()));
    
    if (isWildcardList(allowedUserGroups)) {
      groupAuthorized = true;
    } else if (allowedUserGroups != null && !allowedUserGroups.isEmpty()) {
      for (@OsUntrusted String group : user.getGroupNames()) {
        if (allowedUserGroups.contains(group)) {
          groupAuthorized = true;
          break;
        }
      }
    }

    if (!groupAuthorized) {
      throw new @OsUntrusted AuthorizationException("User: " + superUser.getUserName()
          + " is not allowed to impersonate " + user.getUserName());
    }
    
    @OsUntrusted
    Collection<@OsUntrusted String> ipList = proxyHosts.get(
        getProxySuperuserIpConfKey(superUser.getShortUserName()));
   
    if (isWildcardList(ipList)) {
      ipAuthorized = true;
    } else if (ipList != null && !ipList.isEmpty()) {
      for (@OsUntrusted String allowedHost : ipList) {
        @OsUntrusted
        InetAddress hostAddr;
        try {
          hostAddr = InetAddress.getByName(allowedHost);
        } catch (@OsUntrusted UnknownHostException e) {
          continue;
        }
        if (hostAddr.getHostAddress().equals(remoteAddress)) {
          // Authorization is successful
          ipAuthorized = true;
        }
      }
    }
    if(!ipAuthorized) {
      throw new @OsUntrusted AuthorizationException("Unauthorized connection for super-user: "
          + superUser.getUserName() + " from IP " + remoteAddress);
    }
  }

  /**
   * Return true if the configuration specifies the special configuration value
   * "*", indicating that any group or host list is allowed to use this configuration.
   */
  private static @OsUntrusted boolean isWildcardList(@OsUntrusted Collection<@OsUntrusted String> list) {
    return (list != null) &&
      (list.size() == 1) &&
      (list.contains("*"));
  }

}
