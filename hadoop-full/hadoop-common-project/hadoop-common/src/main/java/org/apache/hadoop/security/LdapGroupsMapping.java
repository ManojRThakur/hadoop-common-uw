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
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.naming.CommunicationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * An implementation of {@link GroupMappingServiceProvider} which
 * connects directly to an LDAP server for determining group membership.
 * 
 * This provider should be used only if it is necessary to map users to
 * groups that reside exclusively in an Active Directory or LDAP installation.
 * The common case for a Hadoop installation will be that LDAP users and groups
 * materialized on the Unix servers, and for an installation like that,
 * ShellBasedUnixGroupsMapping is preferred. However, in cases where
 * those users and groups aren't materialized in Unix, but need to be used for
 * access control, this class may be used to communicate directly with the LDAP
 * server.
 * 
 * It is important to note that resolving group mappings will incur network
 * traffic, and may cause degraded performance, although user-group mappings
 * will be cached via the infrastructure provided by {@link Groups}.
 * 
 * This implementation does not support configurable search limits. If a filter
 * is used for searching users or groups which returns more results than are
 * allowed by the server, an exception will be thrown.
 * 
 * The implementation also does not attempt to resolve group hierarchies. In
 * order to be considered a member of a group, the user must be an explicit
 * member in LDAP.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class LdapGroupsMapping
    implements @OsUntrusted GroupMappingServiceProvider, @OsUntrusted Configurable {
  
  public static final @OsTrusted String LDAP_CONFIG_PREFIX = "hadoop.security.group.mapping.ldap";

  /*
   * URL of the LDAP server
   */
  public static final @OsTrusted String LDAP_URL_KEY = LDAP_CONFIG_PREFIX + ".url";
  public static final @OsTrusted String LDAP_URL_DEFAULT = "";

  /*
   * Should SSL be used to connect to the server
   */
  public static final @OsTrusted String LDAP_USE_SSL_KEY = LDAP_CONFIG_PREFIX + ".ssl";
  public static final @OsTrusted Boolean LDAP_USE_SSL_DEFAULT = false;

  /*
   * File path to the location of the SSL keystore to use
   */
  public static final @OsTrusted String LDAP_KEYSTORE_KEY = LDAP_CONFIG_PREFIX + ".ssl.keystore";
  public static final @OsTrusted String LDAP_KEYSTORE_DEFAULT = "";

  /*
   * Password for the keystore
   */
  public static final @OsTrusted String LDAP_KEYSTORE_PASSWORD_KEY = LDAP_CONFIG_PREFIX + ".ssl.keystore.password";
  public static final @OsTrusted String LDAP_KEYSTORE_PASSWORD_DEFAULT = "";
  
  public static final @OsTrusted String LDAP_KEYSTORE_PASSWORD_FILE_KEY = LDAP_KEYSTORE_PASSWORD_KEY + ".file";
  public static final @OsTrusted String LDAP_KEYSTORE_PASSWORD_FILE_DEFAULT = "";

  /*
   * User to bind to the LDAP server with
   */
  public static final @OsTrusted String BIND_USER_KEY = LDAP_CONFIG_PREFIX + ".bind.user";
  public static final @OsTrusted String BIND_USER_DEFAULT = "";

  /*
   * Password for the bind user
   */
  public static final @OsTrusted String BIND_PASSWORD_KEY = LDAP_CONFIG_PREFIX + ".bind.password";
  public static final @OsTrusted String BIND_PASSWORD_DEFAULT = "";
  
  public static final @OsTrusted String BIND_PASSWORD_FILE_KEY = BIND_PASSWORD_KEY + ".file";
  public static final @OsTrusted String BIND_PASSWORD_FILE_DEFAULT = "";

  /*
   * Base distinguished name to use for searches
   */
  public static final @OsTrusted String BASE_DN_KEY = LDAP_CONFIG_PREFIX + ".base";
  public static final @OsTrusted String BASE_DN_DEFAULT = "";

  /*
   * Any additional filters to apply when searching for users
   */
  public static final @OsTrusted String USER_SEARCH_FILTER_KEY = LDAP_CONFIG_PREFIX + ".search.filter.user";
  public static final @OsTrusted String USER_SEARCH_FILTER_DEFAULT = "(&(objectClass=user)(sAMAccountName={0}))";

  /*
   * Any additional filters to apply when finding relevant groups
   */
  public static final @OsTrusted String GROUP_SEARCH_FILTER_KEY = LDAP_CONFIG_PREFIX + ".search.filter.group";
  public static final @OsTrusted String GROUP_SEARCH_FILTER_DEFAULT = "(objectClass=group)";

  /*
   * LDAP attribute to use for determining group membership
   */
  public static final @OsTrusted String GROUP_MEMBERSHIP_ATTR_KEY = LDAP_CONFIG_PREFIX + ".search.attr.member";
  public static final @OsTrusted String GROUP_MEMBERSHIP_ATTR_DEFAULT = "member";

  /*
   * LDAP attribute to use for identifying a group's name
   */
  public static final @OsTrusted String GROUP_NAME_ATTR_KEY = LDAP_CONFIG_PREFIX + ".search.attr.group.name";
  public static final @OsTrusted String GROUP_NAME_ATTR_DEFAULT = "cn";

  /*
   * LDAP {@link SearchControls} attribute to set the time limit
   * for an invoked directory search. Prevents infinite wait cases.
   */
  public static final @OsTrusted String DIRECTORY_SEARCH_TIMEOUT =
    LDAP_CONFIG_PREFIX + ".directory.search.timeout";
  public static final @OsUntrusted int DIRECTORY_SEARCH_TIMEOUT_DEFAULT = 10000; // 10s

  private static final @OsUntrusted Log LOG = LogFactory.getLog(LdapGroupsMapping.class);

  private static final @OsUntrusted SearchControls SEARCH_CONTROLS = new @OsUntrusted SearchControls();
  static {
    SEARCH_CONTROLS.setSearchScope(SearchControls.SUBTREE_SCOPE);
  }

  private @OsUntrusted DirContext ctx;
  private @OsUntrusted Configuration conf;
  
  private @OsUntrusted String ldapUrl;
  private @OsUntrusted boolean useSsl;
  private @OsTrusted String keystore;
  private @OsTrusted String keystorePass;
  private @OsUntrusted String bindUser;
  private @OsUntrusted String bindPassword;
  private @OsUntrusted String baseDN;
  private @OsUntrusted String groupSearchFilter;
  private @OsUntrusted String userSearchFilter;
  private @OsUntrusted String groupMemberAttr;
  private @OsUntrusted String groupNameAttr;

  public static @OsUntrusted int RECONNECT_RETRY_COUNT = 3;
  
  /**
   * Returns list of groups for a user.
   * 
   * The LdapCtx which underlies the DirContext object is not thread-safe, so
   * we need to block around this whole method. The caching infrastructure will
   * ensure that performance stays in an acceptable range.
   *
   * @param user get groups for this user
   * @return list of groups for a given user
   */
  @Override
  public synchronized @OsUntrusted List<@OsTrusted String> getGroups(@OsUntrusted LdapGroupsMapping this, @OsTrusted String user) throws IOException {
    @OsUntrusted List<@OsTrusted String> emptyResults = new @OsUntrusted ArrayList<@OsTrusted String>();
    /*
     * Normal garbage collection takes care of removing Context instances when they are no longer in use. 
     * Connections used by Context instances being garbage collected will be closed automatically.
     * So in case connection is closed and gets CommunicationException, retry some times with new new DirContext/connection. 
     */
    try {
      return doGetGroups(user);
    } catch (@OsUntrusted CommunicationException e) {
      LOG.warn("Connection is closed, will try to reconnect");
    } catch (@OsUntrusted NamingException e) {
      LOG.warn("Exception trying to get groups for user " + user, e);
      return emptyResults;
    }

    @OsUntrusted
    int retryCount = 0;
    while (retryCount ++ < RECONNECT_RETRY_COUNT) {
      //reset ctx so that new DirContext can be created with new connection
      this.ctx = null;
      
      try {
        return doGetGroups(user);
      } catch (@OsUntrusted CommunicationException e) {
        LOG.warn("Connection being closed, reconnecting failed, retryCount = " + retryCount);
      } catch (@OsUntrusted NamingException e) {
        LOG.warn("Exception trying to get groups for user " + user, e);
        return emptyResults;
      }
    }
    
    return emptyResults;
  }

  //ostrusted, todo: do we need to do sanitation here
  //TODO: Alright we either trust the remote entries on the way in or suggest sanitation here
  @SuppressWarnings("ostrusted:cast.unsafe")
  @OsUntrusted List<@OsTrusted String> doGetGroups(@OsUntrusted LdapGroupsMapping this, @OsTrusted String user) throws NamingException {
    @OsUntrusted List<@OsTrusted String> groups = new @OsUntrusted ArrayList<@OsTrusted String>();

    @OsUntrusted
    DirContext ctx = getDirContext();

    // Search for the user. We'll only ever need to look at the first result
    @OsUntrusted
    NamingEnumeration<@OsUntrusted SearchResult> results = ctx.search(baseDN,
        userSearchFilter,
        new @OsUntrusted Object @OsUntrusted []{user},
        SEARCH_CONTROLS);
    if (results.hasMoreElements()) {
      @OsUntrusted
      SearchResult result = results.nextElement();
      @OsUntrusted
      String userDn = result.getNameInNamespace();


      @OsUntrusted
      NamingEnumeration<@OsUntrusted SearchResult> groupResults =
          ctx.search(baseDN,
              "(&" + groupSearchFilter + "(" + groupMemberAttr + "={0}))",
              new @OsUntrusted Object @OsUntrusted []{userDn},
              SEARCH_CONTROLS);
      while (groupResults.hasMoreElements()) {
        @OsUntrusted
        SearchResult groupResult = groupResults.nextElement();
        @OsUntrusted
        Attribute groupName = groupResult.getAttributes().get(groupNameAttr);
        groups.add( (@OsTrusted String) groupName.get().toString() );
      }
    }

    return groups;
  }

  @SuppressWarnings("deprecation")
  @OsUntrusted
  DirContext getDirContext(@OsUntrusted LdapGroupsMapping this) throws NamingException {
    if (ctx == null) {
      // Set up the initial environment for LDAP connectivity
      @OsUntrusted
      Hashtable<@OsUntrusted String, @OsUntrusted String> env = new @OsUntrusted Hashtable<@OsUntrusted String, @OsUntrusted String>();
      env.put(Context.INITIAL_CONTEXT_FACTORY,
          com.sun.jndi.ldap.LdapCtxFactory.class.getName());
      env.put(Context.PROVIDER_URL, ldapUrl);
      env.put(Context.SECURITY_AUTHENTICATION, "simple");

      // Set up SSL security, if necessary
      if (useSsl) {
        env.put(Context.SECURITY_PROTOCOL, "ssl");
        System.setProperty("javax.net.ssl.keyStore", keystore);
        System.setProperty("javax.net.ssl.keyStorePassword", keystorePass);
      }

      env.put(Context.SECURITY_PRINCIPAL, bindUser);
      env.put(Context.SECURITY_CREDENTIALS, bindPassword);

      ctx = new @OsUntrusted InitialDirContext(env);
    }

    return ctx;
  }
  
  /**
   * Caches groups, no need to do that for this provider
   */
  @Override
  public void cacheGroupsRefresh(@OsUntrusted LdapGroupsMapping this) throws IOException {
    // does nothing in this provider of user to groups mapping
  }

  /** 
   * Adds groups to cache, no need to do that for this provider
   *
   * @param groups unused
   */
  @Override
  public void cacheGroupsAdd(@OsUntrusted LdapGroupsMapping this, @OsUntrusted List<@OsTrusted String> groups) throws IOException {
    // does nothing in this provider of user to groups mapping
  }

  @Override
  public synchronized @OsUntrusted Configuration getConf(@OsUntrusted LdapGroupsMapping this) {
    return conf;
  }

  @Override
  public synchronized void setConf(@OsUntrusted LdapGroupsMapping this, @OsUntrusted Configuration conf) {
    ldapUrl = conf.get(LDAP_URL_KEY, LDAP_URL_DEFAULT);
    if (ldapUrl == null || ldapUrl.isEmpty()) {
      throw new @OsUntrusted RuntimeException("LDAP URL is not configured");
    }
    
    useSsl = conf.getBoolean(LDAP_USE_SSL_KEY, LDAP_USE_SSL_DEFAULT);
    keystore = conf.get(LDAP_KEYSTORE_KEY, LDAP_KEYSTORE_DEFAULT);
    
    keystorePass =
        conf.get(LDAP_KEYSTORE_PASSWORD_KEY, LDAP_KEYSTORE_PASSWORD_DEFAULT);
    if (keystorePass.isEmpty()) {
      keystorePass = extractPassword(
        conf.get(LDAP_KEYSTORE_PASSWORD_KEY, LDAP_KEYSTORE_PASSWORD_DEFAULT));
    }
    
    bindUser = conf.get(BIND_USER_KEY, BIND_USER_DEFAULT);
    bindPassword = conf.get(BIND_PASSWORD_KEY, BIND_PASSWORD_DEFAULT);
    if (bindPassword.isEmpty()) {
      bindPassword = extractPassword(
          conf.get(BIND_PASSWORD_FILE_KEY, BIND_PASSWORD_FILE_DEFAULT));
    }
    
    baseDN = conf.get(BASE_DN_KEY, BASE_DN_DEFAULT);
    groupSearchFilter =
        conf.get(GROUP_SEARCH_FILTER_KEY, GROUP_SEARCH_FILTER_DEFAULT);
    userSearchFilter =
        conf.get(USER_SEARCH_FILTER_KEY, USER_SEARCH_FILTER_DEFAULT);
    groupMemberAttr =
        conf.get(GROUP_MEMBERSHIP_ATTR_KEY, GROUP_MEMBERSHIP_ATTR_DEFAULT);
    groupNameAttr =
        conf.get(GROUP_NAME_ATTR_KEY, GROUP_NAME_ATTR_DEFAULT);

    @OsUntrusted
    int dirSearchTimeout = conf.getInt(DIRECTORY_SEARCH_TIMEOUT, DIRECTORY_SEARCH_TIMEOUT_DEFAULT);
    SEARCH_CONTROLS.setTimeLimit(dirSearchTimeout);

    this.conf = conf;
  }

  @SuppressWarnings("ostrusted:cast.unsafe")
  @OsTrusted String extractPassword(@OsUntrusted LdapGroupsMapping this, @OsTrusted String pwFile) {
    if (pwFile.isEmpty()) {
      // If there is no password file defined, we'll assume that we should do
      // an anonymous bind
      return "";
    }
    
    try {
      @OsUntrusted
      StringBuilder password = new @OsUntrusted StringBuilder();
      @OsUntrusted
      Reader reader = new @OsUntrusted FileReader(pwFile);
      @OsUntrusted
      int c = reader.read();
      while (c > -1) {
        password.append((@OsUntrusted char)c);
        c = reader.read();
      }
      reader.close();
      return (@OsTrusted String) password.toString();
    } catch (@OsUntrusted IOException ioe) {
      throw new @OsUntrusted RuntimeException("Could not read password file: " + pwFile, ioe);
    }
  }
}
