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
import ostrusted.quals.OsTrusted;
import java.io.IOException;
import java.net.InetAddress;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * An authorization manager which handles service-level authorization
 * for incoming service requests.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class ServiceAuthorizationManager {
  private static final @OsUntrusted String HADOOP_POLICY_FILE = "hadoop-policy.xml";

  private @OsUntrusted Map<@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>, @OsUntrusted AccessControlList> protocolToAcl =
    new @OsUntrusted IdentityHashMap<@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>, @OsUntrusted AccessControlList>();
  
  /**
   * Configuration key for controlling service-level authorization for Hadoop.
   * 
   * @deprecated Use
   *             {@link CommonConfigurationKeys#HADOOP_SECURITY_AUTHORIZATION}
   *             instead.
   */
  @Deprecated
  public static final @OsUntrusted String SERVICE_AUTHORIZATION_CONFIG = 
    "hadoop.security.authorization";
  
  public static final @OsUntrusted Log AUDITLOG =
    LogFactory.getLog("SecurityLogger."+ServiceAuthorizationManager.class.getName());

  private static final @OsUntrusted String AUTHZ_SUCCESSFUL_FOR = "Authorization successful for ";
  private static final @OsUntrusted String AUTHZ_FAILED_FOR = "Authorization failed for ";

  
  /**
   * Authorize the user to access the protocol being used.
   * 
   * @param user user accessing the service 
   * @param protocol service being accessed
   * @param conf configuration to use
   * @param addr InetAddress of the client
   * @throws AuthorizationException on authorization failure
   */
  public void authorize(@OsUntrusted ServiceAuthorizationManager this, @OsUntrusted UserGroupInformation user, 
                               @OsUntrusted
                               Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol,
                               @OsUntrusted
                               Configuration conf,
                               @OsUntrusted
                               InetAddress addr
                               ) throws AuthorizationException {
    @OsUntrusted
    AccessControlList acl = protocolToAcl.get(protocol);
    if (acl == null) {
      throw new @OsUntrusted AuthorizationException("Protocol " + protocol + 
                                       " is not known.");
    }
    
    // get client principal key to verify (if available)
    @OsUntrusted
    KerberosInfo krbInfo = SecurityUtil.getKerberosInfo(protocol, conf);
    @OsUntrusted
    String clientPrincipal = null; 
    if (krbInfo != null) {
      @OsUntrusted
      String clientKey = krbInfo.clientPrincipal();
      if (clientKey != null && !clientKey.isEmpty()) {
        try {
          clientPrincipal = SecurityUtil.getServerPrincipal(
              conf.get(clientKey), addr);
        } catch (@OsUntrusted IOException e) {
          throw (@OsUntrusted AuthorizationException) new @OsUntrusted AuthorizationException(
              "Can't figure out Kerberos principal name for connection from "
                  + addr + " for user=" + user + " protocol=" + protocol)
              .initCause(e);
        }
      }
    }
    if((clientPrincipal != null && !clientPrincipal.equals(user.getUserName())) || 
        !acl.isUserAllowed(user)) {
      AUDITLOG.warn(AUTHZ_FAILED_FOR + user + " for protocol=" + protocol
          + ", expected client Kerberos principal is " + clientPrincipal);
      throw new @OsUntrusted AuthorizationException("User " + user + 
          " is not authorized for protocol " + protocol + 
          ", expected client Kerberos principal is " + clientPrincipal);
    }
    AUDITLOG.info(AUTHZ_SUCCESSFUL_FOR + user + " for protocol="+protocol);
  }

  @SuppressWarnings("ostrusted:cast.unsafe")
  public synchronized void refresh(@OsUntrusted ServiceAuthorizationManager this, @OsUntrusted Configuration conf,
                                          @OsUntrusted
                                          PolicyProvider provider) {
    //ostrusted, properties are trusted
    // Get the system property 'hadoop.policy.file'
    @OsTrusted String policyFile = (@OsTrusted String) System.getProperty("hadoop.policy.file", HADOOP_POLICY_FILE);
    
    // Make a copy of the original config, and load the policy file
    @OsUntrusted
    Configuration policyConf = new @OsUntrusted Configuration(conf);
    policyConf.addResource(policyFile);
    
    final @OsUntrusted Map<@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>, @OsUntrusted AccessControlList> newAcls =
      new @OsUntrusted IdentityHashMap<@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>, @OsUntrusted AccessControlList>();

    // Parse the config file
    @OsUntrusted
    Service @OsUntrusted [] services = provider.getServices();
    if (services != null) {
      for (@OsUntrusted Service service : services) {
        @OsUntrusted
        AccessControlList acl = 
          new @OsUntrusted AccessControlList(
              policyConf.get(service.getServiceKey(), 
                             AccessControlList.WILDCARD_ACL_VALUE)
              );
        newAcls.put(service.getProtocol(), acl);
      }
    }

    // Flip to the newly parsed permissions
    protocolToAcl = newAcls;
  }

  // Package-protected for use in tests.
  @OsUntrusted
  Set<@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>> getProtocolsWithAcls(@OsUntrusted ServiceAuthorizationManager this) {
    return protocolToAcl.keySet();
  }
}
