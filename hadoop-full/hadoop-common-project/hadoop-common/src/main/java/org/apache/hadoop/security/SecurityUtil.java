/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.security;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;

import com.google.common.annotations.VisibleForTesting;

//this will need to be replaced someday when there is a suitable replacement
import sun.net.dns.ResolverConfiguration;
import sun.net.util.IPAddressUtil;

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SecurityUtil {
  public static final @OsUntrusted Log LOG = LogFactory.getLog(SecurityUtil.class);
  public static final @OsUntrusted String HOSTNAME_PATTERN = "_HOST";

  // controls whether buildTokenService will use an ip or host/ip as given
  // by the user
  @VisibleForTesting
  static @OsUntrusted boolean useIpForTokenService;
  @VisibleForTesting
  static @OsUntrusted HostResolver hostResolver;

  private static @OsUntrusted SSLFactory sslFactory;

  static {
    @OsUntrusted
    Configuration conf = new @OsUntrusted Configuration();
    @OsUntrusted
    boolean useIp = conf.getBoolean(
      CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP,
      CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP_DEFAULT);
    setTokenServiceUseIp(useIp);
    if (HttpConfig.isSecure()) {
      sslFactory = new @OsUntrusted SSLFactory(SSLFactory.Mode.CLIENT, conf);
      try {
        sslFactory.init();
      } catch (@OsUntrusted Exception ex) {
        throw new @OsUntrusted RuntimeException(ex);
      }
    }
  }
  
  /**
   * For use only by tests and initialization
   */
  @InterfaceAudience.Private
  static void setTokenServiceUseIp(@OsUntrusted boolean flag) {
    useIpForTokenService = flag;
    hostResolver = !useIpForTokenService
        ? new @OsUntrusted QualifiedHostResolver()
        : new @OsUntrusted StandardHostResolver();
  }
  
  /**
   * Find the original TGT within the current subject's credentials. Cross-realm
   * TGT's of the form "krbtgt/TWO.COM@ONE.COM" may be present.
   * 
   * @return The TGT from the current subject
   * @throws IOException
   *           if TGT can't be found
   */
  private static @OsUntrusted KerberosTicket getTgtFromSubject() throws IOException {
    @OsUntrusted
    Subject current = Subject.getSubject(AccessController.getContext());
    if (current == null) {
      throw new @OsUntrusted IOException(
          "Can't get TGT from current Subject, because it is null");
    }
    @OsUntrusted
    Set<@OsUntrusted KerberosTicket> tickets = current
        .getPrivateCredentials(KerberosTicket.class);
    for (@OsUntrusted KerberosTicket t : tickets) {
      if (isOriginalTGT(t))
        return t;
    }
    throw new @OsUntrusted IOException("Failed to find TGT from current Subject:"+current);
  }
  
  /**
   * TGS must have the server principal of the form "krbtgt/FOO@FOO".
   * @param principal
   * @return true or false
   */
  static @OsUntrusted boolean 
  isTGSPrincipal(@OsUntrusted KerberosPrincipal principal) {
    if (principal == null)
      return false;
    if (principal.getName().equals("krbtgt/" + principal.getRealm() + 
        "@" + principal.getRealm())) {
      return true;
    }
    return false;
  }
  
  /**
   * Check whether the server principal is the TGS's principal
   * @param ticket the original TGT (the ticket that is obtained when a 
   * kinit is done)
   * @return true or false
   */
  protected static @OsUntrusted boolean isOriginalTGT(@OsUntrusted KerberosTicket ticket) {
    return isTGSPrincipal(ticket.getServer());
  }

  /**
   * Convert Kerberos principal name pattern to valid Kerberos principal
   * names. It replaces hostname pattern with hostname, which should be
   * fully-qualified domain name. If hostname is null or "0.0.0.0", it uses
   * dynamically looked-up fqdn of the current host instead.
   * 
   * @param principalConfig
   *          the Kerberos principal name conf value to convert
   * @param hostname
   *          the fully-qualified domain name used for substitution
   * @return converted Kerberos principal name
   * @throws IOException if the client address cannot be determined
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static @OsUntrusted String getServerPrincipal(@OsUntrusted String principalConfig,
      @OsUntrusted
      String hostname) throws IOException {
    @OsUntrusted
    String @OsUntrusted [] components = getComponents(principalConfig);
    if (components == null || components.length != 3
        || !components[1].equals(HOSTNAME_PATTERN)) {
      return principalConfig;
    } else {
      return replacePattern(components, hostname);
    }
  }
  
  /**
   * Convert Kerberos principal name pattern to valid Kerberos principal names.
   * This method is similar to {@link #getServerPrincipal(String, String)},
   * except 1) the reverse DNS lookup from addr to hostname is done only when
   * necessary, 2) param addr can't be null (no default behavior of using local
   * hostname when addr is null).
   * 
   * @param principalConfig
   *          Kerberos principal name pattern to convert
   * @param addr
   *          InetAddress of the host used for substitution
   * @return converted Kerberos principal name
   * @throws IOException if the client address cannot be determined
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static @OsUntrusted String getServerPrincipal(@OsUntrusted String principalConfig,
      @OsUntrusted
      InetAddress addr) throws IOException {
    @OsUntrusted
    String @OsUntrusted [] components = getComponents(principalConfig);
    if (components == null || components.length != 3
        || !components[1].equals(HOSTNAME_PATTERN)) {
      return principalConfig;
    } else {
      if (addr == null) {
        throw new @OsUntrusted IOException("Can't replace " + HOSTNAME_PATTERN
            + " pattern since client address is null");
      }
      return replacePattern(components, addr.getCanonicalHostName());
    }
  }
  
  private static @OsUntrusted String @OsUntrusted [] getComponents(@OsUntrusted String principalConfig) {
    if (principalConfig == null)
      return null;
    return principalConfig.split("[/@]");
  }
  
  private static @OsUntrusted String replacePattern(@OsUntrusted String @OsUntrusted [] components, @OsUntrusted String hostname)
      throws IOException {
    @OsUntrusted
    String fqdn = hostname;
    if (fqdn == null || fqdn.isEmpty() || fqdn.equals("0.0.0.0")) {
      fqdn = getLocalHostName();
    }
    return components[0] + "/" + fqdn.toLowerCase(Locale.US) + "@" + components[2];
  }
  
  static @OsUntrusted String getLocalHostName() throws UnknownHostException {
    return InetAddress.getLocalHost().getCanonicalHostName();
  }

  /**
   * Login as a principal specified in config. Substitute $host in
   * user's Kerberos principal name with a dynamically looked-up fully-qualified
   * domain name of the current host.
   * 
   * @param conf
   *          conf to use
   * @param keytabFileKey
   *          the key to look for keytab file in conf
   * @param userNameKey
   *          the key to look for user's Kerberos principal name in conf
   * @throws IOException if login fails
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static void login(final @OsUntrusted Configuration conf,
      final @OsUntrusted String keytabFileKey, final @OsUntrusted String userNameKey) throws IOException {
    login(conf, keytabFileKey, userNameKey, getLocalHostName());
  }

  /**
   * Login as a principal specified in config. Substitute $host in user's Kerberos principal 
   * name with hostname. If non-secure mode - return. If no keytab available -
   * bail out with an exception
   * 
   * @param conf
   *          conf to use
   * @param keytabFileKey
   *          the key to look for keytab file in conf
   * @param userNameKey
   *          the key to look for user's Kerberos principal name in conf
   * @param hostname
   *          hostname to use for substitution
   * @throws IOException if the config doesn't specify a keytab
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static void login(final @OsUntrusted Configuration conf,
      final @OsUntrusted String keytabFileKey, final @OsUntrusted String userNameKey, @OsUntrusted String hostname)
      throws IOException {
    
    if(! UserGroupInformation.isSecurityEnabled()) 
      return;
    
    @OsUntrusted
    String keytabFilename = conf.get(keytabFileKey);
    if (keytabFilename == null || keytabFilename.length() == 0) {
      throw new @OsUntrusted IOException("Running in secure mode, but config doesn't have a keytab");
    }

    @OsUntrusted
    String principalConfig = conf.get(userNameKey, System
        .getProperty("user.name"));
    @OsUntrusted
    String principalName = SecurityUtil.getServerPrincipal(principalConfig,
        hostname);
    UserGroupInformation.loginUserFromKeytab(principalName, keytabFilename);
  }

  /**
   * create the service name for a Delegation token
   * @param uri of the service
   * @param defPort is used if the uri lacks a port
   * @return the token service, or null if no authority
   * @see #buildTokenService(InetSocketAddress)
   */
  public static @OsUntrusted String buildDTServiceName(@OsUntrusted URI uri, @OsUntrusted int defPort) {
    @OsUntrusted
    String authority = uri.getAuthority();
    if (authority == null) {
      return null;
    }
    @OsUntrusted
    InetSocketAddress addr = NetUtils.createSocketAddr(authority, defPort);
    return buildTokenService(addr).toString();
   }
  
  /**
   * Get the host name from the principal name of format <service>/host@realm.
   * @param principalName principal name of format as described above
   * @return host name if the the string conforms to the above format, else null
   */
  public static @OsUntrusted String getHostFromPrincipal(@OsUntrusted String principalName) {
    return new @OsUntrusted HadoopKerberosName(principalName).getHostName();
  }

  private static @OsUntrusted ServiceLoader<@OsUntrusted SecurityInfo> securityInfoProviders = 
    ServiceLoader.load(SecurityInfo.class);
  private static @OsUntrusted SecurityInfo @OsUntrusted [] testProviders = new @OsUntrusted SecurityInfo @OsUntrusted [0];

  /**
   * Test setup method to register additional providers.
   * @param providers a list of high priority providers to use
   */
  @InterfaceAudience.Private
  public static void setSecurityInfoProviders(@OsUntrusted SecurityInfo @OsUntrusted ... providers) {
    testProviders = providers;
  }
  
  /**
   * Look up the KerberosInfo for a given protocol. It searches all known
   * SecurityInfo providers.
   * @param protocol the protocol class to get the information for
   * @param conf configuration object
   * @return the KerberosInfo or null if it has no KerberosInfo defined
   */
  public static @OsUntrusted KerberosInfo 
  getKerberosInfo(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol, @OsUntrusted Configuration conf) {
    synchronized (testProviders) {
      for(@OsUntrusted SecurityInfo provider: testProviders) {
        @OsUntrusted
        KerberosInfo result = provider.getKerberosInfo(protocol, conf);
        if (result != null) {
          return result;
        }
      }
    }
    
    synchronized (securityInfoProviders) {
      for(@OsUntrusted SecurityInfo provider: securityInfoProviders) {
        @OsUntrusted
        KerberosInfo result = provider.getKerberosInfo(protocol, conf);
        if (result != null) {
          return result;
        }
      }
    }
    return null;
  }
 
  /**
   * Look up the TokenInfo for a given protocol. It searches all known
   * SecurityInfo providers.
   * @param protocol The protocol class to get the information for.
   * @param conf Configuration object
   * @return the TokenInfo or null if it has no KerberosInfo defined
   */
  public static @OsUntrusted TokenInfo getTokenInfo(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol, @OsUntrusted Configuration conf) {
    synchronized (testProviders) {
      for(@OsUntrusted SecurityInfo provider: testProviders) {
        @OsUntrusted
        TokenInfo result = provider.getTokenInfo(protocol, conf);
        if (result != null) {
          return result;
        }      
      }
    }
    
    synchronized (securityInfoProviders) {
      for(@OsUntrusted SecurityInfo provider: securityInfoProviders) {
        @OsUntrusted
        TokenInfo result = provider.getTokenInfo(protocol, conf);
        if (result != null) {
          return result;
        }
      } 
    }
    
    return null;
  }

  /**
   * Decode the given token's service field into an InetAddress
   * @param token from which to obtain the service
   * @return InetAddress for the service
   */
  public static @OsUntrusted InetSocketAddress getTokenServiceAddr(@OsUntrusted Token<@OsUntrusted ? extends java.lang.@OsUntrusted Object> token) {
    return NetUtils.createSocketAddr(token.getService().toString());
  }

  /**
   * Set the given token's service to the format expected by the RPC client 
   * @param token a delegation token
   * @param addr the socket for the rpc connection
   */
  public static void setTokenService(@OsUntrusted Token<@OsUntrusted ? extends java.lang.@OsUntrusted Object> token, @OsUntrusted InetSocketAddress addr) {
    @OsUntrusted
    Text service = buildTokenService(addr);
    if (token != null) {
      token.setService(service);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Acquired token "+token);  // Token#toString() prints service
      }
    } else {
      LOG.warn("Failed to get token for service "+service);
    }
  }
  
  /**
   * Construct the service key for a token
   * @param addr InetSocketAddress of remote connection with a token
   * @return "ip:port" or "host:port" depending on the value of
   *          hadoop.security.token.service.use_ip
   */
  public static @OsUntrusted Text buildTokenService(@OsUntrusted InetSocketAddress addr) {
    @OsUntrusted
    String host = null;
    if (useIpForTokenService) {
      if (addr.isUnresolved()) { // host has no ip address
        throw new @OsUntrusted IllegalArgumentException(
            new @OsUntrusted UnknownHostException(addr.getHostName())
        );
      }
      host = addr.getAddress().getHostAddress();
    } else {
      host = addr.getHostName().toLowerCase();
    }
    return new @OsUntrusted Text(host + ":" + addr.getPort());
  }

  /**
   * Construct the service key for a token
   * @param uri of remote connection with a token
   * @return "ip:port" or "host:port" depending on the value of
   *          hadoop.security.token.service.use_ip
   */
  public static @OsUntrusted Text buildTokenService(@OsUntrusted URI uri) {
    return buildTokenService(NetUtils.createSocketAddr(uri.getAuthority()));
  }
  
  /**
   * Perform the given action as the daemon's login user. If the login
   * user cannot be determined, this will log a FATAL error and exit
   * the whole JVM.
   */
  public static <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted T doAsLoginUserOrFatal(@OsUntrusted PrivilegedAction<@OsUntrusted T> action) { 
    if (UserGroupInformation.isSecurityEnabled()) {
      @OsUntrusted
      UserGroupInformation ugi = null;
      try { 
        ugi = UserGroupInformation.getLoginUser();
      } catch (@OsUntrusted IOException e) {
        LOG.fatal("Exception while getting login user", e);
        e.printStackTrace();
        Runtime.getRuntime().exit(-1);
      }
      return ugi.doAs(action);
    } else {
      return action.run();
    }
  }
  
  /**
   * Perform the given action as the daemon's login user. If an
   * InterruptedException is thrown, it is converted to an IOException.
   *
   * @param action the action to perform
   * @return the result of the action
   * @throws IOException in the event of error
   */
  public static <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted T doAsLoginUser(@OsUntrusted PrivilegedExceptionAction<@OsUntrusted T> action)
      throws IOException {
    return doAsUser(UserGroupInformation.getLoginUser(), action);
  }

  /**
   * Perform the given action as the daemon's current user. If an
   * InterruptedException is thrown, it is converted to an IOException.
   *
   * @param action the action to perform
   * @return the result of the action
   * @throws IOException in the event of error
   */
  public static <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted T doAsCurrentUser(@OsUntrusted PrivilegedExceptionAction<@OsUntrusted T> action)
      throws IOException {
    return doAsUser(UserGroupInformation.getCurrentUser(), action);
  }

  private static <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted T doAsUser(@OsUntrusted UserGroupInformation ugi,
      @OsUntrusted
      PrivilegedExceptionAction<@OsUntrusted T> action) throws IOException {
    try {
      return ugi.doAs(action);
    } catch (@OsUntrusted InterruptedException ie) {
      throw new @OsUntrusted IOException(ie);
    }
  }

  /**
   * Open a (if need be) secure connection to a URL in a secure environment
   * that is using SPNEGO to authenticate its URLs. All Namenode and Secondary
   * Namenode URLs that are protected via SPNEGO should be accessed via this
   * method.
   *
   * @param url to authenticate via SPNEGO.
   * @return A connection that has been authenticated via SPNEGO
   * @throws IOException If unable to authenticate via SPNEGO
   */
  public static @OsUntrusted URLConnection openSecureHttpConnection(@OsUntrusted URL url) throws IOException {
    if (!HttpConfig.isSecure() && !UserGroupInformation.isSecurityEnabled()) {
      return url.openConnection();
    }

    AuthenticatedURL.@OsUntrusted Token token = new AuthenticatedURL.@OsUntrusted Token();
    try {
      return new @OsUntrusted AuthenticatedURL(null, sslFactory).openConnection(url, token);
    } catch (@OsUntrusted AuthenticationException e) {
      throw new @OsUntrusted IOException("Exception trying to open authenticated connection to "
              + url, e);
    }
  }

  /**
   * Resolves a host subject to the security requirements determined by
   * hadoop.security.token.service.use_ip.
   * 
   * @param hostname host or ip to resolve
   * @return a resolved host
   * @throws UnknownHostException if the host doesn't exist
   */
  @InterfaceAudience.Private
  public static
  @OsUntrusted
  InetAddress getByName(@OsUntrusted String hostname) throws UnknownHostException {
    return hostResolver.getByName(hostname);
  }
  
  interface HostResolver {
    @OsUntrusted
    InetAddress getByName(SecurityUtil.@OsUntrusted HostResolver this, @OsUntrusted String host) throws UnknownHostException;    
  }
  
  /**
   * Uses standard java host resolution
   */
  static class StandardHostResolver implements @OsUntrusted HostResolver {
    @Override
    public @OsUntrusted InetAddress getByName(SecurityUtil.@OsUntrusted StandardHostResolver this, @OsUntrusted String host) throws UnknownHostException {
      return InetAddress.getByName(host);
    }
  }
  
  /**
   * This an alternate resolver with important properties that the standard
   * java resolver lacks:
   * 1) The hostname is fully qualified.  This avoids security issues if not
   *    all hosts in the cluster do not share the same search domains.  It
   *    also prevents other hosts from performing unnecessary dns searches.
   *    In contrast, InetAddress simply returns the host as given.
   * 2) The InetAddress is instantiated with an exact host and IP to prevent
   *    further unnecessary lookups.  InetAddress may perform an unnecessary
   *    reverse lookup for an IP.
   * 3) A call to getHostName() will always return the qualified hostname, or
   *    more importantly, the IP if instantiated with an IP.  This avoids
   *    unnecessary dns timeouts if the host is not resolvable.
   * 4) Point 3 also ensures that if the host is re-resolved, ex. during a
   *    connection re-attempt, that a reverse lookup to host and forward
   *    lookup to IP is not performed since the reverse/forward mappings may
   *    not always return the same IP.  If the client initiated a connection
   *    with an IP, then that IP is all that should ever be contacted.
   *    
   * NOTE: this resolver is only used if:
   *       hadoop.security.token.service.use_ip=false 
   */
  protected static class QualifiedHostResolver implements @OsUntrusted HostResolver {
    @SuppressWarnings("unchecked")
    private @OsUntrusted List<@OsUntrusted String> searchDomains =
        ResolverConfiguration.open().searchlist();
    
    /**
     * Create an InetAddress with a fully qualified hostname of the given
     * hostname.  InetAddress does not qualify an incomplete hostname that
     * is resolved via the domain search list.
     * {@link InetAddress#getCanonicalHostName()} will fully qualify the
     * hostname, but it always return the A record whereas the given hostname
     * may be a CNAME.
     * 
     * @param host a hostname or ip address
     * @return InetAddress with the fully qualified hostname or ip
     * @throws UnknownHostException if host does not exist
     */
    @Override
    public @OsUntrusted InetAddress getByName(SecurityUtil.@OsUntrusted QualifiedHostResolver this, @OsUntrusted String host) throws UnknownHostException {
      @OsUntrusted
      InetAddress addr = null;

      if (IPAddressUtil.isIPv4LiteralAddress(host)) {
        // use ipv4 address as-is
        @OsUntrusted
        byte @OsUntrusted [] ip = IPAddressUtil.textToNumericFormatV4(host);
        addr = InetAddress.getByAddress(host, ip);
      } else if (IPAddressUtil.isIPv6LiteralAddress(host)) {
        // use ipv6 address as-is
        @OsUntrusted
        byte @OsUntrusted [] ip = IPAddressUtil.textToNumericFormatV6(host);
        addr = InetAddress.getByAddress(host, ip);
      } else if (host.endsWith(".")) {
        // a rooted host ends with a dot, ex. "host."
        // rooted hosts never use the search path, so only try an exact lookup
        addr = getByExactName(host);
      } else if (host.contains(".")) {
        // the host contains a dot (domain), ex. "host.domain"
        // try an exact host lookup, then fallback to search list
        addr = getByExactName(host);
        if (addr == null) {
          addr = getByNameWithSearch(host);
        }
      } else {
        // it's a simple host with no dots, ex. "host"
        // try the search list, then fallback to exact host
        @OsUntrusted
        InetAddress loopback = InetAddress.getByName(null);
        if (host.equalsIgnoreCase(loopback.getHostName())) {
          addr = InetAddress.getByAddress(host, loopback.getAddress());
        } else {
          addr = getByNameWithSearch(host);
          if (addr == null) {
            addr = getByExactName(host);
          }
        }
      }
      // unresolvable!
      if (addr == null) {
        throw new @OsUntrusted UnknownHostException(host);
      }
      return addr;
    }

    @OsUntrusted
    InetAddress getByExactName(SecurityUtil.@OsUntrusted QualifiedHostResolver this, @OsUntrusted String host) {
      @OsUntrusted
      InetAddress addr = null;
      // InetAddress will use the search list unless the host is rooted
      // with a trailing dot.  The trailing dot will disable any use of the
      // search path in a lower level resolver.  See RFC 1535.
      @OsUntrusted
      String fqHost = host;
      if (!fqHost.endsWith(".")) fqHost += ".";
      try {
        addr = getInetAddressByName(fqHost);
        // can't leave the hostname as rooted or other parts of the system
        // malfunction, ex. kerberos principals are lacking proper host
        // equivalence for rooted/non-rooted hostnames
        addr = InetAddress.getByAddress(host, addr.getAddress());
      } catch (@OsUntrusted UnknownHostException e) {
        // ignore, caller will throw if necessary
      }
      return addr;
    }

    @OsUntrusted
    InetAddress getByNameWithSearch(SecurityUtil.@OsUntrusted QualifiedHostResolver this, @OsUntrusted String host) {
      @OsUntrusted
      InetAddress addr = null;
      if (host.endsWith(".")) { // already qualified?
        addr = getByExactName(host); 
      } else {
        for (@OsUntrusted String domain : searchDomains) {
          @OsUntrusted
          String dot = !domain.startsWith(".") ? "." : "";
          addr = getByExactName(host + dot + domain);
          if (addr != null) break;
        }
      }
      return addr;
    }

    // implemented as a separate method to facilitate unit testing
    @OsUntrusted
    InetAddress getInetAddressByName(SecurityUtil.@OsUntrusted QualifiedHostResolver this, @OsUntrusted String host) throws UnknownHostException {
      return InetAddress.getByName(host);
    }

    void setSearchDomains(SecurityUtil.@OsUntrusted QualifiedHostResolver this, @OsUntrusted String @OsUntrusted ... domains) {
      searchDomains = Arrays.asList(domains);
    }
  }

  public static @OsUntrusted AuthenticationMethod getAuthenticationMethod(@OsUntrusted Configuration conf) {
    @OsUntrusted
    String value = conf.get(HADOOP_SECURITY_AUTHENTICATION, "simple");
    try {
      return Enum.valueOf(AuthenticationMethod.class, value.toUpperCase(Locale.ENGLISH));
    } catch (@OsUntrusted IllegalArgumentException iae) {
      throw new @OsUntrusted IllegalArgumentException("Invalid attribute value for " +
          HADOOP_SECURITY_AUTHENTICATION + " of " + value);
    }
  }

  @SuppressWarnings("ostrusted:cast.unsafe")
  public static void setAuthenticationMethod(
      @OsUntrusted
      AuthenticationMethod authenticationMethod, @OsUntrusted Configuration conf) {
    if (authenticationMethod == null) {
      authenticationMethod = AuthenticationMethod.SIMPLE;
    }

    //ostrusted, AuthenticationMethod is an enum composed of Trusted fields, its toString method is trusted
    conf.set(HADOOP_SECURITY_AUTHENTICATION, (@OsTrusted String) authenticationMethod.toString().toLowerCase(Locale.ENGLISH));
  }
}
