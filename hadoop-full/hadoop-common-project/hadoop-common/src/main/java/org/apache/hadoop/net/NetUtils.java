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
package org.apache.hadoop.net;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import ostrusted.quals.PolyOsTrusted;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.NoRouteToHostException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.net.ConnectException;
import java.nio.channels.SocketChannel;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.net.util.SubnetUtils.SubnetInfo;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Preconditions;

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class NetUtils {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(NetUtils.class);
  
  private static @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> hostToResolved = 
                                     new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted String>();
  /** text to point users elsewhere: {@value} */
  private static final @OsUntrusted String FOR_MORE_DETAILS_SEE
      = " For more details see:  ";
  /** text included in wrapped exceptions if the host is null: {@value} */
  public static final @OsUntrusted String UNKNOWN_HOST = "(unknown)";
  /** Base URL of the Hadoop Wiki: {@value} */
  public static final @OsUntrusted String HADOOP_WIKI = "http://wiki.apache.org/hadoop/";

  /**
   * Get the socket factory for the given class according to its
   * configuration parameter
   * <tt>hadoop.rpc.socket.factory.class.&lt;ClassName&gt;</tt>. When no
   * such parameter exists then fall back on the default socket factory as
   * configured by <tt>hadoop.rpc.socket.factory.class.default</tt>. If
   * this default socket factory is not configured, then fall back on the JVM
   * default socket factory.
   * 
   * @param conf the configuration
   * @param clazz the class (usually a {@link VersionedProtocol})
   * @return a socket factory
   */
  public static @OsUntrusted SocketFactory getSocketFactory(@OsUntrusted Configuration conf,
      @OsUntrusted
      Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> clazz) {

    @OsUntrusted
    SocketFactory factory = null;

    @OsUntrusted
    String propValue =
        conf.get("hadoop.rpc.socket.factory.class." + clazz.getSimpleName());
    if ((propValue != null) && (propValue.length() > 0))
      factory = getSocketFactoryFromProperty(conf, propValue);

    if (factory == null)
      factory = getDefaultSocketFactory(conf);

    return factory;
  }

  /**
   * Get the default socket factory as specified by the configuration
   * parameter <tt>hadoop.rpc.socket.factory.default</tt>
   * 
   * @param conf the configuration
   * @return the default socket factory as specified in the configuration or
   *         the JVM default socket factory if the configuration does not
   *         contain a default socket factory property.
   */
  public static @OsUntrusted SocketFactory getDefaultSocketFactory(@OsUntrusted Configuration conf) {

    @OsUntrusted
    String propValue = conf.get(
        CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
        CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_DEFAULT);
    if ((propValue == null) || (propValue.length() == 0))
      return SocketFactory.getDefault();

    return getSocketFactoryFromProperty(conf, propValue);
  }

  /**
   * Get the socket factory corresponding to the given proxy URI. If the
   * given proxy URI corresponds to an absence of configuration parameter,
   * returns null. If the URI is malformed raises an exception.
   * 
   * @param propValue the property which is the class name of the
   *        SocketFactory to instantiate; assumed non null and non empty.
   * @return a socket factory as defined in the property value.
   */
  public static @OsUntrusted SocketFactory getSocketFactoryFromProperty(
      @OsUntrusted
      Configuration conf, @OsUntrusted String propValue) {

    try {
      @OsUntrusted
      Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> theClass = conf.getClassByName(propValue);
      return (@OsUntrusted SocketFactory) ReflectionUtils.newInstance(theClass, conf);

    } catch (@OsUntrusted ClassNotFoundException cnfe) {
      throw new @OsUntrusted RuntimeException("Socket Factory class not found: " + cnfe);
    }
  }

  /**
   * Util method to build socket addr from either:
   *   <host>:<port>
   *   <fs>://<host>:<port>/<path>
   */
  public static @OsUntrusted InetSocketAddress createSocketAddr(@OsUntrusted String target) {
    return createSocketAddr(target, -1);
  }

  /**
   * Util method to build socket addr from either:
   *   <host>
   *   <host>:<port>
   *   <fs>://<host>:<port>/<path>
   */
  public static @OsUntrusted InetSocketAddress createSocketAddr(@OsUntrusted String target,
                                                   @OsUntrusted
                                                   int defaultPort) {
    return createSocketAddr(target, defaultPort, null);
  }

  /**
   * Create an InetSocketAddress from the given target string and
   * default port. If the string cannot be parsed correctly, the
   * <code>configName</code> parameter is used as part of the
   * exception message, allowing the user to better diagnose
   * the misconfiguration.
   *
   * @param target a string of either "host" or "host:port"
   * @param defaultPort the default port if <code>target</code> does not
   *                    include a port number
   * @param configName the name of the configuration from which
   *                   <code>target</code> was loaded. This is used in the
   *                   exception message in the case that parsing fails. 
   */
  public static @OsUntrusted InetSocketAddress createSocketAddr(@OsUntrusted String target,
                                                   @OsUntrusted
                                                   int defaultPort,
                                                   @OsUntrusted
                                                   String configName) {
    @OsUntrusted
    String helpText = "";
    if (configName != null) {
      helpText = " (configuration property '" + configName + "')";
    }
    if (target == null) {
      throw new @OsUntrusted IllegalArgumentException("Target address cannot be null." +
          helpText);
    }
    @OsUntrusted
    boolean hasScheme = target.contains("://");    
    @OsUntrusted
    URI uri = null;
    try {
      uri = hasScheme ? URI.create(target) : URI.create("dummyscheme://"+target);
    } catch (@OsUntrusted IllegalArgumentException e) {
      throw new @OsUntrusted IllegalArgumentException(
          "Does not contain a valid host:port authority: " + target + helpText
      );
    }

    @OsUntrusted
    String host = uri.getHost();
    @OsUntrusted
    int port = uri.getPort();
    if (port == -1) {
      port = defaultPort;
    }
    @OsUntrusted
    String path = uri.getPath();
    
    if ((host == null) || (port < 0) ||
        (!hasScheme && path != null && !path.isEmpty()))
    {
      throw new @OsUntrusted IllegalArgumentException(
          "Does not contain a valid host:port authority: " + target + helpText
      );
    }
    return createSocketAddrForHost(host, port);
  }

  /**
   * Create a socket address with the given host and port.  The hostname
   * might be replaced with another host that was set via
   * {@link #addStaticResolution(String, String)}.  The value of
   * hadoop.security.token.service.use_ip will determine whether the
   * standard java host resolver is used, or if the fully qualified resolver
   * is used.
   * @param host the hostname or IP use to instantiate the object
   * @param port the port number
   * @return InetSocketAddress
   */
  public static @OsUntrusted InetSocketAddress createSocketAddrForHost(@OsUntrusted String host, @OsUntrusted int port) {
    @OsUntrusted
    String staticHost = getStaticResolution(host);
    @OsUntrusted
    String resolveHost = (staticHost != null) ? staticHost : host;
    
    @OsUntrusted
    InetSocketAddress addr;
    try {
      @OsUntrusted
      InetAddress iaddr = SecurityUtil.getByName(resolveHost);
      // if there is a static entry for the host, make the returned
      // address look like the original given host
      if (staticHost != null) {
        iaddr = InetAddress.getByAddress(host, iaddr.getAddress());
      }
      addr = new @OsUntrusted InetSocketAddress(iaddr, port);
    } catch (@OsUntrusted UnknownHostException e) {
      addr = InetSocketAddress.createUnresolved(host, port);
    }
    return addr;
  }
  
  /**
   * Resolve the uri's hostname and add the default port if not in the uri
   * @param uri to resolve
   * @param defaultPort if none is given
   * @return URI
   */
  public static @OsUntrusted URI getCanonicalUri(@OsUntrusted URI uri, @OsUntrusted int defaultPort) {
    // skip if there is no authority, ie. "file" scheme or relative uri
    @OsUntrusted
    String host = uri.getHost();
    if (host == null) {
      return uri;
    }
    @OsUntrusted
    String fqHost = canonicalizeHost(host);
    @OsUntrusted
    int port = uri.getPort();
    // short out if already canonical with a port
    if (host.equals(fqHost) && port != -1) {
      return uri;
    }
    // reconstruct the uri with the canonical host and port
    try {
      uri = new @OsUntrusted URI(uri.getScheme(), uri.getUserInfo(),
          fqHost, (port == -1) ? defaultPort : port,
          uri.getPath(), uri.getQuery(), uri.getFragment());
    } catch (@OsUntrusted URISyntaxException e) {
      throw new @OsUntrusted IllegalArgumentException(e);
    }
    return uri;
  }  

  // cache the canonicalized hostnames;  the cache currently isn't expired,
  // but the canonicals will only change if the host's resolver configuration
  // changes
  private static final @OsUntrusted ConcurrentHashMap<@OsUntrusted String, @OsUntrusted String> canonicalizedHostCache =
      new @OsUntrusted ConcurrentHashMap<@OsUntrusted String, @OsUntrusted String>();

  private static @OsUntrusted String canonicalizeHost(@OsUntrusted String host) {
    // check if the host has already been canonicalized
    @OsUntrusted
    String fqHost = canonicalizedHostCache.get(host);
    if (fqHost == null) {
      try {
        fqHost = SecurityUtil.getByName(host).getHostName();
        // slight race condition, but won't hurt 
        canonicalizedHostCache.put(host, fqHost);
      } catch (@OsUntrusted UnknownHostException e) {
        fqHost = host;
      }
    }
    return fqHost;
  }

  /**
   * Adds a static resolution for host. This can be used for setting up
   * hostnames with names that are fake to point to a well known host. For e.g.
   * in some testcases we require to have daemons with different hostnames
   * running on the same machine. In order to create connections to these
   * daemons, one can set up mappings from those hostnames to "localhost".
   * {@link NetUtils#getStaticResolution(String)} can be used to query for
   * the actual hostname. 
   * @param host
   * @param resolvedName
   */
  public static void addStaticResolution(@OsUntrusted String host, @OsUntrusted String resolvedName) {
    synchronized (hostToResolved) {
      hostToResolved.put(host, resolvedName);
    }
  }
  
  /**
   * Retrieves the resolved name for the passed host. The resolved name must
   * have been set earlier using 
   * {@link NetUtils#addStaticResolution(String, String)}
   * @param host
   * @return the resolution
   */
  public static @OsUntrusted String getStaticResolution(@OsUntrusted String host) {
    synchronized (hostToResolved) {
      return hostToResolved.get(host);
    }
  }
  
  /**
   * This is used to get all the resolutions that were added using
   * {@link NetUtils#addStaticResolution(String, String)}. The return
   * value is a List each element of which contains an array of String 
   * of the form String[0]=hostname, String[1]=resolved-hostname
   * @return the list of resolutions
   */
  public static @OsUntrusted List <@OsUntrusted String @OsUntrusted []> getAllStaticResolutions() {
    synchronized (hostToResolved) {
      @OsUntrusted
      Set <@OsUntrusted Entry <@OsUntrusted String, @OsUntrusted String>>entries = hostToResolved.entrySet();
      if (entries.size() == 0) {
        return null;
      }
      @OsUntrusted
      List <@OsUntrusted String @OsUntrusted []> l = new @OsUntrusted ArrayList<@OsUntrusted String @OsUntrusted []>(entries.size());
      for (@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted String> e : entries) {
        l.add(new @OsUntrusted String @OsUntrusted [] {e.getKey(), e.getValue()});
      }
    return l;
    }
  }
  
  /**
   * Returns InetSocketAddress that a client can use to 
   * connect to the server. Server.getListenerAddress() is not correct when
   * the server binds to "0.0.0.0". This returns "hostname:port" of the server,
   * or "127.0.0.1:port" when the getListenerAddress() returns "0.0.0.0:port".
   * 
   * @param server
   * @return socket address that a client can use to connect to the server.
   */
  public static @OsUntrusted InetSocketAddress getConnectAddress(@OsUntrusted Server server) {
    return getConnectAddress(server.getListenerAddress());
  }
  
  /**
   * Returns an InetSocketAddress that a client can use to connect to the
   * given listening address.
   * 
   * @param addr of a listener
   * @return socket address that a client can use to connect to the server.
   */
  @SuppressWarnings("ostrusted") // TODO warnings on poly return;
  public static @PolyOsTrusted InetSocketAddress getConnectAddress(@PolyOsTrusted InetSocketAddress addr) {
    if (!addr.isUnresolved() && addr.getAddress().isAnyLocalAddress()) {
      try {
        addr = new @OsTrusted InetSocketAddress(InetAddress.getLocalHost(), addr.getPort());
      } catch (@OsUntrusted UnknownHostException uhe) {
        // shouldn't get here unless the host doesn't have a loopback iface
        addr = (@OsTrusted InetSocketAddress) createSocketAddrForHost("127.0.0.1", addr.getPort());
      }
    }
    return addr;
  }
  
  /**
   * Same as <code>getInputStream(socket, socket.getSoTimeout()).</code>
   * <br><br>
   * 
   * @see #getInputStream(Socket, long)
   */
  public static @OsUntrusted SocketInputWrapper getInputStream(@OsUntrusted Socket socket) 
                                           throws IOException {
    return getInputStream(socket, socket.getSoTimeout());
  }

  /**
   * Return a {@link SocketInputWrapper} for the socket and set the given
   * timeout. If the socket does not have an associated channel, then its socket
   * timeout will be set to the specified value. Otherwise, a
   * {@link SocketInputStream} will be created which reads with the configured
   * timeout.
   * 
   * Any socket created using socket factories returned by {@link #NetUtils},
   * must use this interface instead of {@link Socket#getInputStream()}.
   * 
   * In general, this should be called only once on each socket: see the note
   * in {@link SocketInputWrapper#setTimeout(long)} for more information.
   *
   * @see Socket#getChannel()
   * 
   * @param socket
   * @param timeout timeout in milliseconds. zero for waiting as
   *                long as necessary.
   * @return SocketInputWrapper for reading from the socket.
   * @throws IOException
   */
  public static @OsUntrusted SocketInputWrapper getInputStream(@OsUntrusted Socket socket, @OsUntrusted long timeout) 
                                           throws IOException {
    @OsUntrusted
    InputStream stm = (socket.getChannel() == null) ? 
          socket.getInputStream() : new @OsUntrusted SocketInputStream(socket);
    @OsUntrusted
    SocketInputWrapper w = new @OsUntrusted SocketInputWrapper(socket, stm);
    w.setTimeout(timeout);
    return w;
  }
  
  /**
   * Same as getOutputStream(socket, 0). Timeout of zero implies write will
   * wait until data is available.<br><br>
   * 
   * From documentation for {@link #getOutputStream(Socket, long)} : <br>
   * Returns OutputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a 
   * {@link SocketOutputStream} with the given timeout. If the socket does not
   * have a channel, {@link Socket#getOutputStream()} is returned. In the later
   * case, the timeout argument is ignored and the write will wait until 
   * data is available.<br><br>
   * 
   * Any socket created using socket factories returned by {@link NetUtils},
   * must use this interface instead of {@link Socket#getOutputStream()}.
   * 
   * @see #getOutputStream(Socket, long)
   * 
   * @param socket
   * @return OutputStream for writing to the socket.
   * @throws IOException
   */  
  public static @OsUntrusted OutputStream getOutputStream(@OsUntrusted Socket socket) 
                                             throws IOException {
    return getOutputStream(socket, 0);
  }
  
  /**
   * Returns OutputStream for the socket. If the socket has an associated
   * SocketChannel then it returns a 
   * {@link SocketOutputStream} with the given timeout. If the socket does not
   * have a channel, {@link Socket#getOutputStream()} is returned. In the later
   * case, the timeout argument is ignored and the write will wait until 
   * data is available.<br><br>
   * 
   * Any socket created using socket factories returned by {@link NetUtils},
   * must use this interface instead of {@link Socket#getOutputStream()}.
   * 
   * @see Socket#getChannel()
   * 
   * @param socket
   * @param timeout timeout in milliseconds. This may not always apply. zero
   *        for waiting as long as necessary.
   * @return OutputStream for writing to the socket.
   * @throws IOException   
   */
  public static @OsUntrusted OutputStream getOutputStream(@OsUntrusted Socket socket, @OsUntrusted long timeout) 
                                             throws IOException {
    return (socket.getChannel() == null) ? 
            socket.getOutputStream() : new @OsUntrusted SocketOutputStream(socket, timeout);            
  }
  
  /**
   * This is a drop-in replacement for 
   * {@link Socket#connect(SocketAddress, int)}.
   * In the case of normal sockets that don't have associated channels, this 
   * just invokes <code>socket.connect(endpoint, timeout)</code>. If 
   * <code>socket.getChannel()</code> returns a non-null channel,
   * connect is implemented using Hadoop's selectors. This is done mainly
   * to avoid Sun's connect implementation from creating thread-local 
   * selectors, since Hadoop does not have control on when these are closed
   * and could end up taking all the available file descriptors.
   * 
   * @see java.net.Socket#connect(java.net.SocketAddress, int)
   * 
   * @param socket
   * @param address the remote address
   * @param timeout timeout in milliseconds
   */
  public static void connect(@OsUntrusted Socket socket,
      @OsUntrusted
      SocketAddress address,
      @OsUntrusted
      int timeout) throws IOException {
    connect(socket, address, null, timeout);
  }

  /**
   * Like {@link NetUtils#connect(Socket, SocketAddress, int)} but
   * also takes a local address and port to bind the socket to. 
   * 
   * @param socket
   * @param endpoint the remote address
   * @param localAddr the local address to bind the socket to
   * @param timeout timeout in milliseconds
   */
  public static void connect(@OsUntrusted Socket socket, 
                             @OsUntrusted
                             SocketAddress endpoint,
                             @OsUntrusted
                             SocketAddress localAddr,
                             @OsUntrusted
                             int timeout) throws IOException {
    if (socket == null || endpoint == null || timeout < 0) {
      throw new @OsUntrusted IllegalArgumentException("Illegal argument for connect()");
    }
    
    @OsUntrusted
    SocketChannel ch = socket.getChannel();
    
    if (localAddr != null) {
      @OsUntrusted
      Class localClass = localAddr.getClass();
      @OsUntrusted
      Class remoteClass = endpoint.getClass();
      Preconditions.checkArgument(localClass.equals(remoteClass),
          "Local address %s must be of same family as remote address %s.",
          localAddr, endpoint);
      socket.bind(localAddr);
    }

    try {
      if (ch == null) {
        // let the default implementation handle it.
        socket.connect(endpoint, timeout);
      } else {
        SocketIOWithTimeout.connect(ch, endpoint, timeout);
      }
    } catch (@OsUntrusted SocketTimeoutException ste) {
      throw new @OsUntrusted ConnectTimeoutException(ste.getMessage());
    }

    // There is a very rare case allowed by the TCP specification, such that
    // if we are trying to connect to an endpoint on the local machine,
    // and we end up choosing an ephemeral port equal to the destination port,
    // we will actually end up getting connected to ourself (ie any data we
    // send just comes right back). This is only possible if the target
    // daemon is down, so we'll treat it like connection refused.
    if (socket.getLocalPort() == socket.getPort() &&
        socket.getLocalAddress().equals(socket.getInetAddress())) {
      LOG.info("Detected a loopback TCP socket, disconnecting it");
      socket.close();
      throw new @OsUntrusted ConnectException(
        "Localhost targeted connection resulted in a loopback. " +
        "No daemon is listening on the target port.");
    }
  }
  
  /** 
   * Given a string representation of a host, return its ip address
   * in textual presentation.
   * 
   * @param name a string representation of a host:
   *             either a textual representation its IP address or its host name
   * @return its IP address in the string format
   */
  public static @OsUntrusted String normalizeHostName(@OsUntrusted String name) {
    try {
      return InetAddress.getByName(name).getHostAddress();
    } catch (@OsUntrusted UnknownHostException e) {
      return name;
    }    
  }
  
  /** 
   * Given a collection of string representation of hosts, return a list of
   * corresponding IP addresses in the textual representation.
   * 
   * @param names a collection of string representations of hosts
   * @return a list of corresponding IP addresses in the string format
   * @see #normalizeHostName(String)
   */
  public static @OsUntrusted List<@OsUntrusted String> normalizeHostNames(@OsUntrusted Collection<@OsUntrusted String> names) {
    @OsUntrusted
    List<@OsUntrusted String> hostNames = new @OsUntrusted ArrayList<@OsUntrusted String>(names.size());
    for (@OsUntrusted String name : names) {
      hostNames.add(normalizeHostName(name));
    }
    return hostNames;
  }

  /**
   * Performs a sanity check on the list of hostnames/IPs to verify they at least
   * appear to be valid.
   * @param names - List of hostnames/IPs
   * @throws UnknownHostException
   */
  public static void verifyHostnames(@OsUntrusted String @OsUntrusted [] names) throws UnknownHostException {
    for (@OsUntrusted String name: names) {
      if (name == null) {
        throw new @OsUntrusted UnknownHostException("null hostname found");
      }
      // The first check supports URL formats (e.g. hdfs://, etc.). 
      // java.net.URI requires a schema, so we add a dummy one if it doesn't
      // have one already.
      @OsUntrusted
      URI uri = null;
      try {
        uri = new @OsUntrusted URI(name);
        if (uri.getHost() == null) {
          uri = new @OsUntrusted URI("http://" + name);
        }
      } catch (@OsUntrusted URISyntaxException e) {
        uri = null;
      }
      if (uri == null || uri.getHost() == null) {
        throw new @OsUntrusted UnknownHostException(name + " is not a valid Inet address");
      }
    }
  }

  private static final @OsUntrusted Pattern ipPortPattern = // Pattern for matching ip[:port]
    Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(:\\d+)?");
  
  /**
   * Attempt to obtain the host name of the given string which contains
   * an IP address and an optional port.
   * 
   * @param ipPort string of form ip[:port]
   * @return Host name or null if the name can not be determined
   */
  public static @OsUntrusted String getHostNameOfIP(@OsUntrusted String ipPort) {
    if (null == ipPort || !ipPortPattern.matcher(ipPort).matches()) {
      return null;
    }
    
    try {
      @OsUntrusted
      int colonIdx = ipPort.indexOf(':');
      @OsUntrusted
      String ip = (-1 == colonIdx) ? ipPort
          : ipPort.substring(0, ipPort.indexOf(':'));
      return InetAddress.getByName(ip).getHostName();
    } catch (@OsUntrusted UnknownHostException e) {
      return null;
    }
  }

  /**
   * Return hostname without throwing exception.
   * @return hostname
   */
  public static @OsUntrusted String getHostname() {
    try {return "" + InetAddress.getLocalHost();}
    catch(@OsUntrusted UnknownHostException uhe) {return "" + uhe;}
  }
  
  /**
   * Compose a "host:port" string from the address.
   */
  @SuppressWarnings("ostrusted:return.type.incompatible")
  public static @PolyOsTrusted String getHostPortString(@PolyOsTrusted InetSocketAddress addr) {
    return addr.getHostName() + ":" + addr.getPort();
  }
  
  /**
   * Checks if {@code host} is a local host name and return {@link InetAddress}
   * corresponding to that address.
   * 
   * @param host the specified host
   * @return a valid local {@link InetAddress} or null
   * @throws SocketException if an I/O error occurs
   */
  public static @OsUntrusted InetAddress getLocalInetAddress(@OsUntrusted String host)
      throws SocketException {
    if (host == null) {
      return null;
    }
    @OsUntrusted
    InetAddress addr = null;
    try {
      addr = SecurityUtil.getByName(host);
      if (NetworkInterface.getByInetAddress(addr) == null) {
        addr = null; // Not a local address
      }
    } catch (@OsUntrusted UnknownHostException ignore) { }
    return addr;
  }
  
  /**
   * Given an InetAddress, checks to see if the address is a local address, by
   * comparing the address with all the interfaces on the node.
   * @param addr address to check if it is local node's address
   * @return true if the address corresponds to the local node
   */
  public static @OsUntrusted boolean isLocalAddress(@OsUntrusted InetAddress addr) {
    // Check if the address is any local or loop back
    @OsUntrusted
    boolean local = addr.isAnyLocalAddress() || addr.isLoopbackAddress();

    // Check if the address is defined on any interface
    if (!local) {
      try {
        local = NetworkInterface.getByInetAddress(addr) != null;
      } catch (@OsUntrusted SocketException e) {
        local = false;
      }
    }
    return local;
  }

  /**
   * Take an IOException , the local host port and remote host port details and
   * return an IOException with the input exception as the cause and also
   * include the host details. The new exception provides the stack trace of the
   * place where the exception is thrown and some extra diagnostics information.
   * If the exception is BindException or ConnectException or
   * UnknownHostException or SocketTimeoutException, return a new one of the
   * same type; Otherwise return an IOException.
   *
   * @param destHost target host (nullable)
   * @param destPort target port
   * @param localHost local host (nullable)
   * @param localPort local port
   * @param exception the caught exception.
   * @return an exception to throw
   */
  public static @OsUntrusted IOException wrapException(final @OsUntrusted String destHost,
                                          final @OsUntrusted int destPort,
                                          final @OsUntrusted String localHost,
                                          final @OsUntrusted int localPort,
                                          final @OsUntrusted IOException exception) {
    if (exception instanceof @OsUntrusted BindException) {
      return new @OsUntrusted BindException(
          "Problem binding to ["
              + localHost
              + ":"
              + localPort
              + "] "
              + exception
              + ";"
              + see("BindException"));
    } else if (exception instanceof @OsUntrusted ConnectException) {
      // connection refused; include the host:port in the error
      return wrapWithMessage(exception, 
          "Call From "
              + localHost
              + " to "
              + destHost
              + ":"
              + destPort
              + " failed on connection exception: "
              + exception
              + ";"
              + see("ConnectionRefused"));
    } else if (exception instanceof @OsUntrusted UnknownHostException) {
      return wrapWithMessage(exception,
          "Invalid host name: "
              + getHostDetailsAsString(destHost, destPort, localHost)
              + exception
              + ";"
              + see("UnknownHost"));
    } else if (exception instanceof @OsUntrusted SocketTimeoutException) {
      return wrapWithMessage(exception,
          "Call From "
              + localHost + " to " + destHost + ":" + destPort
              + " failed on socket timeout exception: " + exception
              + ";"
              + see("SocketTimeout"));
    } else if (exception instanceof @OsUntrusted NoRouteToHostException) {
      return wrapWithMessage(exception,
          "No Route to Host from  "
              + localHost + " to " + destHost + ":" + destPort
              + " failed on socket timeout exception: " + exception
              + ";"
              + see("NoRouteToHost"));
    }
    else {
      return (@OsUntrusted IOException) new @OsUntrusted IOException("Failed on local exception: "
                                               + exception
                                               + "; Host Details : "
                                               + getHostDetailsAsString(destHost, destPort, localHost))
          .initCause(exception);

    }
  }

  private static @OsUntrusted String see(final @OsUntrusted String entry) {
    return FOR_MORE_DETAILS_SEE + HADOOP_WIKI + entry;
  }
  
  @SuppressWarnings("unchecked")
  private static <@OsUntrusted T extends @OsUntrusted IOException> @OsUntrusted T wrapWithMessage(
      @OsUntrusted
      T exception, @OsUntrusted String msg) {
    @OsUntrusted
    Class<@OsUntrusted ? extends @OsUntrusted Throwable> clazz = exception.getClass();
    try {
      @OsUntrusted
      Constructor<@OsUntrusted ? extends @OsUntrusted Throwable> ctor = clazz.getConstructor(String.class);
      @OsUntrusted
      Throwable t = ctor.newInstance(msg);
      return (@OsUntrusted T)(t.initCause(exception));
    } catch (@OsUntrusted Throwable e) {
      LOG.warn("Unable to wrap exception of type " +
          clazz + ": it has no (String) constructor", e);
      return exception;
    }
  }

  /**
   * Get the host details as a string
   * @param destHost destinatioon host (nullable)
   * @param destPort destination port
   * @param localHost local host (nullable)
   * @return a string describing the destination host:port and the local host
   */
  private static @OsUntrusted String getHostDetailsAsString(final @OsUntrusted String destHost,
                                               final @OsUntrusted int destPort,
                                               final @OsUntrusted String localHost) {
    @OsUntrusted
    StringBuilder hostDetails = new @OsUntrusted StringBuilder(27);
    hostDetails.append("local host is: ")
        .append(quoteHost(localHost))
        .append("; ");
    hostDetails.append("destination host is: ").append(quoteHost(destHost))
        .append(":")
        .append(destPort).append("; ");
    return hostDetails.toString();
  }

  /**
   * Quote a hostname if it is not null
   * @param hostname the hostname; nullable
   * @return a quoted hostname or {@link #UNKNOWN_HOST} if the hostname is null
   */
  private static @OsUntrusted String quoteHost(final @OsUntrusted String hostname) {
    return (hostname != null) ?
        ("\"" + hostname + "\"")
        : UNKNOWN_HOST;
  }

  /**
   * @return true if the given string is a subnet specified
   *     using CIDR notation, false otherwise
   */
  public static @OsUntrusted boolean isValidSubnet(@OsUntrusted String subnet) {
    try {
      new @OsUntrusted SubnetUtils(subnet);
      return true;
    } catch (@OsUntrusted IllegalArgumentException iae) {
      return false;
    }
  }

  /**
   * Add all addresses associated with the given nif in the
   * given subnet to the given list.
   */
  private static void addMatchingAddrs(@OsUntrusted NetworkInterface nif,
      @OsUntrusted
      SubnetInfo subnetInfo, @OsUntrusted List<@OsUntrusted InetAddress> addrs) {
    @OsUntrusted
    Enumeration<@OsUntrusted InetAddress> ifAddrs = nif.getInetAddresses();
    while (ifAddrs.hasMoreElements()) {
      @OsUntrusted
      InetAddress ifAddr = ifAddrs.nextElement();
      if (subnetInfo.isInRange(ifAddr.getHostAddress())) {
        addrs.add(ifAddr);
      }
    }
  }

  /**
   * Return an InetAddress for each interface that matches the
   * given subnet specified using CIDR notation.
   *
   * @param subnet subnet specified using CIDR notation
   * @param returnSubinterfaces
   *            whether to return IPs associated with subinterfaces
   * @throws IllegalArgumentException if subnet is invalid
   */
  public static @OsUntrusted List<@OsUntrusted InetAddress> getIPs(@OsUntrusted String subnet,
      @OsUntrusted
      boolean returnSubinterfaces) {
    @OsUntrusted
    List<@OsUntrusted InetAddress> addrs = new @OsUntrusted ArrayList<@OsUntrusted InetAddress>();
    @OsUntrusted
    SubnetInfo subnetInfo = new @OsUntrusted SubnetUtils(subnet).getInfo();
    @OsUntrusted
    Enumeration<@OsUntrusted NetworkInterface> nifs;

    try {
      nifs = NetworkInterface.getNetworkInterfaces();
    } catch (@OsUntrusted SocketException e) {
      LOG.error("Unable to get host interfaces", e);
      return addrs;
    }

    while (nifs.hasMoreElements()) {
      @OsUntrusted
      NetworkInterface nif = nifs.nextElement();
      // NB: adding addresses even if the nif is not up
      addMatchingAddrs(nif, subnetInfo, addrs);

      if (!returnSubinterfaces) {
        continue;
      }
      @OsUntrusted
      Enumeration<@OsUntrusted NetworkInterface> subNifs = nif.getSubInterfaces();
      while (subNifs.hasMoreElements()) {
        addMatchingAddrs(subNifs.nextElement(), subnetInfo, addrs);
      }
    }
    return addrs;
  }

  /**
   * Return a free port number. There is no guarantee it will remain free, so
   * it should be used immediately.
   *
   * @returns A free port for binding a local socket
   */
  public static @OsUntrusted int getFreeSocketPort() {
    @OsUntrusted
    int port = 0;
    try {
      @OsUntrusted
      ServerSocket s = new @OsUntrusted ServerSocket(0);
      port = s.getLocalPort();
      s.close();
      return port;
    } catch (@OsUntrusted IOException e) {
      // Could not get a free port. Return default port 0.
    }
    return port;
  }
}
