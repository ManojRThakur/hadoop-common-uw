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
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * Specialized SocketFactory to create sockets with a SOCKS proxy
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SocksSocketFactory extends @OsUntrusted SocketFactory implements
    @OsUntrusted
    Configurable {

  private @OsUntrusted Configuration conf;

  private @OsUntrusted Proxy proxy;

  /**
   * Default empty constructor (for use with the reflection API).
   */
  public @OsUntrusted SocksSocketFactory() {
    this.proxy = Proxy.NO_PROXY;
  }

  /**
   * Constructor with a supplied Proxy
   * 
   * @param proxy the proxy to use to create sockets
   */
  public @OsUntrusted SocksSocketFactory(@OsUntrusted Proxy proxy) {
    this.proxy = proxy;
  }

  @Override
  public @OsUntrusted Socket createSocket(@OsUntrusted SocksSocketFactory this) throws IOException {

    return new @OsUntrusted Socket(proxy);
  }

  @Override
  public @OsUntrusted Socket createSocket(@OsUntrusted SocksSocketFactory this, @OsUntrusted InetAddress addr, @OsUntrusted int port) throws IOException {

    @OsUntrusted
    Socket socket = createSocket();
    socket.connect(new @OsUntrusted InetSocketAddress(addr, port));
    return socket;
  }

  @Override
  public @OsUntrusted Socket createSocket(@OsUntrusted SocksSocketFactory this, @OsUntrusted InetAddress addr, @OsUntrusted int port,
      @OsUntrusted
      InetAddress localHostAddr, @OsUntrusted int localPort) throws IOException {

    @OsUntrusted
    Socket socket = createSocket();
    socket.bind(new @OsUntrusted InetSocketAddress(localHostAddr, localPort));
    socket.connect(new @OsUntrusted InetSocketAddress(addr, port));
    return socket;
  }

  @Override
  public @OsUntrusted Socket createSocket(@OsUntrusted SocksSocketFactory this, @OsUntrusted String host, @OsUntrusted int port) throws IOException,
      UnknownHostException {

    @OsUntrusted
    Socket socket = createSocket();
    socket.connect(new @OsUntrusted InetSocketAddress(host, port));
    return socket;
  }

  @Override
  public @OsUntrusted Socket createSocket(@OsUntrusted SocksSocketFactory this, @OsUntrusted String host, @OsUntrusted int port,
      @OsUntrusted
      InetAddress localHostAddr, @OsUntrusted int localPort) throws IOException,
      UnknownHostException {

    @OsUntrusted
    Socket socket = createSocket();
    socket.bind(new @OsUntrusted InetSocketAddress(localHostAddr, localPort));
    socket.connect(new @OsUntrusted InetSocketAddress(host, port));
    return socket;
  }

  @Override
  public @OsUntrusted int hashCode(@OsUntrusted SocksSocketFactory this) {
    return proxy.hashCode();
  }

  @Override
  public @OsUntrusted boolean equals(@OsUntrusted SocksSocketFactory this, @OsUntrusted Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof @OsUntrusted SocksSocketFactory))
      return false;
    final @OsUntrusted SocksSocketFactory other = (@OsUntrusted SocksSocketFactory) obj;
    if (proxy == null) {
      if (other.proxy != null)
        return false;
    } else if (!proxy.equals(other.proxy))
      return false;
    return true;
  }

  @Override
  public @OsUntrusted Configuration getConf(@OsUntrusted SocksSocketFactory this) {
    return this.conf;
  }

  @Override
  public void setConf(@OsUntrusted SocksSocketFactory this, @OsUntrusted Configuration conf) {
    this.conf = conf;
    @OsUntrusted
    String proxyStr = conf.get("hadoop.socks.server");
    if ((proxyStr != null) && (proxyStr.length() > 0)) {
      setProxy(proxyStr);
    }
  }

  /**
   * Set the proxy of this socket factory as described in the string
   * parameter
   * 
   * @param proxyStr the proxy address using the format "host:port"
   */
  private void setProxy(@OsUntrusted SocksSocketFactory this, @OsUntrusted String proxyStr) {
    @OsUntrusted
    String @OsUntrusted [] strs = proxyStr.split(":", 2);
    if (strs.length != 2)
      throw new @OsUntrusted RuntimeException("Bad SOCKS proxy parameter: " + proxyStr);
    @OsUntrusted
    String host = strs[0];
    @OsUntrusted
    int port = Integer.parseInt(strs[1]);
    this.proxy =
        new @OsUntrusted Proxy(Proxy.Type.SOCKS, InetSocketAddress.createUnresolved(host,
            port));
  }
}
