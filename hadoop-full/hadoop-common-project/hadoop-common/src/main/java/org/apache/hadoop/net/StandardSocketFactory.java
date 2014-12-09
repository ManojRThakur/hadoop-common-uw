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
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Specialized SocketFactory to create sockets with a SOCKS proxy
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class StandardSocketFactory extends @OsUntrusted SocketFactory {

  /**
   * Default empty constructor (for use with the reflection API).
   */
  public @OsUntrusted StandardSocketFactory() {
  }

  @Override
  public @OsUntrusted Socket createSocket(@OsUntrusted StandardSocketFactory this) throws IOException {
    /*
     * NOTE: This returns an NIO socket so that it has an associated 
     * SocketChannel. As of now, this unfortunately makes streams returned
     * by Socket.getInputStream() and Socket.getOutputStream() unusable
     * (because a blocking read on input stream blocks write on output stream
     * and vice versa).
     * 
     * So users of these socket factories should use 
     * NetUtils.getInputStream(socket) and 
     * NetUtils.getOutputStream(socket) instead.
     * 
     * A solution for hiding from this from user is to write a 
     * 'FilterSocket' on the lines of FilterInputStream and extend it by
     * overriding getInputStream() and getOutputStream().
     */
    return SocketChannel.open().socket();
  }

  @Override
  public @OsUntrusted Socket createSocket(@OsUntrusted StandardSocketFactory this, @OsUntrusted InetAddress addr, @OsUntrusted int port) throws IOException {

    @OsUntrusted
    Socket socket = createSocket();
    socket.connect(new @OsUntrusted InetSocketAddress(addr, port));
    return socket;
  }

  @Override
  public @OsUntrusted Socket createSocket(@OsUntrusted StandardSocketFactory this, @OsUntrusted InetAddress addr, @OsUntrusted int port,
      @OsUntrusted
      InetAddress localHostAddr, @OsUntrusted int localPort) throws IOException {

    @OsUntrusted
    Socket socket = createSocket();
    socket.bind(new @OsUntrusted InetSocketAddress(localHostAddr, localPort));
    socket.connect(new @OsUntrusted InetSocketAddress(addr, port));
    return socket;
  }

  @Override
  public @OsUntrusted Socket createSocket(@OsUntrusted StandardSocketFactory this, @OsUntrusted String host, @OsUntrusted int port) throws IOException,
      UnknownHostException {

    @OsUntrusted
    Socket socket = createSocket();
    socket.connect(new @OsUntrusted InetSocketAddress(host, port));
    return socket;
  }

  @Override
  public @OsUntrusted Socket createSocket(@OsUntrusted StandardSocketFactory this, @OsUntrusted String host, @OsUntrusted int port,
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
  public @OsUntrusted boolean equals(@OsUntrusted StandardSocketFactory this, @OsUntrusted Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    return obj.getClass().equals(this.getClass());
  }

  @Override
  public @OsUntrusted int hashCode(@OsUntrusted StandardSocketFactory this) {
    return this.getClass().hashCode();
  }

}
