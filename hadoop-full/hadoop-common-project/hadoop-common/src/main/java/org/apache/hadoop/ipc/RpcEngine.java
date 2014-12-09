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

package org.apache.hadoop.ipc;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;

/** An RPC implementation. */
@InterfaceStability.Evolving
public interface RpcEngine {

  /** Construct a client-side proxy object. 
   * @param <T>*/
  <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted ProtocolProxy<@OsUntrusted T> getProxy(@OsUntrusted RpcEngine this, @OsUntrusted Class<@OsUntrusted T> protocol,
                  @OsUntrusted
                  long clientVersion, @OsUntrusted InetSocketAddress addr,
                  @OsUntrusted
                  UserGroupInformation ticket, @OsUntrusted Configuration conf,
                  @OsUntrusted
                  SocketFactory factory, @OsUntrusted int rpcTimeout,
                  @OsUntrusted
                  RetryPolicy connectionRetryPolicy) throws IOException;

  /** 
   * Construct a server for a protocol implementation instance.
   * 
   * @param protocol the class of protocol to use
   * @param instance the instance of protocol whose methods will be called
   * @param conf the configuration to use
   * @param bindAddress the address to bind on to listen for connection
   * @param port the port to listen for connections on
   * @param numHandlers the number of method handler threads to run
   * @param numReaders the number of reader threads to run
   * @param queueSizePerHandler the size of the queue per hander thread
   * @param verbose whether each call should be logged
   * @param secretManager The secret manager to use to validate incoming requests.
   * @param portRangeConfig A config parameter that can be used to restrict
   *        the range of ports used when port is 0 (an ephemeral port)
   * @return The Server instance
   * @throws IOException on any error
   */
  RPC.@OsUntrusted Server getServer(@OsUntrusted RpcEngine this, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol, @OsUntrusted Object instance, @OsUntrusted String bindAddress,
                       @OsUntrusted
                       int port, @OsUntrusted int numHandlers, @OsUntrusted int numReaders,
                       @OsUntrusted
                       int queueSizePerHandler, @OsUntrusted boolean verbose,
                       @OsUntrusted
                       Configuration conf, 
                       @OsUntrusted
                       SecretManager<@OsUntrusted ? extends @OsUntrusted TokenIdentifier> secretManager,
                       @OsUntrusted
                       String portRangeConfig
                       ) throws IOException;

  /**
   * Returns a proxy for ProtocolMetaInfoPB, which uses the given connection
   * id.
   * @param connId, ConnectionId to be used for the proxy.
   * @param conf, Configuration.
   * @param factory, Socket factory.
   * @return Proxy object.
   * @throws IOException
   */
  @OsUntrusted
  ProtocolProxy<@OsUntrusted ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
      @OsUntrusted RpcEngine this, @OsUntrusted
      ConnectionId connId, @OsUntrusted Configuration conf, @OsUntrusted SocketFactory factory)
      throws IOException;
}
