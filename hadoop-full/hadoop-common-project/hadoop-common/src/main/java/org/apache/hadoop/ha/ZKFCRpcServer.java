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
package org.apache.hadoop.ha;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.proto.ZKFCProtocolProtos.ZKFCProtocolService;
import org.apache.hadoop.ha.protocolPB.ZKFCProtocolPB;
import org.apache.hadoop.ha.protocolPB.ZKFCProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.authorize.PolicyProvider;

import com.google.protobuf.BlockingService;

@InterfaceAudience.LimitedPrivate("HDFS")
@InterfaceStability.Evolving
public class ZKFCRpcServer implements @OsUntrusted ZKFCProtocol {

  private static final @OsUntrusted int HANDLER_COUNT = 3;
  private final @OsUntrusted ZKFailoverController zkfc;
  private @OsUntrusted Server server;

  @OsUntrusted
  ZKFCRpcServer(@OsUntrusted Configuration conf,
      @OsUntrusted
      InetSocketAddress bindAddr,
      @OsUntrusted
      ZKFailoverController zkfc,
      @OsUntrusted
      PolicyProvider policy) throws IOException {
    this.zkfc = zkfc;
    
    RPC.setProtocolEngine(conf, ZKFCProtocolPB.class,
        ProtobufRpcEngine.class);
    @OsUntrusted
    ZKFCProtocolServerSideTranslatorPB translator =
        new @OsUntrusted ZKFCProtocolServerSideTranslatorPB(this);
    @OsUntrusted
    BlockingService service = ZKFCProtocolService
        .newReflectiveBlockingService(translator);
    this.server = new RPC.@OsUntrusted Builder(conf).setProtocol(ZKFCProtocolPB.class)
        .setInstance(service).setBindAddress(bindAddr.getHostName())
        .setPort(bindAddr.getPort()).setNumHandlers(HANDLER_COUNT)
        .setVerbose(false).build();
    
    // set service-level authorization security policy
    if (conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      server.refreshServiceAcl(conf, policy);
    }

  }
  
  void start(@OsUntrusted ZKFCRpcServer this) {
    this.server.start();
  }

  public @OsUntrusted InetSocketAddress getAddress(@OsUntrusted ZKFCRpcServer this) {
    return server.getListenerAddress();
  }

  void stopAndJoin(@OsUntrusted ZKFCRpcServer this) throws InterruptedException {
    this.server.stop();
    this.server.join();
  }
  
  @Override
  public void cedeActive(@OsUntrusted ZKFCRpcServer this, @OsUntrusted int millisToCede) throws IOException,
      AccessControlException {
    zkfc.checkRpcAdminAccess();
    zkfc.cedeActive(millisToCede);
  }

  @Override
  public void gracefulFailover(@OsUntrusted ZKFCRpcServer this) throws IOException, AccessControlException {
    zkfc.checkRpcAdminAccess();
    zkfc.gracefulFailoverToYou();
  }

}
