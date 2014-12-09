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
package org.apache.hadoop.ha.protocolPB;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ha.ZKFCProtocol;
import org.apache.hadoop.ha.proto.ZKFCProtocolProtos.CedeActiveRequestProto;
import org.apache.hadoop.ha.proto.ZKFCProtocolProtos.CedeActiveResponseProto;
import org.apache.hadoop.ha.proto.ZKFCProtocolProtos.GracefulFailoverRequestProto;
import org.apache.hadoop.ha.proto.ZKFCProtocolProtos.GracefulFailoverResponseProto;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class ZKFCProtocolServerSideTranslatorPB implements
    @OsUntrusted
    ZKFCProtocolPB {
  private final @OsUntrusted ZKFCProtocol server;
  
  public @OsUntrusted ZKFCProtocolServerSideTranslatorPB(@OsUntrusted ZKFCProtocol server) {
    this.server = server;
  }

  @Override
  public @OsUntrusted CedeActiveResponseProto cedeActive(@OsUntrusted ZKFCProtocolServerSideTranslatorPB this, @OsUntrusted RpcController controller,
      @OsUntrusted
      CedeActiveRequestProto request) throws ServiceException {
    try {
      server.cedeActive(request.getMillisToCede());
      return CedeActiveResponseProto.getDefaultInstance();
    } catch (@OsUntrusted IOException e) {
      throw new @OsUntrusted ServiceException(e);
    }
  }

  @Override
  public @OsUntrusted GracefulFailoverResponseProto gracefulFailover(
      @OsUntrusted ZKFCProtocolServerSideTranslatorPB this, @OsUntrusted
      RpcController controller, @OsUntrusted GracefulFailoverRequestProto request)
      throws ServiceException {
    try {
      server.gracefulFailover();
      return GracefulFailoverResponseProto.getDefaultInstance();
    } catch (@OsUntrusted IOException e) {
      throw new @OsUntrusted ServiceException(e);
    }
  }

  @Override
  public @OsUntrusted long getProtocolVersion(@OsUntrusted ZKFCProtocolServerSideTranslatorPB this, @OsUntrusted String protocol, @OsUntrusted long clientVersion)
      throws IOException {
    return RPC.getProtocolVersion(ZKFCProtocolPB.class);
  }

  @Override
  public @OsUntrusted ProtocolSignature getProtocolSignature(@OsUntrusted ZKFCProtocolServerSideTranslatorPB this, @OsUntrusted String protocol,
      @OsUntrusted
      long clientVersion, @OsUntrusted int clientMethodsHash) throws IOException {
    if (!protocol.equals(RPC.getProtocolName(ZKFCProtocolPB.class))) {
      throw new @OsUntrusted IOException("Serverside implements " +
          RPC.getProtocolName(ZKFCProtocolPB.class) +
          ". The following requested protocol is unknown: " + protocol);
    }

    return ProtocolSignature.getProtocolSignature(clientMethodsHash,
        RPC.getProtocolVersion(ZKFCProtocolPB.class),
        HAServiceProtocolPB.class);
  }

}
