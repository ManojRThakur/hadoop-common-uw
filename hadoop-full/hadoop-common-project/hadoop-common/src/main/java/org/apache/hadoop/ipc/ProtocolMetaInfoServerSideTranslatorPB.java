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
import org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureRequestProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureResponseProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolVersionsRequestProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolVersionsResponseProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolSignatureProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolVersionProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class serves the requests for protocol versions and signatures by
 * looking them up in the server registry.
 */
public class ProtocolMetaInfoServerSideTranslatorPB implements
    @OsUntrusted
    ProtocolMetaInfoPB {

  RPC.@OsUntrusted Server server;
  
  public @OsUntrusted ProtocolMetaInfoServerSideTranslatorPB(RPC.@OsUntrusted Server server) {
    this.server = server;
  }
  
  @Override
  public @OsUntrusted GetProtocolVersionsResponseProto getProtocolVersions(
      @OsUntrusted ProtocolMetaInfoServerSideTranslatorPB this, @OsUntrusted
      RpcController controller, @OsUntrusted GetProtocolVersionsRequestProto request)
      throws ServiceException {
    @OsUntrusted
    String protocol = request.getProtocol();
    GetProtocolVersionsResponseProto.@OsUntrusted Builder builder = 
        GetProtocolVersionsResponseProto.newBuilder();
    for (RPC.@OsUntrusted RpcKind r : RPC.RpcKind.values()) {
      @OsUntrusted
      long @OsUntrusted [] versions;
      try {
        versions = getProtocolVersionForRpcKind(r, protocol);
      } catch (@OsUntrusted ClassNotFoundException e) {
        throw new @OsUntrusted ServiceException(e);
      }
      ProtocolVersionProto.@OsUntrusted Builder b = ProtocolVersionProto.newBuilder();
      if (versions != null) {
        b.setRpcKind(r.toString());
        for (@OsUntrusted long v : versions) {
          b.addVersions(v);
        }
      }
      builder.addProtocolVersions(b.build());
    }
    return builder.build();
  }

  @Override
  public @OsUntrusted GetProtocolSignatureResponseProto getProtocolSignature(
      @OsUntrusted ProtocolMetaInfoServerSideTranslatorPB this, @OsUntrusted
      RpcController controller, @OsUntrusted GetProtocolSignatureRequestProto request)
      throws ServiceException {
    GetProtocolSignatureResponseProto.@OsUntrusted Builder builder = GetProtocolSignatureResponseProto
        .newBuilder();
    @OsUntrusted
    String protocol = request.getProtocol();
    @OsUntrusted
    String rpcKind = request.getRpcKind();
    @OsUntrusted
    long @OsUntrusted [] versions;
    try {
      versions = getProtocolVersionForRpcKind(RPC.RpcKind.valueOf(rpcKind),
          protocol);
    } catch (@OsUntrusted ClassNotFoundException e1) {
      throw new @OsUntrusted ServiceException(e1);
    }
    if (versions == null) {
      return builder.build();
    }
    for (@OsUntrusted long v : versions) {
      ProtocolSignatureProto.@OsUntrusted Builder sigBuilder = ProtocolSignatureProto
          .newBuilder();
      sigBuilder.setVersion(v);
      try {
        @OsUntrusted
        ProtocolSignature signature = ProtocolSignature.getProtocolSignature(
            protocol, v);
        for (@OsUntrusted int m : signature.getMethods()) {
          sigBuilder.addMethods(m);
        }
      } catch (@OsUntrusted ClassNotFoundException e) {
        throw new @OsUntrusted ServiceException(e);
      }
      builder.addProtocolSignature(sigBuilder.build());
    }
    return builder.build();
  }
  
  private @OsUntrusted long @OsUntrusted [] getProtocolVersionForRpcKind(@OsUntrusted ProtocolMetaInfoServerSideTranslatorPB this, RPC.@OsUntrusted RpcKind rpcKind,
      @OsUntrusted
      String protocol) throws ClassNotFoundException {
    @OsUntrusted
    Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocolClass = Class.forName(protocol);
    @OsUntrusted
    String protocolName = RPC.getProtocolName(protocolClass);
    @OsUntrusted
    VerProtocolImpl @OsUntrusted [] vers = server.getSupportedProtocolVersions(rpcKind,
        protocolName);
    if (vers == null) {
      return null;
    }
    @OsUntrusted
    long @OsUntrusted [] versions = new @OsUntrusted long @OsUntrusted [vers.length];
    for (@OsUntrusted int i=0; i<versions.length; i++) {
      versions[i] = vers[i].version;
    }
    return versions;
  }
}
