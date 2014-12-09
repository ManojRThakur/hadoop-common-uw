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

package org.apache.hadoop.tools.protocolPB;

import ostrusted.quals.OsUntrusted;
import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class GetUserMappingsProtocolClientSideTranslatorPB implements
    @OsUntrusted
    ProtocolMetaInterface, @OsUntrusted GetUserMappingsProtocol, @OsUntrusted Closeable {

  /** RpcController is not used and hence is set to null */
  private final static @OsUntrusted RpcController NULL_CONTROLLER = null;
  private final @OsUntrusted GetUserMappingsProtocolPB rpcProxy;
  
  public @OsUntrusted GetUserMappingsProtocolClientSideTranslatorPB(
      @OsUntrusted
      GetUserMappingsProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close(@OsUntrusted GetUserMappingsProtocolClientSideTranslatorPB this) throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public @OsUntrusted String @OsUntrusted [] getGroupsForUser(@OsUntrusted GetUserMappingsProtocolClientSideTranslatorPB this, @OsUntrusted String user) throws IOException {
    @OsUntrusted
    GetGroupsForUserRequestProto request = GetGroupsForUserRequestProto
        .newBuilder().setUser(user).build();
    @OsUntrusted
    GetGroupsForUserResponseProto resp;
    try {
      resp = rpcProxy.getGroupsForUser(NULL_CONTROLLER, request);
    } catch (@OsUntrusted ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
    return resp.getGroupsList().toArray(new @OsUntrusted String @OsUntrusted [resp.getGroupsCount()]);
  }

  @Override
  public @OsUntrusted boolean isMethodSupported(@OsUntrusted GetUserMappingsProtocolClientSideTranslatorPB this, @OsUntrusted String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        GetUserMappingsProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(GetUserMappingsProtocolPB.class), methodName);
  }
}
