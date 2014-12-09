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

package org.apache.hadoop.security.protocolPB;

import ostrusted.quals.OsUntrusted;
import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshServiceAclRequestProto;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class RefreshAuthorizationPolicyProtocolClientSideTranslatorPB implements
    @OsUntrusted
    ProtocolMetaInterface, @OsUntrusted RefreshAuthorizationPolicyProtocol, @OsUntrusted Closeable {

  /** RpcController is not used and hence is set to null */
  private final static @OsUntrusted RpcController NULL_CONTROLLER = null;
  private final @OsUntrusted RefreshAuthorizationPolicyProtocolPB rpcProxy;
  
  private final static @OsUntrusted RefreshServiceAclRequestProto
  VOID_REFRESH_SERVICE_ACL_REQUEST =
      RefreshServiceAclRequestProto.newBuilder().build();

  public @OsUntrusted RefreshAuthorizationPolicyProtocolClientSideTranslatorPB(
      @OsUntrusted
      RefreshAuthorizationPolicyProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close(@OsUntrusted RefreshAuthorizationPolicyProtocolClientSideTranslatorPB this) throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public void refreshServiceAcl(@OsUntrusted RefreshAuthorizationPolicyProtocolClientSideTranslatorPB this) throws IOException {
    try {
      rpcProxy.refreshServiceAcl(NULL_CONTROLLER,
          VOID_REFRESH_SERVICE_ACL_REQUEST);
    } catch (@OsUntrusted ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public @OsUntrusted boolean isMethodSupported(@OsUntrusted RefreshAuthorizationPolicyProtocolClientSideTranslatorPB this, @OsUntrusted String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        RefreshAuthorizationPolicyProtocolPB.class,
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(RefreshAuthorizationPolicyProtocolPB.class),
        methodName);
  }
}
