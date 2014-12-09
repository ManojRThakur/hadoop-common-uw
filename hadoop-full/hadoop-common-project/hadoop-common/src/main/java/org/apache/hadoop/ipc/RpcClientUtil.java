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
import ostrusted.quals.OsTrusted;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureRequestProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.GetProtocolSignatureResponseProto;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolSignatureProto;
import org.apache.hadoop.net.NetUtils;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class maintains a cache of protocol versions and corresponding protocol
 * signatures, keyed by server address, protocol and rpc kind.
 * The cache is lazily populated. 
 */
public class RpcClientUtil {
  private static @OsUntrusted RpcController NULL_CONTROLLER = null;
  private static final @OsUntrusted int PRIME = 16777619;
  
  private static class ProtoSigCacheKey {
    private @OsUntrusted InetSocketAddress serverAddress;
    private @OsUntrusted String protocol;
    private @OsUntrusted String rpcKind;
    
    @OsUntrusted
    ProtoSigCacheKey(@OsUntrusted InetSocketAddress addr, @OsUntrusted String p, @OsUntrusted String rk) {
      this.serverAddress = addr;
      this.protocol = p;
      this.rpcKind = rk;
    }
    
    @Override //Object
    public @OsUntrusted int hashCode(RpcClientUtil.@OsUntrusted ProtoSigCacheKey this) {
      @OsUntrusted
      int result = 1;
      result = PRIME * result
          + ((serverAddress == null) ? 0 : serverAddress.hashCode());
      result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
      result = PRIME * result + ((rpcKind == null) ? 0 : rpcKind.hashCode());
      return result;
    }
    
    @Override //Object
    public @OsUntrusted boolean equals(RpcClientUtil.@OsUntrusted ProtoSigCacheKey this, @OsUntrusted Object other) {
      if (other == this) {
        return true;
      }
      if (other instanceof @OsUntrusted ProtoSigCacheKey) {
        @OsUntrusted
        ProtoSigCacheKey otherKey = (@OsUntrusted ProtoSigCacheKey) other;
        return (serverAddress.equals(otherKey.serverAddress) &&
            protocol.equals(otherKey.protocol) &&
            rpcKind.equals(otherKey.rpcKind));
      }
      return false;
    }
  }
  
  private static @OsUntrusted ConcurrentHashMap<@OsUntrusted ProtoSigCacheKey, @OsUntrusted Map<@OsUntrusted Long, @OsUntrusted ProtocolSignature>> 
  signatureMap = new @OsUntrusted ConcurrentHashMap<@OsUntrusted ProtoSigCacheKey, @OsUntrusted Map<@OsUntrusted Long, @OsUntrusted ProtocolSignature>>();

  private static void putVersionSignatureMap(@OsUntrusted InetSocketAddress addr,
      @OsUntrusted
      String protocol, @OsUntrusted String rpcKind, @OsUntrusted Map<@OsUntrusted Long, @OsUntrusted ProtocolSignature> map) {
    signatureMap.put(new @OsUntrusted ProtoSigCacheKey(addr, protocol, rpcKind), map);
  }
  
  private static @OsUntrusted Map<@OsUntrusted Long, @OsUntrusted ProtocolSignature> getVersionSignatureMap(
      @OsUntrusted
      InetSocketAddress addr, @OsUntrusted String protocol, @OsUntrusted String rpcKind) {
    return signatureMap.get(new @OsUntrusted ProtoSigCacheKey(addr, protocol, rpcKind));
  }

  /**
   * Returns whether the given method is supported or not.
   * The protocol signatures are fetched and cached. The connection id for the
   * proxy provided is re-used.
   * @param rpcProxy Proxy which provides an existing connection id.
   * @param protocol Protocol for which the method check is required.
   * @param rpcKind The RpcKind for which the method check is required.
   * @param version The version at the client.
   * @param methodName Name of the method.
   * @return true if the method is supported, false otherwise.
   * @throws IOException
   */
  public static @OsUntrusted boolean isMethodSupported(@OsUntrusted Object rpcProxy, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol,
      RPC.@OsUntrusted RpcKind rpcKind, @OsUntrusted long version, @OsUntrusted String methodName) throws IOException {
    @OsUntrusted
    InetSocketAddress serverAddress = RPC.getServerAddress(rpcProxy);
    @OsUntrusted
    Map<@OsUntrusted Long, @OsUntrusted ProtocolSignature> versionMap = getVersionSignatureMap(
        serverAddress, protocol.getName(), rpcKind.toString());

    if (versionMap == null) {
      @OsUntrusted
      Configuration conf = new @OsUntrusted Configuration();
      RPC.setProtocolEngine(conf, ProtocolMetaInfoPB.class,
          ProtobufRpcEngine.class);
      @OsUntrusted
      ProtocolMetaInfoPB protocolInfoProxy = getProtocolMetaInfoProxy(rpcProxy,
          conf);
      GetProtocolSignatureRequestProto.@OsUntrusted Builder builder = 
          GetProtocolSignatureRequestProto.newBuilder();
      builder.setProtocol(protocol.getName());
      builder.setRpcKind(rpcKind.toString());
      @OsUntrusted
      GetProtocolSignatureResponseProto resp;
      try {
        resp = protocolInfoProxy.getProtocolSignature(NULL_CONTROLLER,
            builder.build());
      } catch (@OsUntrusted ServiceException se) {
        throw ProtobufHelper.getRemoteException(se);
      }
      versionMap = convertProtocolSignatureProtos(resp
          .getProtocolSignatureList());
      putVersionSignatureMap(serverAddress, protocol.getName(),
          rpcKind.toString(), versionMap);
    }
    // Assuming unique method names.
    @OsUntrusted
    Method desiredMethod;
    @OsUntrusted
    Method @OsUntrusted [] allMethods = protocol.getMethods();
    desiredMethod = null;
    for (@OsUntrusted Method m : allMethods) {
      if (m.getName().equals(methodName)) {
        desiredMethod = m;
        break;
      }
    }
    if (desiredMethod == null) {
      return false;
    }
    @OsUntrusted
    int methodHash = ProtocolSignature.getFingerprint(desiredMethod);
    return methodExists(methodHash, version, versionMap);
  }
  
  private static @OsUntrusted Map<@OsUntrusted Long, @OsUntrusted ProtocolSignature> 
  convertProtocolSignatureProtos(@OsUntrusted List<@OsUntrusted ProtocolSignatureProto> protoList) {
    @OsUntrusted
    Map<@OsUntrusted Long, @OsUntrusted ProtocolSignature> map = new @OsUntrusted TreeMap<@OsUntrusted Long, @OsUntrusted ProtocolSignature>();
    for (@OsUntrusted ProtocolSignatureProto p : protoList) {
      @OsUntrusted
      int @OsUntrusted [] methods = new @OsUntrusted int @OsUntrusted [p.getMethodsList().size()];
      @OsUntrusted
      int index=0;
      for (@OsUntrusted int m : p.getMethodsList()) {
        methods[index++] = m;
      }
      map.put(p.getVersion(), new @OsUntrusted ProtocolSignature(p.getVersion(), methods));
    }
    return map;
  }

  private static @OsUntrusted boolean methodExists(@OsUntrusted int methodHash, @OsUntrusted long version,
      @OsUntrusted
      Map<@OsUntrusted Long, @OsUntrusted ProtocolSignature> versionMap) {
    @OsUntrusted
    ProtocolSignature sig = versionMap.get(version);
    if (sig != null) {
      for (@OsUntrusted int m : sig.getMethods()) {
        if (m == methodHash) {
          return true;
        }
      }
    }
    return false;
  }
  
  // The proxy returned re-uses the underlying connection. This is a special 
  // mechanism for ProtocolMetaInfoPB.
  // Don't do this for any other protocol, it might cause a security hole.
  private static @OsUntrusted ProtocolMetaInfoPB getProtocolMetaInfoProxy(@OsUntrusted Object proxy,
      @OsUntrusted
      Configuration conf) throws IOException {
    @OsUntrusted
    RpcInvocationHandler inv = (@OsUntrusted RpcInvocationHandler) Proxy
        .getInvocationHandler(proxy);
    return RPC
        .getProtocolEngine(ProtocolMetaInfoPB.class, conf)
        .getProtocolMetaInfoProxy(inv.getConnectionId(), conf,
            NetUtils.getDefaultSocketFactory(conf)).getProxy();
  }
}
