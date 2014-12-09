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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputOutputStream;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;

/**
 * RPC Engine for for protobuf based RPCs.
 */
@InterfaceStability.Evolving
public class ProtobufRpcEngine implements @OsUntrusted RpcEngine {
  public static final @OsUntrusted Log LOG = LogFactory.getLog(ProtobufRpcEngine.class);
  
  static { // Register the rpcRequest deserializer for WritableRpcEngine 
    org.apache.hadoop.ipc.Server.registerProtocolEngine(
        RPC.RpcKind.RPC_PROTOCOL_BUFFER, RpcRequestWrapper.class,
        new Server.@OsUntrusted ProtoBufRpcInvoker());
  }

  private static final @OsUntrusted ClientCache CLIENTS = new @OsUntrusted ClientCache();

  public <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted ProtocolProxy<@OsUntrusted T> getProxy(@OsUntrusted ProtobufRpcEngine this, @OsUntrusted Class<@OsUntrusted T> protocol, @OsUntrusted long clientVersion,
      @OsUntrusted
      InetSocketAddress addr, @OsUntrusted UserGroupInformation ticket, @OsUntrusted Configuration conf,
      @OsUntrusted
      SocketFactory factory, @OsUntrusted int rpcTimeout) throws IOException {
    return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
        rpcTimeout, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted ProtocolProxy<@OsUntrusted T> getProxy(@OsUntrusted ProtobufRpcEngine this, @OsUntrusted Class<@OsUntrusted T> protocol, @OsUntrusted long clientVersion,
      @OsUntrusted
      InetSocketAddress addr, @OsUntrusted UserGroupInformation ticket, @OsUntrusted Configuration conf,
      @OsUntrusted
      SocketFactory factory, @OsUntrusted int rpcTimeout, @OsUntrusted RetryPolicy connectionRetryPolicy
      ) throws IOException {

    final @OsUntrusted Invoker invoker = new @OsUntrusted Invoker(protocol, addr, ticket, conf, factory,
        rpcTimeout, connectionRetryPolicy);
    return new @OsUntrusted ProtocolProxy<@OsUntrusted T>(protocol, (@OsUntrusted T) Proxy.newProxyInstance(
        protocol.getClassLoader(), new @OsUntrusted Class @OsUntrusted []{protocol}, invoker), false);
  }
  
  @Override
  public @OsUntrusted ProtocolProxy<@OsUntrusted ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
      @OsUntrusted ProtobufRpcEngine this, @OsUntrusted
      ConnectionId connId, @OsUntrusted Configuration conf, @OsUntrusted SocketFactory factory)
      throws IOException {
    @OsUntrusted
    Class<@OsUntrusted ProtocolMetaInfoPB> protocol = ProtocolMetaInfoPB.class;
    return new @OsUntrusted ProtocolProxy<@OsUntrusted ProtocolMetaInfoPB>(protocol,
        (@OsUntrusted ProtocolMetaInfoPB) Proxy.newProxyInstance(protocol.getClassLoader(),
            new @OsUntrusted Class @OsUntrusted [] { protocol }, new @OsUntrusted Invoker(protocol, connId, conf,
                factory)), false);
  }

  private static class Invoker implements @OsUntrusted RpcInvocationHandler {
    private final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Message> returnTypes = 
        new @OsUntrusted ConcurrentHashMap<@OsUntrusted String, @OsUntrusted Message>();
    private @OsUntrusted boolean isClosed = false;
    private final Client.@OsUntrusted ConnectionId remoteId;
    private final @OsUntrusted Client client;
    private final @OsUntrusted long clientProtocolVersion;
    private final @OsUntrusted String protocolName;

    private @OsUntrusted Invoker(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol, @OsUntrusted InetSocketAddress addr,
        @OsUntrusted
        UserGroupInformation ticket, @OsUntrusted Configuration conf, @OsUntrusted SocketFactory factory,
        @OsUntrusted
        int rpcTimeout, @OsUntrusted RetryPolicy connectionRetryPolicy) throws IOException {
      this(protocol, Client.ConnectionId.getConnectionId(
          addr, protocol, ticket, rpcTimeout, connectionRetryPolicy, conf),
          conf, factory);
    }
    
    /**
     * This constructor takes a connectionId, instead of creating a new one.
     */
    private @OsUntrusted Invoker(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol, Client.@OsUntrusted ConnectionId connId,
        @OsUntrusted
        Configuration conf, @OsUntrusted SocketFactory factory) {
      this.remoteId = connId;
      this.client = CLIENTS.getClient(conf, factory, RpcResponseWrapper.class);
      this.protocolName = RPC.getProtocolName(protocol);
      this.clientProtocolVersion = RPC
          .getProtocolVersion(protocol);
    }

    private @OsUntrusted RequestHeaderProto constructRpcRequestHeader(ProtobufRpcEngine.@OsUntrusted Invoker this, @OsUntrusted Method method) {
      RequestHeaderProto.@OsUntrusted Builder builder = RequestHeaderProto
          .newBuilder();
      builder.setMethodName(method.getName());
     

      // For protobuf, {@code protocol} used when creating client side proxy is
      // the interface extending BlockingInterface, which has the annotations 
      // such as ProtocolName etc.
      //
      // Using Method.getDeclaringClass(), as in WritableEngine to get at
      // the protocol interface will return BlockingInterface, from where 
      // the annotation ProtocolName and Version cannot be
      // obtained.
      //
      // Hence we simply use the protocol class used to create the proxy.
      // For PB this may limit the use of mixins on client side.
      builder.setDeclaringClassProtocolName(protocolName);
      builder.setClientProtocolVersion(clientProtocolVersion);
      return builder.build();
    }

    /**
     * This is the client side invoker of RPC method. It only throws
     * ServiceException, since the invocation proxy expects only
     * ServiceException to be thrown by the method in case protobuf service.
     * 
     * ServiceException has the following causes:
     * <ol>
     * <li>Exceptions encountered on the client side in this method are 
     * set as cause in ServiceException as is.</li>
     * <li>Exceptions from the server are wrapped in RemoteException and are
     * set as cause in ServiceException</li>
     * </ol>
     * 
     * Note that the client calling protobuf RPC methods, must handle
     * ServiceException by getting the cause from the ServiceException. If the
     * cause is RemoteException, then unwrap it to get the exception thrown by
     * the server.
     */
    @Override
    public @OsUntrusted Object invoke(ProtobufRpcEngine.@OsUntrusted Invoker this, @OsUntrusted Object proxy, @OsUntrusted Method method, @OsUntrusted Object @OsUntrusted [] args)
        throws ServiceException {
      @OsUntrusted
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        startTime = Time.now();
      }
      
      if (args.length != 2) { // RpcController + Message
        throw new @OsUntrusted ServiceException("Too many parameters for request. Method: ["
            + method.getName() + "]" + ", Expected: 2, Actual: "
            + args.length);
      }
      if (args[1] == null) {
        throw new @OsUntrusted ServiceException("null param while calling Method: ["
            + method.getName() + "]");
      }

      @OsUntrusted
      RequestHeaderProto rpcRequestHeader = constructRpcRequestHeader(method);
      
      if (LOG.isTraceEnabled()) {
        LOG.trace(Thread.currentThread().getId() + ": Call -> " +
            remoteId + ": " + method.getName() +
            " {" + TextFormat.shortDebugString((@OsUntrusted Message) args[1]) + "}");
      }


      @OsUntrusted
      Message theRequest = (@OsUntrusted Message) args[1];
      final @OsUntrusted RpcResponseWrapper val;
      try {
        val = (@OsUntrusted RpcResponseWrapper) client.call(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
            new @OsUntrusted RpcRequestWrapper(rpcRequestHeader, theRequest), remoteId);

      } catch (@OsUntrusted Throwable e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(Thread.currentThread().getId() + ": Exception <- " +
              remoteId + ": " + method.getName() +
                " {" + e + "}");
        }

        throw new @OsUntrusted ServiceException(e);
      }

      if (LOG.isDebugEnabled()) {
        @OsUntrusted
        long callTime = Time.now() - startTime;
        LOG.debug("Call: " + method.getName() + " took " + callTime + "ms");
      }
      
      @OsUntrusted
      Message prototype = null;
      try {
        prototype = getReturnProtoType(method);
      } catch (@OsUntrusted Exception e) {
        throw new @OsUntrusted ServiceException(e);
      }
      @OsUntrusted
      Message returnMessage;
      try {
        returnMessage = prototype.newBuilderForType()
            .mergeFrom(val.theResponseRead).build();

        if (LOG.isTraceEnabled()) {
          LOG.trace(Thread.currentThread().getId() + ": Response <- " +
              remoteId + ": " + method.getName() +
                " {" + TextFormat.shortDebugString(returnMessage) + "}");
        }

      } catch (@OsUntrusted Throwable e) {
        throw new @OsUntrusted ServiceException(e);
      }
      return returnMessage;
    }

    @Override
    public void close(ProtobufRpcEngine.@OsUntrusted Invoker this) throws IOException {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }

    private @OsUntrusted Message getReturnProtoType(ProtobufRpcEngine.@OsUntrusted Invoker this, @OsUntrusted Method method) throws Exception {
      if (returnTypes.containsKey(method.getName())) {
        return returnTypes.get(method.getName());
      }
      
      @OsUntrusted
      Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> returnType = method.getReturnType();
      @OsUntrusted
      Method newInstMethod = returnType.getMethod("getDefaultInstance");
      newInstMethod.setAccessible(true);
      @OsUntrusted
      Message prototype = (@OsUntrusted Message) newInstMethod.invoke(null, (@OsUntrusted Object[]) null);
      returnTypes.put(method.getName(), prototype);
      return prototype;
    }

    @Override //RpcInvocationHandler
    public @OsUntrusted ConnectionId getConnectionId(ProtobufRpcEngine.@OsUntrusted Invoker this) {
      return remoteId;
    }
  }

  interface RpcWrapper extends @OsUntrusted Writable {
    @OsUntrusted
    int getLength(ProtobufRpcEngine.@OsUntrusted RpcWrapper this);
  }
  /**
   * Wrapper for Protocol Buffer Requests
   * 
   * Note while this wrapper is writable, the request on the wire is in
   * Protobuf. Several methods on {@link org.apache.hadoop.ipc.Server and RPC} 
   * use type Writable as a wrapper to work across multiple RpcEngine kinds.
   */
  private static abstract class RpcMessageWithHeader<@OsUntrusted T extends @OsUntrusted GeneratedMessage>
    implements @OsUntrusted RpcWrapper {
    @OsUntrusted
    T requestHeader;
    @OsUntrusted
    Message theRequest; // for clientSide, the request is here
    @OsUntrusted
    byte @OsUntrusted [] theRequestRead; // for server side, the request is here

    public @OsUntrusted RpcMessageWithHeader() {
    }

    public @OsUntrusted RpcMessageWithHeader(@OsUntrusted T requestHeader, @OsUntrusted Message theRequest) {
      this.requestHeader = requestHeader;
      this.theRequest = theRequest;
    }

    @Override
    public void write(ProtobufRpcEngine.@OsUntrusted RpcMessageWithHeader<T> this, @OsUntrusted DataOutput out) throws IOException {
      @OsUntrusted
      OutputStream os = DataOutputOutputStream.constructOutputStream(out);
      
      ((@OsUntrusted Message)requestHeader).writeDelimitedTo(os);
      theRequest.writeDelimitedTo(os);
    }

    @Override
    public void readFields(ProtobufRpcEngine.@OsUntrusted RpcMessageWithHeader<T> this, @OsUntrusted DataInput in) throws IOException {
      requestHeader = parseHeaderFrom(readVarintBytes(in));
      theRequestRead = readMessageRequest(in);
    }

    abstract @OsUntrusted T parseHeaderFrom(ProtobufRpcEngine.@OsUntrusted RpcMessageWithHeader<T> this, @OsUntrusted byte @OsUntrusted [] bytes) throws IOException;

    @OsUntrusted
    byte @OsUntrusted [] readMessageRequest(ProtobufRpcEngine.@OsUntrusted RpcMessageWithHeader<T> this, @OsUntrusted DataInput in) throws IOException {
      return readVarintBytes(in);
    }

    private static @OsUntrusted byte @OsUntrusted [] readVarintBytes(@OsUntrusted DataInput in) throws IOException {
      final @OsUntrusted int length = ProtoUtil.readRawVarint32(in);
      final @OsUntrusted byte @OsUntrusted [] bytes = new @OsUntrusted byte @OsUntrusted [length];
      in.readFully(bytes);
      return bytes;
    }

    public @OsUntrusted T getMessageHeader(ProtobufRpcEngine.@OsUntrusted RpcMessageWithHeader<T> this) {
      return requestHeader;
    }

    public @OsUntrusted byte @OsUntrusted [] getMessageBytes(ProtobufRpcEngine.@OsUntrusted RpcMessageWithHeader<T> this) {
      return theRequestRead;
    }
    
    @Override
    public @OsUntrusted int getLength(ProtobufRpcEngine.@OsUntrusted RpcMessageWithHeader<T> this) {
      @OsUntrusted
      int headerLen = requestHeader.getSerializedSize();
      @OsUntrusted
      int reqLen;
      if (theRequest != null) {
        reqLen = theRequest.getSerializedSize();
      } else if (theRequestRead != null ) {
        reqLen = theRequestRead.length;
      } else {
        throw new @OsUntrusted IllegalArgumentException(
            "getLength on uninitialized RpcWrapper");      
      }
      return CodedOutputStream.computeRawVarint32Size(headerLen) +  headerLen
          + CodedOutputStream.computeRawVarint32Size(reqLen) + reqLen;
    }
  }
  
  private static class RpcRequestWrapper
  extends @OsUntrusted RpcMessageWithHeader<@OsUntrusted RequestHeaderProto> {
    @SuppressWarnings("unused")
    public @OsUntrusted RpcRequestWrapper() {}
    
    public @OsUntrusted RpcRequestWrapper(
        @OsUntrusted
        RequestHeaderProto requestHeader, @OsUntrusted Message theRequest) {
      super(requestHeader, theRequest);
    }
    
    @Override
    @OsUntrusted
    RequestHeaderProto parseHeaderFrom(ProtobufRpcEngine.@OsUntrusted RpcRequestWrapper this, @OsUntrusted byte @OsUntrusted [] bytes) throws IOException {
      return RequestHeaderProto.parseFrom(bytes);
    }
    
    @Override
    public @OsUntrusted String toString(ProtobufRpcEngine.@OsUntrusted RpcRequestWrapper this) {
      return requestHeader.getDeclaringClassProtocolName() + "." +
          requestHeader.getMethodName();
    }
  }

  @InterfaceAudience.LimitedPrivate({"RPC"})
  public static class RpcRequestMessageWrapper
  extends @OsUntrusted RpcMessageWithHeader<@OsUntrusted RpcRequestHeaderProto> {
    public @OsUntrusted RpcRequestMessageWrapper() {}
    
    public @OsUntrusted RpcRequestMessageWrapper(
        @OsUntrusted
        RpcRequestHeaderProto requestHeader, @OsUntrusted Message theRequest) {
      super(requestHeader, theRequest);
    }
    
    @Override
    @OsUntrusted
    RpcRequestHeaderProto parseHeaderFrom(ProtobufRpcEngine.@OsUntrusted RpcRequestMessageWrapper this, @OsUntrusted byte @OsUntrusted [] bytes) throws IOException {
      return RpcRequestHeaderProto.parseFrom(bytes);
    }
  }

  @InterfaceAudience.LimitedPrivate({"RPC"})
  public static class RpcResponseMessageWrapper
  extends @OsUntrusted RpcMessageWithHeader<@OsUntrusted RpcResponseHeaderProto> {
    public @OsUntrusted RpcResponseMessageWrapper() {}
    
    public @OsUntrusted RpcResponseMessageWrapper(
        @OsUntrusted
        RpcResponseHeaderProto responseHeader, @OsUntrusted Message theRequest) {
      super(responseHeader, theRequest);
    }
    
    @Override
    @OsUntrusted
    byte @OsUntrusted [] readMessageRequest(ProtobufRpcEngine.@OsUntrusted RpcResponseMessageWrapper this, @OsUntrusted DataInput in) throws IOException {
      // error message contain no message body
      switch (requestHeader.getStatus()) {
        case ERROR:
        case FATAL:
          return null;
        default:
          return super.readMessageRequest(in);
      }
    }
    
    @Override
    @OsUntrusted
    RpcResponseHeaderProto parseHeaderFrom(ProtobufRpcEngine.@OsUntrusted RpcResponseMessageWrapper this, @OsUntrusted byte @OsUntrusted [] bytes) throws IOException {
      return RpcResponseHeaderProto.parseFrom(bytes);
    }
  }

  /**
   *  Wrapper for Protocol Buffer Responses
   * 
   * Note while this wrapper is writable, the request on the wire is in
   * Protobuf. Several methods on {@link org.apache.hadoop.ipc.Server and RPC} 
   * use type Writable as a wrapper to work across multiple RpcEngine kinds.
   */
  @InterfaceAudience.LimitedPrivate({"RPC"}) // temporarily exposed 
  public static class RpcResponseWrapper implements @OsUntrusted RpcWrapper {
    @OsUntrusted
    Message theResponse; // for senderSide, the response is here
    @OsUntrusted
    byte @OsUntrusted [] theResponseRead; // for receiver side, the response is here

    public @OsUntrusted RpcResponseWrapper() {
    }

    public @OsUntrusted RpcResponseWrapper(@OsUntrusted Message message) {
      this.theResponse = message;
    }

    @Override
    public void write(ProtobufRpcEngine.@OsUntrusted RpcResponseWrapper this, @OsUntrusted DataOutput out) throws IOException {
      @OsUntrusted
      OutputStream os = DataOutputOutputStream.constructOutputStream(out);
      theResponse.writeDelimitedTo(os);   
    }

    @Override
    public void readFields(ProtobufRpcEngine.@OsUntrusted RpcResponseWrapper this, @OsUntrusted DataInput in) throws IOException {
      @OsUntrusted
      int length = ProtoUtil.readRawVarint32(in);
      theResponseRead = new @OsUntrusted byte @OsUntrusted [length];
      in.readFully(theResponseRead);
    }
    
    @Override
    public @OsUntrusted int getLength(ProtobufRpcEngine.@OsUntrusted RpcResponseWrapper this) {
      @OsUntrusted
      int resLen;
      if (theResponse != null) {
        resLen = theResponse.getSerializedSize();
      } else if (theResponseRead != null ) {
        resLen = theResponseRead.length;
      } else {
        throw new @OsUntrusted IllegalArgumentException(
            "getLength on uninitialized RpcWrapper");      
      }
      return CodedOutputStream.computeRawVarint32Size(resLen) + resLen;
    }
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static @OsUntrusted Client getClient(@OsUntrusted Configuration conf) {
    return CLIENTS.getClient(conf, SocketFactory.getDefault(),
        RpcResponseWrapper.class);
  }
  
 

  @Override
  public RPC.@OsUntrusted Server getServer(@OsUntrusted ProtobufRpcEngine this, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol, @OsUntrusted Object protocolImpl,
      @OsUntrusted
      String bindAddress, @OsUntrusted int port, @OsUntrusted int numHandlers, @OsUntrusted int numReaders,
      @OsUntrusted
      int queueSizePerHandler, @OsUntrusted boolean verbose, @OsUntrusted Configuration conf,
      @OsUntrusted
      SecretManager<@OsUntrusted ? extends @OsUntrusted TokenIdentifier> secretManager,
      @OsUntrusted
      String portRangeConfig)
      throws IOException {
    return new @OsUntrusted Server(protocol, protocolImpl, conf, bindAddress, port,
        numHandlers, numReaders, queueSizePerHandler, verbose, secretManager,
        portRangeConfig);
  }
  
  public static class Server extends RPC.@OsUntrusted Server {
    /**
     * Construct an RPC server.
     * 
     * @param protocolClass the class of protocol
     * @param protocolImpl the protocolImpl whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     * @param portRangeConfig A config parameter that can be used to restrict
     * the range of ports used when port is 0 (an ephemeral port)
     */
    public @OsUntrusted Server(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocolClass, @OsUntrusted Object protocolImpl,
        @OsUntrusted
        Configuration conf, @OsUntrusted String bindAddress, @OsUntrusted int port, @OsUntrusted int numHandlers,
        @OsUntrusted
        int numReaders, @OsUntrusted int queueSizePerHandler, @OsUntrusted boolean verbose,
        @OsUntrusted
        SecretManager<@OsUntrusted ? extends @OsUntrusted TokenIdentifier> secretManager, 
        @OsUntrusted
        String portRangeConfig)
        throws IOException {
      super(bindAddress, port, null, numHandlers,
          numReaders, queueSizePerHandler, conf, classNameBase(protocolImpl
              .getClass().getName()), secretManager, portRangeConfig);
      this.verbose = verbose;  
      registerProtocolAndImpl(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocolClass,
          protocolImpl);
    }
    
    /**
     * Protobuf invoker for {@link RpcInvoker}
     */
    static class ProtoBufRpcInvoker implements @OsUntrusted RpcInvoker {
      private static @OsUntrusted ProtoClassProtoImpl getProtocolImpl(RPC.@OsUntrusted Server server,
          @OsUntrusted
          String protoName, @OsUntrusted long clientVersion) throws RpcServerException {
        @OsUntrusted
        ProtoNameVer pv = new @OsUntrusted ProtoNameVer(protoName, clientVersion);
        @OsUntrusted
        ProtoClassProtoImpl impl = 
            server.getProtocolImplMap(RPC.RpcKind.RPC_PROTOCOL_BUFFER).get(pv);
        if (impl == null) { // no match for Protocol AND Version
          @OsUntrusted
          VerProtocolImpl highest = 
              server.getHighestSupportedProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, 
                  protoName);
          if (highest == null) {
            throw new @OsUntrusted RpcNoSuchProtocolException(
                "Unknown protocol: " + protoName);
          }
          // protocol supported but not the version that client wants
          throw new RPC.@OsUntrusted VersionMismatch(protoName, clientVersion,
              highest.version);
        }
        return impl;
      }

      @Override 
      /**
       * This is a server side method, which is invoked over RPC. On success
       * the return response has protobuf response payload. On failure, the
       * exception name and the stack trace are return in the resposne.
       * See {@link HadoopRpcResponseProto}
       * 
       * In this method there three types of exceptions possible and they are
       * returned in response as follows.
       * <ol>
       * <li> Exceptions encountered in this method that are returned 
       * as {@link RpcServerException} </li>
       * <li> Exceptions thrown by the service is wrapped in ServiceException. 
       * In that this method returns in response the exception thrown by the 
       * service.</li>
       * <li> Other exceptions thrown by the service. They are returned as
       * it is.</li>
       * </ol>
       */
      public @OsUntrusted Writable call(ProtobufRpcEngine.Server.@OsUntrusted ProtoBufRpcInvoker this, RPC.@OsUntrusted Server server, @OsUntrusted String connectionProtocolName,
          @OsUntrusted
          Writable writableRequest, @OsUntrusted long receiveTime) throws Exception {
        @OsUntrusted
        RpcRequestWrapper request = (@OsUntrusted RpcRequestWrapper) writableRequest;
        @OsUntrusted
        RequestHeaderProto rpcRequest = request.requestHeader;
        @OsUntrusted
        String methodName = rpcRequest.getMethodName();
        
        
        /** 
         * RPCs for a particular interface (ie protocol) are done using a
         * IPC connection that is setup using rpcProxy.
         * The rpcProxy's has a declared protocol name that is 
         * sent form client to server at connection time. 
         * 
         * Each Rpc call also sends a protocol name 
         * (called declaringClassprotocolName). This name is usually the same
         * as the connection protocol name except in some cases. 
         * For example metaProtocols such ProtocolInfoProto which get info
         * about the protocol reuse the connection but need to indicate that
         * the actual protocol is different (i.e. the protocol is
         * ProtocolInfoProto) since they reuse the connection; in this case
         * the declaringClassProtocolName field is set to the ProtocolInfoProto.
         */

        @OsUntrusted
        String declaringClassProtoName = 
            rpcRequest.getDeclaringClassProtocolName();
        @OsUntrusted
        long clientVersion = rpcRequest.getClientProtocolVersion();
        if (server.verbose)
          LOG.info("Call: connectionProtocolName=" + connectionProtocolName + 
              ", method=" + methodName);
        
        @OsUntrusted
        ProtoClassProtoImpl protocolImpl = getProtocolImpl(server, 
                              declaringClassProtoName, clientVersion);
        @OsUntrusted
        BlockingService service = (@OsUntrusted BlockingService) protocolImpl.protocolImpl;
        @OsUntrusted
        MethodDescriptor methodDescriptor = service.getDescriptorForType()
            .findMethodByName(methodName);
        if (methodDescriptor == null) {
          @OsUntrusted
          String msg = "Unknown method " + methodName + " called on " 
                                + connectionProtocolName + " protocol.";
          LOG.warn(msg);
          throw new @OsUntrusted RpcNoSuchMethodException(msg);
        }
        @OsUntrusted
        Message prototype = service.getRequestPrototype(methodDescriptor);
        @OsUntrusted
        Message param = prototype.newBuilderForType()
            .mergeFrom(request.theRequestRead).build();
        
        @OsUntrusted
        Message result;
        try {
          @OsUntrusted
          long startTime = Time.now();
          server.rpcDetailedMetrics.init(protocolImpl.protocolClass);
          result = service.callBlockingMethod(methodDescriptor, null, param);
          @OsUntrusted
          int processingTime = (@OsUntrusted int) (Time.now() - startTime);
          @OsUntrusted
          int qTime = (@OsUntrusted int) (startTime - receiveTime);
          if (LOG.isDebugEnabled()) {
            LOG.info("Served: " + methodName + " queueTime= " + qTime +
                      " procesingTime= " + processingTime);
          }
          server.rpcMetrics.addRpcQueueTime(qTime);
          server.rpcMetrics.addRpcProcessingTime(processingTime);
          server.rpcDetailedMetrics.addProcessingTime(methodName,
              processingTime);
        } catch (@OsUntrusted ServiceException e) {
          throw (@OsUntrusted Exception) e.getCause();
        } catch (@OsUntrusted Exception e) {
          throw e;
        }
        return new @OsUntrusted RpcResponseWrapper(result);
      }
    }
  }
}
