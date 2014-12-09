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
import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

import java.net.InetSocketAddress;
import java.io.*;

import javax.net.SocketFactory;

import org.apache.commons.logging.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.*;

/** An RpcEngine implementation for Writable data. */
@InterfaceStability.Evolving
public class WritableRpcEngine implements @OsUntrusted RpcEngine {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(RPC.class);
  
  //writableRpcVersion should be updated if there is a change
  //in format of the rpc messages.
  
  // 2L - added declared class to Invocation
  public static final @OsUntrusted long writableRpcVersion = 2L;
  
  /**
   * Whether or not this class has been initialized.
   */
  private static @OsUntrusted boolean isInitialized = false;
  
  static { 
    ensureInitialized();
  }
  
  /**
   * Initialize this class if it isn't already.
   */
  public static synchronized void ensureInitialized() {
    if (!isInitialized) {
      initialize();
    }
  }
  
  /**
   * Register the rpcRequest deserializer for WritableRpcEngine
   */
  private static synchronized void initialize() {
    org.apache.hadoop.ipc.Server.registerProtocolEngine(RPC.RpcKind.RPC_WRITABLE,
        Invocation.class, new Server.@OsUntrusted WritableRpcInvoker());
    isInitialized = true;
  }

  
  /** A method invocation, including the method name and its parameters.*/
  private static class Invocation implements @OsUntrusted Writable, @OsUntrusted Configurable {
    private @OsUntrusted String methodName;
    private @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> @OsUntrusted [] parameterClasses;
    private @OsUntrusted Object @OsUntrusted [] parameters;
    private @OsUntrusted Configuration conf;
    private @OsUntrusted long clientVersion;
    private @OsUntrusted int clientMethodsHash;
    private @OsUntrusted String declaringClassProtocolName;
    
    //This could be different from static writableRpcVersion when received
    //at server, if client is using a different version.
    private @OsUntrusted long rpcVersion;

    @SuppressWarnings("unused") // called when deserializing an invocation
    public @OsUntrusted Invocation() {}

    public @OsUntrusted Invocation(@OsUntrusted Method method, @OsUntrusted Object @OsUntrusted [] parameters) {
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
      rpcVersion = writableRpcVersion;
      if (method.getDeclaringClass().equals(VersionedProtocol.class)) {
        //VersionedProtocol is exempted from version check.
        clientVersion = 0;
        clientMethodsHash = 0;
      } else {
        this.clientVersion = RPC.getProtocolVersion(method.getDeclaringClass());
        this.clientMethodsHash = ProtocolSignature.getFingerprint(method
            .getDeclaringClass().getMethods());
      }
      this.declaringClassProtocolName = 
          RPC.getProtocolName(method.getDeclaringClass());
    }

    /** The name of the method invoked. */
    public @OsUntrusted String getMethodName(WritableRpcEngine.@OsUntrusted Invocation this) { return methodName; }

    /** The parameter classes. */
    public @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> @OsUntrusted [] getParameterClasses(WritableRpcEngine.@OsUntrusted Invocation this) { return parameterClasses; }

    /** The parameter instances. */
    public @OsUntrusted Object @OsUntrusted [] getParameters(WritableRpcEngine.@OsUntrusted Invocation this) { return parameters; }
    
    private @OsUntrusted long getProtocolVersion(WritableRpcEngine.@OsUntrusted Invocation this) {
      return clientVersion;
    }

    @SuppressWarnings("unused")
    private @OsUntrusted int getClientMethodsHash(WritableRpcEngine.@OsUntrusted Invocation this) {
      return clientMethodsHash;
    }
    
    /**
     * Returns the rpc version used by the client.
     * @return rpcVersion
     */
    public @OsUntrusted long getRpcVersion(WritableRpcEngine.@OsUntrusted Invocation this) {
      return rpcVersion;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void readFields(WritableRpcEngine.@OsUntrusted Invocation this, @OsUntrusted DataInput in) throws IOException {
      rpcVersion = in.readLong();
      declaringClassProtocolName = UTF8.readString(in);
      methodName = UTF8.readString(in);
      clientVersion = in.readLong();
      clientMethodsHash = in.readInt();
      parameters = new @OsUntrusted Object @OsUntrusted [in.readInt()];
      parameterClasses = new @OsUntrusted Class @OsUntrusted [parameters.length];
      @OsUntrusted
      ObjectWritable objectWritable = new @OsUntrusted ObjectWritable();
      for (@OsUntrusted int i = 0; i < parameters.length; i++) {
        parameters[i] = 
            ObjectWritable.readObject(in, objectWritable, this.conf);
        parameterClasses[i] = objectWritable.getDeclaredClass();
      }
    }

    @Override
    @SuppressWarnings("deprecation")
    public void write(WritableRpcEngine.@OsUntrusted Invocation this, @OsUntrusted DataOutput out) throws IOException {
      out.writeLong(rpcVersion);
      UTF8.writeString(out, declaringClassProtocolName);
      UTF8.writeString(out, methodName);
      out.writeLong(clientVersion);
      out.writeInt(clientMethodsHash);
      out.writeInt(parameterClasses.length);
      for (@OsUntrusted int i = 0; i < parameterClasses.length; i++) {
        ObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
                                   conf, true);
      }
    }

    @Override
    public @OsUntrusted String toString(WritableRpcEngine.@OsUntrusted Invocation this) {
      @OsUntrusted
      StringBuilder buffer = new @OsUntrusted StringBuilder();
      buffer.append(methodName);
      buffer.append("(");
      for (@OsUntrusted int i = 0; i < parameters.length; i++) {
        if (i != 0)
          buffer.append(", ");
        buffer.append(parameters[i]);
      }
      buffer.append(")");
      buffer.append(", rpc version="+rpcVersion);
      buffer.append(", client version="+clientVersion);
      buffer.append(", methodsFingerPrint="+clientMethodsHash);
      return buffer.toString();
    }

    @Override
    public void setConf(WritableRpcEngine.@OsUntrusted Invocation this, @OsUntrusted Configuration conf) {
      this.conf = conf;
    }

    @Override
    public @OsUntrusted Configuration getConf(WritableRpcEngine.@OsUntrusted Invocation this) {
      return this.conf;
    }

  }

  private static @OsUntrusted ClientCache CLIENTS=new @OsUntrusted ClientCache();
  
  private static class Invoker implements @OsUntrusted RpcInvocationHandler {
    private Client.@OsUntrusted ConnectionId remoteId;
    private @OsUntrusted Client client;
    private @OsUntrusted boolean isClosed = false;

    public @OsUntrusted Invoker(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol,
                   @OsUntrusted
                   InetSocketAddress address, @OsUntrusted UserGroupInformation ticket,
                   @OsUntrusted
                   Configuration conf, @OsUntrusted SocketFactory factory,
                   @OsUntrusted
                   int rpcTimeout) throws IOException {
      this.remoteId = Client.ConnectionId.getConnectionId(address, protocol,
          ticket, rpcTimeout, conf);
      this.client = CLIENTS.getClient(conf, factory);
    }

    @Override
    public @OsUntrusted Object invoke(WritableRpcEngine.@OsUntrusted Invoker this, @OsUntrusted Object proxy, @OsUntrusted Method method, @OsUntrusted Object @OsUntrusted [] args)
      throws Throwable {
      @OsUntrusted
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        startTime = Time.now();
      }

      @OsUntrusted
      ObjectWritable value = (@OsUntrusted ObjectWritable)
        client.call(RPC.RpcKind.RPC_WRITABLE, new @OsUntrusted Invocation(method, args), remoteId);
      if (LOG.isDebugEnabled()) {
        @OsUntrusted
        long callTime = Time.now() - startTime;
        LOG.debug("Call: " + method.getName() + " " + callTime);
      }
      return value.get();
    }
    
    /* close the IPC client that's responsible for this invoker's RPCs */ 
    @Override
    synchronized public void close(WritableRpcEngine.@OsUntrusted Invoker this) {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }

    @Override
    public @OsUntrusted ConnectionId getConnectionId(WritableRpcEngine.@OsUntrusted Invoker this) {
      return remoteId;
    }
  }
  
  // for unit testing only
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static @OsUntrusted Client getClient(@OsUntrusted Configuration conf) {
    return CLIENTS.getClient(conf);
  }
  
  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. 
   * @param <T>*/
  @Override
  @SuppressWarnings("unchecked")
  public <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted ProtocolProxy<@OsUntrusted T> getProxy(@OsUntrusted WritableRpcEngine this, @OsUntrusted Class<@OsUntrusted T> protocol, @OsUntrusted long clientVersion,
                         @OsUntrusted
                         InetSocketAddress addr, @OsUntrusted UserGroupInformation ticket,
                         @OsUntrusted
                         Configuration conf, @OsUntrusted SocketFactory factory,
                         @OsUntrusted
                         int rpcTimeout, @OsUntrusted RetryPolicy connectionRetryPolicy)
    throws IOException {    

    if (connectionRetryPolicy != null) {
      throw new @OsUntrusted UnsupportedOperationException(
          "Not supported: connectionRetryPolicy=" + connectionRetryPolicy);
    }

    @OsUntrusted
    T proxy = (@OsUntrusted T) Proxy.newProxyInstance(protocol.getClassLoader(),
        new @OsUntrusted Class @OsUntrusted [] { protocol }, new @OsUntrusted Invoker(protocol, addr, ticket, conf,
            factory, rpcTimeout));
    return new @OsUntrusted ProtocolProxy<@OsUntrusted T>(protocol, proxy, true);
  }
  
  /* Construct a server for a protocol implementation instance listening on a
   * port and address. */
  @Override
  public RPC.@OsUntrusted Server getServer(@OsUntrusted WritableRpcEngine this, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocolClass,
                      @OsUntrusted
                      Object protocolImpl, @OsUntrusted String bindAddress, @OsUntrusted int port,
                      @OsUntrusted
                      int numHandlers, @OsUntrusted int numReaders, @OsUntrusted int queueSizePerHandler,
                      @OsUntrusted
                      boolean verbose, @OsUntrusted Configuration conf,
                      @OsUntrusted
                      SecretManager<@OsUntrusted ? extends @OsUntrusted TokenIdentifier> secretManager,
                      @OsUntrusted
                      String portRangeConfig) 
    throws IOException {
    return new @OsUntrusted Server(protocolClass, protocolImpl, conf, bindAddress, port,
        numHandlers, numReaders, queueSizePerHandler, verbose, secretManager,
        portRangeConfig);
  }


  /** An RPC Server. */
  public static class Server extends RPC.@OsUntrusted Server {
    /** 
     * Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * 
     * @deprecated Use #Server(Class, Object, Configuration, String, int)    
     */
    @Deprecated
    public @OsUntrusted Server(@OsUntrusted Object instance, @OsUntrusted Configuration conf, @OsUntrusted String bindAddress,
        @OsUntrusted
        int port) throws IOException {
      this(null, instance, conf,  bindAddress, port);
    }
    
    
    /** Construct an RPC server.
     * @param protocolClass class
     * @param protocolImpl the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     */
    public @OsUntrusted Server(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocolClass, @OsUntrusted Object protocolImpl, 
        @OsUntrusted
        Configuration conf, @OsUntrusted String bindAddress, @OsUntrusted int port) 
      throws IOException {
      this(protocolClass, protocolImpl, conf,  bindAddress, port, 1, -1, -1,
          false, null, null);
    }
    
    /** 
     * Construct an RPC server.
     * @param protocolImpl the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     * 
     * @deprecated use Server#Server(Class, Object, 
     *      Configuration, String, int, int, int, int, boolean, SecretManager)
     */
    @Deprecated
    public @OsUntrusted Server(@OsUntrusted Object protocolImpl, @OsUntrusted Configuration conf, @OsUntrusted String bindAddress,
        @OsUntrusted
        int port, @OsUntrusted int numHandlers, @OsUntrusted int numReaders, @OsUntrusted int queueSizePerHandler,
        @OsUntrusted
        boolean verbose, @OsUntrusted SecretManager<@OsUntrusted ? extends @OsUntrusted TokenIdentifier> secretManager) 
            throws IOException {
       this(null, protocolImpl,  conf,  bindAddress,   port,
                   numHandlers,  numReaders,  queueSizePerHandler,  verbose, 
                   secretManager, null);
   
    }
    
    /** 
     * Construct an RPC server.
     * @param protocolClass - the protocol being registered
     *     can be null for compatibility with old usage (see below for details)
     * @param protocolImpl the protocol impl that will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     */
    public @OsUntrusted Server(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocolClass, @OsUntrusted Object protocolImpl,
        @OsUntrusted
        Configuration conf, @OsUntrusted String bindAddress,  @OsUntrusted int port,
        @OsUntrusted
        int numHandlers, @OsUntrusted int numReaders, @OsUntrusted int queueSizePerHandler, 
        @OsUntrusted
        boolean verbose, @OsUntrusted SecretManager<@OsUntrusted ? extends @OsUntrusted TokenIdentifier> secretManager,
        @OsUntrusted
        String portRangeConfig) 
        throws IOException {
      super(bindAddress, port, null, numHandlers, numReaders,
          queueSizePerHandler, conf,
          classNameBase(protocolImpl.getClass().getName()), secretManager,
          portRangeConfig);

      this.verbose = verbose;
      
      
      @OsUntrusted
      Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> @OsUntrusted [] protocols;
      if (protocolClass == null) { // derive protocol from impl
        /*
         * In order to remain compatible with the old usage where a single
         * target protocolImpl is suppled for all protocol interfaces, and
         * the protocolImpl is derived from the protocolClass(es) 
         * we register all interfaces extended by the protocolImpl
         */
        protocols = RPC.getProtocolInterfaces(protocolImpl.getClass());

      } else {
        if (!protocolClass.isAssignableFrom(protocolImpl.getClass())) {
          throw new @OsUntrusted IOException("protocolClass "+ protocolClass +
              " is not implemented by protocolImpl which is of class " +
              protocolImpl.getClass());
        }
        // register protocol class and its super interfaces
        registerProtocolAndImpl(RPC.RpcKind.RPC_WRITABLE, protocolClass, protocolImpl);
        protocols = RPC.getProtocolInterfaces(protocolClass);
      }
      for (@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> p : protocols) {
        if (!p.equals(VersionedProtocol.class)) {
          registerProtocolAndImpl(RPC.RpcKind.RPC_WRITABLE, p, protocolImpl);
        }
      }

    }

    private static void log(@OsUntrusted String value) {
      if (value!= null && value.length() > 55)
        value = value.substring(0, 55)+"...";
      LOG.info(value);
    }
    
    static class WritableRpcInvoker implements @OsUntrusted RpcInvoker {

     @Override
      public @OsUntrusted Writable call(WritableRpcEngine.Server.@OsUntrusted WritableRpcInvoker this, org.apache.hadoop.ipc.RPC.@OsUntrusted Server server,
          @OsUntrusted
          String protocolName, @OsUntrusted Writable rpcRequest, @OsUntrusted long receivedTime)
          throws IOException, RPC.VersionMismatch {

        @OsUntrusted
        Invocation call = (@OsUntrusted Invocation)rpcRequest;
        if (server.verbose) log("Call: " + call);

        // Verify writable rpc version
        if (call.getRpcVersion() != writableRpcVersion) {
          // Client is using a different version of WritableRpc
          throw new @OsUntrusted RpcServerException(
              "WritableRpc version mismatch, client side version="
                  + call.getRpcVersion() + ", server side version="
                  + writableRpcVersion);
        }

        @OsUntrusted
        long clientVersion = call.getProtocolVersion();
        final @OsUntrusted String protoName;
        @OsUntrusted
        ProtoClassProtoImpl protocolImpl;
        if (call.declaringClassProtocolName.equals(VersionedProtocol.class.getName())) {
          // VersionProtocol methods are often used by client to figure out
          // which version of protocol to use.
          //
          // Versioned protocol methods should go the protocolName protocol
          // rather than the declaring class of the method since the
          // the declaring class is VersionedProtocol which is not 
          // registered directly.
          // Send the call to the highest  protocol version
          @OsUntrusted
          VerProtocolImpl highest = server.getHighestSupportedProtocol(
              RPC.RpcKind.RPC_WRITABLE, protocolName);
          if (highest == null) {
            throw new @OsUntrusted RpcServerException("Unknown protocol: " + protocolName);
          }
          protocolImpl = highest.protocolTarget;
        } else {
          protoName = call.declaringClassProtocolName;

          // Find the right impl for the protocol based on client version.
          @OsUntrusted
          ProtoNameVer pv = 
              new @OsUntrusted ProtoNameVer(call.declaringClassProtocolName, clientVersion);
          protocolImpl = 
              server.getProtocolImplMap(RPC.RpcKind.RPC_WRITABLE).get(pv);
          if (protocolImpl == null) { // no match for Protocol AND Version
             @OsUntrusted
             VerProtocolImpl highest = 
                 server.getHighestSupportedProtocol(RPC.RpcKind.RPC_WRITABLE, 
                     protoName);
            if (highest == null) {
              throw new @OsUntrusted RpcServerException("Unknown protocol: " + protoName);
            } else { // protocol supported but not the version that client wants
              throw new RPC.@OsUntrusted VersionMismatch(protoName, clientVersion,
                highest.version);
            }
          }
        }
          

          // Invoke the protocol method
       try {
          @OsUntrusted
          long startTime = Time.now();
          @OsUntrusted
          Method method = 
              protocolImpl.protocolClass.getMethod(call.getMethodName(),
              call.getParameterClasses());
          method.setAccessible(true);
          server.rpcDetailedMetrics.init(protocolImpl.protocolClass);
          @OsUntrusted
          Object value = 
              method.invoke(protocolImpl.protocolImpl, call.getParameters());
          @OsUntrusted
          int processingTime = (@OsUntrusted int) (Time.now() - startTime);
          @OsUntrusted
          int qTime = (@OsUntrusted int) (startTime-receivedTime);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Served: " + call.getMethodName() +
                      " queueTime= " + qTime +
                      " procesingTime= " + processingTime);
          }
          server.rpcMetrics.addRpcQueueTime(qTime);
          server.rpcMetrics.addRpcProcessingTime(processingTime);
          server.rpcDetailedMetrics.addProcessingTime(call.getMethodName(),
                                               processingTime);
          if (server.verbose) log("Return: "+value);

          return new @OsUntrusted ObjectWritable(method.getReturnType(), value);

        } catch (@OsUntrusted InvocationTargetException e) {
          @OsUntrusted
          Throwable target = e.getTargetException();
          if (target instanceof @OsUntrusted IOException) {
            throw (@OsUntrusted IOException)target;
          } else {
            @OsUntrusted
            IOException ioe = new @OsUntrusted IOException(target.toString());
            ioe.setStackTrace(target.getStackTrace());
            throw ioe;
          }
        } catch (@OsUntrusted Throwable e) {
          if (!(e instanceof @OsUntrusted IOException)) {
            LOG.error("Unexpected throwable object ", e);
          }
          @OsUntrusted
          IOException ioe = new @OsUntrusted IOException(e.toString());
          ioe.setStackTrace(e.getStackTrace());
          throw ioe;
        }
      }
    }
  }

  @Override
  public @OsUntrusted ProtocolProxy<@OsUntrusted ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
      @OsUntrusted WritableRpcEngine this, @OsUntrusted
      ConnectionId connId, @OsUntrusted Configuration conf, @OsUntrusted SocketFactory factory)
      throws IOException {
    throw new @OsUntrusted UnsupportedOperationException("This proxy is not supported");
  }
}
