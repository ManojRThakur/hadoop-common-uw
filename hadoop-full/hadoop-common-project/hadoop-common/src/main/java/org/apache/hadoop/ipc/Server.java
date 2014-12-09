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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import static org.apache.hadoop.ipc.RpcConstants.*;

import org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseMessageWrapper;
import org.apache.hadoop.ipc.ProtobufRpcEngine.RpcResponseWrapper;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.ipc.metrics.RpcDetailedMetrics;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcKindProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

/** An abstract IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Client
 */
@InterfaceAudience.LimitedPrivate(value = { "Common", "HDFS", "MapReduce", "Yarn" })
@InterfaceStability.Evolving
public abstract class Server {
  private final @OsUntrusted boolean authorize;
  private @OsUntrusted List<@OsUntrusted AuthMethod> enabledAuthMethods;
  private @OsUntrusted RpcSaslProto negotiateResponse;
  private @OsUntrusted ExceptionsHandler exceptionsHandler = new @OsUntrusted ExceptionsHandler();
  
  public void addTerseExceptions(@OsUntrusted Server this, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> @OsUntrusted ... exceptionClass) {
    exceptionsHandler.addTerseExceptions(exceptionClass);
  }

  /**
   * ExceptionsHandler manages Exception groups for special handling
   * e.g., terse exception group for concise logging messages
   */
  static class ExceptionsHandler {
    private volatile @OsUntrusted Set<@OsUntrusted String> terseExceptions = new @OsUntrusted HashSet<@OsUntrusted String>();

    /**
     * Add exception class so server won't log its stack trace.
     * Modifying the terseException through this method is thread safe.
     *
     * @param exceptionClass exception classes 
     */
    void addTerseExceptions(Server.@OsUntrusted ExceptionsHandler this, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> @OsUntrusted ... exceptionClass) {

      // Make a copy of terseException for performing modification
      final @OsUntrusted HashSet<@OsUntrusted String> newSet = new @OsUntrusted HashSet<@OsUntrusted String>(terseExceptions);

      // Add all class names into the HashSet
      for (@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> name : exceptionClass) {
        newSet.add(name.toString());
      }
      // Replace terseException set
      terseExceptions = Collections.unmodifiableSet(newSet);
    }

    @OsUntrusted
    boolean isTerse(Server.@OsUntrusted ExceptionsHandler this, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> t) {
      return terseExceptions.contains(t.toString());
    }
  }

  
  /**
   * If the user accidentally sends an HTTP GET to an IPC port, we detect this
   * and send back a nicer response.
   */
  private static final @OsUntrusted ByteBuffer HTTP_GET_BYTES = ByteBuffer.wrap(
      "GET ".getBytes());
  
  /**
   * An HTTP response to send back if we detect an HTTP request to our IPC
   * port.
   */
  static final @OsUntrusted String RECEIVED_HTTP_REQ_RESPONSE =
    "HTTP/1.1 404 Not Found\r\n" +
    "Content-type: text/plain\r\n\r\n" +
    "It looks like you are making an HTTP request to a Hadoop IPC port. " +
    "This is not the correct port for the web interface on this daemon.\r\n";

  /**
   * Initial and max size of response buffer
   */
  static @OsUntrusted int INITIAL_RESP_BUF_SIZE = 10240;
  
  static class RpcKindMapValue {
    final @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Writable> rpcRequestWrapperClass;
    final @OsUntrusted RpcInvoker rpcInvoker;
    @OsUntrusted
    RpcKindMapValue (@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Writable> rpcRequestWrapperClass,
          @OsUntrusted
          RpcInvoker rpcInvoker) {
      this.rpcInvoker = rpcInvoker;
      this.rpcRequestWrapperClass = rpcRequestWrapperClass;
    }   
  }
  static @OsUntrusted Map<RPC.@OsUntrusted RpcKind, @OsUntrusted RpcKindMapValue> rpcKindMap = new
      @OsUntrusted HashMap<RPC.@OsUntrusted RpcKind, @OsUntrusted RpcKindMapValue>(4);
  
  

  /**
   * Register a RPC kind and the class to deserialize the rpc request.
   * 
   * Called by static initializers of rpcKind Engines
   * @param rpcKind
   * @param rpcRequestWrapperClass - this class is used to deserialze the
   *  the rpc request.
   *  @param rpcInvoker - use to process the calls on SS.
   */
  
  public static void registerProtocolEngine(RPC.@OsUntrusted RpcKind rpcKind, 
          @OsUntrusted
          Class<@OsUntrusted ? extends @OsUntrusted Writable> rpcRequestWrapperClass,
          @OsUntrusted
          RpcInvoker rpcInvoker) {
    @OsUntrusted
    RpcKindMapValue  old = 
        rpcKindMap.put(rpcKind, new @OsUntrusted RpcKindMapValue(rpcRequestWrapperClass, rpcInvoker));
    if (old != null) {
      rpcKindMap.put(rpcKind, old);
      throw new @OsUntrusted IllegalArgumentException("ReRegistration of rpcKind: " +
          rpcKind);      
    }
    LOG.debug("rpcKind=" + rpcKind + 
        ", rpcRequestWrapperClass=" + rpcRequestWrapperClass + 
        ", rpcInvoker=" + rpcInvoker);
  }
  
  public @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Writable> getRpcRequestWrapper(
      @OsUntrusted Server this, @OsUntrusted
      RpcKindProto rpcKind) {
    if (rpcRequestClass != null)
       return rpcRequestClass;
    @OsUntrusted
    RpcKindMapValue val = rpcKindMap.get(ProtoUtil.convert(rpcKind));
    return (val == null) ? null : val.rpcRequestWrapperClass; 
  }
  
  public static @OsUntrusted RpcInvoker  getRpcInvoker(RPC.@OsUntrusted RpcKind rpcKind) {
    @OsUntrusted
    RpcKindMapValue val = rpcKindMap.get(rpcKind);
    return (val == null) ? null : val.rpcInvoker; 
  }
  

  public static final @OsUntrusted Log LOG = LogFactory.getLog(Server.class);
  public static final @OsUntrusted Log AUDITLOG = 
    LogFactory.getLog("SecurityLogger."+Server.class.getName());
  private static final @OsUntrusted String AUTH_FAILED_FOR = "Auth failed for ";
  private static final @OsUntrusted String AUTH_SUCCESSFUL_FOR = "Auth successful for ";
  
  private static final @OsUntrusted ThreadLocal<@OsUntrusted Server> SERVER = new @OsUntrusted ThreadLocal<@OsUntrusted Server>();

  private static final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>> PROTOCOL_CACHE = 
    new @OsUntrusted ConcurrentHashMap<@OsUntrusted String, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>>();
  
  static @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> getProtocolClass(@OsUntrusted String protocolName, @OsUntrusted Configuration conf) 
  throws ClassNotFoundException {
    @OsUntrusted
    Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol = PROTOCOL_CACHE.get(protocolName);
    if (protocol == null) {
      protocol = conf.getClassByName(protocolName);
      PROTOCOL_CACHE.put(protocolName, protocol);
    }
    return protocol;
  }
  
  /** Returns the server instance called under or null.  May be called under
   * {@link #call(Writable, long)} implementations, and under {@link Writable}
   * methods of paramters and return values.  Permits applications to access
   * the server context.*/
  public static @OsUntrusted Server get() {
    return SERVER.get();
  }
 
  /** This is set to Call object before Handler invokes an RPC and reset
   * after the call returns.
   */
  private static final @OsUntrusted ThreadLocal<@OsUntrusted Call> CurCall = new @OsUntrusted ThreadLocal<@OsUntrusted Call>();
  
  /** Get the current call */
  @VisibleForTesting
  public static @OsUntrusted ThreadLocal<@OsUntrusted Call> getCurCall() {
    return CurCall;
  }
  
  /**
   * Returns the currently active RPC call's sequential ID number.  A negative
   * call ID indicates an invalid value, such as if there is no currently active
   * RPC call.
   * 
   * @return int sequential ID number of currently active RPC call
   */
  public static @OsUntrusted int getCallId() {
    @OsUntrusted
    Call call = CurCall.get();
    return call != null ? call.callId : RpcConstants.INVALID_CALL_ID;
  }
  
  /**
   * @return The current active RPC call's retry count. -1 indicates the retry
   *         cache is not supported in the client side.
   */
  public static @OsUntrusted int getCallRetryCount() {
    @OsUntrusted
    Call call = CurCall.get();
    return call != null ? call.retryCount : RpcConstants.INVALID_RETRY_COUNT;
  }

  /** Returns the remote side ip address when invoked inside an RPC 
   *  Returns null incase of an error.
   */
  public static @OsUntrusted InetAddress getRemoteIp() {
    @OsUntrusted
    Call call = CurCall.get();
    return (call != null && call.connection != null) ? call.connection
        .getHostInetAddress() : null;
  }
  
  /**
   * Returns the clientId from the current RPC request
   */
  public static @OsUntrusted byte @OsUntrusted [] getClientId() {
    @OsUntrusted
    Call call = CurCall.get();
    return call != null ? call.clientId : RpcConstants.DUMMY_CLIENT_ID;
  }
  
  /** Returns remote address as a string when invoked inside an RPC.
   *  Returns null in case of an error.
   */
  public static @OsUntrusted String getRemoteAddress() {
    @OsUntrusted
    InetAddress addr = getRemoteIp();
    return (addr == null) ? null : addr.getHostAddress();
  }

  /** Returns the RPC remote user when invoked inside an RPC.  Note this
   *  may be different than the current user if called within another doAs
   *  @return connection's UGI or null if not an RPC
   */
  public static @OsUntrusted UserGroupInformation getRemoteUser() {
    @OsUntrusted
    Call call = CurCall.get();
    return (call != null && call.connection != null) ? call.connection.user
        : null;
  }
 
  /** Return true if the invocation was through an RPC.
   */
  public static @OsUntrusted boolean isRpcInvocation() {
    return CurCall.get() != null;
  }

  private @OsUntrusted String bindAddress; 
  private @OsUntrusted int port;                               // port we listen on
  private @OsUntrusted int handlerCount;                       // number of handler threads
  private @OsUntrusted int readThreads;                        // number of read threads
  private @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Writable> rpcRequestClass;   // class used for deserializing the rpc request
  private @OsUntrusted int maxIdleTime;                        // the maximum idle time after 
                                                  // which a client may be disconnected
  private @OsUntrusted int thresholdIdleConnections;           // the number of idle connections
                                                  // after which we will start
                                                  // cleaning up idle 
                                                  // connections
  @OsUntrusted
  int maxConnectionsToNuke;                       // the max number of 
                                                  // connections to nuke
                                                  //during a cleanup
  
  protected @OsUntrusted RpcMetrics rpcMetrics;
  protected @OsUntrusted RpcDetailedMetrics rpcDetailedMetrics;
  
  private @OsUntrusted Configuration conf;
  private @OsUntrusted String portRangeConfig = null;
  private @OsUntrusted SecretManager<@OsUntrusted TokenIdentifier> secretManager;
  private @OsUntrusted ServiceAuthorizationManager serviceAuthorizationManager = new @OsUntrusted ServiceAuthorizationManager();

  private @OsUntrusted int maxQueueSize;
  private final @OsUntrusted int maxRespSize;
  private @OsUntrusted int socketSendBufferSize;
  private final @OsUntrusted int maxDataLength;
  private final @OsUntrusted boolean tcpNoDelay; // if T then disable Nagle's Algorithm

  volatile private @OsUntrusted boolean running = true;         // true while server runs
  private @OsUntrusted BlockingQueue<@OsUntrusted Call> callQueue; // queued calls

  private @OsUntrusted List<@OsUntrusted Connection> connectionList = 
    Collections.synchronizedList(new @OsUntrusted LinkedList<@OsUntrusted Connection>());
  //maintain a list
  //of client connections
  private @OsUntrusted Listener listener = null;
  private @OsUntrusted Responder responder = null;
  private @OsUntrusted int numConnections = 0;
  private @OsUntrusted Handler @OsUntrusted [] handlers = null;

  /**
   * A convenience method to bind to a given address and report 
   * better exceptions if the address is not a valid host.
   * @param socket the socket to bind
   * @param address the address to bind to
   * @param backlog the number of connections allowed in the queue
   * @throws BindException if the address can't be bound
   * @throws UnknownHostException if the address isn't a valid host name
   * @throws IOException other random errors from bind
   */
  public static void bind(@OsUntrusted ServerSocket socket, @OsUntrusted InetSocketAddress address, 
                          @OsUntrusted
                          int backlog) throws IOException {
    bind(socket, address, backlog, null, null);
  }

  public static void bind(@OsUntrusted ServerSocket socket, @OsUntrusted InetSocketAddress address, 
      @OsUntrusted
      int backlog, @OsUntrusted Configuration conf, @OsUntrusted String rangeConf) throws IOException {
    try {
      @OsUntrusted
      IntegerRanges range = null;
      if (rangeConf != null) {
        range = conf.getRange(rangeConf, "");
      }
      if (range == null || range.isEmpty() || (address.getPort() != 0)) {
        socket.bind(address, backlog);
      } else {
        for (@OsUntrusted Integer port : range) {
          if (socket.isBound()) break;
          try {
            @OsUntrusted
            InetSocketAddress temp = new @OsUntrusted InetSocketAddress(address.getAddress(),
                port);
            socket.bind(temp, backlog);
          } catch(@OsUntrusted BindException e) {
            //Ignored
          }
        }
        if (!socket.isBound()) {
          throw new @OsUntrusted BindException("Could not find a free port in "+range);
        }
      }
    } catch (@OsUntrusted SocketException e) {
      throw NetUtils.wrapException(null,
          0,
          address.getHostName(),
          address.getPort(), e);
    }
  }
  
  /**
   * Returns a handle to the rpcMetrics (required in tests)
   * @return rpc metrics
   */
  @VisibleForTesting
  public @OsUntrusted RpcMetrics getRpcMetrics(@OsUntrusted Server this) {
    return rpcMetrics;
  }

  @VisibleForTesting
  public @OsUntrusted RpcDetailedMetrics getRpcDetailedMetrics(@OsUntrusted Server this) {
    return rpcDetailedMetrics;
  }
  
  @VisibleForTesting
  @OsUntrusted
  Iterable<@OsUntrusted ? extends @OsUntrusted Thread> getHandlers(@OsUntrusted Server this) {
    return Arrays.asList(handlers);
  }

  @VisibleForTesting
  @OsUntrusted
  List<@OsUntrusted Connection> getConnections(@OsUntrusted Server this) {
    return connectionList;
  }

  /**
   * Refresh the service authorization ACL for the service handled by this server.
   */
  public void refreshServiceAcl(@OsUntrusted Server this, @OsUntrusted Configuration conf, @OsUntrusted PolicyProvider provider) {
    serviceAuthorizationManager.refresh(conf, provider);
  }

  /**
   * Returns a handle to the serviceAuthorizationManager (required in tests)
   * @return instance of ServiceAuthorizationManager for this server
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  public @OsUntrusted ServiceAuthorizationManager getServiceAuthorizationManager(@OsUntrusted Server this) {
    return serviceAuthorizationManager;
  }

  /** A call queued for handling. */
  public static class Call {
    private final @OsUntrusted int callId;             // the client's call id
    private final @OsUntrusted int retryCount;        // the retry count of the call
    private final @OsUntrusted Writable rpcRequest;    // Serialized Rpc request from client
    private final @OsUntrusted Connection connection;  // connection to client
    private @OsUntrusted long timestamp;               // time received when response is null
                                          // time served when response is not null
    private @OsUntrusted ByteBuffer rpcResponse;       // the response for this call
    private final RPC.@OsUntrusted RpcKind rpcKind;
    private final @OsUntrusted byte @OsUntrusted [] clientId;

    public @OsUntrusted Call(@OsUntrusted int id, @OsUntrusted int retryCount, @OsUntrusted Writable param, 
        @OsUntrusted
        Connection connection) {
      this(id, retryCount, param, connection, RPC.RpcKind.RPC_BUILTIN,
          RpcConstants.DUMMY_CLIENT_ID);
    }

    public @OsUntrusted Call(@OsUntrusted int id, @OsUntrusted int retryCount, @OsUntrusted Writable param, @OsUntrusted Connection connection,
        RPC.@OsUntrusted RpcKind kind, @OsUntrusted byte @OsUntrusted [] clientId) {
      this.callId = id;
      this.retryCount = retryCount;
      this.rpcRequest = param;
      this.connection = connection;
      this.timestamp = Time.now();
      this.rpcResponse = null;
      this.rpcKind = kind;
      this.clientId = clientId;
    }
    
    @Override
    public @OsUntrusted String toString(Server.@OsUntrusted Call this) {
      return rpcRequest + " from " + connection + " Call#" + callId + " Retry#"
          + retryCount;
    }

    public void setResponse(Server.@OsUntrusted Call this, @OsUntrusted ByteBuffer response) {
      this.rpcResponse = response;
    }
  }

  /** Listens on the socket. Creates jobs for the handler threads*/
  private class Listener extends @OsUntrusted Thread {
    
    private @OsUntrusted ServerSocketChannel acceptChannel = null; //the accept channel
    private @OsUntrusted Selector selector = null; //the selector that we use for the server
    private @OsUntrusted Reader @OsUntrusted [] readers = null;
    private @OsUntrusted int currentReader = 0;
    private @OsUntrusted InetSocketAddress address; //the address we bind at
    private @OsUntrusted Random rand = new @OsUntrusted Random();
    private @OsUntrusted long lastCleanupRunTime = 0; //the last time when a cleanup connec-
                                         //-tion (for idle connections) ran
    private @OsUntrusted long cleanupInterval = 10000; //the minimum interval between 
                                          //two cleanup runs
    private @OsUntrusted int backlogLength = conf.getInt(
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);
    
    public @OsUntrusted Listener() throws IOException {
      address = new @OsUntrusted InetSocketAddress(bindAddress, port);
      // Create a new server socket and set to non blocking mode
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the local host and port
      bind(acceptChannel.socket(), address, backlogLength, conf, portRangeConfig);
      port = acceptChannel.socket().getLocalPort(); //Could be an ephemeral port
      // create a selector;
      selector= Selector.open();
      readers = new @OsUntrusted Reader @OsUntrusted [readThreads];
      for (@OsUntrusted int i = 0; i < readThreads; i++) {
        @OsUntrusted
        Reader reader = new @OsUntrusted Reader(
            "Socket Reader #" + (i + 1) + " for port " + port);
        readers[i] = reader;
        reader.start();
      }

      // Register accepts on the server socket with the selector.
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
      this.setName("IPC Server listener on " + port);
      this.setDaemon(true);
    }
    
    private class Reader extends @OsUntrusted Thread {
      private volatile @OsUntrusted boolean adding = false;
      private final @OsUntrusted Selector readSelector;

      @OsUntrusted
      Reader(@OsUntrusted String name) throws IOException {
        super(name);

        this.readSelector = Selector.open();
      }
      
      @Override
      public void run(@OsUntrusted Server.Listener.Reader this) {
        LOG.info("Starting " + getName());
        try {
          doRunLoop();
        } finally {
          try {
            readSelector.close();
          } catch (@OsUntrusted IOException ioe) {
            LOG.error("Error closing read selector in " + this.getName(), ioe);
          }
        }
      }

      private synchronized void doRunLoop(@OsUntrusted Server.Listener.Reader this) {
        while (running) {
          @OsUntrusted
          SelectionKey key = null;
          try {
            readSelector.select();
            while (adding) {
              this.wait(1000);
            }              

            @OsUntrusted
            Iterator<@OsUntrusted SelectionKey> iter = readSelector.selectedKeys().iterator();
            while (iter.hasNext()) {
              key = iter.next();
              iter.remove();
              if (key.isValid()) {
                if (key.isReadable()) {
                  doRead(key);
                }
              }
              key = null;
            }
          } catch (@OsUntrusted InterruptedException e) {
            if (running) {                      // unexpected -- log it
              LOG.info(getName() + " unexpectedly interrupted", e);
            }
          } catch (@OsUntrusted IOException ex) {
            LOG.error("Error in Reader", ex);
          }
        }
      }

      /**
       * This gets reader into the state that waits for the new channel
       * to be registered with readSelector. If it was waiting in select()
       * the thread will be woken up, otherwise whenever select() is called
       * it will return even if there is nothing to read and wait
       * in while(adding) for finishAdd call
       */
      public void startAdd(@OsUntrusted Server.Listener.Reader this) {
        adding = true;
        readSelector.wakeup();
      }
      
      public synchronized @OsUntrusted SelectionKey registerChannel(@OsUntrusted Server.Listener.Reader this, @OsUntrusted SocketChannel channel)
                                                          throws IOException {
          return channel.register(readSelector, SelectionKey.OP_READ);
      }

      public synchronized void finishAdd(@OsUntrusted Server.Listener.Reader this) {
        adding = false;
        this.notify();        
      }

      void shutdown(@OsUntrusted Server.Listener.Reader this) {
        assert !running;
        readSelector.wakeup();
        try {
          join();
        } catch (@OsUntrusted InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
    /** cleanup connections from connectionList. Choose a random range
     * to scan and also have a limit on the number of the connections
     * that will be cleanedup per run. The criteria for cleanup is the time
     * for which the connection was idle. If 'force' is true then all 
     * connections will be looked at for the cleanup.
     */
    private void cleanupConnections(@OsUntrusted Server.Listener this, @OsUntrusted boolean force) {
      if (force || numConnections > thresholdIdleConnections) {
        @OsUntrusted
        long currentTime = Time.now();
        if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
          return;
        }
        @OsUntrusted
        int start = 0;
        @OsUntrusted
        int end = numConnections - 1;
        if (!force) {
          start = rand.nextInt() % numConnections;
          end = rand.nextInt() % numConnections;
          @OsUntrusted
          int temp;
          if (end < start) {
            temp = start;
            start = end;
            end = temp;
          }
        }
        @OsUntrusted
        int i = start;
        @OsUntrusted
        int numNuked = 0;
        while (i <= end) {
          @OsUntrusted
          Connection c;
          synchronized (connectionList) {
            try {
              c = connectionList.get(i);
            } catch (@OsUntrusted Exception e) {return;}
          }
          if (c.timedOut(currentTime)) {
            if (LOG.isDebugEnabled())
              LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
            closeConnection(c);
            numNuked++;
            end--;
            c = null;
            if (!force && numNuked == maxConnectionsToNuke) break;
          }
          else i++;
        }
        lastCleanupRunTime = Time.now();
      }
    }

    @Override
    public void run(@OsUntrusted Server.Listener this) {
      LOG.info(getName() + ": starting");
      SERVER.set(Server.this);
      while (running) {
        @OsUntrusted
        SelectionKey key = null;
        try {
          getSelector().select();
          @OsUntrusted
          Iterator<@OsUntrusted SelectionKey> iter = getSelector().selectedKeys().iterator();
          while (iter.hasNext()) {
            key = iter.next();
            iter.remove();
            try {
              if (key.isValid()) {
                if (key.isAcceptable())
                  doAccept(key);
              }
            } catch (@OsUntrusted IOException e) {
            }
            key = null;
          }
        } catch (@OsUntrusted OutOfMemoryError e) {
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give 
          // some thread(s) a chance to finish
          LOG.warn("Out of Memory in server select", e);
          closeCurrentConnection(key, e);
          cleanupConnections(true);
          try { Thread.sleep(60000); } catch (@OsUntrusted Exception ie) {}
        } catch (@OsUntrusted Exception e) {
          closeCurrentConnection(key, e);
        }
        cleanupConnections(false);
      }
      LOG.info("Stopping " + this.getName());

      synchronized (this) {
        try {
          acceptChannel.close();
          selector.close();
        } catch (@OsUntrusted IOException e) { }

        selector= null;
        acceptChannel= null;
        
        // clean up all connections
        while (!connectionList.isEmpty()) {
          closeConnection(connectionList.remove(0));
        }
      }
    }

    private void closeCurrentConnection(@OsUntrusted Server.Listener this, @OsUntrusted SelectionKey key, @OsUntrusted Throwable e) {
      if (key != null) {
        @OsUntrusted
        Connection c = (@OsUntrusted Connection)key.attachment();
        if (c != null) {
          if (LOG.isDebugEnabled())
            LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
          closeConnection(c);
          c = null;
        }
      }
    }

    @OsUntrusted
    InetSocketAddress getAddress(@OsUntrusted Server.Listener this) {
      return (@OsUntrusted InetSocketAddress)acceptChannel.socket().getLocalSocketAddress();
    }
    
    void doAccept(@OsUntrusted Server.Listener this, @OsUntrusted SelectionKey key) throws IOException,  OutOfMemoryError {
      @OsUntrusted
      Connection c = null;
      @OsUntrusted
      ServerSocketChannel server = (@OsUntrusted ServerSocketChannel) key.channel();
      @OsUntrusted
      SocketChannel channel;
      while ((channel = server.accept()) != null) {

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(tcpNoDelay);
        
        @OsUntrusted
        Reader reader = getReader();
        try {
          reader.startAdd();
          @OsUntrusted
          SelectionKey readKey = reader.registerChannel(channel);
          c = new @OsUntrusted Connection(readKey, channel, Time.now());
          readKey.attach(c);
          synchronized (connectionList) {
            connectionList.add(numConnections, c);
            numConnections++;
          }
          if (LOG.isDebugEnabled())
            LOG.debug("Server connection from " + c.toString() +
                "; # active connections: " + numConnections +
                "; # queued calls: " + callQueue.size());          
        } finally {
          reader.finishAdd(); 
        }
      }
    }

    void doRead(@OsUntrusted Server.Listener this, @OsUntrusted SelectionKey key) throws InterruptedException {
      @OsUntrusted
      int count = 0;
      @OsUntrusted
      Connection c = (@OsUntrusted Connection)key.attachment();
      if (c == null) {
        return;  
      }
      c.setLastContact(Time.now());
      
      try {
        count = c.readAndProcess();
      } catch (@OsUntrusted InterruptedException ieo) {
        LOG.info(getName() + ": readAndProcess caught InterruptedException", ieo);
        throw ieo;
      } catch (@OsUntrusted Exception e) {
        // a WrappedRpcServerException is an exception that has been sent
        // to the client, so the stacktrace is unnecessary; any other
        // exceptions are unexpected internal server errors and thus the
        // stacktrace should be logged
        LOG.info(getName() + ": readAndProcess from client " +
            c.getHostAddress() + " threw exception [" + e + "]",
            (e instanceof @OsUntrusted WrappedRpcServerException) ? null : e);
        count = -1; //so that the (count < 0) block is executed
      }
      if (count < 0) {
        if (LOG.isDebugEnabled())
          LOG.debug(getName() + ": disconnecting client " + 
                    c + ". Number of active connections: "+
                    numConnections);
        closeConnection(c);
        c = null;
      }
      else {
        c.setLastContact(Time.now());
      }
    }   

    synchronized void doStop(@OsUntrusted Server.Listener this) {
      if (selector != null) {
        selector.wakeup();
        Thread.yield();
      }
      if (acceptChannel != null) {
        try {
          acceptChannel.socket().close();
        } catch (@OsUntrusted IOException e) {
          LOG.info(getName() + ":Exception in closing listener socket. " + e);
        }
      }
      for (@OsUntrusted Reader r : readers) {
        r.shutdown();
      }
    }
    
    synchronized @OsUntrusted Selector getSelector(@OsUntrusted Server.Listener this) { return selector; }
    // The method that will return the next reader to work with
    // Simplistic implementation of round robin for now
    @OsUntrusted
    Reader getReader(@OsUntrusted Server.Listener this) {
      currentReader = (currentReader + 1) % readers.length;
      return readers[currentReader];
    }
  }

  // Sends responses of RPC back to clients.
  private class Responder extends @OsUntrusted Thread {
    private final @OsUntrusted Selector writeSelector;
    private @OsUntrusted int pending;         // connections waiting to register
    
    final static @OsUntrusted int PURGE_INTERVAL = 900000; // 15mins

    @OsUntrusted
    Responder() throws IOException {
      this.setName("IPC Server Responder");
      this.setDaemon(true);
      writeSelector = Selector.open(); // create a selector
      pending = 0;
    }

    @Override
    public void run(@OsUntrusted Server.Responder this) {
      LOG.info(getName() + ": starting");
      SERVER.set(Server.this);
      try {
        doRunLoop();
      } finally {
        LOG.info("Stopping " + this.getName());
        try {
          writeSelector.close();
        } catch (@OsUntrusted IOException ioe) {
          LOG.error("Couldn't close write selector in " + this.getName(), ioe);
        }
      }
    }
    
    private void doRunLoop(@OsUntrusted Server.Responder this) {
      @OsUntrusted
      long lastPurgeTime = 0;   // last check for old calls.

      while (running) {
        try {
          waitPending();     // If a channel is being registered, wait.
          writeSelector.select(PURGE_INTERVAL);
          @OsUntrusted
          Iterator<@OsUntrusted SelectionKey> iter = writeSelector.selectedKeys().iterator();
          while (iter.hasNext()) {
            @OsUntrusted
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isValid() && key.isWritable()) {
                  doAsyncWrite(key);
              }
            } catch (@OsUntrusted IOException e) {
              LOG.info(getName() + ": doAsyncWrite threw exception " + e);
            }
          }
          @OsUntrusted
          long now = Time.now();
          if (now < lastPurgeTime + PURGE_INTERVAL) {
            continue;
          }
          lastPurgeTime = now;
          //
          // If there were some calls that have not been sent out for a
          // long time, discard them.
          //
          if(LOG.isDebugEnabled()) {
            LOG.debug("Checking for old call responses.");
          }
          @OsUntrusted
          ArrayList<@OsUntrusted Call> calls;
          
          // get the list of channels from list of keys.
          synchronized (writeSelector.keys()) {
            calls = new @OsUntrusted ArrayList<@OsUntrusted Call>(writeSelector.keys().size());
            iter = writeSelector.keys().iterator();
            while (iter.hasNext()) {
              @OsUntrusted
              SelectionKey key = iter.next();
              @OsUntrusted
              Call call = (@OsUntrusted Call)key.attachment();
              if (call != null && key.channel() == call.connection.channel) { 
                calls.add(call);
              }
            }
          }
          
          for(@OsUntrusted Call call : calls) {
            doPurge(call, now);
          }
        } catch (@OsUntrusted OutOfMemoryError e) {
          //
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give
          // some thread(s) a chance to finish
          //
          LOG.warn("Out of Memory in server select", e);
          try { Thread.sleep(60000); } catch (@OsUntrusted Exception ie) {}
        } catch (@OsUntrusted Exception e) {
          LOG.warn("Exception in Responder", e);
        }
      }
    }

    private void doAsyncWrite(@OsUntrusted Server.Responder this, @OsUntrusted SelectionKey key) throws IOException {
      @OsUntrusted
      Call call = (@OsUntrusted Call)key.attachment();
      if (call == null) {
        return;
      }
      if (key.channel() != call.connection.channel) {
        throw new @OsUntrusted IOException("doAsyncWrite: bad channel");
      }

      synchronized(call.connection.responseQueue) {
        if (processResponse(call.connection.responseQueue, false)) {
          try {
            key.interestOps(0);
          } catch (@OsUntrusted CancelledKeyException e) {
            /* The Listener/reader might have closed the socket.
             * We don't explicitly cancel the key, so not sure if this will
             * ever fire.
             * This warning could be removed.
             */
            LOG.warn("Exception while changing ops : " + e);
          }
        }
      }
    }

    //
    // Remove calls that have been pending in the responseQueue 
    // for a long time.
    //
    private void doPurge(@OsUntrusted Server.Responder this, @OsUntrusted Call call, @OsUntrusted long now) {
      @OsUntrusted
      LinkedList<@OsUntrusted Call> responseQueue = call.connection.responseQueue;
      synchronized (responseQueue) {
        @OsUntrusted
        Iterator<@OsUntrusted Call> iter = responseQueue.listIterator(0);
        while (iter.hasNext()) {
          call = iter.next();
          if (now > call.timestamp + PURGE_INTERVAL) {
            closeConnection(call.connection);
            break;
          }
        }
      }
    }

    // Processes one response. Returns true if there are no more pending
    // data for this channel.
    //
    private @OsUntrusted boolean processResponse(@OsUntrusted Server.Responder this, @OsUntrusted LinkedList<@OsUntrusted Call> responseQueue,
                                    @OsUntrusted
                                    boolean inHandler) throws IOException {
      @OsUntrusted
      boolean error = true;
      @OsUntrusted
      boolean done = false;       // there is more data for this channel.
      @OsUntrusted
      int numElements = 0;
      @OsUntrusted
      Call call = null;
      try {
        synchronized (responseQueue) {
          //
          // If there are no items for this channel, then we are done
          //
          numElements = responseQueue.size();
          if (numElements == 0) {
            error = false;
            return true;              // no more data for this channel.
          }
          //
          // Extract the first call
          //
          call = responseQueue.removeFirst();
          @OsUntrusted
          SocketChannel channel = call.connection.channel;
          if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": responding to " + call);
          }
          //
          // Send as much data as we can in the non-blocking fashion
          //
          @OsUntrusted
          int numBytes = channelWrite(channel, call.rpcResponse);
          if (numBytes < 0) {
            return true;
          }
          if (!call.rpcResponse.hasRemaining()) {
            //Clear out the response buffer so it can be collected
            call.rpcResponse = null;
            call.connection.decRpcCount();
            if (numElements == 1) {    // last call fully processes.
              done = true;             // no more data for this channel.
            } else {
              done = false;            // more calls pending to be sent.
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to " + call
                  + " Wrote " + numBytes + " bytes.");
            }
          } else {
            //
            // If we were unable to write the entire response out, then 
            // insert in Selector queue. 
            //
            call.connection.responseQueue.addFirst(call);
            
            if (inHandler) {
              // set the serve time when the response has to be sent later
              call.timestamp = Time.now();
              
              incPending();
              try {
                // Wakeup the thread blocked on select, only then can the call 
                // to channel.register() complete.
                writeSelector.wakeup();
                channel.register(writeSelector, SelectionKey.OP_WRITE, call);
              } catch (@OsUntrusted ClosedChannelException e) {
                //Its ok. channel might be closed else where.
                done = true;
              } finally {
                decPending();
              }
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to " + call
                  + " Wrote partial " + numBytes + " bytes.");
            }
          }
          error = false;              // everything went off well
        }
      } finally {
        if (error && call != null) {
          LOG.warn(getName()+", call " + call + ": output error");
          done = true;               // error. no more data for this channel.
          closeConnection(call.connection);
        }
      }
      return done;
    }

    //
    // Enqueue a response from the application.
    //
    void doRespond(@OsUntrusted Server.Responder this, @OsUntrusted Call call) throws IOException {
      synchronized (call.connection.responseQueue) {
        call.connection.responseQueue.addLast(call);
        if (call.connection.responseQueue.size() == 1) {
          processResponse(call.connection.responseQueue, true);
        }
      }
    }

    private synchronized void incPending(@OsUntrusted Server.Responder this) {   // call waiting to be enqueued.
      pending++;
    }

    private synchronized void decPending(@OsUntrusted Server.Responder this) { // call done enqueueing.
      pending--;
      notify();
    }

    private synchronized void waitPending(@OsUntrusted Server.Responder this) throws InterruptedException {
      while (pending > 0) {
        wait();
      }
    }
  }

  @InterfaceAudience.Private
  public static enum AuthProtocol {

@OsUntrusted  NONE(0),

@OsUntrusted  SASL(-33);
    
    public final @OsUntrusted int callId;
    @OsUntrusted
    AuthProtocol(@OsUntrusted int callId) {
      this.callId = callId;
    }
    
    static @OsUntrusted AuthProtocol valueOf(@OsUntrusted int callId) {
      for (@OsUntrusted AuthProtocol authType : AuthProtocol.values()) {
        if (authType.callId == callId) {
          return authType;
        }
      }
      return null;
    }
  };
  
  /**
   * Wrapper for RPC IOExceptions to be returned to the client.  Used to
   * let exceptions bubble up to top of processOneRpc where the correct
   * callId can be associated with the response.  Also used to prevent
   * unnecessary stack trace logging if it's not an internal server error. 
   */
  @SuppressWarnings("serial")
  private static class WrappedRpcServerException extends @OsUntrusted RpcServerException {
    private final @OsUntrusted RpcErrorCodeProto errCode;
    public @OsUntrusted WrappedRpcServerException(@OsUntrusted RpcErrorCodeProto errCode, @OsUntrusted IOException ioe) {
      super(ioe.toString(), ioe);
      this.errCode = errCode;
    }
    public @OsUntrusted WrappedRpcServerException(@OsUntrusted RpcErrorCodeProto errCode, @OsUntrusted String message) {
      this(errCode, new @OsUntrusted RpcServerException(message));
    }
    @Override
    public @OsUntrusted RpcErrorCodeProto getRpcErrorCodeProto(Server.@OsUntrusted WrappedRpcServerException this) {
      return errCode;
    }
    @Override
    public @OsUntrusted String toString(Server.@OsUntrusted WrappedRpcServerException this) {
      return getCause().toString();
    }
  }

  /** Reads calls from a connection and queues them for handling. */
  public class Connection {
    private @OsUntrusted boolean connectionHeaderRead = false; // connection  header is read?
    private @OsUntrusted boolean connectionContextRead = false; //if connection context that
                                            //follows connection header is read

    private @OsUntrusted SocketChannel channel;
    private @OsUntrusted ByteBuffer data;
    private @OsUntrusted ByteBuffer dataLengthBuffer;
    private @OsUntrusted LinkedList<@OsUntrusted Call> responseQueue;
    private volatile @OsUntrusted int rpcCount = 0; // number of outstanding rpcs
    private @OsUntrusted long lastContact;
    private @OsUntrusted int dataLength;
    private @OsUntrusted Socket socket;
    // Cache the remote host & port info so that even if the socket is 
    // disconnected, we can say where it used to connect to.
    private @OsUntrusted String hostAddress;
    private @OsUntrusted int remotePort;
    private @OsUntrusted InetAddress addr;
    
    @OsUntrusted
    IpcConnectionContextProto connectionContext;
    @OsUntrusted
    String protocolName;
    @OsUntrusted
    SaslServer saslServer;
    private @OsUntrusted AuthMethod authMethod;
    private @OsUntrusted AuthProtocol authProtocol;
    private @OsUntrusted boolean saslContextEstablished;
    private @OsUntrusted ByteBuffer connectionHeaderBuf = null;
    private @OsUntrusted ByteBuffer unwrappedData;
    private @OsUntrusted ByteBuffer unwrappedDataLengthBuffer;
    private @OsUntrusted int serviceClass;
    
    @OsUntrusted
    UserGroupInformation user = null;
    public @OsUntrusted UserGroupInformation attemptingUser = null; // user name before auth

    // Fake 'call' for failed authorization response
    private final @OsUntrusted Call authFailedCall = new @OsUntrusted Call(AUTHORIZATION_FAILED_CALL_ID,
        RpcConstants.INVALID_RETRY_COUNT, null, this);
    private @OsUntrusted ByteArrayOutputStream authFailedResponse = new @OsUntrusted ByteArrayOutputStream();
    
    private final @OsUntrusted Call saslCall = new @OsUntrusted Call(AuthProtocol.SASL.callId,
        RpcConstants.INVALID_RETRY_COUNT, null, this);
    private final @OsUntrusted ByteArrayOutputStream saslResponse = new @OsUntrusted ByteArrayOutputStream();
    
    private @OsUntrusted boolean sentNegotiate = false;
    private @OsUntrusted boolean useWrap = false;
    
    public @OsUntrusted Connection(@OsUntrusted SelectionKey key, @OsUntrusted SocketChannel channel, 
                      @OsUntrusted
                      long lastContact) {
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      this.dataLengthBuffer = ByteBuffer.allocate(4);
      this.unwrappedData = null;
      this.unwrappedDataLengthBuffer = ByteBuffer.allocate(4);
      this.socket = channel.socket();
      this.addr = socket.getInetAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = addr.getHostAddress();
      }
      this.remotePort = socket.getPort();
      this.responseQueue = new @OsUntrusted LinkedList<@OsUntrusted Call>();
      if (socketSendBufferSize != 0) {
        try {
          socket.setSendBufferSize(socketSendBufferSize);
        } catch (@OsUntrusted IOException e) {
          LOG.warn("Connection: unable to set socket send buffer size to " +
                   socketSendBufferSize);
        }
      }
    }   

    @Override
    public @OsUntrusted String toString(@OsUntrusted Server.Connection this) {
      return getHostAddress() + ":" + remotePort; 
    }
    
    public @OsUntrusted String getHostAddress(@OsUntrusted Server.Connection this) {
      return hostAddress;
    }

    public @OsUntrusted InetAddress getHostInetAddress(@OsUntrusted Server.Connection this) {
      return addr;
    }
    
    public void setLastContact(@OsUntrusted Server.Connection this, @OsUntrusted long lastContact) {
      this.lastContact = lastContact;
    }

    public @OsUntrusted long getLastContact(@OsUntrusted Server.Connection this) {
      return lastContact;
    }

    /* Return true if the connection has no outstanding rpc */
    private @OsUntrusted boolean isIdle(@OsUntrusted Server.Connection this) {
      return rpcCount == 0;
    }
    
    /* Decrement the outstanding RPC count */
    private void decRpcCount(@OsUntrusted Server.Connection this) {
      rpcCount--;
    }
    
    /* Increment the outstanding RPC count */
    private void incRpcCount(@OsUntrusted Server.Connection this) {
      rpcCount++;
    }
    
    private @OsUntrusted boolean timedOut(@OsUntrusted Server.Connection this, @OsUntrusted long currentTime) {
      if (isIdle() && currentTime -  lastContact > maxIdleTime)
        return true;
      return false;
    }
    
    private @OsUntrusted UserGroupInformation getAuthorizedUgi(@OsUntrusted Server.Connection this, @OsUntrusted String authorizedId)
        throws InvalidToken, AccessControlException {
      if (authMethod == AuthMethod.TOKEN) {
        @OsUntrusted
        TokenIdentifier tokenId = SaslRpcServer.getIdentifier(authorizedId,
            secretManager);
        @OsUntrusted
        UserGroupInformation ugi = tokenId.getUser();
        if (ugi == null) {
          throw new @OsUntrusted AccessControlException(
              "Can't retrieve username from tokenIdentifier.");
        }
        ugi.addTokenIdentifier(tokenId);
        return ugi;
      } else {
        return UserGroupInformation.createRemoteUser(authorizedId);
      }
    }

    private void saslReadAndProcess(@OsUntrusted Server.Connection this, @OsUntrusted DataInputStream dis) throws
    WrappedRpcServerException, IOException, InterruptedException {
      final @OsUntrusted RpcSaslProto saslMessage =
          decodeProtobufFromStream(RpcSaslProto.newBuilder(), dis);
      switch (saslMessage.getState()) {
        case WRAP: {
          if (!saslContextEstablished || !useWrap) {
            throw new @OsUntrusted WrappedRpcServerException(
                RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                new @OsUntrusted SaslException("Server is not wrapping data"));
          }
          // loops over decoded data and calls processOneRpc
          unwrapPacketAndProcessRpcs(saslMessage.getToken().toByteArray());
          break;
        }
        default:
          saslProcess(saslMessage);
      }
    }

    private @OsUntrusted Throwable getCauseForInvalidToken(@OsUntrusted Server.Connection this, @OsUntrusted IOException e) {
      @OsUntrusted
      Throwable cause = e;
      while (cause != null) {
        if (cause instanceof @OsUntrusted RetriableException) {
          return (@OsUntrusted RetriableException) cause;
        } else if (cause instanceof @OsUntrusted StandbyException) {
          return (@OsUntrusted StandbyException) cause;
        } else if (cause instanceof @OsUntrusted InvalidToken) {
          // FIXME: hadoop method signatures are restricting the SASL
          // callbacks to only returning InvalidToken, but some services
          // need to throw other exceptions (ex. NN + StandyException),
          // so for now we'll tunnel the real exceptions via an
          // InvalidToken's cause which normally is not set 
          if (cause.getCause() != null) {
            cause = cause.getCause();
          }
          return cause;
        }
        cause = cause.getCause();
      }
      return e;
    }
    
    private void saslProcess(@OsUntrusted Server.Connection this, @OsUntrusted RpcSaslProto saslMessage)
        throws WrappedRpcServerException, IOException, InterruptedException {
      if (saslContextEstablished) {
        throw new @OsUntrusted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            new @OsUntrusted SaslException("Negotiation is already complete"));
      }
      @OsUntrusted
      RpcSaslProto saslResponse = null;
      try {
        try {
          saslResponse = processSaslMessage(saslMessage);
        } catch (@OsUntrusted IOException e) {
          rpcMetrics.incrAuthenticationFailures();
          // attempting user could be null
          AUDITLOG.warn(AUTH_FAILED_FOR + this.toString() + ":"
              + attemptingUser + " (" + e.getLocalizedMessage() + ")");
          throw (@OsUntrusted IOException) getCauseForInvalidToken(e);
        }
        
        if (saslServer != null && saslServer.isComplete()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server context established. Negotiated QoP is "
                + saslServer.getNegotiatedProperty(Sasl.QOP));
          }
          user = getAuthorizedUgi(saslServer.getAuthorizationID());
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server successfully authenticated client: " + user);
          }
          rpcMetrics.incrAuthenticationSuccesses();
          AUDITLOG.info(AUTH_SUCCESSFUL_FOR + user);
          saslContextEstablished = true;
        }
      } catch (@OsUntrusted WrappedRpcServerException wrse) { // don't re-wrap
        throw wrse;
      } catch (@OsUntrusted IOException ioe) {
        throw new @OsUntrusted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_UNAUTHORIZED, ioe);
      }
      // send back response if any, may throw IOException
      if (saslResponse != null) {
        doSaslReply(saslResponse);
      }
      // do NOT enable wrapping until the last auth response is sent
      if (saslContextEstablished) {
        @OsUntrusted
        String qop = (@OsUntrusted String) saslServer.getNegotiatedProperty(Sasl.QOP);
        // SASL wrapping is only used if the connection has a QOP, and
        // the value is not auth.  ex. auth-int & auth-priv
        useWrap = (qop != null && !"auth".equalsIgnoreCase(qop));        
      }
    }
    
    private @OsUntrusted RpcSaslProto processSaslMessage(@OsUntrusted Server.Connection this, @OsUntrusted RpcSaslProto saslMessage)
        throws IOException, InterruptedException {
      @OsUntrusted
      RpcSaslProto saslResponse = null;
      final @OsUntrusted SaslState state = saslMessage.getState(); // required      
      switch (state) {
        case NEGOTIATE: {
          if (sentNegotiate) {
            throw new @OsUntrusted AccessControlException(
                "Client already attempted negotiation");
          }
          saslResponse = buildSaslNegotiateResponse();
          // simple-only server negotiate response is success which client
          // interprets as switch to simple
          if (saslResponse.getState() == SaslState.SUCCESS) {
            switchToSimple();
          }
          break;
        }
        case INITIATE: {
          if (saslMessage.getAuthsCount() != 1) {
            throw new @OsUntrusted SaslException("Client mechanism is malformed");
          }
          // verify the client requested an advertised authType
          @OsUntrusted
          SaslAuth clientSaslAuth = saslMessage.getAuths(0);
          if (!negotiateResponse.getAuthsList().contains(clientSaslAuth)) {
            if (sentNegotiate) {
              throw new @OsUntrusted AccessControlException(
                  clientSaslAuth.getMethod() + " authentication is not enabled."
                      + "  Available:" + enabledAuthMethods);
            }
            saslResponse = buildSaslNegotiateResponse();
            break;
          }
          authMethod = AuthMethod.valueOf(clientSaslAuth.getMethod());
          // abort SASL for SIMPLE auth, server has already ensured that
          // SIMPLE is a legit option above.  we will send no response
          if (authMethod == AuthMethod.SIMPLE) {
            switchToSimple();
            break;
          }
          // sasl server for tokens may already be instantiated
          if (saslServer == null || authMethod != AuthMethod.TOKEN) {
            saslServer = createSaslServer(authMethod);
          }
          // fallthru to process sasl token
        }
        case RESPONSE: {
          if (!saslMessage.hasToken()) {
            throw new @OsUntrusted SaslException("Client did not send a token");
          }
          @OsUntrusted
          byte @OsUntrusted [] saslToken = saslMessage.getToken().toByteArray();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Have read input token of size " + saslToken.length
                + " for processing by saslServer.evaluateResponse()");
          }
          saslToken = saslServer.evaluateResponse(saslToken);
          saslResponse = buildSaslResponse(
              saslServer.isComplete() ? SaslState.SUCCESS : SaslState.CHALLENGE,
              saslToken);
          break;
        }
        default:
          throw new @OsUntrusted SaslException("Client sent unsupported state " + state);
      }
      return saslResponse;
    }

    private void switchToSimple(@OsUntrusted Server.Connection this) {
      // disable SASL and blank out any SASL server
      authProtocol = AuthProtocol.NONE;
      saslServer = null;
    }
    
    private @OsUntrusted RpcSaslProto buildSaslResponse(@OsUntrusted Server.Connection this, @OsUntrusted SaslState state, @OsUntrusted byte @OsUntrusted [] replyToken) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Will send " + state + " token of size "
            + ((replyToken != null) ? replyToken.length : null)
            + " from saslServer.");
      }
      RpcSaslProto.@OsUntrusted Builder response = RpcSaslProto.newBuilder();
      response.setState(state);
      if (replyToken != null) {
        response.setToken(ByteString.copyFrom(replyToken));
      }
      return response.build();
    }
    
    private void doSaslReply(@OsUntrusted Server.Connection this, @OsUntrusted Message message) throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sending sasl message "+message);
      }
      setupResponse(saslResponse, saslCall,
          RpcStatusProto.SUCCESS, null,
          new @OsUntrusted RpcResponseWrapper(message), null, null);
      responder.doRespond(saslCall);
    }
    
    private void doSaslReply(@OsUntrusted Server.Connection this, @OsUntrusted Exception ioe) throws IOException {
      setupResponse(authFailedResponse, authFailedCall,
          RpcStatusProto.FATAL, RpcErrorCodeProto.FATAL_UNAUTHORIZED,
          null, ioe.getClass().getName(), ioe.getLocalizedMessage());
      responder.doRespond(authFailedCall);
    }
    
    private void disposeSasl(@OsUntrusted Server.Connection this) {
      if (saslServer != null) {
        try {
          saslServer.dispose();
        } catch (@OsUntrusted SaslException ignored) {
        }
      }
    }

    private void checkDataLength(@OsUntrusted Server.Connection this, @OsUntrusted int dataLength) throws IOException {
      if (dataLength < 0) {
        @OsUntrusted
        String error = "Unexpected data length " + dataLength +
                       "!! from " + getHostAddress();
        LOG.warn(error);
        throw new @OsUntrusted IOException(error);
      } else if (dataLength > maxDataLength) {
        @OsUntrusted
        String error = "Requested data length " + dataLength +
              " is longer than maximum configured RPC length " + 
            maxDataLength + ".  RPC came from " + getHostAddress();
        LOG.warn(error);
        throw new @OsUntrusted IOException(error);
      }
    }

    public @OsUntrusted int readAndProcess(@OsUntrusted Server.Connection this)
        throws WrappedRpcServerException, IOException, InterruptedException {
      while (true) {
        /* Read at most one RPC. If the header is not read completely yet
         * then iterate until we read first RPC or until there is no data left.
         */    
        @OsUntrusted
        int count = -1;
        if (dataLengthBuffer.remaining() > 0) {
          count = channelRead(channel, dataLengthBuffer);       
          if (count < 0 || dataLengthBuffer.remaining() > 0) 
            return count;
        }
        
        if (!connectionHeaderRead) {
          //Every connection is expected to send the header.
          if (connectionHeaderBuf == null) {
            connectionHeaderBuf = ByteBuffer.allocate(3);
          }
          count = channelRead(channel, connectionHeaderBuf);
          if (count < 0 || connectionHeaderBuf.remaining() > 0) {
            return count;
          }
          @OsUntrusted
          int version = connectionHeaderBuf.get(0);
          // TODO we should add handler for service class later
          this.setServiceClass(connectionHeaderBuf.get(1));
          dataLengthBuffer.flip();
          
          // Check if it looks like the user is hitting an IPC port
          // with an HTTP GET - this is a common error, so we can
          // send back a simple string indicating as much.
          if (HTTP_GET_BYTES.equals(dataLengthBuffer)) {
            setupHttpRequestOnIpcPortResponse();
            return -1;
          }
          
          if (!RpcConstants.HEADER.equals(dataLengthBuffer)
              || version != CURRENT_VERSION) {
            //Warning is ok since this is not supposed to happen.
            LOG.warn("Incorrect header or version mismatch from " + 
                     hostAddress + ":" + remotePort +
                     " got version " + version + 
                     " expected version " + CURRENT_VERSION);
            setupBadVersionResponse(version);
            return -1;
          }
          
          // this may switch us into SIMPLE
          authProtocol = initializeAuthContext(connectionHeaderBuf.get(2));          
          
          dataLengthBuffer.clear();
          connectionHeaderBuf = null;
          connectionHeaderRead = true;
          continue;
        }
        
        if (data == null) {
          dataLengthBuffer.flip();
          dataLength = dataLengthBuffer.getInt();
          checkDataLength(dataLength);
          data = ByteBuffer.allocate(dataLength);
        }
        
        count = channelRead(channel, data);
        
        if (data.remaining() == 0) {
          dataLengthBuffer.clear();
          data.flip();
          @OsUntrusted
          boolean isHeaderRead = connectionContextRead;
          processOneRpc(data.array());
          data = null;
          if (!isHeaderRead) {
            continue;
          }
        } 
        return count;
      }
    }

    private @OsUntrusted AuthProtocol initializeAuthContext(@OsUntrusted Server.Connection this, @OsUntrusted int authType)
        throws IOException {
      @OsUntrusted
      AuthProtocol authProtocol = AuthProtocol.valueOf(authType);
      if (authProtocol == null) {
        @OsUntrusted
        IOException ioe = new @OsUntrusted IpcException("Unknown auth protocol:" + authType);
        doSaslReply(ioe);
        throw ioe;        
      }
      @OsUntrusted
      boolean isSimpleEnabled = enabledAuthMethods.contains(AuthMethod.SIMPLE);
      switch (authProtocol) {
        case NONE: {
          // don't reply if client is simple and server is insecure
          if (!isSimpleEnabled) {
            @OsUntrusted
            IOException ioe = new @OsUntrusted AccessControlException(
                "SIMPLE authentication is not enabled."
                    + "  Available:" + enabledAuthMethods);
            doSaslReply(ioe);
            throw ioe;
          }
          break;
        }
        default: {
          break;
        }
      }
      return authProtocol;
    }

    private @OsUntrusted RpcSaslProto buildSaslNegotiateResponse(@OsUntrusted Server.Connection this)
        throws IOException, InterruptedException {
      @OsUntrusted
      RpcSaslProto negotiateMessage = negotiateResponse;
      // accelerate token negotiation by sending initial challenge
      // in the negotiation response
      if (enabledAuthMethods.contains(AuthMethod.TOKEN)) {
        saslServer = createSaslServer(AuthMethod.TOKEN);
        @OsUntrusted
        byte @OsUntrusted [] challenge = saslServer.evaluateResponse(new @OsUntrusted byte @OsUntrusted [0]);
        RpcSaslProto.@OsUntrusted Builder negotiateBuilder =
            RpcSaslProto.newBuilder(negotiateResponse);
        negotiateBuilder.getAuthsBuilder(0)  // TOKEN is always first
            .setChallenge(ByteString.copyFrom(challenge));
        negotiateMessage = negotiateBuilder.build();
      }
      sentNegotiate = true;
      return negotiateMessage;
    }
    
    private @OsUntrusted SaslServer createSaslServer(@OsUntrusted Server.Connection this, @OsUntrusted AuthMethod authMethod)
        throws IOException, InterruptedException {
      return new @OsUntrusted SaslRpcServer(authMethod).create(this, secretManager);
    }
    
    /**
     * Try to set up the response to indicate that the client version
     * is incompatible with the server. This can contain special-case
     * code to speak enough of past IPC protocols to pass back
     * an exception to the caller.
     * @param clientVersion the version the caller is using 
     * @throws IOException
     */
    private void setupBadVersionResponse(@OsUntrusted Server.Connection this, @OsUntrusted int clientVersion) throws IOException {
      @OsUntrusted
      String errMsg = "Server IPC version " + CURRENT_VERSION +
      " cannot communicate with client version " + clientVersion;
      @OsUntrusted
      ByteArrayOutputStream buffer = new @OsUntrusted ByteArrayOutputStream();
      
      if (clientVersion >= 9) {
        // Versions >>9  understand the normal response
        @OsUntrusted
        Call fakeCall = new @OsUntrusted Call(-1, RpcConstants.INVALID_RETRY_COUNT, null,
            this);
        setupResponse(buffer, fakeCall, 
            RpcStatusProto.FATAL, RpcErrorCodeProto.FATAL_VERSION_MISMATCH,
            null, VersionMismatch.class.getName(), errMsg);
        responder.doRespond(fakeCall);
      } else if (clientVersion >= 3) {
        @OsUntrusted
        Call fakeCall = new @OsUntrusted Call(-1, RpcConstants.INVALID_RETRY_COUNT, null,
            this);
        // Versions 3 to 8 use older response
        setupResponseOldVersionFatal(buffer, fakeCall,
            null, VersionMismatch.class.getName(), errMsg);

        responder.doRespond(fakeCall);
      } else if (clientVersion == 2) { // Hadoop 0.18.3
        @OsUntrusted
        Call fakeCall = new @OsUntrusted Call(0, RpcConstants.INVALID_RETRY_COUNT, null,
            this);
        @OsUntrusted
        DataOutputStream out = new @OsUntrusted DataOutputStream(buffer);
        out.writeInt(0); // call ID
        out.writeBoolean(true); // error
        WritableUtils.writeString(out, VersionMismatch.class.getName());
        WritableUtils.writeString(out, errMsg);
        fakeCall.setResponse(ByteBuffer.wrap(buffer.toByteArray()));
        
        responder.doRespond(fakeCall);
      }
    }
    
    private void setupHttpRequestOnIpcPortResponse(@OsUntrusted Server.Connection this) throws IOException {
      @OsUntrusted
      Call fakeCall = new @OsUntrusted Call(0, RpcConstants.INVALID_RETRY_COUNT, null, this);
      fakeCall.setResponse(ByteBuffer.wrap(
          RECEIVED_HTTP_REQ_RESPONSE.getBytes()));
      responder.doRespond(fakeCall);
    }

    /** Reads the connection context following the connection header
     * @param dis - DataInputStream from which to read the header 
     * @throws WrappedRpcServerException - if the header cannot be
     *         deserialized, or the user is not authorized
     */ 
    private void processConnectionContext(@OsUntrusted Server.Connection this, @OsUntrusted DataInputStream dis)
        throws WrappedRpcServerException {
      // allow only one connection context during a session
      if (connectionContextRead) {
        throw new @OsUntrusted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            "Connection context already processed");
      }
      connectionContext = decodeProtobufFromStream(
          IpcConnectionContextProto.newBuilder(), dis);
      protocolName = connectionContext.hasProtocol() ? connectionContext
          .getProtocol() : null;

      @OsUntrusted
      UserGroupInformation protocolUser = ProtoUtil.getUgi(connectionContext);
      if (saslServer == null) {
        user = protocolUser;
      } else {
        // user is authenticated
        user.setAuthenticationMethod(authMethod);
        //Now we check if this is a proxy user case. If the protocol user is
        //different from the 'user', it is a proxy user scenario. However, 
        //this is not allowed if user authenticated with DIGEST.
        if ((protocolUser != null)
            && (!protocolUser.getUserName().equals(user.getUserName()))) {
          if (authMethod == AuthMethod.TOKEN) {
            // Not allowed to doAs if token authentication is used
            throw new @OsUntrusted WrappedRpcServerException(
                RpcErrorCodeProto.FATAL_UNAUTHORIZED,
                new @OsUntrusted AccessControlException("Authenticated user (" + user
                    + ") doesn't match what the client claims to be ("
                    + protocolUser + ")"));
          } else {
            // Effective user can be different from authenticated user
            // for simple auth or kerberos auth
            // The user is the real user. Now we create a proxy user
            @OsUntrusted
            UserGroupInformation realUser = user;
            user = UserGroupInformation.createProxyUser(protocolUser
                .getUserName(), realUser);
          }
        }
      }
      authorizeConnection();
      // don't set until after authz because connection isn't established
      connectionContextRead = true;
    }
    
    /**
     * Process a wrapped RPC Request - unwrap the SASL packet and process
     * each embedded RPC request 
     * @param buf - SASL wrapped request of one or more RPCs
     * @throws IOException - SASL packet cannot be unwrapped
     * @throws InterruptedException
     */    
    private void unwrapPacketAndProcessRpcs(@OsUntrusted Server.Connection this, @OsUntrusted byte @OsUntrusted [] inBuf)
        throws WrappedRpcServerException, IOException, InterruptedException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Have read input token of size " + inBuf.length
            + " for processing by saslServer.unwrap()");
      }
      inBuf = saslServer.unwrap(inBuf, 0, inBuf.length);
      @OsUntrusted
      ReadableByteChannel ch = Channels.newChannel(new @OsUntrusted ByteArrayInputStream(
          inBuf));
      // Read all RPCs contained in the inBuf, even partial ones
      while (true) {
        @OsUntrusted
        int count = -1;
        if (unwrappedDataLengthBuffer.remaining() > 0) {
          count = channelRead(ch, unwrappedDataLengthBuffer);
          if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0)
            return;
        }

        if (unwrappedData == null) {
          unwrappedDataLengthBuffer.flip();
          @OsUntrusted
          int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();
          unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
        }

        count = channelRead(ch, unwrappedData);
        if (count <= 0 || unwrappedData.remaining() > 0)
          return;

        if (unwrappedData.remaining() == 0) {
          unwrappedDataLengthBuffer.clear();
          unwrappedData.flip();
          processOneRpc(unwrappedData.array());
          unwrappedData = null;
        }
      }
    }
    
    /**
     * Process an RPC Request - handle connection setup and decoding of
     * request into a Call
     * @param buf - contains the RPC request header and the rpc request
     * @throws IOException - internal error that should not be returned to
     *         client, typically failure to respond to client
     * @throws WrappedRpcServerException - an exception to be sent back to
     *         the client that does not require verbose logging by the
     *         Listener thread
     * @throws InterruptedException
     */    
    private void processOneRpc(@OsUntrusted Server.Connection this, @OsUntrusted byte @OsUntrusted [] buf)
        throws IOException, WrappedRpcServerException, InterruptedException {
      @OsUntrusted
      int callId = -1;
      @OsUntrusted
      int retry = RpcConstants.INVALID_RETRY_COUNT;
      try {
        final @OsUntrusted DataInputStream dis =
            new @OsUntrusted DataInputStream(new @OsUntrusted ByteArrayInputStream(buf));
        final @OsUntrusted RpcRequestHeaderProto header =
            decodeProtobufFromStream(RpcRequestHeaderProto.newBuilder(), dis);
        callId = header.getCallId();
        retry = header.getRetryCount();
        if (LOG.isDebugEnabled()) {
          LOG.debug(" got #" + callId);
        }
        checkRpcHeaders(header);
        
        if (callId < 0) { // callIds typically used during connection setup
          processRpcOutOfBandRequest(header, dis);
        } else if (!connectionContextRead) {
          throw new @OsUntrusted WrappedRpcServerException(
              RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
              "Connection context not established");
        } else {
          processRpcRequest(header, dis);
        }
      } catch (@OsUntrusted WrappedRpcServerException wrse) { // inform client of error
        @OsUntrusted
        Throwable ioe = wrse.getCause();
        final @OsUntrusted Call call = new @OsUntrusted Call(callId, retry, null, this);
        setupResponse(authFailedResponse, call,
            RpcStatusProto.FATAL, wrse.getRpcErrorCodeProto(), null,
            ioe.getClass().getName(), ioe.getMessage());
        responder.doRespond(call);
        throw wrse;
      }
    }

    /**
     * Verify RPC header is valid
     * @param header - RPC request header
     * @throws WrappedRpcServerException - header contains invalid values 
     */
    private void checkRpcHeaders(@OsUntrusted Server.Connection this, @OsUntrusted RpcRequestHeaderProto header)
        throws WrappedRpcServerException {
      if (!header.hasRpcOp()) {
        @OsUntrusted
        String err = " IPC Server: No rpc op in rpcRequestHeader";
        throw new @OsUntrusted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
      if (header.getRpcOp() != 
          RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET) {
        @OsUntrusted
        String err = "IPC Server does not implement rpc header operation" + 
                header.getRpcOp();
        throw new @OsUntrusted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
      // If we know the rpc kind, get its class so that we can deserialize
      // (Note it would make more sense to have the handler deserialize but 
      // we continue with this original design.
      if (!header.hasRpcKind()) {
        @OsUntrusted
        String err = " IPC Server: No rpc kind in rpcRequestHeader";
        throw new @OsUntrusted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
    }

    /**
     * Process an RPC Request - the connection headers and context must
     * have been already read
     * @param header - RPC request header
     * @param dis - stream to request payload
     * @throws WrappedRpcServerException - due to fatal rpc layer issues such
     *   as invalid header or deserialization error. In this case a RPC fatal
     *   status response will later be sent back to client.
     * @throws InterruptedException
     */
    private void processRpcRequest(@OsUntrusted Server.Connection this, @OsUntrusted RpcRequestHeaderProto header,
        @OsUntrusted
        DataInputStream dis) throws WrappedRpcServerException,
        InterruptedException {
      @OsUntrusted
      Class<@OsUntrusted ? extends @OsUntrusted Writable> rpcRequestClass = 
          getRpcRequestWrapper(header.getRpcKind());
      if (rpcRequestClass == null) {
        LOG.warn("Unknown rpc kind "  + header.getRpcKind() + 
            " from client " + getHostAddress());
        final @OsUntrusted String err = "Unknown rpc kind in rpc header"  + 
            header.getRpcKind();
        throw new @OsUntrusted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);   
      }
      @OsUntrusted
      Writable rpcRequest;
      try { //Read the rpc request
        rpcRequest = ReflectionUtils.newInstance(rpcRequestClass, conf);
        rpcRequest.readFields(dis);
      } catch (@OsUntrusted Throwable t) { // includes runtime exception from newInstance
        LOG.warn("Unable to read call parameters for client " +
                 getHostAddress() + "on connection protocol " +
            this.protocolName + " for rpcKind " + header.getRpcKind(),  t);
        @OsUntrusted
        String err = "IPC server unable to read call parameters: "+ t.getMessage();
        throw new @OsUntrusted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST, err);
      }
        
      @OsUntrusted
      Call call = new @OsUntrusted Call(header.getCallId(), header.getRetryCount(),
          rpcRequest, this, ProtoUtil.convert(header.getRpcKind()), header
              .getClientId().toByteArray());
      callQueue.put(call);              // queue the call; maybe blocked here
      incRpcCount();  // Increment the rpc count
    }


    /**
     * Establish RPC connection setup by negotiating SASL if required, then
     * reading and authorizing the connection header
     * @param header - RPC header
     * @param dis - stream to request payload
     * @throws WrappedRpcServerException - setup failed due to SASL
     *         negotiation failure, premature or invalid connection context,
     *         or other state errors 
     * @throws IOException - failed to send a response back to the client
     * @throws InterruptedException
     */
    private void processRpcOutOfBandRequest(@OsUntrusted Server.Connection this, @OsUntrusted RpcRequestHeaderProto header,
        @OsUntrusted
        DataInputStream dis) throws WrappedRpcServerException, IOException,
        InterruptedException {
      final @OsUntrusted int callId = header.getCallId();
      if (callId == CONNECTION_CONTEXT_CALL_ID) {
        // SASL must be established prior to connection context
        if (authProtocol == AuthProtocol.SASL && !saslContextEstablished) {
          throw new @OsUntrusted WrappedRpcServerException(
              RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
              "Connection header sent during SASL negotiation");
        }
        // read and authorize the user
        processConnectionContext(dis);
      } else if (callId == AuthProtocol.SASL.callId) {
        // if client was switched to simple, ignore first SASL message
        if (authProtocol != AuthProtocol.SASL) {
          throw new @OsUntrusted WrappedRpcServerException(
              RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
              "SASL protocol not requested by client");
        }
        saslReadAndProcess(dis);
      } else if (callId == PING_CALL_ID) {
        LOG.debug("Received ping message");
      } else {
        throw new @OsUntrusted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            "Unknown out of band call #" + callId);
      }
    }    

    /**
     * Authorize proxy users to access this server
     * @throws WrappedRpcServerException - user is not allowed to proxy
     */
    private void authorizeConnection(@OsUntrusted Server.Connection this) throws WrappedRpcServerException {
      try {
        // If auth method is TOKEN, the token was obtained by the
        // real user for the effective user, therefore not required to
        // authorize real user. doAs is allowed only for simple or kerberos
        // authentication
        if (user != null && user.getRealUser() != null
            && (authMethod != AuthMethod.TOKEN)) {
          ProxyUsers.authorize(user, this.getHostAddress(), conf);
        }
        authorize(user, protocolName, getHostInetAddress());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Successfully authorized " + connectionContext);
        }
        rpcMetrics.incrAuthorizationSuccesses();
      } catch (@OsUntrusted AuthorizationException ae) {
        LOG.info("Connection from " + this
            + " for protocol " + connectionContext.getProtocol()
            + " is unauthorized for user " + user);
        rpcMetrics.incrAuthorizationFailures();
        throw new @OsUntrusted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_UNAUTHORIZED, ae);
      }
    }
    
    /**
     * Decode the a protobuf from the given input stream 
     * @param builder - Builder of the protobuf to decode
     * @param dis - DataInputStream to read the protobuf
     * @return Message - decoded protobuf
     * @throws WrappedRpcServerException - deserialization failed
     */
    @SuppressWarnings("unchecked")
    private <@OsUntrusted T extends @OsUntrusted Message> @OsUntrusted T decodeProtobufFromStream(@OsUntrusted Server.Connection this, @OsUntrusted Builder builder,
        @OsUntrusted
        DataInputStream dis) throws WrappedRpcServerException {
      try {
        builder.mergeDelimitedFrom(dis);
        return (@OsUntrusted T)builder.build();
      } catch (@OsUntrusted Exception ioe) {
        @OsUntrusted
        Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protoClass = builder.getDefaultInstanceForType().getClass();
        throw new @OsUntrusted WrappedRpcServerException(
            RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST,
            "Error decoding " + protoClass.getSimpleName() + ": "+ ioe);
      }
    }

    /**
     * Get service class for connection
     * @return the serviceClass
     */
    public @OsUntrusted int getServiceClass(@OsUntrusted Server.Connection this) {
      return serviceClass;
    }

    /**
     * Set service class for connection
     * @param serviceClass the serviceClass to set
     */
    public void setServiceClass(@OsUntrusted Server.Connection this, @OsUntrusted int serviceClass) {
      this.serviceClass = serviceClass;
    }

    private synchronized void close(@OsUntrusted Server.Connection this) {
      disposeSasl();
      data = null;
      dataLengthBuffer = null;
      if (!channel.isOpen())
        return;
      try {socket.shutdownOutput();} catch(@OsUntrusted Exception e) {
        LOG.debug("Ignoring socket shutdown exception", e);
      }
      if (channel.isOpen()) {
        try {channel.close();} catch(@OsUntrusted Exception e) {}
      }
      try {socket.close();} catch(@OsUntrusted Exception e) {}
    }
  }

  /** Handles queued calls . */
  private class Handler extends @OsUntrusted Thread {
    public @OsUntrusted Handler(@OsUntrusted int instanceNumber) {
      this.setDaemon(true);
      this.setName("IPC Server handler "+ instanceNumber + " on " + port);
    }

    @Override
    public void run(@OsUntrusted Server.Handler this) {
      LOG.debug(getName() + ": starting");
      SERVER.set(Server.this);
      @OsUntrusted
      ByteArrayOutputStream buf = 
        new @OsUntrusted ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE);
      while (running) {
        try {
          final @OsUntrusted Call call = callQueue.take(); // pop the queue; maybe blocked here
          if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": " + call + " for RpcKind " + call.rpcKind);
          }
          @OsUntrusted
          String errorClass = null;
          @OsUntrusted
          String error = null;
          @OsUntrusted
          RpcStatusProto returnStatus = RpcStatusProto.SUCCESS;
          @OsUntrusted
          RpcErrorCodeProto detailedErr = null;
          @OsUntrusted
          Writable value = null;

          CurCall.set(call);
          try {
            // Make the call as the user via Subject.doAs, thus associating
            // the call with the Subject
            if (call.connection.user == null) {
              value = call(call.rpcKind, call.connection.protocolName, call.rpcRequest, 
                           call.timestamp);
            } else {
              value = 
                call.connection.user.doAs
                  (new @OsUntrusted PrivilegedExceptionAction<@OsUntrusted Writable>() {
                     @Override
                     public @OsUntrusted Writable run() throws Exception {
                       // make the call
                       return call(call.rpcKind, call.connection.protocolName, 
                                   call.rpcRequest, call.timestamp);

                     }
                   }
                  );
            }
          } catch (@OsUntrusted Throwable e) {
            if (e instanceof @OsUntrusted UndeclaredThrowableException) {
              e = e.getCause();
            }
            @OsUntrusted
            String logMsg = getName() + ", call " + call + ": error: " + e;
            if (e instanceof @OsUntrusted RuntimeException || e instanceof @OsUntrusted Error) {
              // These exception types indicate something is probably wrong
              // on the server side, as opposed to just a normal exceptional
              // result.
              LOG.warn(logMsg, e);
            } else if (exceptionsHandler.isTerse(e.getClass())) {
             // Don't log the whole stack trace of these exceptions.
              // Way too noisy!
              LOG.info(logMsg);
            } else {
              LOG.info(logMsg, e);
            }
            if (e instanceof @OsUntrusted RpcServerException) {
              @OsUntrusted
              RpcServerException rse = ((@OsUntrusted RpcServerException)e); 
              returnStatus = rse.getRpcStatusProto();
              detailedErr = rse.getRpcErrorCodeProto();
            } else {
              returnStatus = RpcStatusProto.ERROR;
              detailedErr = RpcErrorCodeProto.ERROR_APPLICATION;
            }
            errorClass = e.getClass().getName();
            error = StringUtils.stringifyException(e);
            // Remove redundant error class name from the beginning of the stack trace
            @OsUntrusted
            String exceptionHdr = errorClass + ": ";
            if (error.startsWith(exceptionHdr)) {
              error = error.substring(exceptionHdr.length());
            }
          }
          CurCall.set(null);
          synchronized (call.connection.responseQueue) {
            // setupResponse() needs to be sync'ed together with 
            // responder.doResponse() since setupResponse may use
            // SASL to encrypt response data and SASL enforces
            // its own message ordering.
            setupResponse(buf, call, returnStatus, detailedErr, 
                value, errorClass, error);
            
            // Discard the large buf and reset it back to smaller size 
            // to free up heap
            if (buf.size() > maxRespSize) {
              LOG.warn("Large response size " + buf.size() + " for call "
                  + call.toString());
              buf = new @OsUntrusted ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE);
            }
            responder.doRespond(call);
          }
        } catch (@OsUntrusted InterruptedException e) {
          if (running) {                          // unexpected -- log it
            LOG.info(getName() + " unexpectedly interrupted", e);
          }
        } catch (@OsUntrusted Exception e) {
          LOG.info(getName() + " caught an exception", e);
        }
      }
      LOG.debug(getName() + ": exiting");
    }

  }
  
  protected @OsUntrusted Server(@OsUntrusted String bindAddress, @OsUntrusted int port,
                  @OsUntrusted
                  Class<@OsUntrusted ? extends @OsUntrusted Writable> paramClass, @OsUntrusted int handlerCount, 
                  @OsUntrusted
                  Configuration conf)
    throws IOException 
  {
    this(bindAddress, port, paramClass, handlerCount, -1, -1, conf, Integer
        .toString(port), null, null);
  }
  
  protected @OsUntrusted Server(@OsUntrusted String bindAddress, @OsUntrusted int port,
      @OsUntrusted
      Class<@OsUntrusted ? extends @OsUntrusted Writable> rpcRequestClass, @OsUntrusted int handlerCount,
      @OsUntrusted
      int numReaders, @OsUntrusted int queueSizePerHandler, @OsUntrusted Configuration conf,
      @OsUntrusted
      String serverName, @OsUntrusted SecretManager<@OsUntrusted ? extends @OsUntrusted TokenIdentifier> secretManager)
    throws IOException {
    this(bindAddress, port, rpcRequestClass, handlerCount, numReaders, 
        queueSizePerHandler, conf, serverName, secretManager, null);
  }
  
  /** 
   * Constructs a server listening on the named port and address.  Parameters passed must
   * be of the named class.  The <code>handlerCount</handlerCount> determines
   * the number of handler threads that will be used to process calls.
   * If queueSizePerHandler or numReaders are not -1 they will be used instead of parameters
   * from configuration. Otherwise the configuration will be picked up.
   * 
   * If rpcRequestClass is null then the rpcRequestClass must have been 
   * registered via {@link #registerProtocolEngine(RPC.RpcKind,
   *  Class, RPC.RpcInvoker)}
   * This parameter has been retained for compatibility with existing tests
   * and usage.
   */
  @SuppressWarnings("unchecked")
  protected @OsUntrusted Server(@OsUntrusted String bindAddress, @OsUntrusted int port,
      @OsUntrusted
      Class<@OsUntrusted ? extends @OsUntrusted Writable> rpcRequestClass, @OsUntrusted int handlerCount,
      @OsUntrusted
      int numReaders, @OsUntrusted int queueSizePerHandler, @OsUntrusted Configuration conf,
      @OsUntrusted
      String serverName, @OsUntrusted SecretManager<@OsUntrusted ? extends @OsUntrusted TokenIdentifier> secretManager,
      @OsUntrusted
      String portRangeConfig)
    throws IOException {
    this.bindAddress = bindAddress;
    this.conf = conf;
    this.portRangeConfig = portRangeConfig;
    this.port = port;
    this.rpcRequestClass = rpcRequestClass; 
    this.handlerCount = handlerCount;
    this.socketSendBufferSize = 0;
    this.maxDataLength = conf.getInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    if (queueSizePerHandler != -1) {
      this.maxQueueSize = queueSizePerHandler;
    } else {
      this.maxQueueSize = handlerCount * conf.getInt(
          CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
          CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);      
    }
    this.maxRespSize = conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY,
        CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT);
    if (numReaders != -1) {
      this.readThreads = numReaders;
    } else {
      this.readThreads = conf.getInt(
          CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY,
          CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_DEFAULT);
    }
    this.callQueue  = new @OsUntrusted LinkedBlockingQueue<@OsUntrusted Call>(maxQueueSize); 
    this.maxIdleTime = 2 * conf.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
    this.maxConnectionsToNuke = conf.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_KEY,
        CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_DEFAULT);
    this.thresholdIdleConnections = conf.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY,
        CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_DEFAULT);
    this.secretManager = (@OsUntrusted SecretManager<@OsUntrusted TokenIdentifier>) secretManager;
    this.authorize = 
      conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, 
                      false);

    // configure supported authentications
    this.enabledAuthMethods = getAuthMethods(secretManager, conf);
    this.negotiateResponse = buildNegotiateResponse(enabledAuthMethods);
    
    // Start the listener here and let it bind to the port
    listener = new @OsUntrusted Listener();
    this.port = listener.getAddress().getPort();    
    this.rpcMetrics = RpcMetrics.create(this);
    this.rpcDetailedMetrics = RpcDetailedMetrics.create(this.port);
    this.tcpNoDelay = conf.getBoolean(
        CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_DEFAULT);

    // Create the responder here
    responder = new @OsUntrusted Responder();
    
    if (secretManager != null) {
      SaslRpcServer.init(conf);
    }
    
    this.exceptionsHandler.addTerseExceptions(StandbyException.class);
  }
  
  private @OsUntrusted RpcSaslProto buildNegotiateResponse(@OsUntrusted Server this, @OsUntrusted List<@OsUntrusted AuthMethod> authMethods)
      throws IOException {
    RpcSaslProto.@OsUntrusted Builder negotiateBuilder = RpcSaslProto.newBuilder();
    if (authMethods.contains(AuthMethod.SIMPLE) && authMethods.size() == 1) {
      // SIMPLE-only servers return success in response to negotiate
      negotiateBuilder.setState(SaslState.SUCCESS);
    } else {
      negotiateBuilder.setState(SaslState.NEGOTIATE);
      for (@OsUntrusted AuthMethod authMethod : authMethods) {
        @OsUntrusted
        SaslRpcServer saslRpcServer = new @OsUntrusted SaslRpcServer(authMethod);      
        SaslAuth.@OsUntrusted Builder builder = negotiateBuilder.addAuthsBuilder()
            .setMethod(authMethod.toString())
            .setMechanism(saslRpcServer.mechanism);
        if (saslRpcServer.protocol != null) {
          builder.setProtocol(saslRpcServer.protocol);
        }
        if (saslRpcServer.serverId != null) {
          builder.setServerId(saslRpcServer.serverId);
        }
      }
    }
    return negotiateBuilder.build();
  }

  // get the security type from the conf. implicitly include token support
  // if a secret manager is provided, or fail if token is the conf value but
  // there is no secret manager
  private @OsUntrusted List<@OsUntrusted AuthMethod> getAuthMethods(@OsUntrusted Server this, @OsUntrusted SecretManager<@OsUntrusted ? extends java.lang.@OsUntrusted Object> secretManager,
                                             @OsUntrusted
                                             Configuration conf) {
    @OsUntrusted
    AuthenticationMethod confAuthenticationMethod =
        SecurityUtil.getAuthenticationMethod(conf);        
    @OsUntrusted
    List<@OsUntrusted AuthMethod> authMethods = new @OsUntrusted ArrayList<@OsUntrusted AuthMethod>();
    if (confAuthenticationMethod == AuthenticationMethod.TOKEN) {
      if (secretManager == null) {
        throw new @OsUntrusted IllegalArgumentException(AuthenticationMethod.TOKEN +
            " authentication requires a secret manager");
      } 
    } else if (secretManager != null) {
      LOG.debug(AuthenticationMethod.TOKEN +
          " authentication enabled for secret manager");
      // most preferred, go to the front of the line!
      authMethods.add(AuthenticationMethod.TOKEN.getAuthMethod());
    }
    authMethods.add(confAuthenticationMethod.getAuthMethod());        
    
    LOG.debug("Server accepts auth methods:" + authMethods);
    return authMethods;
  }
  
  private void closeConnection(@OsUntrusted Server this, @OsUntrusted Connection connection) {
    synchronized (connectionList) {
      if (connectionList.remove(connection))
        numConnections--;
    }
    connection.close();
  }
  
  /**
   * Setup response for the IPC Call.
   * 
   * @param responseBuf buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param status of the IPC call
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponse(@OsUntrusted Server this, @OsUntrusted ByteArrayOutputStream responseBuf,
                             @OsUntrusted
                             Call call, @OsUntrusted RpcStatusProto status, @OsUntrusted RpcErrorCodeProto erCode,
                             @OsUntrusted
                             Writable rv, @OsUntrusted String errorClass, @OsUntrusted String error) 
  throws IOException {
    responseBuf.reset();
    @OsUntrusted
    DataOutputStream out = new @OsUntrusted DataOutputStream(responseBuf);
    RpcResponseHeaderProto.@OsUntrusted Builder headerBuilder =  
        RpcResponseHeaderProto.newBuilder();
    headerBuilder.setClientId(ByteString.copyFrom(call.clientId));
    headerBuilder.setCallId(call.callId);
    headerBuilder.setRetryCount(call.retryCount);
    headerBuilder.setStatus(status);
    headerBuilder.setServerIpcVersionNum(CURRENT_VERSION);

    if (status == RpcStatusProto.SUCCESS) {
      @OsUntrusted
      RpcResponseHeaderProto header = headerBuilder.build();
      final @OsUntrusted int headerLen = header.getSerializedSize();
      @OsUntrusted
      int fullLength  = CodedOutputStream.computeRawVarint32Size(headerLen) +
          headerLen;
      try {
        if (rv instanceof ProtobufRpcEngine.@OsUntrusted RpcWrapper) {
          ProtobufRpcEngine.@OsUntrusted RpcWrapper resWrapper = 
              (ProtobufRpcEngine.@OsUntrusted RpcWrapper) rv;
          fullLength += resWrapper.getLength();
          out.writeInt(fullLength);
          header.writeDelimitedTo(out);
          rv.write(out);
        } else { // Have to serialize to buffer to get len
          final @OsUntrusted DataOutputBuffer buf = new @OsUntrusted DataOutputBuffer();
          rv.write(buf);
          @OsUntrusted
          byte @OsUntrusted [] data = buf.getData();
          fullLength += buf.getLength();
          out.writeInt(fullLength);
          header.writeDelimitedTo(out);
          out.write(data, 0, buf.getLength());
        }
      } catch (@OsUntrusted Throwable t) {
        LOG.warn("Error serializing call response for call " + call, t);
        // Call back to same function - this is OK since the
        // buffer is reset at the top, and since status is changed
        // to ERROR it won't infinite loop.
        setupResponse(responseBuf, call, RpcStatusProto.ERROR,
            RpcErrorCodeProto.ERROR_SERIALIZING_RESPONSE,
            null, t.getClass().getName(),
            StringUtils.stringifyException(t));
        return;
      }
    } else { // Rpc Failure
      headerBuilder.setExceptionClassName(errorClass);
      headerBuilder.setErrorMsg(error);
      headerBuilder.setErrorDetail(erCode);
      @OsUntrusted
      RpcResponseHeaderProto header = headerBuilder.build();
      @OsUntrusted
      int headerLen = header.getSerializedSize();
      final @OsUntrusted int fullLength  = 
          CodedOutputStream.computeRawVarint32Size(headerLen) + headerLen;
      out.writeInt(fullLength);
      header.writeDelimitedTo(out);
    }
    if (call.connection.useWrap) {
      wrapWithSasl(responseBuf, call);
    }
    call.setResponse(ByteBuffer.wrap(responseBuf.toByteArray()));
  }
  
  /**
   * Setup response for the IPC Call on Fatal Error from a 
   * client that is using old version of Hadoop.
   * The response is serialized using the previous protocol's response
   * layout.
   * 
   * @param response buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponseOldVersionFatal(@OsUntrusted Server this, @OsUntrusted ByteArrayOutputStream response, 
                             @OsUntrusted
                             Call call,
                             @OsUntrusted
                             Writable rv, @OsUntrusted String errorClass, @OsUntrusted String error) 
  throws IOException {
    final @OsUntrusted int OLD_VERSION_FATAL_STATUS = -1;
    response.reset();
    @OsUntrusted
    DataOutputStream out = new @OsUntrusted DataOutputStream(response);
    out.writeInt(call.callId);                // write call id
    out.writeInt(OLD_VERSION_FATAL_STATUS);   // write FATAL_STATUS
    WritableUtils.writeString(out, errorClass);
    WritableUtils.writeString(out, error);

    if (call.connection.useWrap) {
      wrapWithSasl(response, call);
    }
    call.setResponse(ByteBuffer.wrap(response.toByteArray()));
  }
  
  
  private void wrapWithSasl(@OsUntrusted Server this, @OsUntrusted ByteArrayOutputStream response, @OsUntrusted Call call)
      throws IOException {
    if (call.connection.saslServer != null) {
      @OsUntrusted
      byte @OsUntrusted [] token = response.toByteArray();
      // synchronization may be needed since there can be multiple Handler
      // threads using saslServer to wrap responses.
      synchronized (call.connection.saslServer) {
        token = call.connection.saslServer.wrap(token, 0, token.length);
      }
      if (LOG.isDebugEnabled())
        LOG.debug("Adding saslServer wrapped token of size " + token.length
            + " as call response.");
      response.reset();
      // rebuild with sasl header and payload
      @OsUntrusted
      RpcResponseHeaderProto saslHeader = RpcResponseHeaderProto.newBuilder()
          .setCallId(AuthProtocol.SASL.callId)
          .setStatus(RpcStatusProto.SUCCESS)
          .build();
      @OsUntrusted
      RpcSaslProto saslMessage = RpcSaslProto.newBuilder()
          .setState(SaslState.WRAP)
          .setToken(ByteString.copyFrom(token, 0, token.length))
          .build();
      @OsUntrusted
      RpcResponseMessageWrapper saslResponse =
          new @OsUntrusted RpcResponseMessageWrapper(saslHeader, saslMessage);

      @OsUntrusted
      DataOutputStream out = new @OsUntrusted DataOutputStream(response);
      out.writeInt(saslResponse.getLength());
      saslResponse.write(out);
    }
  }
  
  @OsUntrusted
  Configuration getConf(@OsUntrusted Server this) {
    return conf;
  }
  
  /** Sets the socket buffer size used for responding to RPCs */
  public void setSocketSendBufSize(@OsUntrusted Server this, @OsUntrusted int size) { this.socketSendBufferSize = size; }

  /** Starts the service.  Must be called before any calls will be handled. */
  public synchronized void start(@OsUntrusted Server this) {
    responder.start();
    listener.start();
    handlers = new @OsUntrusted Handler @OsUntrusted [handlerCount];
    
    for (@OsUntrusted int i = 0; i < handlerCount; i++) {
      handlers[i] = new @OsUntrusted Handler(i);
      handlers[i].start();
    }
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  public synchronized void stop(@OsUntrusted Server this) {
    LOG.info("Stopping server on " + port);
    running = false;
    if (handlers != null) {
      for (@OsUntrusted int i = 0; i < handlerCount; i++) {
        if (handlers[i] != null) {
          handlers[i].interrupt();
        }
      }
    }
    listener.interrupt();
    listener.doStop();
    responder.interrupt();
    notifyAll();
    if (this.rpcMetrics != null) {
      this.rpcMetrics.shutdown();
    }
    if (this.rpcDetailedMetrics != null) {
      this.rpcDetailedMetrics.shutdown();
    }
  }

  /** Wait for the server to be stopped.
   * Does not wait for all subthreads to finish.
   *  See {@link #stop()}.
   */
  public synchronized void join(@OsUntrusted Server this) throws InterruptedException {
    while (running) {
      wait();
    }
  }

  /**
   * Return the socket (ip+port) on which the RPC server is listening to.
   * @return the socket (ip+port) on which the RPC server is listening to.
   */
  public synchronized @OsUntrusted InetSocketAddress getListenerAddress(@OsUntrusted Server this) {
    return listener.getAddress();
  }
  
  /** 
   * Called for each call. 
   * @deprecated Use  {@link #call(RPC.RpcKind, String,
   *  Writable, long)} instead
   */
  @Deprecated
  public @OsUntrusted Writable call(@OsUntrusted Server this, @OsUntrusted Writable param, @OsUntrusted long receiveTime) throws Exception {
    return call(RPC.RpcKind.RPC_BUILTIN, null, param, receiveTime);
  }
  
  /** Called for each call. */
  public abstract @OsUntrusted Writable call(@OsUntrusted Server this, RPC.@OsUntrusted RpcKind rpcKind, @OsUntrusted String protocol,
      @OsUntrusted
      Writable param, @OsUntrusted long receiveTime) throws Exception;
  
  /**
   * Authorize the incoming client connection.
   * 
   * @param user client user
   * @param protocolName - the protocol
   * @param addr InetAddress of incoming connection
   * @throws AuthorizationException when the client isn't authorized to talk the protocol
   */
  private void authorize(@OsUntrusted Server this, @OsUntrusted UserGroupInformation user, @OsUntrusted String protocolName,
      @OsUntrusted
      InetAddress addr) throws AuthorizationException {
    if (authorize) {
      if (protocolName == null) {
        throw new @OsUntrusted AuthorizationException("Null protocol not authorized");
      }
      @OsUntrusted
      Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol = null;
      try {
        protocol = getProtocolClass(protocolName, getConf());
      } catch (@OsUntrusted ClassNotFoundException cfne) {
        throw new @OsUntrusted AuthorizationException("Unknown protocol: " + 
                                         protocolName);
      }
      serviceAuthorizationManager.authorize(user, protocol, getConf(), addr);
    }
  }
  
  /**
   * Get the port on which the IPC Server is listening for incoming connections.
   * This could be an ephemeral port too, in which case we return the real
   * port on which the Server has bound.
   * @return port on which IPC Server is listening
   */
  public @OsUntrusted int getPort(@OsUntrusted Server this) {
    return port;
  }
  
  /**
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  public @OsUntrusted int getNumOpenConnections(@OsUntrusted Server this) {
    return numConnections;
  }
  
  /**
   * The number of rpc calls in the queue.
   * @return The number of rpc calls in the queue.
   */
  public @OsUntrusted int getCallQueueLen(@OsUntrusted Server this) {
    return callQueue.size();
  }
  
  /**
   * The maximum size of the rpc call queue of this server.
   * @return The maximum size of the rpc call queue.
   */
  public @OsUntrusted int getMaxQueueSize(@OsUntrusted Server this) {
    return maxQueueSize;
  }

  /**
   * The number of reader threads for this server.
   * @return The number of reader threads.
   */
  public @OsUntrusted int getNumReaders(@OsUntrusted Server this) {
    return readThreads;
  }

  /**
   * When the read or write buffer size is larger than this limit, i/o will be 
   * done in chunks of this size. Most RPC requests and responses would be
   * be smaller.
   */
  private static @OsUntrusted int NIO_BUFFER_LIMIT = 8*1024; //should not be more than 64KB.
  
  /**
   * This is a wrapper around {@link WritableByteChannel#write(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks. 
   * This is to avoid jdk from creating many direct buffers as the size of 
   * buffer increases. This also minimizes extra copies in NIO layer
   * as a result of multiple write operations required to write a large 
   * buffer.  
   *
   * @see WritableByteChannel#write(ByteBuffer)
   */
  private @OsUntrusted int channelWrite(@OsUntrusted Server this, @OsUntrusted WritableByteChannel channel, 
                           @OsUntrusted
                           ByteBuffer buffer) throws IOException {
    
    @OsUntrusted
    int count =  (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                 channel.write(buffer) : channelIO(null, channel, buffer);
    if (count > 0) {
      rpcMetrics.incrSentBytes(count);
    }
    return count;
  }
  
  
  /**
   * This is a wrapper around {@link ReadableByteChannel#read(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks. 
   * This is to avoid jdk from creating many direct buffers as the size of 
   * ByteBuffer increases. There should not be any performance degredation.
   * 
   * @see ReadableByteChannel#read(ByteBuffer)
   */
  private @OsUntrusted int channelRead(@OsUntrusted Server this, @OsUntrusted ReadableByteChannel channel, 
                          @OsUntrusted
                          ByteBuffer buffer) throws IOException {
    
    @OsUntrusted
    int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                channel.read(buffer) : channelIO(channel, null, buffer);
    if (count > 0) {
      rpcMetrics.incrReceivedBytes(count);
    }
    return count;
  }
  
  /**
   * Helper for {@link #channelRead(ReadableByteChannel, ByteBuffer)}
   * and {@link #channelWrite(WritableByteChannel, ByteBuffer)}. Only
   * one of readCh or writeCh should be non-null.
   * 
   * @see #channelRead(ReadableByteChannel, ByteBuffer)
   * @see #channelWrite(WritableByteChannel, ByteBuffer)
   */
  private static @OsUntrusted int channelIO(@OsUntrusted ReadableByteChannel readCh, 
                               @OsUntrusted
                               WritableByteChannel writeCh,
                               @OsUntrusted
                               ByteBuffer buf) throws IOException {
    
    @OsUntrusted
    int originalLimit = buf.limit();
    @OsUntrusted
    int initialRemaining = buf.remaining();
    @OsUntrusted
    int ret = 0;
    
    while (buf.remaining() > 0) {
      try {
        @OsUntrusted
        int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
        buf.limit(buf.position() + ioSize);
        
        ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf); 
        
        if (ret < ioSize) {
          break;
        }

      } finally {
        buf.limit(originalLimit);        
      }
    }

    @OsUntrusted
    int nBytes = initialRemaining - buf.remaining(); 
    return (nBytes > 0) ? nBytes : ret;
  }
}
