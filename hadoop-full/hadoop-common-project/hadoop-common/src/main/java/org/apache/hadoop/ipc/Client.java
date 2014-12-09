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
import static org.apache.hadoop.ipc.RpcConstants.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.SocketFactory;
import javax.security.sasl.Sasl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestMessageWrapper;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.Server.AuthProtocol;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SaslRpcClient;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.CodedOutputStream;

/** A client for an IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Server
 */
@InterfaceAudience.LimitedPrivate(value = { "Common", "HDFS", "MapReduce", "Yarn" })
@InterfaceStability.Evolving
public class Client {
  
  public static final @OsUntrusted Log LOG = LogFactory.getLog(Client.class);

  /** A counter for generating call IDs. */
  private static final @OsUntrusted AtomicInteger callIdCounter = new @OsUntrusted AtomicInteger();

  private static final @OsUntrusted ThreadLocal<@OsUntrusted Integer> callId = new @OsUntrusted ThreadLocal<@OsUntrusted Integer>();
  private static final @OsUntrusted ThreadLocal<@OsUntrusted Integer> retryCount = new @OsUntrusted ThreadLocal<@OsUntrusted Integer>();

  /** Set call id and retry count for the next call. */
  public static void setCallIdAndRetryCount(@OsUntrusted int cid, @OsUntrusted int rc) {
    Preconditions.checkArgument(cid != RpcConstants.INVALID_CALL_ID);
    Preconditions.checkState(callId.get() == null);
    Preconditions.checkArgument(rc != RpcConstants.INVALID_RETRY_COUNT);

    callId.set(cid);
    retryCount.set(rc);
  }

  private @OsUntrusted Hashtable<@OsUntrusted ConnectionId, @OsUntrusted Connection> connections =
    new @OsUntrusted Hashtable<@OsUntrusted ConnectionId, @OsUntrusted Connection>();

  private @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Writable> valueClass;   // class of call values
  private @OsUntrusted AtomicBoolean running = new @OsUntrusted AtomicBoolean(true); // if client runs
  final private @OsUntrusted Configuration conf;

  private @OsUntrusted SocketFactory socketFactory;           // how to create sockets
  private @OsUntrusted int refCount = 1;

  private final @OsUntrusted int connectionTimeout;

  private final @OsUntrusted boolean fallbackAllowed;
  private final @OsUntrusted byte @OsUntrusted [] clientId;
  
  /**
   * Executor on which IPC calls' parameters are sent.
   * Deferring the sending of parameters to a separate
   * thread isolates them from thread interruptions in the
   * calling code.
   */
  private final @OsUntrusted ExecutorService sendParamsExecutor;
  private final static @OsUntrusted ClientExecutorServiceFactory clientExcecutorFactory =
      new @OsUntrusted ClientExecutorServiceFactory();

  private static class ClientExecutorServiceFactory {
    private @OsUntrusted int executorRefCount = 0;
    private @OsUntrusted ExecutorService clientExecutor = null;
    
    /**
     * Get Executor on which IPC calls' parameters are sent.
     * If the internal reference counter is zero, this method
     * creates the instance of Executor. If not, this method
     * just returns the reference of clientExecutor.
     * 
     * @return An ExecutorService instance
     */
    synchronized @OsUntrusted ExecutorService refAndGetInstance(Client.@OsUntrusted ClientExecutorServiceFactory this) {
      if (executorRefCount == 0) {
        clientExecutor = Executors.newCachedThreadPool(
            new @OsUntrusted ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("IPC Parameter Sending Thread #%d")
            .build());
      }
      executorRefCount++;
      
      return clientExecutor;
    }
    
    /**
     * Cleanup Executor on which IPC calls' parameters are sent.
     * If reference counter is zero, this method discards the
     * instance of the Executor. If not, this method
     * just decrements the internal reference counter.
     * 
     * @return An ExecutorService instance if it exists.
     *   Null is returned if not.
     */
    synchronized @OsUntrusted ExecutorService unrefAndCleanup(Client.@OsUntrusted ClientExecutorServiceFactory this) {
      executorRefCount--;
      assert(executorRefCount >= 0);
      
      if (executorRefCount == 0) {
        clientExecutor.shutdown();
        try {
          if (!clientExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
            clientExecutor.shutdownNow();
          }
        } catch (@OsUntrusted InterruptedException e) {
          LOG.error("Interrupted while waiting for clientExecutor" +
              "to stop", e);
          clientExecutor.shutdownNow();
        }
        clientExecutor = null;
      }
      
      return clientExecutor;
    }
  };
  
  /**
   * set the ping interval value in configuration
   * 
   * @param conf Configuration
   * @param pingInterval the ping interval
   */
  final public static void setPingInterval(@OsUntrusted Configuration conf, @OsUntrusted int pingInterval) {
    conf.setInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, pingInterval);
  }

  /**
   * Get the ping interval from configuration;
   * If not set in the configuration, return the default value.
   * 
   * @param conf Configuration
   * @return the ping interval
   */
  final static @OsUntrusted int getPingInterval(@OsUntrusted Configuration conf) {
    return conf.getInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
        CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);
  }

  /**
   * The time after which a RPC will timeout.
   * If ping is not enabled (via ipc.client.ping), then the timeout value is the 
   * same as the pingInterval.
   * If ping is enabled, then there is no timeout value.
   * 
   * @param conf Configuration
   * @return the timeout period in milliseconds. -1 if no timeout value is set
   */
  final public static @OsUntrusted int getTimeout(@OsUntrusted Configuration conf) {
    if (!conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, true)) {
      return getPingInterval(conf);
    }
    return -1;
  }
  /**
   * set the connection timeout value in configuration
   * 
   * @param conf Configuration
   * @param timeout the socket connect timeout value
   */
  public static final void setConnectTimeout(@OsUntrusted Configuration conf, @OsUntrusted int timeout) {
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY, timeout);
  }

  /**
   * Increment this client's reference count
   *
   */
  synchronized void incCount(@OsUntrusted Client this) {
    refCount++;
  }
  
  /**
   * Decrement this client's reference count
   *
   */
  synchronized void decCount(@OsUntrusted Client this) {
    refCount--;
  }
  
  /**
   * Return if this client has no reference
   * 
   * @return true if this client has no reference; false otherwise
   */
  synchronized @OsUntrusted boolean isZeroReference(@OsUntrusted Client this) {
    return refCount==0;
  }

  /** Check the rpc response header. */
  void checkResponse(@OsUntrusted Client this, @OsUntrusted RpcResponseHeaderProto header) throws IOException {
    if (header == null) {
      throw new @OsUntrusted IOException("Response is null.");
    }
    if (header.hasClientId()) {
      // check client IDs
      final @OsUntrusted byte @OsUntrusted [] id = header.getClientId().toByteArray();
      if (!Arrays.equals(id, RpcConstants.DUMMY_CLIENT_ID)) {
        if (!Arrays.equals(id, clientId)) {
          throw new @OsUntrusted IOException("Client IDs not matched: local ID="
              + StringUtils.byteToHexString(clientId) + ", ID in reponse="
              + StringUtils.byteToHexString(header.getClientId().toByteArray()));
        }
      }
    }
  }

  @OsUntrusted
  Call createCall(@OsUntrusted Client this, RPC.@OsUntrusted RpcKind rpcKind, @OsUntrusted Writable rpcRequest) {
    return new @OsUntrusted Call(rpcKind, rpcRequest);
  }

  /** 
   * Class that represents an RPC call
   */
  static class Call {
    final @OsUntrusted int id;               // call id
    final @OsUntrusted int retry;           // retry count
    final @OsUntrusted Writable rpcRequest;  // the serialized rpc request
    @OsUntrusted
    Writable rpcResponse;       // null if rpc has error
    @OsUntrusted
    IOException error;          // exception, null if success
    final RPC.@OsUntrusted RpcKind rpcKind;      // Rpc EngineKind
    @OsUntrusted
    boolean done;               // true when call is done

    private @OsUntrusted Call(RPC.@OsUntrusted RpcKind rpcKind, @OsUntrusted Writable param) {
      this.rpcKind = rpcKind;
      this.rpcRequest = param;

      final @OsUntrusted Integer id = callId.get();
      if (id == null) {
        this.id = nextCallId();
      } else {
        callId.set(null);
        this.id = id;
      }
      
      final @OsUntrusted Integer rc = retryCount.get();
      if (rc == null) {
        this.retry = 0;
      } else {
        this.retry = rc;
      }
    }

    /** Indicate when the call is complete and the
     * value or error are available.  Notifies by default.  */
    protected synchronized void callComplete(Client.@OsUntrusted Call this) {
      this.done = true;
      notify();                                 // notify caller
    }

    /** Set the exception when there is an error.
     * Notify the caller the call is done.
     * 
     * @param error exception thrown by the call; either local or remote
     */
    public synchronized void setException(Client.@OsUntrusted Call this, @OsUntrusted IOException error) {
      this.error = error;
      callComplete();
    }
    
    /** Set the return value when there is no error. 
     * Notify the caller the call is done.
     * 
     * @param rpcResponse return value of the rpc call.
     */
    public synchronized void setRpcResponse(Client.@OsUntrusted Call this, @OsUntrusted Writable rpcResponse) {
      this.rpcResponse = rpcResponse;
      callComplete();
    }
    
    public synchronized @OsUntrusted Writable getRpcResponse(Client.@OsUntrusted Call this) {
      return rpcResponse;
    }
  }

  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  private class Connection extends @OsUntrusted Thread {
    private @OsUntrusted InetSocketAddress server;             // server ip:port
    private final @OsUntrusted ConnectionId remoteId;                // connection id
    private @OsUntrusted AuthMethod authMethod; // authentication method
    private @OsUntrusted AuthProtocol authProtocol;
    private @OsUntrusted int serviceClass;
    private @OsUntrusted SaslRpcClient saslRpcClient;
    
    private @OsUntrusted Socket socket = null;                 // connected socket
    private @OsUntrusted DataInputStream in;
    private @OsUntrusted DataOutputStream out;
    private @OsUntrusted int rpcTimeout;
    private @OsUntrusted int maxIdleTime; //connections will be culled if it was idle for 
    //maxIdleTime msecs
    private final @OsUntrusted RetryPolicy connectionRetryPolicy;
    private @OsUntrusted int maxRetriesOnSocketTimeouts;
    private @OsUntrusted boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    private @OsUntrusted boolean doPing; //do we need to send ping message
    private @OsUntrusted int pingInterval; // how often sends ping to the server in msecs
    private @OsUntrusted ByteArrayOutputStream pingRequest; // ping message
    
    // currently active calls
    private @OsUntrusted Hashtable<@OsUntrusted Integer, @OsUntrusted Call> calls = new @OsUntrusted Hashtable<@OsUntrusted Integer, @OsUntrusted Call>();
    private @OsUntrusted AtomicLong lastActivity = new @OsUntrusted AtomicLong();// last I/O activity time
    private @OsUntrusted AtomicBoolean shouldCloseConnection = new @OsUntrusted AtomicBoolean();  // indicate if the connection is closed
    private @OsUntrusted IOException closeException; // close reason
    
    private final @OsUntrusted Object sendRpcRequestLock = new @OsUntrusted Object();

    public @OsUntrusted Connection(@OsUntrusted ConnectionId remoteId, @OsUntrusted int serviceClass) throws IOException {
      this.remoteId = remoteId;
      this.server = remoteId.getAddress();
      if (server.isUnresolved()) {
        throw NetUtils.wrapException(server.getHostName(),
            server.getPort(),
            null,
            0,
            new @OsUntrusted UnknownHostException());
      }
      this.rpcTimeout = remoteId.getRpcTimeout();
      this.maxIdleTime = remoteId.getMaxIdleTime();
      this.connectionRetryPolicy = remoteId.connectionRetryPolicy;
      this.maxRetriesOnSocketTimeouts = remoteId.getMaxRetriesOnSocketTimeouts();
      this.tcpNoDelay = remoteId.getTcpNoDelay();
      this.doPing = remoteId.getDoPing();
      if (doPing) {
        // construct a RPC header with the callId as the ping callId
        pingRequest = new @OsUntrusted ByteArrayOutputStream();
        @OsUntrusted
        RpcRequestHeaderProto pingHeader = ProtoUtil
            .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
                OperationProto.RPC_FINAL_PACKET, PING_CALL_ID,
                RpcConstants.INVALID_RETRY_COUNT, clientId);
        pingHeader.writeDelimitedTo(pingRequest);
      }
      this.pingInterval = remoteId.getPingInterval();
      this.serviceClass = serviceClass;
      if (LOG.isDebugEnabled()) {
        LOG.debug("The ping interval is " + this.pingInterval + " ms.");
      }

      @OsUntrusted
      UserGroupInformation ticket = remoteId.getTicket();
      // try SASL if security is enabled or if the ugi contains tokens.
      // this causes a SIMPLE client with tokens to attempt SASL
      @OsUntrusted
      boolean trySasl = UserGroupInformation.isSecurityEnabled() ||
                        (ticket != null && !ticket.getTokens().isEmpty());
      this.authProtocol = trySasl ? AuthProtocol.SASL : AuthProtocol.NONE;
      
      this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " +
          server.toString() +
          " from " + ((ticket==null)?"an unknown user":ticket.getUserName()));
      this.setDaemon(true);
    }

    /** Update lastActivity with the current time. */
    private void touch(@OsUntrusted Client.Connection this) {
      lastActivity.set(Time.now());
    }

    /**
     * Add a call to this connection's call queue and notify
     * a listener; synchronized.
     * Returns false if called during shutdown.
     * @param call to add
     * @return true if the call was added.
     */
    private synchronized @OsUntrusted boolean addCall(@OsUntrusted Client.Connection this, @OsUntrusted Call call) {
      if (shouldCloseConnection.get())
        return false;
      calls.put(call.id, call);
      notify();
      return true;
    }

    /** This class sends a ping to the remote side when timeout on
     * reading. If no failure is detected, it retries until at least
     * a byte is read.
     */
    private class PingInputStream extends @OsUntrusted FilterInputStream {
      /* constructor */
      protected @OsUntrusted PingInputStream(@OsUntrusted InputStream in) {
        super(in);
      }

      /* Process timeout exception
       * if the connection is not going to be closed or 
       * is not configured to have a RPC timeout, send a ping.
       * (if rpcTimeout is not set to be 0, then RPC should timeout.
       * otherwise, throw the timeout exception.
       */
      private void handleTimeout(@OsUntrusted Client.Connection.PingInputStream this, @OsUntrusted SocketTimeoutException e) throws IOException {
        if (shouldCloseConnection.get() || !running.get() || rpcTimeout > 0) {
          throw e;
        } else {
          sendPing();
        }
      }
      
      /** Read a byte from the stream.
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * @throws IOException for any IO problem other than socket timeout
       */
      @Override
      public @OsUntrusted int read(@OsUntrusted Client.Connection.PingInputStream this) throws IOException {
        do {
          try {
            return super.read();
          } catch (@OsUntrusted SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }

      /** Read bytes into a buffer starting from offset <code>off</code>
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * 
       * @return the total number of bytes read; -1 if the connection is closed.
       */
      @Override
      public @OsUntrusted int read(@OsUntrusted Client.Connection.PingInputStream this, @OsUntrusted byte @OsUntrusted [] buf, @OsUntrusted int off, @OsUntrusted int len) throws IOException {
        do {
          try {
            return super.read(buf, off, len);
          } catch (@OsUntrusted SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }
    }
    
    private synchronized void disposeSasl(@OsUntrusted Client.Connection this) {
      if (saslRpcClient != null) {
        try {
          saslRpcClient.dispose();
          saslRpcClient = null;
        } catch (@OsUntrusted IOException ignored) {
        }
      }
    }
    
    private synchronized @OsUntrusted boolean shouldAuthenticateOverKrb(@OsUntrusted Client.Connection this) throws IOException {
      @OsUntrusted
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      @OsUntrusted
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      @OsUntrusted
      UserGroupInformation realUser = currentUser.getRealUser();
      if (authMethod == AuthMethod.KERBEROS && loginUser != null &&
      // Make sure user logged in using Kerberos either keytab or TGT
          loginUser.hasKerberosCredentials() &&
          // relogin only in case it is the login user (e.g. JT)
          // or superuser (like oozie).
          (loginUser.equals(currentUser) || loginUser.equals(realUser))) {
        return true;
      }
      return false;
    }
    
    private synchronized @OsUntrusted AuthMethod setupSaslConnection(@OsUntrusted Client.Connection this, final @OsUntrusted InputStream in2, 
        final @OsUntrusted OutputStream out2) throws IOException, InterruptedException {
      saslRpcClient = new @OsUntrusted SaslRpcClient(remoteId.getTicket(),
          remoteId.getProtocol(), remoteId.getAddress(), conf);
      return saslRpcClient.saslConnect(in2, out2);
    }

    /**
     * Update the server address if the address corresponding to the host
     * name has changed.
     *
     * @return true if an addr change was detected.
     * @throws IOException when the hostname cannot be resolved.
     */
    private synchronized @OsUntrusted boolean updateAddress(@OsUntrusted Client.Connection this) throws IOException {
      // Do a fresh lookup with the old host name.
      @OsUntrusted
      InetSocketAddress currentAddr = NetUtils.createSocketAddrForHost(
                               server.getHostName(), server.getPort());

      if (!server.equals(currentAddr)) {
        LOG.warn("Address change detected. Old: " + server.toString() +
                                 " New: " + currentAddr.toString());
        server = currentAddr;
        return true;
      }
      return false;
    }
    
    private synchronized void setupConnection(@OsUntrusted Client.Connection this) throws IOException {
      @OsUntrusted
      short ioFailures = 0;
      @OsUntrusted
      short timeoutFailures = 0;
      while (true) {
        try {
          this.socket = socketFactory.createSocket();
          this.socket.setTcpNoDelay(tcpNoDelay);
          
          /*
           * Bind the socket to the host specified in the principal name of the
           * client, to ensure Server matching address of the client connection
           * to host name in principal passed.
           */
          @OsUntrusted
          UserGroupInformation ticket = remoteId.getTicket();
          if (ticket != null && ticket.hasKerberosCredentials()) {
            @OsUntrusted
            KerberosInfo krbInfo = 
              remoteId.getProtocol().getAnnotation(KerberosInfo.class);
            if (krbInfo != null && krbInfo.clientPrincipal() != null) {
              @OsUntrusted
              String host = 
                SecurityUtil.getHostFromPrincipal(remoteId.getTicket().getUserName());
              
              // If host name is a valid local address then bind socket to it
              @OsUntrusted
              InetAddress localAddr = NetUtils.getLocalInetAddress(host);
              if (localAddr != null) {
                this.socket.bind(new @OsUntrusted InetSocketAddress(localAddr, 0));
              }
            }
          }
          
          NetUtils.connect(this.socket, server, connectionTimeout);
          if (rpcTimeout > 0) {
            pingInterval = rpcTimeout;  // rpcTimeout overwrites pingInterval
          }
          this.socket.setSoTimeout(pingInterval);
          return;
        } catch (@OsUntrusted ConnectTimeoutException toe) {
          /* Check for an address change and update the local reference.
           * Reset the failure counter if the address was changed
           */
          if (updateAddress()) {
            timeoutFailures = ioFailures = 0;
          }
          handleConnectionTimeout(timeoutFailures++,
              maxRetriesOnSocketTimeouts, toe);
        } catch (@OsUntrusted IOException ie) {
          if (updateAddress()) {
            timeoutFailures = ioFailures = 0;
          }
          handleConnectionFailure(ioFailures++, ie);
        }
      }
    }

    /**
     * If multiple clients with the same principal try to connect to the same
     * server at the same time, the server assumes a replay attack is in
     * progress. This is a feature of kerberos. In order to work around this,
     * what is done is that the client backs off randomly and tries to initiate
     * the connection again. The other problem is to do with ticket expiry. To
     * handle that, a relogin is attempted.
     */
    private synchronized void handleSaslConnectionFailure(
        @OsUntrusted Client.Connection this, final @OsUntrusted int currRetries, final @OsUntrusted int maxRetries, final @OsUntrusted Exception ex,
        final @OsUntrusted Random rand, final @OsUntrusted UserGroupInformation ugi) throws IOException,
        InterruptedException {
      ugi.doAs(new @OsUntrusted PrivilegedExceptionAction<@OsUntrusted Object>() {
        @Override
        public @OsUntrusted Object run() throws IOException, InterruptedException {
          final @OsUntrusted short MAX_BACKOFF = 5000;
          closeConnection();
          disposeSasl();
          if (shouldAuthenticateOverKrb()) {
            if (currRetries < maxRetries) {
              if(LOG.isDebugEnabled()) {
                LOG.debug("Exception encountered while connecting to "
                    + "the server : " + ex);
              }
              // try re-login
              if (UserGroupInformation.isLoginKeytabBased()) {
                UserGroupInformation.getLoginUser().reloginFromKeytab();
              } else {
                UserGroupInformation.getLoginUser().reloginFromTicketCache();
              }
              // have granularity of milliseconds
              //we are sleeping with the Connection lock held but since this
              //connection instance is being used for connecting to the server
              //in question, it is okay
              Thread.sleep((rand.nextInt(MAX_BACKOFF) + 1));
              return null;
            } else {
              @OsUntrusted
              String msg = "Couldn't setup connection for "
                  + UserGroupInformation.getLoginUser().getUserName() + " to "
                  + remoteId;
              LOG.warn(msg);
              throw (@OsUntrusted IOException) new @OsUntrusted IOException(msg).initCause(ex);
            }
          } else {
            LOG.warn("Exception encountered while connecting to "
                + "the server : " + ex);
          }
          if (ex instanceof @OsUntrusted RemoteException)
            throw (@OsUntrusted RemoteException) ex;
          throw new @OsUntrusted IOException(ex);
        }
      });
    }

    
    /** Connect to the server and set up the I/O streams. It then sends
     * a header to the server and starts
     * the connection thread that waits for responses.
     */
    private synchronized void setupIOstreams(@OsUntrusted Client.Connection this) {
      if (socket != null || shouldCloseConnection.get()) {
        return;
      } 
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to "+server);
        }
        @OsUntrusted
        short numRetries = 0;
        final @OsUntrusted short MAX_RETRIES = 5;
        @OsUntrusted
        Random rand = null;
        while (true) {
          setupConnection();
          @OsUntrusted
          InputStream inStream = NetUtils.getInputStream(socket);
          @OsUntrusted
          OutputStream outStream = NetUtils.getOutputStream(socket);
          writeConnectionHeader(outStream);
          if (authProtocol == AuthProtocol.SASL) {
            final @OsUntrusted InputStream in2 = inStream;
            final @OsUntrusted OutputStream out2 = outStream;
            @OsUntrusted
            UserGroupInformation ticket = remoteId.getTicket();
            if (ticket.getRealUser() != null) {
              ticket = ticket.getRealUser();
            }
            try {
              authMethod = ticket
                  .doAs(new @OsUntrusted PrivilegedExceptionAction<@OsUntrusted AuthMethod>() {
                    @Override
                    public @OsUntrusted AuthMethod run()
                        throws IOException, InterruptedException {
                      return setupSaslConnection(in2, out2);
                    }
                  });
            } catch (@OsUntrusted Exception ex) {
              authMethod = saslRpcClient.getAuthMethod();
              if (rand == null) {
                rand = new @OsUntrusted Random();
              }
              handleSaslConnectionFailure(numRetries++, MAX_RETRIES, ex, rand,
                  ticket);
              continue;
            }
            if (authMethod != AuthMethod.SIMPLE) {
              // Sasl connect is successful. Let's set up Sasl i/o streams.
              inStream = saslRpcClient.getInputStream(inStream);
              outStream = saslRpcClient.getOutputStream(outStream);
              // for testing
              remoteId.saslQop =
                  (@OsUntrusted String)saslRpcClient.getNegotiatedProperty(Sasl.QOP);
            } else if (UserGroupInformation.isSecurityEnabled() &&
                       !fallbackAllowed) {
              throw new @OsUntrusted IOException("Server asks us to fall back to SIMPLE " +
                  "auth, but this client is configured to only allow secure " +
                  "connections.");
            }
          }
        
          if (doPing) {
            inStream = new @OsUntrusted PingInputStream(inStream);
          }
          this.in = new @OsUntrusted DataInputStream(new @OsUntrusted BufferedInputStream(inStream));

          // SASL may have already buffered the stream
          if (!(outStream instanceof @OsUntrusted BufferedOutputStream)) {
            outStream = new @OsUntrusted BufferedOutputStream(outStream);
          }
          this.out = new @OsUntrusted DataOutputStream(outStream);
          
          writeConnectionContext(remoteId, authMethod);

          // update last activity time
          touch();

          // start the receiver thread after the socket connection has been set
          // up
          start();
          return;
        }
      } catch (@OsUntrusted Throwable t) {
        if (t instanceof @OsUntrusted IOException) {
          markClosed((@OsUntrusted IOException)t);
        } else {
          markClosed(new @OsUntrusted IOException("Couldn't set up IO streams", t));
        }
        close();
      }
    }
    
    private void closeConnection(@OsUntrusted Client.Connection this) {
      if (socket == null) {
        return;
      }
      // close the current connection
      try {
        socket.close();
      } catch (@OsUntrusted IOException e) {
        LOG.warn("Not able to close a socket", e);
      }
      // set socket to null so that the next call to setupIOstreams
      // can start the process of connect all over again.
      socket = null;
    }

    /* Handle connection failures due to timeout on connect
     *
     * If the current number of retries is equal to the max number of retries,
     * stop retrying and throw the exception; Otherwise backoff 1 second and
     * try connecting again.
     *
     * This Method is only called from inside setupIOstreams(), which is
     * synchronized. Hence the sleep is synchronized; the locks will be retained.
     *
     * @param curRetries current number of retries
     * @param maxRetries max number of retries allowed
     * @param ioe failure reason
     * @throws IOException if max number of retries is reached
     */
    private void handleConnectionTimeout(
        @OsUntrusted Client.Connection this, @OsUntrusted
        int curRetries, @OsUntrusted int maxRetries, @OsUntrusted IOException ioe) throws IOException {

      closeConnection();

      // throw the exception if the maximum number of retries is reached
      if (curRetries >= maxRetries) {
        throw ioe;
      }
      LOG.info("Retrying connect to server: " + server + ". Already tried "
          + curRetries + " time(s); maxRetries=" + maxRetries);
    }

    private void handleConnectionFailure(@OsUntrusted Client.Connection this, @OsUntrusted int curRetries, @OsUntrusted IOException ioe
        ) throws IOException {
      closeConnection();

      final @OsUntrusted RetryAction action;
      try {
        action = connectionRetryPolicy.shouldRetry(ioe, curRetries, 0, true);
      } catch(@OsUntrusted Exception e) {
        throw e instanceof @OsUntrusted IOException? (@OsUntrusted IOException)e: new @OsUntrusted IOException(e);
      }
      if (action.action == RetryAction.RetryDecision.FAIL) {
        if (action.reason != null) {
          LOG.warn("Failed to connect to server: " + server + ": "
              + action.reason, ioe);
        }
        throw ioe;
      }

      try {
        Thread.sleep(action.delayMillis);
      } catch (@OsUntrusted InterruptedException e) {
        throw (@OsUntrusted IOException)new @OsUntrusted InterruptedIOException("Interrupted: action="
            + action + ", retry policy=" + connectionRetryPolicy).initCause(e);
      }
      LOG.info("Retrying connect to server: " + server + ". Already tried "
          + curRetries + " time(s); retry policy is " + connectionRetryPolicy);
    }

    /**
     * Write the connection header - this is sent when connection is established
     * +----------------------------------+
     * |  "hrpc" 4 bytes                  |      
     * +----------------------------------+
     * |  Version (1 byte)                |
     * +----------------------------------+
     * |  Service Class (1 byte)          |
     * +----------------------------------+
     * |  AuthProtocol (1 byte)           |      
     * +----------------------------------+
     */
    private void writeConnectionHeader(@OsUntrusted Client.Connection this, @OsUntrusted OutputStream outStream)
        throws IOException {
      @OsUntrusted
      DataOutputStream out = new @OsUntrusted DataOutputStream(new @OsUntrusted BufferedOutputStream(outStream));
      // Write out the header, version and authentication method
      out.write(RpcConstants.HEADER.array());
      out.write(RpcConstants.CURRENT_VERSION);
      out.write(serviceClass);
      out.write(authProtocol.callId);
      out.flush();
    }
    
    /* Write the connection context header for each connection
     * Out is not synchronized because only the first thread does this.
     */
    private void writeConnectionContext(@OsUntrusted Client.Connection this, @OsUntrusted ConnectionId remoteId,
                                        @OsUntrusted
                                        AuthMethod authMethod)
                                            throws IOException {
      // Write out the ConnectionHeader
      @OsUntrusted
      IpcConnectionContextProto message = ProtoUtil.makeIpcConnectionContext(
          RPC.getProtocolName(remoteId.getProtocol()),
          remoteId.getTicket(),
          authMethod);
      @OsUntrusted
      RpcRequestHeaderProto connectionContextHeader = ProtoUtil
          .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
              OperationProto.RPC_FINAL_PACKET, CONNECTION_CONTEXT_CALL_ID,
              RpcConstants.INVALID_RETRY_COUNT, clientId);
      @OsUntrusted
      RpcRequestMessageWrapper request =
          new @OsUntrusted RpcRequestMessageWrapper(connectionContextHeader, message);
      
      // Write out the packet length
      out.writeInt(request.getLength());
      request.write(out);
    }
    
    /* wait till someone signals us to start reading RPC response or
     * it is idle too long, it is marked as to be closed, 
     * or the client is marked as not running.
     * 
     * Return true if it is time to read a response; false otherwise.
     */
    private synchronized @OsUntrusted boolean waitForWork(@OsUntrusted Client.Connection this) {
      if (calls.isEmpty() && !shouldCloseConnection.get()  && running.get())  {
        @OsUntrusted
        long timeout = maxIdleTime-
              (Time.now()-lastActivity.get());
        if (timeout>0) {
          try {
            wait(timeout);
          } catch (@OsUntrusted InterruptedException e) {}
        }
      }
      
      if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
        return true;
      } else if (shouldCloseConnection.get()) {
        return false;
      } else if (calls.isEmpty()) { // idle connection closed or stopped
        markClosed(null);
        return false;
      } else { // get stopped but there are still pending requests 
        markClosed((@OsUntrusted IOException)new @OsUntrusted IOException().initCause(
            new @OsUntrusted InterruptedException()));
        return false;
      }
    }

    public @OsUntrusted InetSocketAddress getRemoteAddress(@OsUntrusted Client.Connection this) {
      return server;
    }

    /* Send a ping to the server if the time elapsed 
     * since last I/O activity is equal to or greater than the ping interval
     */
    private synchronized void sendPing(@OsUntrusted Client.Connection this) throws IOException {
      @OsUntrusted
      long curTime = Time.now();
      if ( curTime - lastActivity.get() >= pingInterval) {
        lastActivity.set(curTime);
        synchronized (out) {
          out.writeInt(pingRequest.size());
          pingRequest.writeTo(out);
          out.flush();
        }
      }
    }

    @Override
    public void run(@OsUntrusted Client.Connection this) {
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": starting, having connections " 
            + connections.size());

      try {
        while (waitForWork()) {//wait here for work - read or close connection
          receiveRpcResponse();
        }
      } catch (@OsUntrusted Throwable t) {
        // This truly is unexpected, since we catch IOException in receiveResponse
        // -- this is only to be really sure that we don't leave a client hanging
        // forever.
        LOG.warn("Unexpected error reading responses on connection " + this, t);
        markClosed(new @OsUntrusted IOException("Error reading responses", t));
      }
      
      close();
      
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": stopped, remaining connections "
            + connections.size());
    }

    /** Initiates a rpc call by sending the rpc request to the remote server.
     * Note: this is not called from the Connection thread, but by other
     * threads.
     * @param call - the rpc request
     */
    public void sendRpcRequest(@OsUntrusted Client.Connection this, final @OsUntrusted Call call)
        throws InterruptedException, IOException {
      if (shouldCloseConnection.get()) {
        return;
      }

      // Serialize the call to be sent. This is done from the actual
      // caller thread, rather than the sendParamsExecutor thread,
      
      // so that if the serialization throws an error, it is reported
      // properly. This also parallelizes the serialization.
      //
      // Format of a call on the wire:
      // 0) Length of rest below (1 + 2)
      // 1) RpcRequestHeader  - is serialized Delimited hence contains length
      // 2) RpcRequest
      //
      // Items '1' and '2' are prepared here. 
      final @OsUntrusted DataOutputBuffer d = new @OsUntrusted DataOutputBuffer();
      @OsUntrusted
      RpcRequestHeaderProto header = ProtoUtil.makeRpcRequestHeader(
          call.rpcKind, OperationProto.RPC_FINAL_PACKET, call.id, call.retry,
          clientId);
      header.writeDelimitedTo(d);
      call.rpcRequest.write(d);

      synchronized (sendRpcRequestLock) {
        @OsUntrusted
        Future<@OsUntrusted ? extends java.lang.@OsUntrusted Object> senderFuture = sendParamsExecutor.submit(new @OsUntrusted Runnable() {
          @Override
          public void run() {
            try {
              synchronized (Connection.this.out) {
                if (shouldCloseConnection.get()) {
                  return;
                }
                
                if (LOG.isDebugEnabled())
                  LOG.debug(getName() + " sending #" + call.id);
         
                @OsUntrusted
                byte @OsUntrusted [] data = d.getData();
                @OsUntrusted
                int totalLength = d.getLength();
                out.writeInt(totalLength); // Total Length
                out.write(data, 0, totalLength);// RpcRequestHeader + RpcRequest
                out.flush();
              }
            } catch (@OsUntrusted IOException e) {
              // exception at this point would leave the connection in an
              // unrecoverable state (eg half a call left on the wire).
              // So, close the connection, killing any outstanding calls
              markClosed(e);
            } finally {
              //the buffer is just an in-memory buffer, but it is still polite to
              // close early
              IOUtils.closeStream(d);
            }
          }
        });
      
        try {
          senderFuture.get();
        } catch (@OsUntrusted ExecutionException e) {
          @OsUntrusted
          Throwable cause = e.getCause();
          
          // cause should only be a RuntimeException as the Runnable above
          // catches IOException
          if (cause instanceof @OsUntrusted RuntimeException) {
            throw (@OsUntrusted RuntimeException) cause;
          } else {
            throw new @OsUntrusted RuntimeException("unexpected checked exception", cause);
          }
        }
      }
    }

    /* Receive a response.
     * Because only one receiver, so no synchronization on in.
     */
    private void receiveRpcResponse(@OsUntrusted Client.Connection this) {
      if (shouldCloseConnection.get()) {
        return;
      }
      touch();
      
      try {
        @OsUntrusted
        int totalLen = in.readInt();
        @OsUntrusted
        RpcResponseHeaderProto header = 
            RpcResponseHeaderProto.parseDelimitedFrom(in);
        checkResponse(header);

        @OsUntrusted
        int headerLen = header.getSerializedSize();
        headerLen += CodedOutputStream.computeRawVarint32Size(headerLen);

        @OsUntrusted
        int callId = header.getCallId();
        if (LOG.isDebugEnabled())
          LOG.debug(getName() + " got value #" + callId);

        @OsUntrusted
        Call call = calls.get(callId);
        @OsUntrusted
        RpcStatusProto status = header.getStatus();
        if (status == RpcStatusProto.SUCCESS) {
          @OsUntrusted
          Writable value = ReflectionUtils.newInstance(valueClass, conf);
          value.readFields(in);                 // read value
          calls.remove(callId);
          call.setRpcResponse(value);
          
          // verify that length was correct
          // only for ProtobufEngine where len can be verified easily
          if (call.getRpcResponse() instanceof ProtobufRpcEngine.@OsUntrusted RpcWrapper) {
            ProtobufRpcEngine.@OsUntrusted RpcWrapper resWrapper = 
                (ProtobufRpcEngine.@OsUntrusted RpcWrapper) call.getRpcResponse();
            if (totalLen != headerLen + resWrapper.getLength()) { 
              throw new @OsUntrusted RpcClientException(
                  "RPC response length mismatch on rpc success");
            }
          }
        } else { // Rpc Request failed
          // Verify that length was correct
          if (totalLen != headerLen) {
            throw new @OsUntrusted RpcClientException(
                "RPC response length mismatch on rpc error");
          }
          
          final @OsUntrusted String exceptionClassName = header.hasExceptionClassName() ?
                header.getExceptionClassName() : 
                  "ServerDidNotSetExceptionClassName";
          final @OsUntrusted String errorMsg = header.hasErrorMsg() ? 
                header.getErrorMsg() : "ServerDidNotSetErrorMsg" ;
          final @OsUntrusted RpcErrorCodeProto erCode = 
                    (header.hasErrorDetail() ? header.getErrorDetail() : null);
          if (erCode == null) {
             LOG.warn("Detailed error code not set by server on rpc error");
          }
          @OsUntrusted
          RemoteException re = 
              ( (erCode == null) ? 
                  new @OsUntrusted RemoteException(exceptionClassName, errorMsg) :
              new @OsUntrusted RemoteException(exceptionClassName, errorMsg, erCode));
          if (status == RpcStatusProto.ERROR) {
            calls.remove(callId);
            call.setException(re);
          } else if (status == RpcStatusProto.FATAL) {
            // Close the connection
            markClosed(re);
          }
        }
      } catch (@OsUntrusted IOException e) {
        markClosed(e);
      }
    }
    
    private synchronized void markClosed(@OsUntrusted Client.Connection this, @OsUntrusted IOException e) {
      if (shouldCloseConnection.compareAndSet(false, true)) {
        closeException = e;
        notifyAll();
      }
    }
    
    /** Close the connection. */
    private synchronized void close(@OsUntrusted Client.Connection this) {
      if (!shouldCloseConnection.get()) {
        LOG.error("The connection is not in the closed state");
        return;
      }

      // release the resources
      // first thing to do;take the connection out of the connection list
      synchronized (connections) {
        if (connections.get(remoteId) == this) {
          connections.remove(remoteId);
        }
      }

      // close the streams and therefore the socket
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
      disposeSasl();

      // clean up all calls
      if (closeException == null) {
        if (!calls.isEmpty()) {
          LOG.warn(
              "A connection is closed for no cause and calls are not empty");

          // clean up calls anyway
          closeException = new @OsUntrusted IOException("Unexpected closed connection");
          cleanupCalls();
        }
      } else {
        // log the info
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing ipc connection to " + server + ": " +
              closeException.getMessage(),closeException);
        }

        // cleanup calls
        cleanupCalls();
      }
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": closed");
    }
    
    /* Cleanup all calls and mark them as done */
    private void cleanupCalls(@OsUntrusted Client.Connection this) {
      @OsUntrusted
      Iterator<@OsUntrusted Entry<@OsUntrusted Integer, @OsUntrusted Call>> itor = calls.entrySet().iterator() ;
      while (itor.hasNext()) {
        @OsUntrusted
        Call c = itor.next().getValue(); 
        itor.remove();
        c.setException(closeException); // local exception
      }
    }
  }

  /** Construct an IPC client whose values are of the given {@link Writable}
   * class. */
  public @OsUntrusted Client(@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Writable> valueClass, @OsUntrusted Configuration conf, 
      @OsUntrusted
      SocketFactory factory) {
    this.valueClass = valueClass;
    this.conf = conf;
    this.socketFactory = factory;
    this.connectionTimeout = conf.getInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
        CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT);
    this.fallbackAllowed = conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
    this.clientId = ClientId.getClientId();
    this.sendParamsExecutor = clientExcecutorFactory.refAndGetInstance();
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * @param valueClass
   * @param conf
   */
  public @OsUntrusted Client(@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Writable> valueClass, @OsUntrusted Configuration conf) {
    this(valueClass, conf, NetUtils.getDefaultSocketFactory(conf));
  }
 
  /** Return the socket factory of this client
   *
   * @return this client's socket factory
   */
  @OsUntrusted
  SocketFactory getSocketFactory(@OsUntrusted Client this) {
    return socketFactory;
  }

  /** Stop all threads related to this client.  No further calls may be made
   * using this client. */
  public void stop(@OsUntrusted Client this) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping client");
    }

    if (!running.compareAndSet(true, false)) {
      return;
    }
    
    // wake up all connections
    synchronized (connections) {
      for (@OsUntrusted Connection conn : connections.values()) {
        conn.interrupt();
      }
    }
    
    // wait until all connections are closed
    while (!connections.isEmpty()) {
      try {
        Thread.sleep(100);
      } catch (@OsUntrusted InterruptedException e) {
      }
    }
    
    clientExcecutorFactory.unrefAndCleanup();
  }

  /**
   * Same as {@link #call(RPC.RpcKind, Writable, ConnectionId)}
   *  for RPC_BUILTIN
   */
  public @OsUntrusted Writable call(@OsUntrusted Client this, @OsUntrusted Writable param, @OsUntrusted InetSocketAddress address)
      throws IOException {
    return call(RPC.RpcKind.RPC_BUILTIN, param, address);
    
  }
  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code>, returning the value.  Throws exceptions if there are
   * network problems or if the remote code threw an exception.
   * @deprecated Use {@link #call(RPC.RpcKind, Writable,
   *  ConnectionId)} instead 
   */
  @Deprecated
  public @OsUntrusted Writable call(@OsUntrusted Client this, RPC.@OsUntrusted RpcKind rpcKind, @OsUntrusted Writable param, @OsUntrusted InetSocketAddress address)
  throws IOException {
      return call(rpcKind, param, address, null);
  }
  
  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> with the <code>ticket</code> credentials, returning 
   * the value.  
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception.
   * @deprecated Use {@link #call(RPC.RpcKind, Writable, 
   * ConnectionId)} instead 
   */
  @Deprecated
  public @OsUntrusted Writable call(@OsUntrusted Client this, RPC.@OsUntrusted RpcKind rpcKind, @OsUntrusted Writable param, @OsUntrusted InetSocketAddress addr, 
      @OsUntrusted
      UserGroupInformation ticket) throws IOException {
    @OsUntrusted
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, null, ticket, 0,
        conf);
    return call(rpcKind, param, remoteId);
  }
  
  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol, 
   * with the <code>ticket</code> credentials and <code>rpcTimeout</code> as 
   * timeout, returning the value.  
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception. 
   * @deprecated Use {@link #call(RPC.RpcKind, Writable,
   *  ConnectionId)} instead 
   */
  @Deprecated
  public @OsUntrusted Writable call(@OsUntrusted Client this, RPC.@OsUntrusted RpcKind rpcKind, @OsUntrusted Writable param, @OsUntrusted InetSocketAddress addr, 
                       @OsUntrusted
                       Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol, @OsUntrusted UserGroupInformation ticket,
                       @OsUntrusted
                       int rpcTimeout) throws IOException {
    @OsUntrusted
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        ticket, rpcTimeout, conf);
    return call(rpcKind, param, remoteId);
  }

  
  /**
   * Same as {@link #call(RPC.RpcKind, Writable, InetSocketAddress,
   * Class, UserGroupInformation, int, Configuration)}
   * except that rpcKind is writable.
   */
  public @OsUntrusted Writable call(@OsUntrusted Client this, @OsUntrusted Writable param, @OsUntrusted InetSocketAddress addr,
      @OsUntrusted
      Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol, @OsUntrusted UserGroupInformation ticket,
      @OsUntrusted
      int rpcTimeout, @OsUntrusted Configuration conf) throws IOException {
    @OsUntrusted
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        ticket, rpcTimeout, conf);
    return call(RPC.RpcKind.RPC_BUILTIN, param, remoteId);
  }
  
  /**
   * Same as {@link #call(Writable, InetSocketAddress,
   * Class, UserGroupInformation, int, Configuration)}
   * except that specifying serviceClass.
   */
  public @OsUntrusted Writable call(@OsUntrusted Client this, @OsUntrusted Writable param, @OsUntrusted InetSocketAddress addr,
      @OsUntrusted
      Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol, @OsUntrusted UserGroupInformation ticket,
      @OsUntrusted
      int rpcTimeout, @OsUntrusted int serviceClass, @OsUntrusted Configuration conf)
      throws IOException {
    @OsUntrusted
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        ticket, rpcTimeout, conf);
    return call(RPC.RpcKind.RPC_BUILTIN, param, remoteId, serviceClass);
  }

  /**
   * Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol,
   * with the <code>ticket</code> credentials, <code>rpcTimeout</code> as
   * timeout and <code>conf</code> as conf for this connection, returning the
   * value. Throws exceptions if there are network problems or if the remote
   * code threw an exception.
   */
  public @OsUntrusted Writable call(@OsUntrusted Client this, RPC.@OsUntrusted RpcKind rpcKind, @OsUntrusted Writable param, @OsUntrusted InetSocketAddress addr, 
                       @OsUntrusted
                       Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol, @OsUntrusted UserGroupInformation ticket,
                       @OsUntrusted
                       int rpcTimeout, @OsUntrusted Configuration conf) throws IOException {
    @OsUntrusted
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        ticket, rpcTimeout, conf);
    return call(rpcKind, param, remoteId);
  }
  
  /**
   * Same as {link {@link #call(RPC.RpcKind, Writable, ConnectionId)}
   * except the rpcKind is RPC_BUILTIN
   */
  public @OsUntrusted Writable call(@OsUntrusted Client this, @OsUntrusted Writable param, @OsUntrusted ConnectionId remoteId)
      throws IOException {
     return call(RPC.RpcKind.RPC_BUILTIN, param, remoteId);
  }
  
  /**
   * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the rpc respond.
   *
   * @param rpcKind
   * @param rpcRequest -  contains serialized method and method parameters
   * @param remoteId - the target rpc server
   * @returns the rpc response
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception.
   */
  public @OsUntrusted Writable call(@OsUntrusted Client this, RPC.@OsUntrusted RpcKind rpcKind, @OsUntrusted Writable rpcRequest,
      @OsUntrusted
      ConnectionId remoteId) throws IOException {
    return call(rpcKind, rpcRequest, remoteId, RPC.RPC_SERVICE_CLASS_DEFAULT);
  }

  /** 
   * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the rpc respond.
   * 
   * @param rpcKind
   * @param rpcRequest -  contains serialized method and method parameters
   * @param remoteId - the target rpc server
   * @param serviceClass - service class for RPC
   * @returns the rpc response
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception.
   */
  public @OsUntrusted Writable call(@OsUntrusted Client this, RPC.@OsUntrusted RpcKind rpcKind, @OsUntrusted Writable rpcRequest,
      @OsUntrusted
      ConnectionId remoteId, @OsUntrusted int serviceClass) throws IOException {
    final @OsUntrusted Call call = createCall(rpcKind, rpcRequest);
    @OsUntrusted
    Connection connection = getConnection(remoteId, call, serviceClass);
    try {
      connection.sendRpcRequest(call);                 // send the rpc request
    } catch (@OsUntrusted RejectedExecutionException e) {
      throw new @OsUntrusted IOException("connection has been closed", e);
    } catch (@OsUntrusted InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("interrupted waiting to send rpc request to server", e);
      throw new @OsUntrusted IOException(e);
    }

    @OsUntrusted
    boolean interrupted = false;
    synchronized (call) {
      while (!call.done) {
        try {
          call.wait();                           // wait for the result
        } catch (@OsUntrusted InterruptedException ie) {
          // save the fact that we were interrupted
          interrupted = true;
        }
      }

      if (interrupted) {
        // set the interrupt flag now that we are done waiting
        Thread.currentThread().interrupt();
      }

      if (call.error != null) {
        if (call.error instanceof @OsUntrusted RemoteException) {
          call.error.fillInStackTrace();
          throw call.error;
        } else { // local exception
          @OsUntrusted
          InetSocketAddress address = connection.getRemoteAddress();
          throw NetUtils.wrapException(address.getHostName(),
                  address.getPort(),
                  NetUtils.getHostname(),
                  0,
                  call.error);
        }
      } else {
        return call.getRpcResponse();
      }
    }
  }

  // for unit testing only
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  @OsUntrusted
  Set<@OsUntrusted ConnectionId> getConnectionIds(@OsUntrusted Client this) {
    synchronized (connections) {
      return connections.keySet();
    }
  }
  
  /** Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given ConnectionId are reused. */
  private @OsUntrusted Connection getConnection(@OsUntrusted Client this, @OsUntrusted ConnectionId remoteId,
      @OsUntrusted
      Call call, @OsUntrusted int serviceClass) throws IOException {
    if (!running.get()) {
      // the client is stopped
      throw new @OsUntrusted IOException("The client is stopped");
    }
    @OsUntrusted
    Connection connection;
    /* we could avoid this allocation for each RPC by having a  
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
    do {
      synchronized (connections) {
        connection = connections.get(remoteId);
        if (connection == null) {
          connection = new @OsUntrusted Connection(remoteId, serviceClass);
          connections.put(remoteId, connection);
        }
      }
    } while (!connection.addCall(call));
    
    //we don't invoke the method below inside "synchronized (connections)"
    //block above. The reason for that is if the server happens to be slow,
    //it will take longer to establish a connection and that will slow the
    //entire system down.
    connection.setupIOstreams();
    return connection;
  }
  
  /**
   * This class holds the address and the user ticket. The client connections
   * to servers are uniquely identified by <remoteAddress, protocol, ticket>
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  @InterfaceStability.Evolving
  public static class ConnectionId {
    @OsUntrusted
    InetSocketAddress address;
    @OsUntrusted
    UserGroupInformation ticket;
    final @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol;
    private static final @OsUntrusted int PRIME = 16777619;
    private final @OsUntrusted int rpcTimeout;
    private final @OsUntrusted int maxIdleTime; //connections will be culled if it was idle for 
    //maxIdleTime msecs
    private final @OsUntrusted RetryPolicy connectionRetryPolicy;
    // the max. no. of retries for socket connections on time out exceptions
    private final @OsUntrusted int maxRetriesOnSocketTimeouts;
    private final @OsUntrusted boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    private final @OsUntrusted boolean doPing; //do we need to send ping message
    private final @OsUntrusted int pingInterval; // how often sends ping to the server in msecs
    private @OsUntrusted String saslQop; // here for testing
    
    @OsUntrusted
    ConnectionId(@OsUntrusted InetSocketAddress address, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol, 
                 @OsUntrusted
                 UserGroupInformation ticket, @OsUntrusted int rpcTimeout, @OsUntrusted int maxIdleTime, 
                 @OsUntrusted
                 RetryPolicy connectionRetryPolicy, @OsUntrusted int maxRetriesOnSocketTimeouts,
                 @OsUntrusted
                 boolean tcpNoDelay, @OsUntrusted boolean doPing, @OsUntrusted int pingInterval) {
      this.protocol = protocol;
      this.address = address;
      this.ticket = ticket;
      this.rpcTimeout = rpcTimeout;
      this.maxIdleTime = maxIdleTime;
      this.connectionRetryPolicy = connectionRetryPolicy;
      this.maxRetriesOnSocketTimeouts = maxRetriesOnSocketTimeouts;
      this.tcpNoDelay = tcpNoDelay;
      this.doPing = doPing;
      this.pingInterval = pingInterval;
    }
    
    @OsUntrusted
    InetSocketAddress getAddress(Client.@OsUntrusted ConnectionId this) {
      return address;
    }
    
    @OsUntrusted
    Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> getProtocol(Client.@OsUntrusted ConnectionId this) {
      return protocol;
    }
    
    @OsUntrusted
    UserGroupInformation getTicket(Client.@OsUntrusted ConnectionId this) {
      return ticket;
    }
    
    private @OsUntrusted int getRpcTimeout(Client.@OsUntrusted ConnectionId this) {
      return rpcTimeout;
    }
    
    @OsUntrusted
    int getMaxIdleTime(Client.@OsUntrusted ConnectionId this) {
      return maxIdleTime;
    }
    
    /** max connection retries on socket time outs */
    public @OsUntrusted int getMaxRetriesOnSocketTimeouts(Client.@OsUntrusted ConnectionId this) {
      return maxRetriesOnSocketTimeouts;
    }
    
    @OsUntrusted
    boolean getTcpNoDelay(Client.@OsUntrusted ConnectionId this) {
      return tcpNoDelay;
    }
    
    @OsUntrusted
    boolean getDoPing(Client.@OsUntrusted ConnectionId this) {
      return doPing;
    }
    
    @OsUntrusted
    int getPingInterval(Client.@OsUntrusted ConnectionId this) {
      return pingInterval;
    }
    
    @VisibleForTesting
    @OsUntrusted
    String getSaslQop(Client.@OsUntrusted ConnectionId this) {
      return saslQop;
    }
    
    static @OsUntrusted ConnectionId getConnectionId(@OsUntrusted InetSocketAddress addr,
        @OsUntrusted
        Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol, @OsUntrusted UserGroupInformation ticket, @OsUntrusted int rpcTimeout,
        @OsUntrusted
        Configuration conf) throws IOException {
      return getConnectionId(addr, protocol, ticket, rpcTimeout, null, conf);
    }

    /**
     * Returns a ConnectionId object. 
     * @param addr Remote address for the connection.
     * @param protocol Protocol for RPC.
     * @param ticket UGI
     * @param rpcTimeout timeout
     * @param conf Configuration object
     * @return A ConnectionId instance
     * @throws IOException
     */
    static @OsUntrusted ConnectionId getConnectionId(@OsUntrusted InetSocketAddress addr,
        @OsUntrusted
        Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol, @OsUntrusted UserGroupInformation ticket, @OsUntrusted int rpcTimeout,
        @OsUntrusted
        RetryPolicy connectionRetryPolicy, @OsUntrusted Configuration conf) throws IOException {

      if (connectionRetryPolicy == null) {
        final @OsUntrusted int max = conf.getInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT);
        connectionRetryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            max, 1, TimeUnit.SECONDS);
      }

      @OsUntrusted
      boolean doPing =
        conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, true);
      return new @OsUntrusted ConnectionId(addr, protocol, ticket, rpcTimeout,
          conf.getInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
              CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT),
          connectionRetryPolicy,
          conf.getInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT),
          conf.getBoolean(CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
              CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_DEFAULT),
          doPing, 
          (doPing ? Client.getPingInterval(conf) : 0));
    }
    
    static @OsUntrusted boolean isEqual(@OsUntrusted Object a, @OsUntrusted Object b) {
      return a == null ? b == null : a.equals(b);
    }

    @Override
    public @OsUntrusted boolean equals(Client.@OsUntrusted ConnectionId this, @OsUntrusted Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof @OsUntrusted ConnectionId) {
        @OsUntrusted
        ConnectionId that = (@OsUntrusted ConnectionId) obj;
        return isEqual(this.address, that.address)
            && this.doPing == that.doPing
            && this.maxIdleTime == that.maxIdleTime
            && isEqual(this.connectionRetryPolicy, that.connectionRetryPolicy)
            && this.pingInterval == that.pingInterval
            && isEqual(this.protocol, that.protocol)
            && this.rpcTimeout == that.rpcTimeout
            && this.tcpNoDelay == that.tcpNoDelay
            && isEqual(this.ticket, that.ticket);
      }
      return false;
    }
    
    @Override
    public @OsUntrusted int hashCode(Client.@OsUntrusted ConnectionId this) {
      @OsUntrusted
      int result = connectionRetryPolicy.hashCode();
      result = PRIME * result + ((address == null) ? 0 : address.hashCode());
      result = PRIME * result + (doPing ? 1231 : 1237);
      result = PRIME * result + maxIdleTime;
      result = PRIME * result + pingInterval;
      result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
      result = PRIME * result + rpcTimeout;
      result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
      result = PRIME * result + ((ticket == null) ? 0 : ticket.hashCode());
      return result;
    }
    
    @Override
    public @OsUntrusted String toString(Client.@OsUntrusted ConnectionId this) {
      return address.toString();
    }
  }  

  /**
   * Returns the next valid sequential call ID by incrementing an atomic counter
   * and masking off the sign bit.  Valid call IDs are non-negative integers in
   * the range [ 0, 2^31 - 1 ].  Negative numbers are reserved for special
   * purposes.  The values can overflow back to 0 and be reused.  Note that prior
   * versions of the client did not mask off the sign bit, so a server may still
   * see a negative call ID if it receives connections from an old client.
   * 
   * @return next call ID
   */
  public static @OsUntrusted int nextCallId() {
    return callIdCounter.getAndIncrement() & 0x7FFFFFFF;
  }
}
