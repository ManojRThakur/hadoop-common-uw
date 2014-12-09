/*
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
package org.apache.hadoop.io.retry;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.ipc.RpcInvocationHandler;
import org.apache.hadoop.util.ThreadUtil;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class implements RpcInvocationHandler and supports retry on the client 
 * side.
 */
@InterfaceAudience.Private
public class RetryInvocationHandler<@OsUntrusted T extends java.lang.@OsUntrusted Object> implements @OsUntrusted RpcInvocationHandler {
  public static final @OsUntrusted Log LOG = LogFactory.getLog(RetryInvocationHandler.class);
  private final @OsUntrusted FailoverProxyProvider<@OsUntrusted T> proxyProvider;

  /**
   * The number of times the associated proxyProvider has ever been failed over.
   */
  private @OsUntrusted long proxyProviderFailoverCount = 0;
  private volatile @OsUntrusted boolean hasMadeASuccessfulCall = false;
  
  private final @OsUntrusted RetryPolicy defaultPolicy;
  private final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted RetryPolicy> methodNameToPolicyMap;
  private @OsUntrusted T currentProxy;

  protected @OsUntrusted RetryInvocationHandler(@OsUntrusted FailoverProxyProvider<@OsUntrusted T> proxyProvider,
      @OsUntrusted
      RetryPolicy retryPolicy) {
    this(proxyProvider, retryPolicy, Collections.<@OsUntrusted String, @OsUntrusted RetryPolicy>emptyMap());
  }

  protected @OsUntrusted RetryInvocationHandler(@OsUntrusted FailoverProxyProvider<@OsUntrusted T> proxyProvider,
      @OsUntrusted
      RetryPolicy defaultPolicy,
      @OsUntrusted
      Map<@OsUntrusted String, @OsUntrusted RetryPolicy> methodNameToPolicyMap) {
    this.proxyProvider = proxyProvider;
    this.defaultPolicy = defaultPolicy;
    this.methodNameToPolicyMap = methodNameToPolicyMap;
    this.currentProxy = proxyProvider.getProxy();
  }

  @Override
  public @OsUntrusted Object invoke(@OsUntrusted RetryInvocationHandler<T> this, @OsUntrusted Object proxy, @OsUntrusted Method method, @OsUntrusted Object @OsUntrusted [] args)
    throws Throwable {
    @OsUntrusted
    RetryPolicy policy = methodNameToPolicyMap.get(method.getName());
    if (policy == null) {
      policy = defaultPolicy;
    }
    
    // The number of times this method invocation has been failed over.
    @OsUntrusted
    int invocationFailoverCount = 0;
    final @OsUntrusted boolean isRpc = isRpcInvocation(currentProxy);
    final @OsUntrusted int callId = isRpc? Client.nextCallId(): RpcConstants.INVALID_CALL_ID;
    @OsUntrusted
    int retries = 0;
    while (true) {
      // The number of times this invocation handler has ever been failed over,
      // before this method invocation attempt. Used to prevent concurrent
      // failed method invocations from triggering multiple failover attempts.
      @OsUntrusted
      long invocationAttemptFailoverCount;
      synchronized (proxyProvider) {
        invocationAttemptFailoverCount = proxyProviderFailoverCount;
      }

      if (isRpc) {
        Client.setCallIdAndRetryCount(callId, retries);
      }
      try {
        @OsUntrusted
        Object ret = invokeMethod(method, args);
        hasMadeASuccessfulCall = true;
        return ret;
      } catch (@OsUntrusted Exception e) {
        @OsUntrusted
        boolean isIdempotentOrAtMostOnce = proxyProvider.getInterface()
            .getMethod(method.getName(), method.getParameterTypes())
            .isAnnotationPresent(Idempotent.class);
        if (!isIdempotentOrAtMostOnce) {
          isIdempotentOrAtMostOnce = proxyProvider.getInterface()
              .getMethod(method.getName(), method.getParameterTypes())
              .isAnnotationPresent(AtMostOnce.class);
        }
        @OsUntrusted
        RetryAction action = policy.shouldRetry(e, retries++,
            invocationFailoverCount, isIdempotentOrAtMostOnce);
        if (action.action == RetryAction.RetryDecision.FAIL) {
          if (action.reason != null) {
            LOG.warn("Exception while invoking " + 
                currentProxy.getClass() + "." + method.getName() +
                ". Not retrying because " + action.reason, e);
          }
          throw e;
        } else { // retry or failover
          // avoid logging the failover if this is the first call on this
          // proxy object, and we successfully achieve the failover without
          // any flip-flopping
          @OsUntrusted
          boolean worthLogging = 
            !(invocationFailoverCount == 0 && !hasMadeASuccessfulCall);
          worthLogging |= LOG.isDebugEnabled();
          if (action.action == RetryAction.RetryDecision.FAILOVER_AND_RETRY &&
              worthLogging) {
            @OsUntrusted
            String msg = "Exception while invoking " + method.getName()
              + " of class " + currentProxy.getClass().getSimpleName();
            if (invocationFailoverCount > 0) {
              msg += " after " + invocationFailoverCount + " fail over attempts"; 
            }
            msg += ". Trying to fail over " + formatSleepMessage(action.delayMillis);
            if (LOG.isDebugEnabled()) {
              LOG.debug(msg, e);
            }
          } else {
            if(LOG.isDebugEnabled()) {
              LOG.debug("Exception while invoking " + method.getName()
                  + " of class " + currentProxy.getClass().getSimpleName() +
                  ". Retrying " + formatSleepMessage(action.delayMillis), e);
            }
          }
          
          if (action.delayMillis > 0) {
            ThreadUtil.sleepAtLeastIgnoreInterrupts(action.delayMillis);
          }
          
          if (action.action == RetryAction.RetryDecision.FAILOVER_AND_RETRY) {
            // Make sure that concurrent failed method invocations only cause a
            // single actual fail over.
            synchronized (proxyProvider) {
              if (invocationAttemptFailoverCount == proxyProviderFailoverCount) {
                proxyProvider.performFailover(currentProxy);
                proxyProviderFailoverCount++;
                currentProxy = proxyProvider.getProxy();
              } else {
                LOG.warn("A failover has occurred since the start of this method"
                    + " invocation attempt.");
              }
            }
            invocationFailoverCount++;
          }
        }
      }
    }
  }
  
  private static @OsUntrusted String formatSleepMessage(@OsUntrusted long millis) {
    if (millis > 0) {
      return "after sleeping for " + millis + "ms.";
    } else {
      return "immediately.";
    }
  }
  
  protected @OsUntrusted Object invokeMethod(@OsUntrusted RetryInvocationHandler<T> this, @OsUntrusted Method method, @OsUntrusted Object @OsUntrusted [] args) throws Throwable {
    try {
      if (!method.isAccessible()) {
        method.setAccessible(true);
      }
      return method.invoke(currentProxy, args);
    } catch (@OsUntrusted InvocationTargetException e) {
      throw e.getCause();
    }
  }

  @VisibleForTesting
  static @OsUntrusted boolean isRpcInvocation(@OsUntrusted Object proxy) {
    if (proxy instanceof @OsUntrusted ProtocolTranslator) {
      proxy = ((@OsUntrusted ProtocolTranslator) proxy).getUnderlyingProxyObject();
    }
    if (!Proxy.isProxyClass(proxy.getClass())) {
      return false;
    }
    final @OsUntrusted InvocationHandler ih = Proxy.getInvocationHandler(proxy);
    return ih instanceof @OsUntrusted RpcInvocationHandler;
  }

  @Override
  public void close(@OsUntrusted RetryInvocationHandler<T> this) throws IOException {
    proxyProvider.close();
  }

  @Override //RpcInvocationHandler
  public @OsUntrusted ConnectionId getConnectionId(@OsUntrusted RetryInvocationHandler<T> this) {
    return RPC.getConnectionIdForProxy(currentProxy);
  }

}
