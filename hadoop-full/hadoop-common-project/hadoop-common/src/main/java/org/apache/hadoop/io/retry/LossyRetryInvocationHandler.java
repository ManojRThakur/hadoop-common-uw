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
package org.apache.hadoop.io.retry;

import ostrusted.quals.OsUntrusted;
import java.lang.reflect.Method;
import java.net.UnknownHostException;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A dummy invocation handler extending RetryInvocationHandler. It drops the
 * first N number of responses. This invocation handler is only used for testing.
 */
@InterfaceAudience.Private
public class LossyRetryInvocationHandler<@OsUntrusted T extends java.lang.@OsUntrusted Object> extends @OsUntrusted RetryInvocationHandler<T> {
  private final @OsUntrusted int numToDrop;
  private static final @OsUntrusted ThreadLocal<@OsUntrusted Integer> RetryCount = 
      new @OsUntrusted ThreadLocal<@OsUntrusted Integer>();

  public @OsUntrusted LossyRetryInvocationHandler(@OsUntrusted int numToDrop,
      @OsUntrusted
      FailoverProxyProvider<@OsUntrusted T> proxyProvider, @OsUntrusted RetryPolicy retryPolicy) {
    super(proxyProvider, retryPolicy);
    this.numToDrop = numToDrop;
  }

  @Override
  public @OsUntrusted Object invoke(@OsUntrusted LossyRetryInvocationHandler<T> this, @OsUntrusted Object proxy, @OsUntrusted Method method, @OsUntrusted Object @OsUntrusted [] args)
      throws Throwable {
    RetryCount.set(0);
    return super.invoke(proxy, method, args);
  }

  @Override
  protected @OsUntrusted Object invokeMethod(@OsUntrusted LossyRetryInvocationHandler<T> this, @OsUntrusted Method method, @OsUntrusted Object @OsUntrusted [] args) throws Throwable {
    @OsUntrusted
    Object result = super.invokeMethod(method, args);
    @OsUntrusted
    int retryCount = RetryCount.get();
    if (retryCount < this.numToDrop) {
      RetryCount.set(++retryCount);
      LOG.info("Drop the response. Current retryCount == " + retryCount);
      throw new @OsUntrusted UnknownHostException("Fake Exception");
    } else {
      LOG.info("retryCount == " + retryCount
          + ". It's time to normally process the response");
      return result;
    }
  }
}
