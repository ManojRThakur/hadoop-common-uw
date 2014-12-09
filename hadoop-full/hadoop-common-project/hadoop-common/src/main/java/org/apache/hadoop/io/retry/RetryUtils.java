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
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RemoteException;

import com.google.protobuf.ServiceException;

public class RetryUtils {
  public static final @OsUntrusted Log LOG = LogFactory.getLog(RetryUtils.class);
  
  /**
   * Return the default retry policy set in conf.
   * 
   * If the value retryPolicyEnabledKey is set to false in conf,
   * use TRY_ONCE_THEN_FAIL.
   * 
   * Otherwise, get the MultipleLinearRandomRetry policy specified in the conf
   * and then
   * (1) use multipleLinearRandomRetry for
   *     - remoteExceptionToRetry, or
   *     - IOException other than RemoteException, or
   *     - ServiceException; and
   * (2) use TRY_ONCE_THEN_FAIL for
   *     - non-remoteExceptionToRetry RemoteException, or
   *     - non-IOException.
   *     
   *
   * @param conf
   * @param retryPolicyEnabledKey     conf property key for enabling retry
   * @param defaultRetryPolicyEnabled default retryPolicyEnabledKey conf value 
   * @param retryPolicySpecKey        conf property key for retry policy spec
   * @param defaultRetryPolicySpec    default retryPolicySpecKey conf value
   * @param remoteExceptionToRetry    The particular RemoteException to retry
   * @return the default retry policy.
   */
  public static @OsUntrusted RetryPolicy getDefaultRetryPolicy(
      @OsUntrusted
      Configuration conf,
      @OsUntrusted
      String retryPolicyEnabledKey,
      @OsUntrusted
      boolean defaultRetryPolicyEnabled,
      @OsUntrusted
      String retryPolicySpecKey,
      @OsUntrusted
      String defaultRetryPolicySpec,
      final @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Exception> remoteExceptionToRetry
      ) {
    
    final @OsUntrusted RetryPolicy multipleLinearRandomRetry = 
        getMultipleLinearRandomRetry(
            conf, 
            retryPolicyEnabledKey, defaultRetryPolicyEnabled, 
            retryPolicySpecKey, defaultRetryPolicySpec
            );
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("multipleLinearRandomRetry = " + multipleLinearRandomRetry);
    }

    if (multipleLinearRandomRetry == null) {
      //no retry
      return RetryPolicies.TRY_ONCE_THEN_FAIL;
    } else {
      return new @OsUntrusted RetryPolicy() {
        @Override
        public @OsUntrusted RetryAction shouldRetry(@OsUntrusted Exception e, @OsUntrusted int retries, @OsUntrusted int failovers,
            @OsUntrusted
            boolean isMethodIdempotent) throws Exception {
          if (e instanceof @OsUntrusted ServiceException) {
            //unwrap ServiceException
            final @OsUntrusted Throwable cause = e.getCause();
            if (cause != null && cause instanceof @OsUntrusted Exception) {
              e = (@OsUntrusted Exception)cause;
            }
          }

          //see (1) and (2) in the javadoc of this method.
          final @OsUntrusted RetryPolicy p;
          if (e instanceof @OsUntrusted RemoteException) {
            final @OsUntrusted RemoteException re = (@OsUntrusted RemoteException)e;
            p = remoteExceptionToRetry.getName().equals(re.getClassName())?
                multipleLinearRandomRetry: RetryPolicies.TRY_ONCE_THEN_FAIL;
          } else if (e instanceof @OsUntrusted IOException || e instanceof @OsUntrusted ServiceException) {
            p = multipleLinearRandomRetry;
          } else { //non-IOException
            p = RetryPolicies.TRY_ONCE_THEN_FAIL;
          }

          if (LOG.isDebugEnabled()) {
            LOG.debug("RETRY " + retries + ") policy="
                + p.getClass().getSimpleName() + ", exception=" + e);
          }
          LOG.info("RETRY " + retries + ") policy="
              + p.getClass().getSimpleName() + ", exception=" + e);
          return p.shouldRetry(e, retries, failovers, isMethodIdempotent);
        }

        @Override
        public @OsUntrusted String toString() {
          return "RetryPolicy[" + multipleLinearRandomRetry + ", "
              + RetryPolicies.TRY_ONCE_THEN_FAIL.getClass().getSimpleName()
              + "]";
        }
      };
    }
  }

  /**
   * Return the MultipleLinearRandomRetry policy specified in the conf,
   * or null if the feature is disabled.
   * If the policy is specified in the conf but the policy cannot be parsed,
   * the default policy is returned.
   * 
   * Retry policy spec:
   *   N pairs of sleep-time and number-of-retries "s1,n1,s2,n2,..."
   * 
   * @param conf
   * @param retryPolicyEnabledKey     conf property key for enabling retry
   * @param defaultRetryPolicyEnabled default retryPolicyEnabledKey conf value 
   * @param retryPolicySpecKey        conf property key for retry policy spec
   * @param defaultRetryPolicySpec    default retryPolicySpecKey conf value
   * @return the MultipleLinearRandomRetry policy specified in the conf,
   *         or null if the feature is disabled.
   */
  public static @OsUntrusted RetryPolicy getMultipleLinearRandomRetry(
      @OsUntrusted
      Configuration conf,
      @OsUntrusted
      String retryPolicyEnabledKey,
      @OsUntrusted
      boolean defaultRetryPolicyEnabled,
      @OsUntrusted
      String retryPolicySpecKey,
      @OsUntrusted
      String defaultRetryPolicySpec
      ) {
    final @OsUntrusted boolean enabled = 
        conf.getBoolean(retryPolicyEnabledKey, defaultRetryPolicyEnabled);
    if (!enabled) {
      return null;
    }

    final @OsUntrusted String policy = conf.get(retryPolicySpecKey, defaultRetryPolicySpec);

    final @OsUntrusted RetryPolicy r = 
        RetryPolicies.MultipleLinearRandomRetry.parseCommaSeparatedString(
            policy);
    return (r != null) ? 
        r : 
        RetryPolicies.MultipleLinearRandomRetry.parseCommaSeparatedString(
        defaultRetryPolicySpec);
  }
}
