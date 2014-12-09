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
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.net.ConnectTimeoutException;

/**
 * <p>
 * A collection of useful implementations of {@link RetryPolicy}.
 * </p>
 */
public class RetryPolicies {
  
  public static final @OsUntrusted Log LOG = LogFactory.getLog(RetryPolicies.class);
  
  private static @OsUntrusted ThreadLocal<@OsUntrusted Random> RANDOM = new @OsUntrusted ThreadLocal<@OsUntrusted Random>() {
    @Override
    protected @OsUntrusted Random initialValue() {
      return new @OsUntrusted Random();
    }
  };
  
  /**
   * <p>
   * Try once, and fail by re-throwing the exception.
   * This corresponds to having no retry mechanism in place.
   * </p>
   */
  public static final @OsUntrusted RetryPolicy TRY_ONCE_THEN_FAIL = new @OsUntrusted TryOnceThenFail();
  
  /**
   * <p>
   * Keep trying forever.
   * </p>
   */
  public static final @OsUntrusted RetryPolicy RETRY_FOREVER = new @OsUntrusted RetryForever();
  
  /**
   * <p>
   * Keep trying a limited number of times, waiting a fixed time between attempts,
   * and then fail by re-throwing the exception.
   * </p>
   */
  public static final @OsUntrusted RetryPolicy retryUpToMaximumCountWithFixedSleep(@OsUntrusted int maxRetries, @OsUntrusted long sleepTime, @OsUntrusted TimeUnit timeUnit) {
    return new @OsUntrusted RetryUpToMaximumCountWithFixedSleep(maxRetries, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Keep trying for a maximum time, waiting a fixed time between attempts,
   * and then fail by re-throwing the exception.
   * </p>
   */
  public static final @OsUntrusted RetryPolicy retryUpToMaximumTimeWithFixedSleep(@OsUntrusted long maxTime, @OsUntrusted long sleepTime, @OsUntrusted TimeUnit timeUnit) {
    return new @OsUntrusted RetryUpToMaximumTimeWithFixedSleep(maxTime, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Keep trying a limited number of times, waiting a growing amount of time between attempts,
   * and then fail by re-throwing the exception.
   * The time between attempts is <code>sleepTime</code> mutliplied by the number of tries so far.
   * </p>
   */
  public static final @OsUntrusted RetryPolicy retryUpToMaximumCountWithProportionalSleep(@OsUntrusted int maxRetries, @OsUntrusted long sleepTime, @OsUntrusted TimeUnit timeUnit) {
    return new @OsUntrusted RetryUpToMaximumCountWithProportionalSleep(maxRetries, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Keep trying a limited number of times, waiting a growing amount of time between attempts,
   * and then fail by re-throwing the exception.
   * The time between attempts is <code>sleepTime</code> mutliplied by a random
   * number in the range of [0, 2 to the number of retries)
   * </p>
   */
  public static final @OsUntrusted RetryPolicy exponentialBackoffRetry(
      @OsUntrusted
      int maxRetries, @OsUntrusted long sleepTime, @OsUntrusted TimeUnit timeUnit) {
    return new @OsUntrusted ExponentialBackoffRetry(maxRetries, sleepTime, timeUnit);
  }
  
  /**
   * <p>
   * Set a default policy with some explicit handlers for specific exceptions.
   * </p>
   */
  public static final @OsUntrusted RetryPolicy retryByException(@OsUntrusted RetryPolicy defaultPolicy,
                                                   @OsUntrusted
                                                   Map<@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Exception>, @OsUntrusted RetryPolicy> exceptionToPolicyMap) {
    return new @OsUntrusted ExceptionDependentRetry(defaultPolicy, exceptionToPolicyMap);
  }
  
  /**
   * <p>
   * A retry policy for RemoteException
   * Set a default policy with some explicit handlers for specific exceptions.
   * </p>
   */
  public static final @OsUntrusted RetryPolicy retryByRemoteException(
      @OsUntrusted
      RetryPolicy defaultPolicy,
      @OsUntrusted
      Map<@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Exception>, @OsUntrusted RetryPolicy> exceptionToPolicyMap) {
    return new @OsUntrusted RemoteExceptionDependentRetry(defaultPolicy, exceptionToPolicyMap);
  }
  
  public static final @OsUntrusted RetryPolicy failoverOnNetworkException(@OsUntrusted int maxFailovers) {
    return failoverOnNetworkException(TRY_ONCE_THEN_FAIL, maxFailovers);
  }
  
  public static final @OsUntrusted RetryPolicy failoverOnNetworkException(
      @OsUntrusted
      RetryPolicy fallbackPolicy, @OsUntrusted int maxFailovers) {
    return failoverOnNetworkException(fallbackPolicy, maxFailovers, 0, 0);
  }
  
  public static final @OsUntrusted RetryPolicy failoverOnNetworkException(
      @OsUntrusted
      RetryPolicy fallbackPolicy, @OsUntrusted int maxFailovers, @OsUntrusted long delayMillis,
      @OsUntrusted
      long maxDelayBase) {
    return new @OsUntrusted FailoverOnNetworkExceptionRetry(fallbackPolicy, maxFailovers,
        delayMillis, maxDelayBase);
  }
  
  static class TryOnceThenFail implements @OsUntrusted RetryPolicy {
    @Override
    public @OsUntrusted RetryAction shouldRetry(RetryPolicies.@OsUntrusted TryOnceThenFail this, @OsUntrusted Exception e, @OsUntrusted int retries, @OsUntrusted int failovers,
        @OsUntrusted
        boolean isIdempotentOrAtMostOnce) throws Exception {
      return RetryAction.FAIL;
    }
  }
  
  static class RetryForever implements @OsUntrusted RetryPolicy {
    @Override
    public @OsUntrusted RetryAction shouldRetry(RetryPolicies.@OsUntrusted RetryForever this, @OsUntrusted Exception e, @OsUntrusted int retries, @OsUntrusted int failovers,
        @OsUntrusted
        boolean isIdempotentOrAtMostOnce) throws Exception {
      return RetryAction.RETRY;
    }
  }
  
  /**
   * Retry up to maxRetries.
   * The actual sleep time of the n-th retry is f(n, sleepTime),
   * where f is a function provided by the subclass implementation.
   *
   * The object of the subclasses should be immutable;
   * otherwise, the subclass must override hashCode(), equals(..) and toString().
   */
  static abstract class RetryLimited implements @OsUntrusted RetryPolicy {
    final @OsUntrusted int maxRetries;
    final @OsUntrusted long sleepTime;
    final @OsUntrusted TimeUnit timeUnit;
    
    private @OsUntrusted String myString;

    @OsUntrusted
    RetryLimited(@OsUntrusted int maxRetries, @OsUntrusted long sleepTime, @OsUntrusted TimeUnit timeUnit) {
      if (maxRetries < 0) {
        throw new @OsUntrusted IllegalArgumentException("maxRetries = " + maxRetries+" < 0");
      }
      if (sleepTime < 0) {
        throw new @OsUntrusted IllegalArgumentException("sleepTime = " + sleepTime + " < 0");
      }

      this.maxRetries = maxRetries;
      this.sleepTime = sleepTime;
      this.timeUnit = timeUnit;
    }

    @Override
    public @OsUntrusted RetryAction shouldRetry(RetryPolicies.@OsUntrusted RetryLimited this, @OsUntrusted Exception e, @OsUntrusted int retries, @OsUntrusted int failovers,
        @OsUntrusted
        boolean isIdempotentOrAtMostOnce) throws Exception {
      if (retries >= maxRetries) {
        return RetryAction.FAIL;
      }
      return new @OsUntrusted RetryAction(RetryAction.RetryDecision.RETRY,
          timeUnit.toMillis(calculateSleepTime(retries)));
    }
    
    protected abstract @OsUntrusted long calculateSleepTime(RetryPolicies.@OsUntrusted RetryLimited this, @OsUntrusted int retries);
    
    @Override
    public @OsUntrusted int hashCode(RetryPolicies.@OsUntrusted RetryLimited this) {
      return toString().hashCode();
    }
    
    @Override
    public @OsUntrusted boolean equals(RetryPolicies.@OsUntrusted RetryLimited this, final @OsUntrusted Object that) {
      if (this == that) {
        return true;
      } else if (that == null || this.getClass() != that.getClass()) {
        return false;
      }
      return this.toString().equals(that.toString());
    }

    @Override
    public @OsUntrusted String toString(RetryPolicies.@OsUntrusted RetryLimited this) {
      if (myString == null) {
        myString = getClass().getSimpleName() + "(maxRetries=" + maxRetries
            + ", sleepTime=" + sleepTime + " " + timeUnit + ")";
      }
      return myString;
    }
  }
  
  static class RetryUpToMaximumCountWithFixedSleep extends @OsUntrusted RetryLimited {
    public @OsUntrusted RetryUpToMaximumCountWithFixedSleep(@OsUntrusted int maxRetries, @OsUntrusted long sleepTime, @OsUntrusted TimeUnit timeUnit) {
      super(maxRetries, sleepTime, timeUnit);
    }
    
    @Override
    protected @OsUntrusted long calculateSleepTime(RetryPolicies.@OsUntrusted RetryUpToMaximumCountWithFixedSleep this, @OsUntrusted int retries) {
      return sleepTime;
    }
  }
  
  static class RetryUpToMaximumTimeWithFixedSleep extends @OsUntrusted RetryUpToMaximumCountWithFixedSleep {
    public @OsUntrusted RetryUpToMaximumTimeWithFixedSleep(@OsUntrusted long maxTime, @OsUntrusted long sleepTime, @OsUntrusted TimeUnit timeUnit) {
      super((@OsUntrusted int) (maxTime / sleepTime), sleepTime, timeUnit);
    }
  }
  
  static class RetryUpToMaximumCountWithProportionalSleep extends @OsUntrusted RetryLimited {
    public @OsUntrusted RetryUpToMaximumCountWithProportionalSleep(@OsUntrusted int maxRetries, @OsUntrusted long sleepTime, @OsUntrusted TimeUnit timeUnit) {
      super(maxRetries, sleepTime, timeUnit);
    }
    
    @Override
    protected @OsUntrusted long calculateSleepTime(RetryPolicies.@OsUntrusted RetryUpToMaximumCountWithProportionalSleep this, @OsUntrusted int retries) {
      return sleepTime * (retries + 1);
    }
  }
  
  /**
   * Given pairs of number of retries and sleep time (n0, t0), (n1, t1), ...,
   * the first n0 retries sleep t0 milliseconds on average,
   * the following n1 retries sleep t1 milliseconds on average, and so on.
   * 
   * For all the sleep, the actual sleep time is randomly uniform distributed
   * in the close interval [0.5t, 1.5t], where t is the sleep time specified.
   *
   * The objects of this class are immutable.
   */
  public static class MultipleLinearRandomRetry implements @OsUntrusted RetryPolicy {
    /** Pairs of numRetries and sleepSeconds */
    public static class Pair {
      final @OsUntrusted int numRetries;
      final @OsUntrusted int sleepMillis;
      
      public @OsUntrusted Pair(final @OsUntrusted int numRetries, final @OsUntrusted int sleepMillis) {
        if (numRetries < 0) {
          throw new @OsUntrusted IllegalArgumentException("numRetries = " + numRetries+" < 0");
        }
        if (sleepMillis < 0) {
          throw new @OsUntrusted IllegalArgumentException("sleepMillis = " + sleepMillis + " < 0");
        }

        this.numRetries = numRetries;
        this.sleepMillis = sleepMillis;
      }
      
      @Override
      public @OsUntrusted String toString(RetryPolicies.MultipleLinearRandomRetry.@OsUntrusted Pair this) {
        return numRetries + "x" + sleepMillis + "ms";
      }
    }

    private final @OsUntrusted List<@OsUntrusted Pair> pairs;
    private @OsUntrusted String myString;

    public @OsUntrusted MultipleLinearRandomRetry(@OsUntrusted List<@OsUntrusted Pair> pairs) {
      if (pairs == null || pairs.isEmpty()) {
        throw new @OsUntrusted IllegalArgumentException("pairs must be neither null nor empty.");
      }
      this.pairs = Collections.unmodifiableList(pairs);
    }

    @Override
    public @OsUntrusted RetryAction shouldRetry(RetryPolicies.@OsUntrusted MultipleLinearRandomRetry this, @OsUntrusted Exception e, @OsUntrusted int curRetry, @OsUntrusted int failovers,
        @OsUntrusted
        boolean isIdempotentOrAtMostOnce) throws Exception {
      final @OsUntrusted Pair p = searchPair(curRetry);
      if (p == null) {
        //no more retries.
        return RetryAction.FAIL;
      }

      //calculate sleep time and return.
      final @OsUntrusted double ratio = RANDOM.get().nextDouble() + 0.5;//0.5 <= ratio <=1.5
      final @OsUntrusted long sleepTime = Math.round(p.sleepMillis * ratio);
      return new @OsUntrusted RetryAction(RetryAction.RetryDecision.RETRY, sleepTime);
    }

    /**
     * Given the current number of retry, search the corresponding pair.
     * @return the corresponding pair,
     *   or null if the current number of retry > maximum number of retry. 
     */
    private @OsUntrusted Pair searchPair(RetryPolicies.@OsUntrusted MultipleLinearRandomRetry this, @OsUntrusted int curRetry) {
      @OsUntrusted
      int i = 0;
      for(; i < pairs.size() && curRetry > pairs.get(i).numRetries; i++) {
        curRetry -= pairs.get(i).numRetries;
      }
      return i == pairs.size()? null: pairs.get(i);
    }
    
    @Override
    public @OsUntrusted int hashCode(RetryPolicies.@OsUntrusted MultipleLinearRandomRetry this) {
      return toString().hashCode();
    }
    
    @Override
    public @OsUntrusted boolean equals(RetryPolicies.@OsUntrusted MultipleLinearRandomRetry this, final @OsUntrusted Object that) {
      if (this == that) {
        return true;
      } else if (that == null || this.getClass() != that.getClass()) {
        return false;
      }
      return this.toString().equals(that.toString());
    }

    @Override
    public @OsUntrusted String toString(RetryPolicies.@OsUntrusted MultipleLinearRandomRetry this) {
      if (myString == null) {
        myString = getClass().getSimpleName() + pairs;
      }
      return myString;
    }

    /**
     * Parse the given string as a MultipleLinearRandomRetry object.
     * The format of the string is "t_1, n_1, t_2, n_2, ...",
     * where t_i and n_i are the i-th pair of sleep time and number of retires.
     * Note that the white spaces in the string are ignored.
     *
     * @return the parsed object, or null if the parsing fails.
     */
    public static @OsUntrusted MultipleLinearRandomRetry parseCommaSeparatedString(@OsUntrusted String s) {
      final @OsUntrusted String @OsUntrusted [] elements = s.split(",");
      if (elements.length == 0) {
        LOG.warn("Illegal value: there is no element in \"" + s + "\".");
        return null;
      }
      if (elements.length % 2 != 0) {
        LOG.warn("Illegal value: the number of elements in \"" + s + "\" is "
            + elements.length + " but an even number of elements is expected.");
        return null;
      }

      final @OsUntrusted List<RetryPolicies.MultipleLinearRandomRetry.@OsUntrusted Pair> pairs
          = new @OsUntrusted ArrayList<RetryPolicies.MultipleLinearRandomRetry.@OsUntrusted Pair>();
   
      for(@OsUntrusted int i = 0; i < elements.length; ) {
        //parse the i-th sleep-time
        final @OsUntrusted int sleep = parsePositiveInt(elements, i++, s);
        if (sleep == -1) {
          return null; //parse fails
        }

        //parse the i-th number-of-retries
        final @OsUntrusted int retries = parsePositiveInt(elements, i++, s);
        if (retries == -1) {
          return null; //parse fails
        }

        pairs.add(new RetryPolicies.MultipleLinearRandomRetry.@OsUntrusted Pair(retries, sleep));
      }
      return new RetryPolicies.@OsUntrusted MultipleLinearRandomRetry(pairs);
    }

    /**
     * Parse the i-th element as an integer.
     * @return -1 if the parsing fails or the parsed value <= 0;
     *   otherwise, return the parsed value.
     */
    private static @OsUntrusted int parsePositiveInt(final @OsUntrusted String @OsUntrusted [] elements,
        final @OsUntrusted int i, final @OsUntrusted String originalString) {
      final @OsUntrusted String s = elements[i].trim();
      final @OsUntrusted int n;
      try {
        n = Integer.parseInt(s);
      } catch(@OsUntrusted NumberFormatException nfe) {
        LOG.warn("Failed to parse \"" + s + "\", which is the index " + i
            + " element in \"" + originalString + "\"", nfe);
        return -1;
      }

      if (n <= 0) {
        LOG.warn("The value " + n + " <= 0: it is parsed from the string \""
            + s + "\" which is the index " + i + " element in \""
            + originalString + "\"");
        return -1;
      }
      return n;
    }
  }

  static class ExceptionDependentRetry implements @OsUntrusted RetryPolicy {

    @OsUntrusted
    RetryPolicy defaultPolicy;
    @OsUntrusted
    Map<@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Exception>, @OsUntrusted RetryPolicy> exceptionToPolicyMap;
    
    public @OsUntrusted ExceptionDependentRetry(@OsUntrusted RetryPolicy defaultPolicy,
                                   @OsUntrusted
                                   Map<@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Exception>, @OsUntrusted RetryPolicy> exceptionToPolicyMap) {
      this.defaultPolicy = defaultPolicy;
      this.exceptionToPolicyMap = exceptionToPolicyMap;
    }

    @Override
    public @OsUntrusted RetryAction shouldRetry(RetryPolicies.@OsUntrusted ExceptionDependentRetry this, @OsUntrusted Exception e, @OsUntrusted int retries, @OsUntrusted int failovers,
        @OsUntrusted
        boolean isIdempotentOrAtMostOnce) throws Exception {
      @OsUntrusted
      RetryPolicy policy = exceptionToPolicyMap.get(e.getClass());
      if (policy == null) {
        policy = defaultPolicy;
      }
      return policy.shouldRetry(e, retries, failovers, isIdempotentOrAtMostOnce);
    }
    
  }
  
  static class RemoteExceptionDependentRetry implements @OsUntrusted RetryPolicy {

    @OsUntrusted
    RetryPolicy defaultPolicy;
    @OsUntrusted
    Map<@OsUntrusted String, @OsUntrusted RetryPolicy> exceptionNameToPolicyMap;
    
    public @OsUntrusted RemoteExceptionDependentRetry(@OsUntrusted RetryPolicy defaultPolicy,
                                   @OsUntrusted
                                   Map<@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Exception>,
                                   @OsUntrusted
                                   RetryPolicy> exceptionToPolicyMap) {
      this.defaultPolicy = defaultPolicy;
      this.exceptionNameToPolicyMap = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted RetryPolicy>();
      for (@OsUntrusted Entry<@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Exception>, @OsUntrusted RetryPolicy> e :
          exceptionToPolicyMap.entrySet()) {
        exceptionNameToPolicyMap.put(e.getKey().getName(), e.getValue());
      }
    }

    @Override
    public @OsUntrusted RetryAction shouldRetry(RetryPolicies.@OsUntrusted RemoteExceptionDependentRetry this, @OsUntrusted Exception e, @OsUntrusted int retries, @OsUntrusted int failovers,
        @OsUntrusted
        boolean isIdempotentOrAtMostOnce) throws Exception {
      @OsUntrusted
      RetryPolicy policy = null;
      if (e instanceof @OsUntrusted RemoteException) {
        policy = exceptionNameToPolicyMap.get(
            ((@OsUntrusted RemoteException) e).getClassName());
      }
      if (policy == null) {
        policy = defaultPolicy;
      }
      return policy.shouldRetry(e, retries, failovers, isIdempotentOrAtMostOnce);
    }
  }
  
  static class ExponentialBackoffRetry extends @OsUntrusted RetryLimited {
    
    public @OsUntrusted ExponentialBackoffRetry(
        @OsUntrusted
        int maxRetries, @OsUntrusted long sleepTime, @OsUntrusted TimeUnit timeUnit) {
      super(maxRetries, sleepTime, timeUnit);

      if (maxRetries < 0) {
        throw new @OsUntrusted IllegalArgumentException("maxRetries = " + maxRetries + " < 0");
      } else if (maxRetries >= Long.SIZE - 1) {
        //calculateSleepTime may overflow. 
        throw new @OsUntrusted IllegalArgumentException("maxRetries = " + maxRetries
            + " >= " + (Long.SIZE - 1));
      }
    }
    
    @Override
    protected @OsUntrusted long calculateSleepTime(RetryPolicies.@OsUntrusted ExponentialBackoffRetry this, @OsUntrusted int retries) {
      return calculateExponentialTime(sleepTime, retries + 1);
    }
  }
  
  /**
   * Fail over and retry in the case of:
   *   Remote StandbyException (server is up, but is not the active server)
   *   Immediate socket exceptions (e.g. no route to host, econnrefused)
   *   Socket exceptions after initial connection when operation is idempotent
   * 
   * The first failover is immediate, while all subsequent failovers wait an
   * exponentially-increasing random amount of time.
   * 
   * Fail immediately in the case of:
   *   Socket exceptions after initial connection when operation is not idempotent
   * 
   * Fall back on underlying retry policy otherwise.
   */
  static class FailoverOnNetworkExceptionRetry implements @OsUntrusted RetryPolicy {
    
    private @OsUntrusted RetryPolicy fallbackPolicy;
    private @OsUntrusted int maxFailovers;
    private @OsUntrusted long delayMillis;
    private @OsUntrusted long maxDelayBase;
    
    public @OsUntrusted FailoverOnNetworkExceptionRetry(@OsUntrusted RetryPolicy fallbackPolicy,
        @OsUntrusted
        int maxFailovers) {
      this(fallbackPolicy, maxFailovers, 0, 0);
    }
    
    public @OsUntrusted FailoverOnNetworkExceptionRetry(@OsUntrusted RetryPolicy fallbackPolicy,
        @OsUntrusted
        int maxFailovers, @OsUntrusted long delayMillis, @OsUntrusted long maxDelayBase) {
      this.fallbackPolicy = fallbackPolicy;
      this.maxFailovers = maxFailovers;
      this.delayMillis = delayMillis;
      this.maxDelayBase = maxDelayBase;
    }

    /**
     * @return 0 if this is our first failover/retry (i.e., retry immediately),
     *         sleep exponentially otherwise
     */
    private @OsUntrusted long getFailoverOrRetrySleepTime(RetryPolicies.@OsUntrusted FailoverOnNetworkExceptionRetry this, @OsUntrusted int times) {
      return times == 0 ? 0 : 
        calculateExponentialTime(delayMillis, times, maxDelayBase);
    }
    
    @Override
    public @OsUntrusted RetryAction shouldRetry(RetryPolicies.@OsUntrusted FailoverOnNetworkExceptionRetry this, @OsUntrusted Exception e, @OsUntrusted int retries,
        @OsUntrusted
        int failovers, @OsUntrusted boolean isIdempotentOrAtMostOnce) throws Exception {
      if (failovers >= maxFailovers) {
        return new @OsUntrusted RetryAction(RetryAction.RetryDecision.FAIL, 0,
            "failovers (" + failovers + ") exceeded maximum allowed ("
            + maxFailovers + ")");
      }
      
      if (e instanceof @OsUntrusted ConnectException ||
          e instanceof @OsUntrusted NoRouteToHostException ||
          e instanceof @OsUntrusted UnknownHostException ||
          e instanceof @OsUntrusted StandbyException ||
          e instanceof @OsUntrusted ConnectTimeoutException ||
          isWrappedStandbyException(e)) {
        return new @OsUntrusted RetryAction(RetryAction.RetryDecision.FAILOVER_AND_RETRY,
            getFailoverOrRetrySleepTime(failovers));
      } else if (e instanceof @OsUntrusted SocketException ||
                 (e instanceof @OsUntrusted IOException && !(e instanceof @OsUntrusted RemoteException))) {
        if (isIdempotentOrAtMostOnce) {
          return RetryAction.FAILOVER_AND_RETRY;
        } else {
          return new @OsUntrusted RetryAction(RetryAction.RetryDecision.FAIL, 0,
              "the invoked method is not idempotent, and unable to determine " +
              "whether it was invoked");
        }
      } else {
        @OsUntrusted
        RetriableException re = getWrappedRetriableException(e);
        if (re != null) {
          return new @OsUntrusted RetryAction(RetryAction.RetryDecision.RETRY,
              getFailoverOrRetrySleepTime(retries));
        } else {
          return fallbackPolicy.shouldRetry(e, retries, failovers,
              isIdempotentOrAtMostOnce);
        }
      }
    }
    
  }

  /**
   * Return a value which is <code>time</code> increasing exponentially as a
   * function of <code>retries</code>, +/- 0%-50% of that value, chosen
   * randomly.
   * 
   * @param time the base amount of time to work with
   * @param retries the number of retries that have so occurred so far
   * @param cap value at which to cap the base sleep time
   * @return an amount of time to sleep
   */
  private static @OsUntrusted long calculateExponentialTime(@OsUntrusted long time, @OsUntrusted int retries,
      @OsUntrusted
      long cap) {
    @OsUntrusted
    long baseTime = Math.min(time * (1L << retries), cap);
    return (@OsUntrusted long) (baseTime * (RANDOM.get().nextDouble() + 0.5));
  }

  private static @OsUntrusted long calculateExponentialTime(@OsUntrusted long time, @OsUntrusted int retries) {
    return calculateExponentialTime(time, retries, Long.MAX_VALUE);
  }
  
  private static @OsUntrusted boolean isWrappedStandbyException(@OsUntrusted Exception e) {
    if (!(e instanceof @OsUntrusted RemoteException)) {
      return false;
    }
    @OsUntrusted
    Exception unwrapped = ((@OsUntrusted RemoteException)e).unwrapRemoteException(
        StandbyException.class);
    return unwrapped instanceof @OsUntrusted StandbyException;
  }
  
  private static @OsUntrusted RetriableException getWrappedRetriableException(@OsUntrusted Exception e) {
    if (!(e instanceof @OsUntrusted RemoteException)) {
      return null;
    }
    @OsUntrusted
    Exception unwrapped = ((@OsUntrusted RemoteException)e).unwrapRemoteException(
        RetriableException.class);
    return unwrapped instanceof @OsUntrusted RetriableException ? 
        (@OsUntrusted RetriableException) unwrapped : null;
  }
}
