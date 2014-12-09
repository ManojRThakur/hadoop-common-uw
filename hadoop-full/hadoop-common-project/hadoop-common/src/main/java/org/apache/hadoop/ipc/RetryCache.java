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
import java.util.Arrays;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.LightWeightCache;
import org.apache.hadoop.util.LightWeightGSet;
import org.apache.hadoop.util.LightWeightGSet.LinkedElement;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Maintains a cache of non-idempotent requests that have been successfully
 * processed by the RPC server implementation, to handle the retries. A request
 * is uniquely identified by the unique client ID + call ID of the RPC request.
 * On receiving retried request, an entry will be found in the
 * {@link RetryCache} and the previous response is sent back to the request.
 * <p>
 * To look an implementation using this cache, see HDFS FSNamesystem class.
 */
@InterfaceAudience.Private
public class RetryCache {
  public static final @OsUntrusted Log LOG = LogFactory.getLog(RetryCache.class);
  /**
   * CacheEntry is tracked using unique client ID and callId of the RPC request
   */
  public static class CacheEntry implements LightWeightCache.@OsUntrusted Entry {
    /**
     * Processing state of the requests
     */
    private static @OsUntrusted byte INPROGRESS = 0;
    private static @OsUntrusted byte SUCCESS = 1;
    private static @OsUntrusted byte FAILED = 2;

    private @OsUntrusted byte state = INPROGRESS;
    
    // Store uuid as two long for better memory utilization
    private final @OsUntrusted long clientIdMsb; // Most signficant bytes
    private final @OsUntrusted long clientIdLsb; // Least significant bytes
    
    private final @OsUntrusted int callId;
    private final @OsUntrusted long expirationTime;
    private LightWeightGSet.@OsUntrusted LinkedElement next;

    @OsUntrusted
    CacheEntry(@OsUntrusted byte @OsUntrusted [] clientId, @OsUntrusted int callId, @OsUntrusted long expirationTime) {
      // ClientId must be a UUID - that is 16 octets.
      Preconditions.checkArgument(clientId.length == ClientId.BYTE_LENGTH,
          "Invalid clientId - length is " + clientId.length
              + " expected length " + ClientId.BYTE_LENGTH);
      // Convert UUID bytes to two longs
      clientIdMsb = ClientId.getMsb(clientId);
      clientIdLsb = ClientId.getLsb(clientId);
      this.callId = callId;
      this.expirationTime = expirationTime;
    }

    @OsUntrusted
    CacheEntry(@OsUntrusted byte @OsUntrusted [] clientId, @OsUntrusted int callId, @OsUntrusted long expirationTime,
        @OsUntrusted
        boolean success) {
      this(clientId, callId, expirationTime);
      this.state = success ? SUCCESS : FAILED;
    }

    private static @OsUntrusted int hashCode(@OsUntrusted long value) {
      return (@OsUntrusted int)(value ^ (value >>> 32));
    }
    
    @Override
    public @OsUntrusted int hashCode(RetryCache.@OsUntrusted CacheEntry this) {
      return (hashCode(clientIdMsb) * 31 + hashCode(clientIdLsb)) * 31 + callId;
    }

    @Override
    public @OsUntrusted boolean equals(RetryCache.@OsUntrusted CacheEntry this, @OsUntrusted Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof @OsUntrusted CacheEntry)) {
        return false;
      }
      @OsUntrusted
      CacheEntry other = (@OsUntrusted CacheEntry) obj;
      return callId == other.callId && clientIdMsb == other.clientIdMsb
          && clientIdLsb == other.clientIdLsb;
    }

    @Override
    public void setNext(RetryCache.@OsUntrusted CacheEntry this, @OsUntrusted LinkedElement next) {
      this.next = next;
    }

    @Override
    public @OsUntrusted LinkedElement getNext(RetryCache.@OsUntrusted CacheEntry this) {
      return next;
    }

    synchronized void completed(RetryCache.@OsUntrusted CacheEntry this, @OsUntrusted boolean success) {
      state = success ? SUCCESS : FAILED;
      this.notifyAll();
    }

    public synchronized @OsUntrusted boolean isSuccess(RetryCache.@OsUntrusted CacheEntry this) {
      return state == SUCCESS;
    }

    @Override
    public void setExpirationTime(RetryCache.@OsUntrusted CacheEntry this, @OsUntrusted long timeNano) {
      // expiration time does not change
    }

    @Override
    public @OsUntrusted long getExpirationTime(RetryCache.@OsUntrusted CacheEntry this) {
      return expirationTime;
    }
    
    @Override
    public @OsUntrusted String toString(RetryCache.@OsUntrusted CacheEntry this) {
      return (new @OsUntrusted UUID(this.clientIdMsb, this.clientIdLsb)).toString() + ":"
          + this.callId + ":" + this.state;
    }
  }

  /**
   * CacheEntry with payload that tracks the previous response or parts of
   * previous response to be used for generating response for retried requests.
   */
  public static class CacheEntryWithPayload extends @OsUntrusted CacheEntry {
    private @OsUntrusted Object payload;

    @OsUntrusted
    CacheEntryWithPayload(@OsUntrusted byte @OsUntrusted [] clientId, @OsUntrusted int callId, @OsUntrusted Object payload,
        @OsUntrusted
        long expirationTime) {
      super(clientId, callId, expirationTime);
      this.payload = payload;
    }

    @OsUntrusted
    CacheEntryWithPayload(@OsUntrusted byte @OsUntrusted [] clientId, @OsUntrusted int callId, @OsUntrusted Object payload,
        @OsUntrusted
        long expirationTime, @OsUntrusted boolean success) {
     super(clientId, callId, expirationTime, success);
     this.payload = payload;
   }

    /** Override equals to avoid findbugs warnings */
    @Override
    public @OsUntrusted boolean equals(RetryCache.@OsUntrusted CacheEntryWithPayload this, @OsUntrusted Object obj) {
      return super.equals(obj);
    }

    /** Override hashcode to avoid findbugs warnings */
    @Override
    public @OsUntrusted int hashCode(RetryCache.@OsUntrusted CacheEntryWithPayload this) {
      return super.hashCode();
    }

    public @OsUntrusted Object getPayload(RetryCache.@OsUntrusted CacheEntryWithPayload this) {
      return payload;
    }
  }

  private final @OsUntrusted LightWeightGSet<@OsUntrusted CacheEntry, @OsUntrusted CacheEntry> set;
  private final @OsUntrusted long expirationTime;

  /**
   * Constructor
   * @param cacheName name to identify the cache by
   * @param percentage percentage of total java heap space used by this cache
   * @param expirationTime time for an entry to expire in nanoseconds
   */
  public @OsUntrusted RetryCache(@OsUntrusted String cacheName, @OsUntrusted double percentage, @OsUntrusted long expirationTime) {
    @OsUntrusted
    int capacity = LightWeightGSet.computeCapacity(percentage, cacheName);
    capacity = capacity > 16 ? capacity : 16;
    this.set = new @OsUntrusted LightWeightCache<@OsUntrusted CacheEntry, @OsUntrusted CacheEntry>(capacity, capacity,
        expirationTime, 0);
    this.expirationTime = expirationTime;
  }

  private static @OsUntrusted boolean skipRetryCache() {
    // Do not track non RPC invocation or RPC requests with
    // invalid callId or clientId in retry cache
    return !Server.isRpcInvocation() || Server.getCallId() < 0
        || Arrays.equals(Server.getClientId(), RpcConstants.DUMMY_CLIENT_ID);
  }
  
  @VisibleForTesting
  public @OsUntrusted LightWeightGSet<@OsUntrusted CacheEntry, @OsUntrusted CacheEntry> getCacheSet(@OsUntrusted RetryCache this) {
    return set;
  }

  /**
   * This method handles the following conditions:
   * <ul>
   * <li>If retry is not to be processed, return null</li>
   * <li>If there is no cache entry, add a new entry {@code newEntry} and return
   * it.</li>
   * <li>If there is an existing entry, wait for its completion. If the
   * completion state is {@link CacheEntry#FAILED}, the expectation is that the
   * thread that waited for completion, retries the request. the
   * {@link CacheEntry} state is set to {@link CacheEntry#INPROGRESS} again.
   * <li>If the completion state is {@link CacheEntry#SUCCESS}, the entry is
   * returned so that the thread that waits for it can can return previous
   * response.</li>
   * <ul>
   * 
   * @return {@link CacheEntry}.
   */
  private @OsUntrusted CacheEntry waitForCompletion(@OsUntrusted RetryCache this, @OsUntrusted CacheEntry newEntry) {
    @OsUntrusted
    CacheEntry mapEntry = null;
    synchronized (this) {
      mapEntry = set.get(newEntry);
      // If an entry in the cache does not exist, add a new one
      if (mapEntry == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Adding Rpc request clientId "
              + newEntry.clientIdMsb + newEntry.clientIdLsb + " callId "
              + newEntry.callId + " to retryCache");
        }
        set.put(newEntry);
        return newEntry;
      }
    }
    // Entry already exists in cache. Wait for completion and return its state
    Preconditions.checkNotNull(mapEntry,
        "Entry from the cache should not be null");
    // Wait for in progress request to complete
    synchronized (mapEntry) {
      while (mapEntry.state == CacheEntry.INPROGRESS) {
        try {
          mapEntry.wait();
        } catch (@OsUntrusted InterruptedException ie) {
          // Restore the interrupted status
          Thread.currentThread().interrupt();
        }
      }
      // Previous request has failed, the expectation is is that it will be
      // retried again.
      if (mapEntry.state != CacheEntry.SUCCESS) {
        mapEntry.state = CacheEntry.INPROGRESS;
      }
    }
    return mapEntry;
  }
  
  /** 
   * Add a new cache entry into the retry cache. The cache entry consists of 
   * clientId and callId extracted from editlog.
   */
  public void addCacheEntry(@OsUntrusted RetryCache this, @OsUntrusted byte @OsUntrusted [] clientId, @OsUntrusted int callId) {
    @OsUntrusted
    CacheEntry newEntry = new @OsUntrusted CacheEntry(clientId, callId, System.nanoTime()
        + expirationTime, true);
    synchronized(this) {
      set.put(newEntry);
    }
  }
  
  public void addCacheEntryWithPayload(@OsUntrusted RetryCache this, @OsUntrusted byte @OsUntrusted [] clientId, @OsUntrusted int callId,
      @OsUntrusted
      Object payload) {
    // since the entry is loaded from editlog, we can assume it succeeded.    
    @OsUntrusted
    CacheEntry newEntry = new @OsUntrusted CacheEntryWithPayload(clientId, callId, payload,
        System.nanoTime() + expirationTime, true);
    synchronized(this) {
      set.put(newEntry);
    }
  }

  private static @OsUntrusted CacheEntry newEntry(@OsUntrusted long expirationTime) {
    return new @OsUntrusted CacheEntry(Server.getClientId(), Server.getCallId(),
        System.nanoTime() + expirationTime);
  }

  private static @OsUntrusted CacheEntryWithPayload newEntry(@OsUntrusted Object payload,
      @OsUntrusted
      long expirationTime) {
    return new @OsUntrusted CacheEntryWithPayload(Server.getClientId(), Server.getCallId(),
        payload, System.nanoTime() + expirationTime);
  }

  /** Static method that provides null check for retryCache */
  public static @OsUntrusted CacheEntry waitForCompletion(@OsUntrusted RetryCache cache) {
    if (skipRetryCache()) {
      return null;
    }
    return cache != null ? cache
        .waitForCompletion(newEntry(cache.expirationTime)) : null;
  }

  /** Static method that provides null check for retryCache */
  public static @OsUntrusted CacheEntryWithPayload waitForCompletion(@OsUntrusted RetryCache cache,
      @OsUntrusted
      Object payload) {
    if (skipRetryCache()) {
      return null;
    }
    return (@OsUntrusted CacheEntryWithPayload) (cache != null ? cache
        .waitForCompletion(newEntry(payload, cache.expirationTime)) : null);
  }

  public static void setState(@OsUntrusted CacheEntry e, @OsUntrusted boolean success) {
    if (e == null) {
      return;
    }
    e.completed(success);
  }

  public static void setState(@OsUntrusted CacheEntryWithPayload e, @OsUntrusted boolean success,
      @OsUntrusted
      Object payload) {
    if (e == null) {
      return;
    }
    e.payload = payload;
    e.completed(success);
  }

  public static void clear(@OsUntrusted RetryCache cache) {
    if (cache != null) {
      cache.set.clear();
    }
  }
}
