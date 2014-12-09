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

package org.apache.hadoop.fs;

import ostrusted.quals.OsUntrusted;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Time;

/**
 * A daemon thread that waits for the next file system to renew.
 */
@InterfaceAudience.Private
public class DelegationTokenRenewer
    extends @OsUntrusted Thread {
  private static final @OsUntrusted Log LOG = LogFactory
      .getLog(DelegationTokenRenewer.class);

  /** The renewable interface used by the renewer. */
  public interface Renewable {
    /** @return the renew token. */
    public @OsUntrusted Token<@OsUntrusted ? extends java.lang.@OsUntrusted Object> getRenewToken(DelegationTokenRenewer.@OsUntrusted Renewable this);

    /** Set delegation token. */
    public <@OsUntrusted T extends @OsUntrusted TokenIdentifier> void setDelegationToken(DelegationTokenRenewer.@OsUntrusted Renewable this, Token<T> token);
  }

  /**
   * An action that will renew and replace the file system's delegation 
   * tokens automatically.
   */
  public static class RenewAction<@OsUntrusted T extends FileSystem & Renewable>
      implements @OsUntrusted Delayed {
    /** when should the renew happen */
    private @OsUntrusted long renewalTime;
    /** a weak reference to the file system so that it can be garbage collected */
    private final @OsUntrusted WeakReference<@OsUntrusted T> weakFs;
    private @OsUntrusted Token<@OsUntrusted ? extends java.lang.@OsUntrusted Object> token; 
    @OsUntrusted
    boolean isValid = true;

    private @OsUntrusted RenewAction(final @OsUntrusted T fs) {
      this.weakFs = new @OsUntrusted WeakReference<T>(fs);
      this.token = fs.getRenewToken();
      updateRenewalTime(renewCycle);
    }
 
    public @OsUntrusted boolean isValid(DelegationTokenRenewer.@OsUntrusted RenewAction<T> this) {
      return isValid;
    }
    
    /** Get the delay until this event should happen. */
    @Override
    public @OsUntrusted long getDelay(DelegationTokenRenewer.@OsUntrusted RenewAction<T> this, final @OsUntrusted TimeUnit unit) {
      final @OsUntrusted long millisLeft = renewalTime - Time.now();
      return unit.convert(millisLeft, TimeUnit.MILLISECONDS);
    }

    @Override
    public @OsUntrusted int compareTo(DelegationTokenRenewer.@OsUntrusted RenewAction<T> this, final @OsUntrusted Delayed delayed) {
      final @OsUntrusted RenewAction<@OsUntrusted ? extends java.lang.@OsUntrusted Object> that = (@OsUntrusted RenewAction<@OsUntrusted ? extends java.lang.@OsUntrusted Object>)delayed;
      return this.renewalTime < that.renewalTime? -1
          : this.renewalTime == that.renewalTime? 0: 1;
    }

    @Override
    public @OsUntrusted int hashCode(DelegationTokenRenewer.@OsUntrusted RenewAction<T> this) {
      return token.hashCode();
    }

    @Override
    public @OsUntrusted boolean equals(DelegationTokenRenewer.@OsUntrusted RenewAction<T> this, final @OsUntrusted Object that) {
      if (this == that) {
        return true;
      } else if (that == null || !(that instanceof @OsUntrusted RenewAction)) {
        return false;
      }
      return token.equals(((@OsUntrusted RenewAction<@OsUntrusted ? extends java.lang.@OsUntrusted Object>)that).token);
    }

    /**
     * Set a new time for the renewal.
     * It can only be called when the action is not in the queue or any
     * collection because the hashCode may change
     * @param newTime the new time
     */
    private void updateRenewalTime(DelegationTokenRenewer.@OsUntrusted RenewAction<T> this, @OsUntrusted long delay) {
      renewalTime = Time.now() + delay - delay/10;
    }

    /**
     * Renew or replace the delegation token for this file system.
     * It can only be called when the action is not in the queue.
     * @return
     * @throws IOException
     */
    private @OsUntrusted boolean renew(DelegationTokenRenewer.@OsUntrusted RenewAction<T> this) throws IOException, InterruptedException {
      final T fs = weakFs.get();
      final @OsUntrusted boolean b = fs != null;
      if (b) {
        synchronized(fs) {
          try {
            @OsUntrusted
            long expires = token.renew(fs.getConf());
            updateRenewalTime(expires - Time.now());
          } catch (@OsUntrusted IOException ie) {
            try {
              @OsUntrusted
              Token<@OsUntrusted ? extends java.lang.@OsUntrusted Object> @OsUntrusted [] tokens = fs.addDelegationTokens(null, null);
              if (tokens.length == 0) {
                throw new @OsUntrusted IOException("addDelegationTokens returned no tokens");
              }
              token = tokens[0];
              updateRenewalTime(renewCycle);
              fs.setDelegationToken(token);
            } catch (@OsUntrusted IOException ie2) {
              isValid = false;
              throw new @OsUntrusted IOException("Can't renew or get new delegation token ", ie);
            }
          }
        }
      }
      return b;
    }

    private void cancel(DelegationTokenRenewer.@OsUntrusted RenewAction<T> this) throws IOException, InterruptedException {
      final T fs = weakFs.get();
      if (fs != null) {
        token.cancel(fs.getConf());
      }
    }

    @Override
    public @OsUntrusted String toString(DelegationTokenRenewer.@OsUntrusted RenewAction<T> this) {
      @OsUntrusted
      Renewable fs = weakFs.get();
      return fs == null? "evaporated token renew"
          : "The token will be renewed in " + getDelay(TimeUnit.SECONDS)
            + " secs, renewToken=" + token;
    }
  }

  /** assumes renew cycle for a token is 24 hours... */
  private static final @OsUntrusted long RENEW_CYCLE = 24 * 60 * 60 * 1000; 

  @InterfaceAudience.Private
  @VisibleForTesting
  public static @OsUntrusted long renewCycle = RENEW_CYCLE;

  /** Queue to maintain the RenewActions to be processed by the {@link #run()} */
  private volatile @OsUntrusted DelayQueue<@OsUntrusted RenewAction<@OsUntrusted ? extends java.lang.@OsUntrusted Object>> queue = new @OsUntrusted DelayQueue<@OsUntrusted RenewAction<@OsUntrusted ? extends java.lang.@OsUntrusted Object>>();
  
  /** For testing purposes */
  @VisibleForTesting
  protected @OsUntrusted int getRenewQueueLength(@OsUntrusted DelegationTokenRenewer this) {
    return queue.size();
  }

  /**
   * Create the singleton instance. However, the thread can be started lazily in
   * {@link #addRenewAction(FileSystem)}
   */
  private static @OsUntrusted DelegationTokenRenewer INSTANCE = null;

  private @OsUntrusted DelegationTokenRenewer(final @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted FileSystem> clazz) {
    super(clazz.getSimpleName() + "-" + DelegationTokenRenewer.class.getSimpleName());
    setDaemon(true);
  }

  public static synchronized @OsUntrusted DelegationTokenRenewer getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new @OsUntrusted DelegationTokenRenewer(FileSystem.class);
    }
    return INSTANCE;
  }

  @VisibleForTesting
  static synchronized void reset() {
    if (INSTANCE != null) {
      INSTANCE.queue.clear();
      INSTANCE.interrupt();
      try {
        INSTANCE.join();
      } catch (@OsUntrusted InterruptedException e) {
        LOG.warn("Failed to reset renewer");
      } finally {
        INSTANCE = null;
      }
    }
  }
  
  /** Add a renew action to the queue. */
  @SuppressWarnings("static-access")
  public <@OsUntrusted T extends FileSystem & Renewable> @OsUntrusted RenewAction<@OsUntrusted T> addRenewAction(@OsUntrusted DelegationTokenRenewer this, final @OsUntrusted T fs) {
    synchronized (this) {
      if (!isAlive()) {
        start();
      }
    }
    @OsUntrusted
    RenewAction<@OsUntrusted T> action = new @OsUntrusted RenewAction<@OsUntrusted T>(fs);
    if (action.token != null) {
      queue.add(action);
    } else {
      fs.LOG.error("does not have a token for renewal");
    }
    return action;
  }

  /**
   * Remove the associated renew action from the queue
   * 
   * @throws IOException
   */
  public <@OsUntrusted T extends FileSystem & Renewable> void removeRenewAction(
      @OsUntrusted DelegationTokenRenewer this, final @OsUntrusted T fs) throws IOException {
    @OsUntrusted
    RenewAction<@OsUntrusted T> action = new @OsUntrusted RenewAction<@OsUntrusted T>(fs);
    if (queue.remove(action)) {
      try {
        action.cancel();
      } catch (@OsUntrusted InterruptedException ie) {
        LOG.error("Interrupted while canceling token for " + fs.getUri()
            + "filesystem");
        if (LOG.isDebugEnabled()) {
          LOG.debug(ie.getStackTrace());
        }
      }
    }
  }

  @SuppressWarnings("static-access")
  @Override
  public void run(@OsUntrusted DelegationTokenRenewer this) {
    for(;;) {
      @OsUntrusted
      RenewAction<@OsUntrusted ? extends java.lang.@OsUntrusted Object> action = null;
      try {
        action = queue.take();
        if (action.renew()) {
          queue.add(action);
        }
      } catch (@OsUntrusted InterruptedException ie) {
        return;
      } catch (@OsUntrusted Exception ie) {
        action.weakFs.get().LOG.warn("Failed to renew token, action=" + action,
            ie);
      }
    }
  }
}
