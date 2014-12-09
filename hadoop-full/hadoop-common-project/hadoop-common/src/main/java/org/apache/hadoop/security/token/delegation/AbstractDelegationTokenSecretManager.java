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

package org.apache.hadoop.security.token.delegation;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce", "Hive"})
@InterfaceStability.Evolving
public abstract 
class AbstractDelegationTokenSecretManager<@OsUntrusted TokenIdent 
extends @OsUntrusted AbstractDelegationTokenIdentifier> 
   extends @OsUntrusted SecretManager<TokenIdent> {
  private static final @OsUntrusted Log LOG = LogFactory
      .getLog(AbstractDelegationTokenSecretManager.class);

  /** 
   * Cache of currently valid tokens, mapping from DelegationTokenIdentifier 
   * to DelegationTokenInformation. Protected by this object lock.
   */
  protected final @OsUntrusted Map<@OsUntrusted TokenIdent, @OsUntrusted DelegationTokenInformation> currentTokens 
      = new @OsUntrusted HashMap<TokenIdent, @OsUntrusted DelegationTokenInformation>();
  
  /**
   * Sequence number to create DelegationTokenIdentifier.
   * Protected by this object lock.
   */
  protected @OsUntrusted int delegationTokenSequenceNumber = 0;
  
  /**
   * Access to allKeys is protected by this object lock
   */
  protected final @OsUntrusted Map<@OsUntrusted Integer, @OsUntrusted DelegationKey> allKeys 
      = new @OsUntrusted HashMap<@OsUntrusted Integer, @OsUntrusted DelegationKey>();
  
  /**
   * Access to currentId is protected by this object lock.
   */
  protected @OsUntrusted int currentId = 0;
  /**
   * Access to currentKey is protected by this object lock
   */
  private @OsUntrusted DelegationKey currentKey;
  
  private @OsUntrusted long keyUpdateInterval;
  private @OsUntrusted long tokenMaxLifetime;
  private @OsUntrusted long tokenRemoverScanInterval;
  private @OsUntrusted long tokenRenewInterval;
  /**
   * Whether to store a token's tracking ID in its TokenInformation.
   * Can be overridden by a subclass.
   */
  protected @OsUntrusted boolean storeTokenTrackingId;
  private @OsUntrusted Thread tokenRemoverThread;
  protected volatile @OsUntrusted boolean running;

  /**
   * If the delegation token update thread holds this lock, it will
   * not get interrupted.
   */
  protected @OsUntrusted Object noInterruptsLock = new @OsUntrusted Object();

  public @OsUntrusted AbstractDelegationTokenSecretManager(@OsUntrusted long delegationKeyUpdateInterval,
      @OsUntrusted
      long delegationTokenMaxLifetime, @OsUntrusted long delegationTokenRenewInterval,
      @OsUntrusted
      long delegationTokenRemoverScanInterval) {
    this.keyUpdateInterval = delegationKeyUpdateInterval;
    this.tokenMaxLifetime = delegationTokenMaxLifetime;
    this.tokenRenewInterval = delegationTokenRenewInterval;
    this.tokenRemoverScanInterval = delegationTokenRemoverScanInterval;
    this.storeTokenTrackingId = false;
  }

  /** should be called before this object is used */
  public void startThreads(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this) throws IOException {
    Preconditions.checkState(!running);
    updateCurrentKey();
    synchronized (this) {
      running = true;
      tokenRemoverThread = new @OsUntrusted Daemon(new @OsUntrusted ExpiredTokenRemover());
      tokenRemoverThread.start();
    }
  }
  
  /**
   * Reset all data structures and mutable state.
   */
  public synchronized void reset(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this) {
    currentId = 0;
    allKeys.clear();
    delegationTokenSequenceNumber = 0;
    currentTokens.clear();
  }
  
  /** 
   * Add a previously used master key to cache (when NN restarts), 
   * should be called before activate().
   * */
  public synchronized void addKey(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted DelegationKey key) throws IOException {
    if (running) // a safety check
      throw new @OsUntrusted IOException("Can't add delegation key to a running SecretManager.");
    if (key.getKeyId() > currentId) {
      currentId = key.getKeyId();
    }
    allKeys.put(key.getKeyId(), key);
  }

  public synchronized @OsUntrusted DelegationKey @OsUntrusted [] getAllKeys(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this) {
    return allKeys.values().toArray(new @OsUntrusted DelegationKey @OsUntrusted [0]);
  }

  // HDFS
  protected void logUpdateMasterKey(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted DelegationKey key) throws IOException {
    return;
  }

  // HDFS
  protected void logExpireToken(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted TokenIdent ident) throws IOException {
    return;
  }

  // RM
  protected void storeNewMasterKey(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted DelegationKey key) throws IOException {
    return;
  }

  // RM
  protected void removeStoredMasterKey(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted DelegationKey key) {
    return;
  }

  // RM
  protected void storeNewToken(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted TokenIdent ident, @OsUntrusted long renewDate) {
    return;
  }
  // RM
  protected void removeStoredToken(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted TokenIdent ident) throws IOException {

  }
  // RM
  protected void updateStoredToken(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted TokenIdent ident, @OsUntrusted long renewDate) {
    return;
  }

  /**
   * This method is intended to be used for recovering persisted delegation
   * tokens
   * @param identifier identifier read from persistent storage
   * @param renewDate token renew time
   * @throws IOException
   */
  public synchronized void addPersistedDelegationToken(
      @OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted
      TokenIdent identifier, @OsUntrusted long renewDate) throws IOException {
    if (running) {
      // a safety check
      throw new @OsUntrusted IOException(
          "Can't add persisted delegation token to a running SecretManager.");
    }
    @OsUntrusted
    int keyId = identifier.getMasterKeyId();
    @OsUntrusted
    DelegationKey dKey = allKeys.get(keyId);
    if (dKey == null) {
      LOG.warn("No KEY found for persisted identifier " + identifier.toString());
      return;
    }
    @OsUntrusted
    byte @OsUntrusted [] password = createPassword(identifier.getBytes(), dKey.getKey());
    if (identifier.getSequenceNumber() > this.delegationTokenSequenceNumber) {
      this.delegationTokenSequenceNumber = identifier.getSequenceNumber();
    }
    if (currentTokens.get(identifier) == null) {
      currentTokens.put(identifier, new @OsUntrusted DelegationTokenInformation(renewDate,
          password, getTrackingIdIfEnabled(identifier)));
    } else {
      throw new @OsUntrusted IOException(
          "Same delegation token being added twice.");
    }
  }

  /** 
   * Update the current master key 
   * This is called once by startThreads before tokenRemoverThread is created, 
   * and only by tokenRemoverThread afterwards.
   */
  private void updateCurrentKey(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this) throws IOException {
    LOG.info("Updating the current master key for generating delegation tokens");
    /* Create a new currentKey with an estimated expiry date. */
    @OsUntrusted
    int newCurrentId;
    synchronized (this) {
      newCurrentId = currentId+1;
    }
    @OsUntrusted
    DelegationKey newKey = new @OsUntrusted DelegationKey(newCurrentId, System
        .currentTimeMillis()
        + keyUpdateInterval + tokenMaxLifetime, generateSecret());
    //Log must be invoked outside the lock on 'this'
    logUpdateMasterKey(newKey);
    storeNewMasterKey(newKey);
    synchronized (this) {
      currentId = newKey.getKeyId();
      currentKey = newKey;
      allKeys.put(currentKey.getKeyId(), currentKey);
    }
  }
  
  /** 
   * Update the current master key for generating delegation tokens 
   * It should be called only by tokenRemoverThread.
   */
  void rollMasterKey(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this) throws IOException {
    synchronized (this) {
      removeExpiredKeys();
      /* set final expiry date for retiring currentKey */
      currentKey.setExpiryDate(Time.now() + tokenMaxLifetime);
      /*
       * currentKey might have been removed by removeExpiredKeys(), if
       * updateMasterKey() isn't called at expected interval. Add it back to
       * allKeys just in case.
       */
      allKeys.put(currentKey.getKeyId(), currentKey);
    }
    updateCurrentKey();
  }

  private synchronized void removeExpiredKeys(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this) {
    @OsUntrusted
    long now = Time.now();
    for (@OsUntrusted Iterator<Map.@OsUntrusted Entry<@OsUntrusted Integer, @OsUntrusted DelegationKey>> it = allKeys.entrySet()
        .iterator(); it.hasNext();) {
      Map.@OsUntrusted Entry<@OsUntrusted Integer, @OsUntrusted DelegationKey> e = it.next();
      if (e.getValue().getExpiryDate() < now) {
        it.remove();
        // ensure the tokens generated by this current key can be recovered
        // with this current key after this current key is rolled
        if(!e.getValue().equals(currentKey))
          removeStoredMasterKey(e.getValue());
      }
    }
  }
  
  @Override
  protected synchronized @OsUntrusted byte @OsUntrusted [] createPassword(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted TokenIdent identifier) {
    @OsUntrusted
    int sequenceNum;
    @OsUntrusted
    long now = Time.now();
    sequenceNum = ++delegationTokenSequenceNumber;
    identifier.setIssueDate(now);
    identifier.setMaxDate(now + tokenMaxLifetime);
    identifier.setMasterKeyId(currentId);
    identifier.setSequenceNumber(sequenceNum);
    LOG.info("Creating password for identifier: " + identifier);
    @OsUntrusted
    byte @OsUntrusted [] password = createPassword(identifier.getBytes(), currentKey.getKey());
    storeNewToken(identifier, now + tokenRenewInterval);
    currentTokens.put(identifier, new @OsUntrusted DelegationTokenInformation(now
        + tokenRenewInterval, password, getTrackingIdIfEnabled(identifier)));
    return password;
  }
  
  /**
   * Find the DelegationTokenInformation for the given token id, and verify that
   * if the token is expired. Note that this method should be called with 
   * acquiring the secret manager's monitor.
   */
  protected @OsUntrusted DelegationTokenInformation checkToken(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted TokenIdent identifier)
      throws InvalidToken {
    assert Thread.holdsLock(this);
    @OsUntrusted
    DelegationTokenInformation info = currentTokens.get(identifier);
    if (info == null) {
      throw new @OsUntrusted InvalidToken("token (" + identifier.toString()
          + ") can't be found in cache");
    }
    if (info.getRenewDate() < Time.now()) {
      throw new @OsUntrusted InvalidToken("token (" + identifier.toString() + ") is expired");
    }
    return info;
  }
  
  @Override
  public synchronized @OsUntrusted byte @OsUntrusted [] retrievePassword(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted TokenIdent identifier)
      throws InvalidToken {
    return checkToken(identifier).getPassword();
  }

  protected @OsUntrusted String getTrackingIdIfEnabled(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted TokenIdent ident) {
    if (storeTokenTrackingId) {
      return ident.getTrackingId();
    }
    return null;
  }

  public synchronized @OsUntrusted String getTokenTrackingId(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted TokenIdent identifier) {
    @OsUntrusted
    DelegationTokenInformation info = currentTokens.get(identifier);
    if (info == null) {
      return null;
    }
    return info.getTrackingId();
  }

  /**
   * Verifies that the given identifier and password are valid and match.
   * @param identifier Token identifier.
   * @param password Password in the token.
   * @throws InvalidToken
   */
  public synchronized void verifyToken(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted TokenIdent identifier, @OsUntrusted byte @OsUntrusted [] password)
      throws InvalidToken {
    @OsUntrusted
    byte @OsUntrusted [] storedPassword = retrievePassword(identifier);
    if (!Arrays.equals(password, storedPassword)) {
      throw new @OsUntrusted InvalidToken("token (" + identifier
          + ") is invalid, password doesn't match");
    }
  }
  
  /**
   * Renew a delegation token.
   * @param token the token to renew
   * @param renewer the full principal name of the user doing the renewal
   * @return the new expiration time
   * @throws InvalidToken if the token is invalid
   * @throws AccessControlException if the user can't renew token
   */
  public synchronized @OsUntrusted long renewToken(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted Token<@OsUntrusted TokenIdent> token,
                         @OsUntrusted
                         String renewer) throws InvalidToken, IOException {
    @OsUntrusted
    long now = Time.now();
    @OsUntrusted
    ByteArrayInputStream buf = new @OsUntrusted ByteArrayInputStream(token.getIdentifier());
    @OsUntrusted
    DataInputStream in = new @OsUntrusted DataInputStream(buf);
    TokenIdent id = createIdentifier();
    id.readFields(in);
    LOG.info("Token renewal requested for identifier: "+id);
    
    if (id.getMaxDate() < now) {
      throw new @OsUntrusted InvalidToken("User " + renewer + 
                             " tried to renew an expired token");
    }
    if ((id.getRenewer() == null) || (id.getRenewer().toString().isEmpty())) {
      throw new @OsUntrusted AccessControlException("User " + renewer + 
                                       " tried to renew a token without " +
                                       "a renewer");
    }
    if (!id.getRenewer().toString().equals(renewer)) {
      throw new @OsUntrusted AccessControlException("Client " + renewer + 
                                       " tries to renew a token with " +
                                       "renewer specified as " + 
                                       id.getRenewer());
    }
    @OsUntrusted
    DelegationKey key = allKeys.get(id.getMasterKeyId());
    if (key == null) {
      throw new @OsUntrusted InvalidToken("Unable to find master key for keyId="
          + id.getMasterKeyId()
          + " from cache. Failed to renew an unexpired token"
          + " with sequenceNumber=" + id.getSequenceNumber());
    }
    @OsUntrusted
    byte @OsUntrusted [] password = createPassword(token.getIdentifier(), key.getKey());
    if (!Arrays.equals(password, token.getPassword())) {
      throw new @OsUntrusted AccessControlException("Client " + renewer
          + " is trying to renew a token with " + "wrong password");
    }
    @OsUntrusted
    long renewTime = Math.min(id.getMaxDate(), now + tokenRenewInterval);
    @OsUntrusted
    String trackingId = getTrackingIdIfEnabled(id);
    @OsUntrusted
    DelegationTokenInformation info = new @OsUntrusted DelegationTokenInformation(renewTime,
        password, trackingId);

    if (currentTokens.get(id) == null) {
      throw new @OsUntrusted InvalidToken("Renewal request for unknown token");
    }
    currentTokens.put(id, info);
    updateStoredToken(id, renewTime);
    return renewTime;
  }
  
  /**
   * Cancel a token by removing it from cache.
   * @return Identifier of the canceled token
   * @throws InvalidToken for invalid token
   * @throws AccessControlException if the user isn't allowed to cancel
   */
  public synchronized @OsUntrusted TokenIdent cancelToken(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this, @OsUntrusted Token<@OsUntrusted TokenIdent> token,
      @OsUntrusted
      String canceller) throws IOException {
    @OsUntrusted
    ByteArrayInputStream buf = new @OsUntrusted ByteArrayInputStream(token.getIdentifier());
    @OsUntrusted
    DataInputStream in = new @OsUntrusted DataInputStream(buf);
    TokenIdent id = createIdentifier();
    id.readFields(in);
    LOG.info("Token cancelation requested for identifier: "+id);
    
    if (id.getUser() == null) {
      throw new @OsUntrusted InvalidToken("Token with no owner");
    }
    @OsUntrusted
    String owner = id.getUser().getUserName();
    @OsUntrusted
    Text renewer = id.getRenewer();
    @OsUntrusted
    HadoopKerberosName cancelerKrbName = new @OsUntrusted HadoopKerberosName(canceller);
    @OsUntrusted
    String cancelerShortName = cancelerKrbName.getShortName();
    if (!canceller.equals(owner)
        && (renewer == null || renewer.toString().isEmpty() || !cancelerShortName
            .equals(renewer.toString()))) {
      throw new @OsUntrusted AccessControlException(canceller
          + " is not authorized to cancel the token");
    }
    @OsUntrusted
    DelegationTokenInformation info = null;
    info = currentTokens.remove(id);
    if (info == null) {
      throw new @OsUntrusted InvalidToken("Token not found");
    }
    removeStoredToken(id);
    return id;
  }
  
  /**
   * Convert the byte[] to a secret key
   * @param key the byte[] to create the secret key from
   * @return the secret key
   */
  public static @OsUntrusted SecretKey createSecretKey(@OsUntrusted byte @OsUntrusted [] key) {
    return SecretManager.createSecretKey(key);
  }

  /** Class to encapsulate a token's renew date and password. */
  @InterfaceStability.Evolving
  public static class DelegationTokenInformation {
    @OsUntrusted
    long renewDate;
    @OsUntrusted
    byte @OsUntrusted [] password;
    @OsUntrusted
    String trackingId;

    public @OsUntrusted DelegationTokenInformation(@OsUntrusted long renewDate, @OsUntrusted byte @OsUntrusted [] password) {
      this(renewDate, password, null);
    }

    public @OsUntrusted DelegationTokenInformation(@OsUntrusted long renewDate, @OsUntrusted byte @OsUntrusted [] password,
        @OsUntrusted
        String trackingId) {
      this.renewDate = renewDate;
      this.password = password;
      this.trackingId = trackingId;
    }
    /** returns renew date */
    public @OsUntrusted long getRenewDate(AbstractDelegationTokenSecretManager.@OsUntrusted DelegationTokenInformation this) {
      return renewDate;
    }
    /** returns password */
    @OsUntrusted
    byte @OsUntrusted [] getPassword(AbstractDelegationTokenSecretManager.@OsUntrusted DelegationTokenInformation this) {
      return password;
    }
    /** returns tracking id */
    public @OsUntrusted String getTrackingId(AbstractDelegationTokenSecretManager.@OsUntrusted DelegationTokenInformation this) {
      return trackingId;
    }
  }
  
  /** Remove expired delegation tokens from cache */
  private void removeExpiredToken(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this) throws IOException {
    @OsUntrusted
    long now = Time.now();
    @OsUntrusted
    Set<TokenIdent> expiredTokens = new @OsUntrusted HashSet<TokenIdent>();
    synchronized (this) {
      @OsUntrusted
      Iterator<Map.@OsUntrusted Entry<TokenIdent, @OsUntrusted DelegationTokenInformation>> i =
          currentTokens.entrySet().iterator();
      while (i.hasNext()) {
        Map.@OsUntrusted Entry<TokenIdent, @OsUntrusted DelegationTokenInformation> entry = i.next();
        @OsUntrusted
        long renewDate = entry.getValue().getRenewDate();
        if (renewDate < now) {
          expiredTokens.add(entry.getKey());
          i.remove();
        }
      }
    }
    // don't hold lock on 'this' to avoid edit log updates blocking token ops
    for (TokenIdent ident : expiredTokens) {
      logExpireToken(ident);
      removeStoredToken(ident);
    }
  }

  public void stopThreads(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this) {
    if (LOG.isDebugEnabled())
      LOG.debug("Stopping expired delegation token remover thread");
    running = false;
    
    if (tokenRemoverThread != null) {
      synchronized (noInterruptsLock) {
        tokenRemoverThread.interrupt();
      }
      try {
        tokenRemoverThread.join();
      } catch (@OsUntrusted InterruptedException e) {
        throw new @OsUntrusted RuntimeException(
            "Unable to join on token removal thread", e);
      }
    }
  }
  
  /**
   * is secretMgr running
   * @return true if secret mgr is running
   */
  public synchronized @OsUntrusted boolean isRunning(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent> this) {
    return running;
  }
  
  private class ExpiredTokenRemover extends @OsUntrusted Thread {
    private @OsUntrusted long lastMasterKeyUpdate;
    private @OsUntrusted long lastTokenCacheCleanup;

    @Override
    public void run(@OsUntrusted AbstractDelegationTokenSecretManager<TokenIdent>.ExpiredTokenRemover this) {
      LOG.info("Starting expired delegation token remover thread, "
          + "tokenRemoverScanInterval=" + tokenRemoverScanInterval
          / (60 * 1000) + " min(s)");
      try {
        while (running) {
          @OsUntrusted
          long now = Time.now();
          if (lastMasterKeyUpdate + keyUpdateInterval < now) {
            try {
              rollMasterKey();
              lastMasterKeyUpdate = now;
            } catch (@OsUntrusted IOException e) {
              LOG.error("Master key updating failed: ", e);
            }
          }
          if (lastTokenCacheCleanup + tokenRemoverScanInterval < now) {
            removeExpiredToken();
            lastTokenCacheCleanup = now;
          }
          try {
            Thread.sleep(Math.min(5000, keyUpdateInterval)); // 5 seconds
          } catch (@OsUntrusted InterruptedException ie) {
            LOG
            .error("InterruptedExcpetion recieved for ExpiredTokenRemover thread "
                + ie);
          }
        }
      } catch (@OsUntrusted Throwable t) {
        LOG.error("ExpiredTokenRemover thread received unexpected exception. "
            + t);
        Runtime.getRuntime().exit(-1);
      }
    }
  }
}
