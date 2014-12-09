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
package org.apache.hadoop.ha;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ipc.RPC;

import com.google.common.base.Preconditions;

/**
 * The FailOverController is responsible for electing an active service
 * on startup or when the current active is changing (eg due to failure),
 * monitoring the health of a service, and performing a fail-over when a
 * new active service is either manually selected by a user or elected.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FailoverController {

  private static final @OsUntrusted Log LOG = LogFactory.getLog(FailoverController.class);

  private final @OsUntrusted int gracefulFenceTimeout;
  private final @OsUntrusted int rpcTimeoutToNewActive;
  
  private final @OsUntrusted Configuration conf;
  /*
   * Need a copy of conf for graceful fence to set 
   * configurable retries for IPC client.
   * Refer HDFS-3561
   */
  private final @OsUntrusted Configuration gracefulFenceConf;

  private final @OsUntrusted RequestSource requestSource;
  
  public @OsUntrusted FailoverController(@OsUntrusted Configuration conf,
      @OsUntrusted
      RequestSource source) {
    this.conf = conf;
    this.gracefulFenceConf = new @OsUntrusted Configuration(conf);
    this.requestSource = source;
    
    this.gracefulFenceTimeout = getGracefulFenceTimeout(conf);
    this.rpcTimeoutToNewActive = getRpcTimeoutToNewActive(conf);
    
    //Configure less retries for graceful fence 
    @OsUntrusted
    int gracefulFenceConnectRetries = conf.getInt(
        CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES,
        CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES_DEFAULT);
    gracefulFenceConf.setInt(
        CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        gracefulFenceConnectRetries);
    gracefulFenceConf.setInt(
        CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        gracefulFenceConnectRetries);
  }

  static @OsUntrusted int getGracefulFenceTimeout(@OsUntrusted Configuration conf) {
    return conf.getInt(
        CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_TIMEOUT_KEY,
        CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_TIMEOUT_DEFAULT);
  }
  
  static @OsUntrusted int getRpcTimeoutToNewActive(@OsUntrusted Configuration conf) {
    return conf.getInt(
        CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_KEY,
        CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_DEFAULT);
  }
  
  /**
   * Perform pre-failover checks on the given service we plan to
   * failover to, eg to prevent failing over to a service (eg due
   * to it being inaccessible, already active, not healthy, etc).
   *
   * An option to ignore toSvc if it claims it is not ready to
   * become active is provided in case performing a failover will
   * allow it to become active, eg because it triggers a log roll
   * so the standby can learn about new blocks and leave safemode.
   *
   * @param from currently active service
   * @param target service to make active
   * @param forceActive ignore toSvc if it reports that it is not ready
   * @throws FailoverFailedException if we should avoid failover
   */
  private void preFailoverChecks(@OsUntrusted FailoverController this, @OsUntrusted HAServiceTarget from,
                                 @OsUntrusted
                                 HAServiceTarget target,
                                 @OsUntrusted
                                 boolean forceActive)
      throws FailoverFailedException {
    @OsUntrusted
    HAServiceStatus toSvcStatus;
    @OsUntrusted
    HAServiceProtocol toSvc;

    if (from.getAddress().equals(target.getAddress())) {
      throw new @OsUntrusted FailoverFailedException(
          "Can't failover a service to itself");
    }

    try {
      toSvc = target.getProxy(conf, rpcTimeoutToNewActive);
      toSvcStatus = toSvc.getServiceStatus();
    } catch (@OsUntrusted IOException e) {
      @OsUntrusted
      String msg = "Unable to get service state for " + target;
      LOG.error(msg + ": " + e.getLocalizedMessage());
      throw new @OsUntrusted FailoverFailedException(msg, e);
    }

    if (!toSvcStatus.getState().equals(HAServiceState.STANDBY)) {
      throw new @OsUntrusted FailoverFailedException(
          "Can't failover to an active service");
    }
    
    if (!toSvcStatus.isReadyToBecomeActive()) {
      @OsUntrusted
      String notReadyReason = toSvcStatus.getNotReadyReason();
      if (!forceActive) {
        throw new @OsUntrusted FailoverFailedException(
            target + " is not ready to become active: " +
            notReadyReason);
      } else {
        LOG.warn("Service is not ready to become active, but forcing: " +
            notReadyReason);
      }
    }

    try {
      HAServiceProtocolHelper.monitorHealth(toSvc, createReqInfo());
    } catch (@OsUntrusted HealthCheckFailedException hce) {
      throw new @OsUntrusted FailoverFailedException(
          "Can't failover to an unhealthy service", hce);
    } catch (@OsUntrusted IOException e) {
      throw new @OsUntrusted FailoverFailedException(
          "Got an IO exception", e);
    }
  }
  
  private @OsUntrusted StateChangeRequestInfo createReqInfo(@OsUntrusted FailoverController this) {
    return new @OsUntrusted StateChangeRequestInfo(requestSource);
  }

  /**
   * Try to get the HA state of the node at the given address. This
   * function is guaranteed to be "quick" -- ie it has a short timeout
   * and no retries. Its only purpose is to avoid fencing a node that
   * has already restarted.
   */
  @OsUntrusted
  boolean tryGracefulFence(@OsUntrusted FailoverController this, @OsUntrusted HAServiceTarget svc) {
    @OsUntrusted
    HAServiceProtocol proxy = null;
    try {
      proxy = svc.getProxy(gracefulFenceConf, gracefulFenceTimeout);
      proxy.transitionToStandby(createReqInfo());
      return true;
    } catch (@OsUntrusted ServiceFailedException sfe) {
      LOG.warn("Unable to gracefully make " + svc + " standby (" +
          sfe.getMessage() + ")");
    } catch (@OsUntrusted IOException ioe) {
      LOG.warn("Unable to gracefully make " + svc +
          " standby (unable to connect)", ioe);
    } finally {
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
    }
    return false;
  }
  
  /**
   * Failover from service 1 to service 2. If the failover fails
   * then try to failback.
   *
   * @param fromSvc currently active service
   * @param toSvc service to make active
   * @param forceFence to fence fromSvc even if not strictly necessary
   * @param forceActive try to make toSvc active even if it is not ready
   * @throws FailoverFailedException if the failover fails
   */
  public void failover(@OsUntrusted FailoverController this, @OsUntrusted HAServiceTarget fromSvc,
                       @OsUntrusted
                       HAServiceTarget toSvc,
                       @OsUntrusted
                       boolean forceFence,
                       @OsUntrusted
                       boolean forceActive)
      throws FailoverFailedException {
    Preconditions.checkArgument(fromSvc.getFencer() != null,
        "failover requires a fencer");
    preFailoverChecks(fromSvc, toSvc, forceActive);

    // Try to make fromSvc standby
    @OsUntrusted
    boolean tryFence = true;
    
    if (tryGracefulFence(fromSvc)) {
      tryFence = forceFence;
    }

    // Fence fromSvc if it's required or forced by the user
    if (tryFence) {
      if (!fromSvc.getFencer().fence(fromSvc)) {
        throw new @OsUntrusted FailoverFailedException("Unable to fence " +
            fromSvc + ". Fencing failed.");
      }
    }

    // Try to make toSvc active
    @OsUntrusted
    boolean failed = false;
    @OsUntrusted
    Throwable cause = null;
    try {
      HAServiceProtocolHelper.transitionToActive(
          toSvc.getProxy(conf, rpcTimeoutToNewActive),
          createReqInfo());
    } catch (@OsUntrusted ServiceFailedException sfe) {
      LOG.error("Unable to make " + toSvc + " active (" +
          sfe.getMessage() + "). Failing back.");
      failed = true;
      cause = sfe;
    } catch (@OsUntrusted IOException ioe) {
      LOG.error("Unable to make " + toSvc +
          " active (unable to connect). Failing back.", ioe);
      failed = true;
      cause = ioe;
    }

    // We failed to make toSvc active
    if (failed) {
      @OsUntrusted
      String msg = "Unable to failover to " + toSvc;
      // Only try to failback if we didn't fence fromSvc
      if (!tryFence) {
        try {
          // Unconditionally fence toSvc in case it is still trying to
          // become active, eg we timed out waiting for its response.
          // Unconditionally force fromSvc to become active since it
          // was previously active when we initiated failover.
          failover(toSvc, fromSvc, true, true);
        } catch (@OsUntrusted FailoverFailedException ffe) {
          msg += ". Failback to " + fromSvc +
            " failed (" + ffe.getMessage() + ")";
          LOG.fatal(msg);
        }
      }
      throw new @OsUntrusted FailoverFailedException(msg, cause);
    }
  }
}
