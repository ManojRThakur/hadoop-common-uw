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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.fs.CommonConfigurationKeys.*;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.Daemon;

import com.google.common.base.Preconditions;

/**
 * This class is a daemon which runs in a loop, periodically heartbeating
 * with an HA service. It is responsible for keeping track of that service's
 * health and exposing callbacks to the failover controller when the health
 * status changes.
 * 
 * Classes which need callbacks should implement the {@link Callback}
 * interface.
 */
@InterfaceAudience.Private
public class HealthMonitor {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(
      HealthMonitor.class);

  private @OsUntrusted Daemon daemon;
  private @OsUntrusted long connectRetryInterval;
  private @OsUntrusted long checkIntervalMillis;
  private @OsUntrusted long sleepAfterDisconnectMillis;

  private @OsUntrusted int rpcTimeout;

  private volatile @OsUntrusted boolean shouldRun = true;

  /** The connected proxy */
  private @OsUntrusted HAServiceProtocol proxy;

  /** The HA service to monitor */
  private final @OsUntrusted HAServiceTarget targetToMonitor;

  private final @OsUntrusted Configuration conf;
  
  private @OsUntrusted State state = State.INITIALIZING;

  /**
   * Listeners for state changes
   */
  private @OsUntrusted List<@OsUntrusted Callback> callbacks = Collections.synchronizedList(
      new @OsUntrusted LinkedList<@OsUntrusted Callback>());

  private @OsUntrusted HAServiceStatus lastServiceState = new @OsUntrusted HAServiceStatus(
      HAServiceState.INITIALIZING);
  
  @InterfaceAudience.Private
  public enum State {
    /**
     * The health monitor is still starting up.
     */

@OsUntrusted  INITIALIZING,

    /**
     * The service is not responding to health check RPCs.
     */

@OsUntrusted  SERVICE_NOT_RESPONDING,

    /**
     * The service is connected and healthy.
     */

@OsUntrusted  SERVICE_HEALTHY,
    
    /**
     * The service is running but unhealthy.
     */

@OsUntrusted  SERVICE_UNHEALTHY,
    
    /**
     * The health monitor itself failed unrecoverably and can
     * no longer provide accurate information.
     */

@OsUntrusted  HEALTH_MONITOR_FAILED;
  }


  @OsUntrusted
  HealthMonitor(@OsUntrusted Configuration conf, @OsUntrusted HAServiceTarget target) {
    this.targetToMonitor = target;
    this.conf = conf;
    
    this.sleepAfterDisconnectMillis = conf.getLong(
        HA_HM_SLEEP_AFTER_DISCONNECT_KEY,
        HA_HM_SLEEP_AFTER_DISCONNECT_DEFAULT);
    this.checkIntervalMillis = conf.getLong(
        HA_HM_CHECK_INTERVAL_KEY,
        HA_HM_CHECK_INTERVAL_DEFAULT);
    this.connectRetryInterval = conf.getLong(
        HA_HM_CONNECT_RETRY_INTERVAL_KEY,
        HA_HM_CONNECT_RETRY_INTERVAL_DEFAULT);
    this.rpcTimeout = conf.getInt(
        HA_HM_RPC_TIMEOUT_KEY,
        HA_HM_RPC_TIMEOUT_DEFAULT);
    
    this.daemon = new @OsUntrusted MonitorDaemon();
  }
  
  public void addCallback(@OsUntrusted HealthMonitor this, @OsUntrusted Callback cb) {
    this.callbacks.add(cb);
  }
  
  public void removeCallback(@OsUntrusted HealthMonitor this, @OsUntrusted Callback cb) {
    callbacks.remove(cb);
  }
  
  public void shutdown(@OsUntrusted HealthMonitor this) {
    LOG.info("Stopping HealthMonitor thread");
    shouldRun = false;
    daemon.interrupt();
  }

  /**
   * @return the current proxy object to the underlying service.
   * Note that this may return null in the case that the service
   * is not responding. Also note that, even if the last indicated
   * state is healthy, the service may have gone down in the meantime.
   */
  public synchronized @OsUntrusted HAServiceProtocol getProxy(@OsUntrusted HealthMonitor this) {
    return proxy;
  }
  
  private void loopUntilConnected(@OsUntrusted HealthMonitor this) throws InterruptedException {
    tryConnect();
    while (proxy == null) {
      Thread.sleep(connectRetryInterval);
      tryConnect();
    }
    assert proxy != null;
  }

  private void tryConnect(@OsUntrusted HealthMonitor this) {
    Preconditions.checkState(proxy == null);
    
    try {
      synchronized (this) {
        proxy = createProxy();
      }
    } catch (@OsUntrusted IOException e) {
      LOG.warn("Could not connect to local service at " + targetToMonitor +
          ": " + e.getMessage());
      proxy = null;
      enterState(State.SERVICE_NOT_RESPONDING);
    }
  }
  
  /**
   * Connect to the service to be monitored. Stubbed out for easier testing.
   */
  protected @OsUntrusted HAServiceProtocol createProxy(@OsUntrusted HealthMonitor this) throws IOException {
    return targetToMonitor.getProxy(conf, rpcTimeout);
  }

  private void doHealthChecks(@OsUntrusted HealthMonitor this) throws InterruptedException {
    while (shouldRun) {
      @OsUntrusted
      HAServiceStatus status = null;
      @OsUntrusted
      boolean healthy = false;
      try {
        status = proxy.getServiceStatus();
        proxy.monitorHealth();
        healthy = true;
      } catch (@OsUntrusted HealthCheckFailedException e) {
        LOG.warn("Service health check failed for " + targetToMonitor
            + ": " + e.getMessage());
        enterState(State.SERVICE_UNHEALTHY);
      } catch (@OsUntrusted Throwable t) {
        LOG.warn("Transport-level exception trying to monitor health of " +
            targetToMonitor + ": " + t.getLocalizedMessage());
        RPC.stopProxy(proxy);
        proxy = null;
        enterState(State.SERVICE_NOT_RESPONDING);
        Thread.sleep(sleepAfterDisconnectMillis);
        return;
      }
      
      if (status != null) {
        setLastServiceStatus(status);
      }
      if (healthy) {
        enterState(State.SERVICE_HEALTHY);
      }

      Thread.sleep(checkIntervalMillis);
    }
  }
  
  private synchronized void setLastServiceStatus(@OsUntrusted HealthMonitor this, @OsUntrusted HAServiceStatus status) {
    this.lastServiceState = status;
  }

  private synchronized void enterState(@OsUntrusted HealthMonitor this, @OsUntrusted State newState) {
    if (newState != state) {
      LOG.info("Entering state " + newState);
      state = newState;
      synchronized (callbacks) {
        for (@OsUntrusted Callback cb : callbacks) {
          cb.enteredState(newState);
        }
      }
    }
  }

  synchronized @OsUntrusted State getHealthState(@OsUntrusted HealthMonitor this) {
    return state;
  }
  
  synchronized @OsUntrusted HAServiceStatus getLastServiceStatus(@OsUntrusted HealthMonitor this) {
    return lastServiceState;
  }
  
  @OsUntrusted
  boolean isAlive(@OsUntrusted HealthMonitor this) {
    return daemon.isAlive();
  }

  void join(@OsUntrusted HealthMonitor this) throws InterruptedException {
    daemon.join();
  }

  void start(@OsUntrusted HealthMonitor this) {
    daemon.start();
  }

  private class MonitorDaemon extends @OsUntrusted Daemon {
    private @OsUntrusted MonitorDaemon() {
      super();
      setName("Health Monitor for " + targetToMonitor);
      setUncaughtExceptionHandler(new  @OsUntrusted  UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(@OsUntrusted Thread t, @OsUntrusted Throwable e) {
          LOG.fatal("Health monitor failed", e);
          enterState(HealthMonitor.State.HEALTH_MONITOR_FAILED);
        }
      });
    }
    
    @Override
    public void run(@OsUntrusted HealthMonitor.MonitorDaemon this) {
      while (shouldRun) {
        try { 
          loopUntilConnected();
          doHealthChecks();
        } catch (@OsUntrusted InterruptedException ie) {
          Preconditions.checkState(!shouldRun,
              "Interrupted but still supposed to run");
        }
      }
    }
  }
  
  /**
   * Callback interface for state change events.
   * 
   * This interface is called from a single thread which also performs
   * the health monitoring. If the callback processing takes a long time,
   * no further health checks will be made during this period, nor will
   * other registered callbacks be called.
   * 
   * If the callback itself throws an unchecked exception, no other
   * callbacks following it will be called, and the health monitor
   * will terminate, entering HEALTH_MONITOR_FAILED state.
   */
  static interface Callback {
    void enteredState(HealthMonitor.@OsUntrusted Callback this, @OsUntrusted State newState);
  }
}
