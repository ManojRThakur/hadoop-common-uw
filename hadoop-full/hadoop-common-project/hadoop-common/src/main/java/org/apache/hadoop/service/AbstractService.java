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

package org.apache.hadoop.service;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

/**
 * This is the base implementation class for services.
 */
@Public
@Evolving
public abstract class AbstractService implements @OsUntrusted Service {

  private static final @OsUntrusted Log LOG = LogFactory.getLog(AbstractService.class);

  /**
   * Service name.
   */
  private final @OsUntrusted String name;

  /** service state */
  private final @OsUntrusted ServiceStateModel stateModel;

  /**
   * Service start time. Will be zero until the service is started.
   */
  private @OsUntrusted long startTime;

  /**
   * The configuration. Will be null until the service is initialized.
   */
  private volatile @OsUntrusted Configuration config;

  /**
   * List of state change listeners; it is final to ensure
   * that it will never be null.
   */
  private final ServiceOperations.@OsUntrusted ServiceListeners listeners
    = new ServiceOperations.@OsUntrusted ServiceListeners();
  /**
   * Static listeners to all events across all services
   */
  private static ServiceOperations.@OsUntrusted ServiceListeners globalListeners
    = new ServiceOperations.@OsUntrusted ServiceListeners();

  /**
   * The cause of any failure -will be null.
   * if a service did not stop due to a failure.
   */
  private @OsUntrusted Exception failureCause;

  /**
   * the state in which the service was when it failed.
   * Only valid when the service is stopped due to a failure
   */
  private @OsUntrusted STATE failureState = null;

  /**
   * object used to co-ordinate {@link #waitForServiceToStop(long)}
   * across threads.
   */
  private final @OsUntrusted AtomicBoolean terminationNotification =
    new @OsUntrusted AtomicBoolean(false);

  /**
   * History of lifecycle transitions
   */
  private final @OsUntrusted List<@OsUntrusted LifecycleEvent> lifecycleHistory
    = new @OsUntrusted ArrayList<@OsUntrusted LifecycleEvent>(5);

  /**
   * Map of blocking dependencies
   */
  private final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> blockerMap = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted String>();

  private final @OsUntrusted Object stateChangeLock = new @OsUntrusted Object();
 
  /**
   * Construct the service.
   * @param name service name
   */
  public @OsUntrusted AbstractService(@OsUntrusted String name) {
    this.name = name;
    stateModel = new @OsUntrusted ServiceStateModel(name);
  }

  @Override
  public final @OsUntrusted STATE getServiceState(@OsUntrusted AbstractService this) {
    return stateModel.getState();
  }

  @Override
  public final synchronized @OsUntrusted Throwable getFailureCause(@OsUntrusted AbstractService this) {
    return failureCause;
  }

  @Override
  public synchronized @OsUntrusted STATE getFailureState(@OsUntrusted AbstractService this) {
    return failureState;
  }

  /**
   * Set the configuration for this service.
   * This method is called during {@link #init(Configuration)}
   * and should only be needed if for some reason a service implementation
   * needs to override that initial setting -for example replacing
   * it with a new subclass of {@link Configuration}
   * @param conf new configuration.
   */
  protected void setConfig(@OsUntrusted AbstractService this, @OsUntrusted Configuration conf) {
    this.config = conf;
  }

  /**
   * {@inheritDoc}
   * This invokes {@link #serviceInit}
   * @param conf the configuration of the service. This must not be null
   * @throws ServiceStateException if the configuration was null,
   * the state change not permitted, or something else went wrong
   */
  @Override
  public void init(@OsUntrusted AbstractService this, @OsUntrusted Configuration conf) {
    if (conf == null) {
      throw new @OsUntrusted ServiceStateException("Cannot initialize service "
                                      + getName() + ": null configuration");
    }
    if (isInState(STATE.INITED)) {
      return;
    }
    synchronized (stateChangeLock) {
      if (enterState(STATE.INITED) != STATE.INITED) {
        setConfig(conf);
        try {
          serviceInit(config);
          if (isInState(STATE.INITED)) {
            //if the service ended up here during init,
            //notify the listeners
            notifyListeners();
          }
        } catch (@OsUntrusted Exception e) {
          noteFailure(e);
          ServiceOperations.stopQuietly(LOG, this);
          throw ServiceStateException.convert(e);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   * @throws ServiceStateException if the current service state does not permit
   * this action
   */
  @Override
  public void start(@OsUntrusted AbstractService this) {
    if (isInState(STATE.STARTED)) {
      return;
    }
    //enter the started state
    synchronized (stateChangeLock) {
      if (stateModel.enterState(STATE.STARTED) != STATE.STARTED) {
        try {
          startTime = System.currentTimeMillis();
          serviceStart();
          if (isInState(STATE.STARTED)) {
            //if the service started (and isn't now in a later state), notify
            if (LOG.isDebugEnabled()) {
              LOG.debug("Service " + getName() + " is started");
            }
            notifyListeners();
          }
        } catch (@OsUntrusted Exception e) {
          noteFailure(e);
          ServiceOperations.stopQuietly(LOG, this);
          throw ServiceStateException.convert(e);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop(@OsUntrusted AbstractService this) {
    if (isInState(STATE.STOPPED)) {
      return;
    }
    synchronized (stateChangeLock) {
      if (enterState(STATE.STOPPED) != STATE.STOPPED) {
        try {
          serviceStop();
        } catch (@OsUntrusted Exception e) {
          //stop-time exceptions are logged if they are the first one,
          noteFailure(e);
          throw ServiceStateException.convert(e);
        } finally {
          //report that the service has terminated
          terminationNotification.set(true);
          synchronized (terminationNotification) {
            terminationNotification.notifyAll();
          }
          //notify anything listening for events
          notifyListeners();
        }
      } else {
        //already stopped: note it
        if (LOG.isDebugEnabled()) {
          LOG.debug("Ignoring re-entrant call to stop()");
        }
      }
    }
  }

  /**
   * Relay to {@link #stop()}
   * @throws IOException
   */
  @Override
  public final void close(@OsUntrusted AbstractService this) throws IOException {
    stop();
  }

  /**
   * Failure handling: record the exception
   * that triggered it -if there was not one already.
   * Services are free to call this themselves.
   * @param exception the exception
   */
  protected final void noteFailure(@OsUntrusted AbstractService this, @OsUntrusted Exception exception) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("noteFailure " + exception, null);
    }
    if (exception == null) {
      //make sure failure logic doesn't itself cause problems
      return;
    }
    //record the failure details, and log it
    synchronized (this) {
      if (failureCause == null) {
        failureCause = exception;
        failureState = getServiceState();
        LOG.info("Service " + getName()
                 + " failed in state " + failureState
                 + "; cause: " + exception,
                 exception);
      }
    }
  }

  @Override
  public final @OsUntrusted boolean waitForServiceToStop(@OsUntrusted AbstractService this, @OsUntrusted long timeout) {
    @OsUntrusted
    boolean completed = terminationNotification.get();
    while (!completed) {
      try {
        synchronized(terminationNotification) {
          terminationNotification.wait(timeout);
        }
        // here there has been a timeout, the object has terminated,
        // or there has been a spurious wakeup (which we ignore)
        completed = true;
      } catch (@OsUntrusted InterruptedException e) {
        // interrupted; have another look at the flag
        completed = terminationNotification.get();
      }
    }
    return terminationNotification.get();
  }

  /* ===================================================================== */
  /* Override Points */
  /* ===================================================================== */

  /**
   * All initialization code needed by a service.
   *
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   *
   * Implementations do not need to be synchronized as the logic
   * in {@link #init(Configuration)} prevents re-entrancy.
   *
   * The base implementation checks to see if the subclass has created
   * a new configuration instance, and if so, updates the base class value
   * @param conf configuration
   * @throws Exception on a failure -these will be caught,
   * possibly wrapped, and wil; trigger a service stop
   */
  protected void serviceInit(@OsUntrusted AbstractService this, @OsUntrusted Configuration conf) throws Exception {
    if (conf != config) {
      LOG.debug("Config has been overridden during init");
      setConfig(conf);
    }
  }

  /**
   * Actions called during the INITED to STARTED transition.
   *
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   *
   * Implementations do not need to be synchronized as the logic
   * in {@link #start()} prevents re-entrancy.
   *
   * @throws Exception if needed -these will be caught,
   * wrapped, and trigger a service stop
   */
  protected void serviceStart(@OsUntrusted AbstractService this) throws Exception {

  }

  /**
   * Actions called during the transition to the STOPPED state.
   *
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   *
   * Implementations do not need to be synchronized as the logic
   * in {@link #stop()} prevents re-entrancy.
   *
   * Implementations MUST write this to be robust against failures, including
   * checks for null references -and for the first failure to not stop other
   * attempts to shut down parts of the service.
   *
   * @throws Exception if needed -these will be caught and logged.
   */
  protected void serviceStop(@OsUntrusted AbstractService this) throws Exception {

  }

  @Override
  public void registerServiceListener(@OsUntrusted AbstractService this, @OsUntrusted ServiceStateChangeListener l) {
    listeners.add(l);
  }

  @Override
  public void unregisterServiceListener(@OsUntrusted AbstractService this, @OsUntrusted ServiceStateChangeListener l) {
    listeners.remove(l);
  }

  /**
   * Register a global listener, which receives notifications
   * from the state change events of all services in the JVM
   * @param l listener
   */
  public static void registerGlobalListener(@OsUntrusted ServiceStateChangeListener l) {
    globalListeners.add(l);
  }

  /**
   * unregister a global listener.
   * @param l listener to unregister
   * @return true if the listener was found (and then deleted)
   */
  public static @OsUntrusted boolean unregisterGlobalListener(@OsUntrusted ServiceStateChangeListener l) {
    return globalListeners.remove(l);
  }

  /**
   * Package-scoped method for testing -resets the global listener list
   */
  @VisibleForTesting
  static void resetGlobalListeners() {
    globalListeners.reset();
  }

  @Override
  public @OsUntrusted String getName(@OsUntrusted AbstractService this) {
    return name;
  }

  @Override
  public synchronized @OsUntrusted Configuration getConfig(@OsUntrusted AbstractService this) {
    return config;
  }

  @Override
  public @OsUntrusted long getStartTime(@OsUntrusted AbstractService this) {
    return startTime;
  }

  /**
   * Notify local and global listeners of state changes.
   * Exceptions raised by listeners are NOT passed up.
   */
  private void notifyListeners(@OsUntrusted AbstractService this) {
    try {
      listeners.notifyListeners(this);
      globalListeners.notifyListeners(this);
    } catch (@OsUntrusted Throwable e) {
      LOG.warn("Exception while notifying listeners of " + this + ": " + e,
               e);
    }
  }

  /**
   * Add a state change event to the lifecycle history
   */
  private void recordLifecycleEvent(@OsUntrusted AbstractService this) {
    @OsUntrusted
    LifecycleEvent event = new @OsUntrusted LifecycleEvent();
    event.time = System.currentTimeMillis();
    event.state = getServiceState();
    lifecycleHistory.add(event);
  }

  @Override
  public synchronized @OsUntrusted List<@OsUntrusted LifecycleEvent> getLifecycleHistory(@OsUntrusted AbstractService this) {
    return new @OsUntrusted ArrayList<@OsUntrusted LifecycleEvent>(lifecycleHistory);
  }

  /**
   * Enter a state; record this via {@link #recordLifecycleEvent}
   * and log at the info level.
   * @param newState the proposed new state
   * @return the original state
   * it wasn't already in that state, and the state model permits state re-entrancy.
   */
  private @OsUntrusted STATE enterState(@OsUntrusted AbstractService this, @OsUntrusted STATE newState) {
    assert stateModel != null : "null state in " + name + " " + this.getClass();
    @OsUntrusted
    STATE oldState = stateModel.enterState(newState);
    if (oldState != newState) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Service: " + getName() + " entered state " + getServiceState());
      }
      recordLifecycleEvent();
    }
    return oldState;
  }

  @Override
  public final @OsUntrusted boolean isInState(@OsUntrusted AbstractService this, Service.@OsUntrusted STATE expected) {
    return stateModel.isInState(expected);
  }

  @Override
  public @OsUntrusted String toString(@OsUntrusted AbstractService this) {
    return "Service " + name + " in state " + stateModel;
  }

  /**
   * Put a blocker to the blocker map -replacing any
   * with the same name.
   * @param name blocker name
   * @param details any specifics on the block. This must be non-null.
   */
  protected void putBlocker(@OsUntrusted AbstractService this, @OsUntrusted String name, @OsUntrusted String details) {
    synchronized (blockerMap) {
      blockerMap.put(name, details);
    }
  }

  /**
   * Remove a blocker from the blocker map -
   * this is a no-op if the blocker is not present
   * @param name the name of the blocker
   */
  public void removeBlocker(@OsUntrusted AbstractService this, @OsUntrusted String name) {
    synchronized (blockerMap) {
      blockerMap.remove(name);
    }
  }

  @Override
  public @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> getBlockers(@OsUntrusted AbstractService this) {
    synchronized (blockerMap) {
      @OsUntrusted
      Map<@OsUntrusted String, @OsUntrusted String> map = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted String>(blockerMap);
      return map;
    }
  }
}
