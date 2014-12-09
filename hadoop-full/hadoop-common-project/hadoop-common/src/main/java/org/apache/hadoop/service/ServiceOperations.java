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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * This class contains a set of methods to work with services, especially
 * to walk them through their lifecycle.
 */
@Public
@Evolving
public final class ServiceOperations {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(AbstractService.class);

  private @OsUntrusted ServiceOperations() {
  }

  /**
   * Stop a service.
   * <p/>Do nothing if the service is null or not
   * in a state in which it can be/needs to be stopped.
   * <p/>
   * The service state is checked <i>before</i> the operation begins.
   * This process is <i>not</i> thread safe.
   * @param service a service or null
   */
  public static void stop(@OsUntrusted Service service) {
    if (service != null) {
      service.stop();
    }
  }

  /**
   * Stop a service; if it is null do nothing. Exceptions are caught and
   * logged at warn level. (but not Throwables). This operation is intended to
   * be used in cleanup operations
   *
   * @param service a service; may be null
   * @return any exception that was caught; null if none was.
   */
  public static @OsUntrusted Exception stopQuietly(@OsUntrusted Service service) {
    return stopQuietly(LOG, service);
  }

  /**
   * Stop a service; if it is null do nothing. Exceptions are caught and
   * logged at warn level. (but not Throwables). This operation is intended to
   * be used in cleanup operations
   *
   * @param log the log to warn at
   * @param service a service; may be null
   * @return any exception that was caught; null if none was.
   * @see ServiceOperations#stopQuietly(Service)
   */
  public static @OsUntrusted Exception stopQuietly(@OsUntrusted Log log, @OsUntrusted Service service) {
    try {
      stop(service);
    } catch (@OsUntrusted Exception e) {
      log.warn("When stopping the service " + service.getName()
               + " : " + e,
               e);
      return e;
    }
    return null;
  }


  /**
   * Class to manage a list of {@link ServiceStateChangeListener} instances,
   * including a notification loop that is robust against changes to the list
   * during the notification process.
   */
  public static class ServiceListeners {
    /**
     * List of state change listeners; it is final to guarantee
     * that it will never be null.
     */
    private final @OsUntrusted List<@OsUntrusted ServiceStateChangeListener> listeners =
      new @OsUntrusted ArrayList<@OsUntrusted ServiceStateChangeListener>();

    /**
     * Thread-safe addition of a new listener to the end of a list.
     * Attempts to re-register a listener that is already registered
     * will be ignored.
     * @param l listener
     */
    public synchronized void add(ServiceOperations.@OsUntrusted ServiceListeners this, @OsUntrusted ServiceStateChangeListener l) {
      if(!listeners.contains(l)) {
        listeners.add(l);
      }
    }

    /**
     * Remove any registration of a listener from the listener list.
     * @param l listener
     * @return true if the listener was found (and then removed)
     */
    public synchronized @OsUntrusted boolean remove(ServiceOperations.@OsUntrusted ServiceListeners this, @OsUntrusted ServiceStateChangeListener l) {
      return listeners.remove(l);
    }

    /**
     * Reset the listener list
     */
    public synchronized void reset(ServiceOperations.@OsUntrusted ServiceListeners this) {
      listeners.clear();
    }

    /**
     * Change to a new state and notify all listeners.
     * This method will block until all notifications have been issued.
     * It caches the list of listeners before the notification begins,
     * so additions or removal of listeners will not be visible.
     * @param service the service that has changed state
     */
    public void notifyListeners(ServiceOperations.@OsUntrusted ServiceListeners this, @OsUntrusted Service service) {
      //take a very fast snapshot of the callback list
      //very much like CopyOnWriteArrayList, only more minimal
      @OsUntrusted
      ServiceStateChangeListener @OsUntrusted [] callbacks;
      synchronized (this) {
        callbacks = listeners.toArray(new @OsUntrusted ServiceStateChangeListener @OsUntrusted [listeners.size()]);
      }
      //iterate through the listeners outside the synchronized method,
      //ensuring that listener registration/unregistration doesn't break anything
      for (@OsUntrusted ServiceStateChangeListener l : callbacks) {
        l.stateChanged(service);
      }
    }
  }

}