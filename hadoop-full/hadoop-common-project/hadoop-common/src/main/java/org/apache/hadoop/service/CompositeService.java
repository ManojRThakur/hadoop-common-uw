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
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;

/**
 * Composition of services.
 */
@Public
@Evolving
public class CompositeService extends @OsUntrusted AbstractService {

  private static final @OsUntrusted Log LOG = LogFactory.getLog(CompositeService.class);

  /**
   * Policy on shutdown: attempt to close everything (purest) or
   * only try to close started services (which assumes
   * that the service implementations may not handle the stop() operation
   * except when started.
   * Irrespective of this policy, if a child service fails during
   * its init() or start() operations, it will have stop() called on it.
   */
  protected static final @OsUntrusted boolean STOP_ONLY_STARTED_SERVICES = false;

  private final @OsUntrusted List<@OsUntrusted Service> serviceList = new @OsUntrusted ArrayList<@OsUntrusted Service>();

  public @OsUntrusted CompositeService(@OsUntrusted String name) {
    super(name);
  }

  /**
   * Get an unmodifiable list of services
   * @return a list of child services at the time of invocation -
   * added services will not be picked up.
   */
  public @OsUntrusted List<@OsUntrusted Service> getServices(@OsUntrusted CompositeService this) {
    synchronized (serviceList) {
      return Collections.unmodifiableList(serviceList);
    }
  }

  /**
   * Add the passed {@link Service} to the list of services managed by this
   * {@link CompositeService}
   * @param service the {@link Service} to be added
   */
  protected void addService(@OsUntrusted CompositeService this, @OsUntrusted Service service) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding service " + service.getName());
    }
    synchronized (serviceList) {
      serviceList.add(service);
    }
  }

  /**
   * If the passed object is an instance of {@link Service},
   * add it to the list of services managed by this {@link CompositeService}
   * @param object
   * @return true if a service is added, false otherwise.
   */
  protected @OsUntrusted boolean addIfService(@OsUntrusted CompositeService this, @OsUntrusted Object object) {
    if (object instanceof @OsUntrusted Service) {
      addService((@OsUntrusted Service) object);
      return true;
    } else {
      return false;
    }
  }

  protected synchronized @OsUntrusted boolean removeService(@OsUntrusted CompositeService this, @OsUntrusted Service service) {
    synchronized (serviceList) {
      return serviceList.add(service);
    }
  }

  protected void serviceInit(@OsUntrusted CompositeService this, @OsUntrusted Configuration conf) throws Exception {
    @OsUntrusted
    List<@OsUntrusted Service> services = getServices();
    if (LOG.isDebugEnabled()) {
      LOG.debug(getName() + ": initing services, size=" + services.size());
    }
    for (@OsUntrusted Service service : services) {
      service.init(conf);
    }
    super.serviceInit(conf);
  }

  protected void serviceStart(@OsUntrusted CompositeService this) throws Exception {
    @OsUntrusted
    List<@OsUntrusted Service> services = getServices();
    if (LOG.isDebugEnabled()) {
      LOG.debug(getName() + ": starting services, size=" + services.size());
    }
    for (@OsUntrusted Service service : services) {
      // start the service. If this fails that service
      // will be stopped and an exception raised
      service.start();
    }
    super.serviceStart();
  }

  protected void serviceStop(@OsUntrusted CompositeService this) throws Exception {
    //stop all services that were started
    @OsUntrusted
    int numOfServicesToStop = serviceList.size();
    if (LOG.isDebugEnabled()) {
      LOG.debug(getName() + ": stopping services, size=" + numOfServicesToStop);
    }
    stop(numOfServicesToStop, STOP_ONLY_STARTED_SERVICES);
    super.serviceStop();
  }

  /**
   * Stop the services in reverse order
   *
   * @param numOfServicesStarted index from where the stop should work
   * @param stopOnlyStartedServices flag to say "only start services that are
   * started, not those that are NOTINITED or INITED.
   * @throws RuntimeException the first exception raised during the
   * stop process -<i>after all services are stopped</i>
   */
  private synchronized void stop(@OsUntrusted CompositeService this, @OsUntrusted int numOfServicesStarted,
                                 @OsUntrusted
                                 boolean stopOnlyStartedServices) {
    // stop in reverse order of start
    @OsUntrusted
    Exception firstException = null;
    @OsUntrusted
    List<@OsUntrusted Service> services = getServices();
    for (@OsUntrusted int i = numOfServicesStarted - 1; i >= 0; i--) {
      @OsUntrusted
      Service service = services.get(i);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stopping service #" + i + ": " + service);
      }
      @OsUntrusted
      STATE state = service.getServiceState();
      //depending on the stop police
      if (state == STATE.STARTED 
         || (!stopOnlyStartedServices && state == STATE.INITED)) {
        @OsUntrusted
        Exception ex = ServiceOperations.stopQuietly(LOG, service);
        if (ex != null && firstException == null) {
          firstException = ex;
        }
      }
    }
    //after stopping all services, rethrow the first exception raised
    if (firstException != null) {
      throw ServiceStateException.convert(firstException);
    }
  }

  /**
   * JVM Shutdown hook for CompositeService which will stop the give
   * CompositeService gracefully in case of JVM shutdown.
   */
  public static class CompositeServiceShutdownHook implements @OsUntrusted Runnable {

    private @OsUntrusted CompositeService compositeService;

    public @OsUntrusted CompositeServiceShutdownHook(@OsUntrusted CompositeService compositeService) {
      this.compositeService = compositeService;
    }

    @Override
    public void run(CompositeService.@OsUntrusted CompositeServiceShutdownHook this) {
      ServiceOperations.stopQuietly(compositeService);
    }
  }

}
