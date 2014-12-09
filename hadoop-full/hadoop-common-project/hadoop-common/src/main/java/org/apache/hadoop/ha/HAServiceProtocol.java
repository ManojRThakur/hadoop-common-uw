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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.KerberosInfo;

import java.io.IOException;

/**
 * Protocol interface that provides High Availability related primitives to
 * monitor and fail-over the service.
 * 
 * This interface could be used by HA frameworks to manage the service.
 */
@KerberosInfo(
    serverPrincipal=CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY)
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface HAServiceProtocol {
  /**
   * Initial version of the protocol
   */
  public static final @OsUntrusted long versionID = 1L;

  /**
   * An HA service may be in active or standby state. During startup, it is in
   * an unknown INITIALIZING state. During shutdown, it is in the STOPPING state
   * and can no longer return to active/standby states.
   */
  public enum HAServiceState {

@OsUntrusted  INITIALIZING("initializing"),

@OsUntrusted  ACTIVE("active"),

@OsUntrusted  STANDBY("standby"),

@OsUntrusted  STOPPING("stopping");

    private @OsUntrusted String name;

    @OsUntrusted
    HAServiceState(@OsUntrusted String name) {
      this.name = name;
    }

    @Override
    public @OsUntrusted String toString(HAServiceProtocol.@OsUntrusted HAServiceState this) {
      return name;
    }
  }
  
  public static enum RequestSource {

@OsUntrusted  REQUEST_BY_USER,

@OsUntrusted  REQUEST_BY_USER_FORCED,

@OsUntrusted  REQUEST_BY_ZKFC;
  }
  
  /**
   * Information describing the source for a request to change state.
   * This is used to differentiate requests from automatic vs CLI
   * failover controllers, and in the future may include epoch
   * information.
   */
  public static class StateChangeRequestInfo {
    private final @OsUntrusted RequestSource source;

    public @OsUntrusted StateChangeRequestInfo(@OsUntrusted RequestSource source) {
      super();
      this.source = source;
    }

    public @OsUntrusted RequestSource getSource(HAServiceProtocol.@OsUntrusted StateChangeRequestInfo this) {
      return source;
    }
  }

  /**
   * Monitor the health of service. This periodically called by the HA
   * frameworks to monitor the health of the service.
   * 
   * Service is expected to perform checks to ensure it is functional.
   * If the service is not healthy due to failure or partial failure,
   * it is expected to throw {@link HealthCheckFailedException}.
   * The definition of service not healthy is left to the service.
   * 
   * Note that when health check of an Active service fails,
   * failover to standby may be done.
   * 
   * @throws HealthCheckFailedException
   *           if the health check of a service fails.
   * @throws AccessControlException
   *           if access is denied.
   * @throws IOException
   *           if other errors happen
   */
  @Idempotent
  public void monitorHealth(@OsUntrusted HAServiceProtocol this) throws HealthCheckFailedException,
                                     AccessControlException,
                                     IOException;

  /**
   * Request service to transition to active state. No operation, if the
   * service is already in active state.
   * 
   * @throws ServiceFailedException
   *           if transition from standby to active fails.
   * @throws AccessControlException
   *           if access is denied.
   * @throws IOException
   *           if other errors happen
   */
  @Idempotent
  public void transitionToActive(@OsUntrusted HAServiceProtocol this, @OsUntrusted StateChangeRequestInfo reqInfo)
                                   throws ServiceFailedException,
                                          AccessControlException,
                                          IOException;

  /**
   * Request service to transition to standby state. No operation, if the
   * service is already in standby state.
   * 
   * @throws ServiceFailedException
   *           if transition from active to standby fails.
   * @throws AccessControlException
   *           if access is denied.
   * @throws IOException
   *           if other errors happen
   */
  @Idempotent
  public void transitionToStandby(@OsUntrusted HAServiceProtocol this, @OsUntrusted StateChangeRequestInfo reqInfo)
                                    throws ServiceFailedException,
                                           AccessControlException,
                                           IOException;

  /**
   * Return the current status of the service. The status indicates
   * the current <em>state</em> (e.g ACTIVE/STANDBY) as well as
   * some additional information.
   * 
   * @throws AccessControlException
   *           if access is denied.
   * @throws IOException
   *           if other errors happen
   * @see HAServiceStatus
   */
  @Idempotent
  public @OsUntrusted HAServiceStatus getServiceStatus(@OsUntrusted HAServiceProtocol this) throws AccessControlException,
                                                   IOException;
}
