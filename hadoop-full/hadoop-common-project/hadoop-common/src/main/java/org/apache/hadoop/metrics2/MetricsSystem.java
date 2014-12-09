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

package org.apache.hadoop.metrics2;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The metrics system interface.
 * 
 * The following components are used for metrics.
 * <ul>
 * <li>{@link MetricsSource} generate and update metrics information.</li>
 * <li>{@link MetricsSink} consume the metrics information</li>
 * </ul>
 * 
 * {@link MetricsSource} and {@link MetricsSink} register with the metrics
 * system. Implementations of {@link MetricsSystem} polls the
 * {@link MetricsSource}s periodically and pass the {@link MetricsRecord}s to
 * {@link MetricsSink}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class MetricsSystem implements @OsUntrusted MetricsSystemMXBean {

  @InterfaceAudience.Private
  public abstract @OsUntrusted MetricsSystem init(@OsUntrusted MetricsSystem this, @OsUntrusted String prefix);

  /**
   * Register a metrics source
   * @param <T>   the actual type of the source object
   * @param source object to register
   * @param name  of the source. Must be unique or null (then extracted from
   *              the annotations of the source object.)
   * @param desc  the description of the source (or null. See above.)
   * @return the source object
   * @exception MetricsException
   */
  public abstract <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted T register(@OsUntrusted MetricsSystem this, @OsUntrusted String name, @OsUntrusted String desc, @OsUntrusted T source);

  /**
   * Register a metrics source (deriving name and description from the object)
   * @param <T>   the actual type of the source object
   * @param source  object to register
   * @return  the source object
   * @exception MetricsException
   */
  public <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted T register(@OsUntrusted MetricsSystem this, @OsUntrusted T source) {
    return register(null, null, source);
  }

  /**
   * @param name  of the metrics source
   * @return the metrics source (potentially wrapped) object
   */
  @InterfaceAudience.Private
  public abstract @OsUntrusted MetricsSource getSource(@OsUntrusted MetricsSystem this, @OsUntrusted String name);

  /**
   * Register a metrics sink
   * @param <T>   the type of the sink
   * @param sink  to register
   * @param name  of the sink. Must be unique.
   * @param desc  the description of the sink
   * @return the sink
   * @exception MetricsException
   */
  public abstract <@OsUntrusted T extends @OsUntrusted MetricsSink>
  @OsUntrusted
  T register(@OsUntrusted MetricsSystem this, @OsUntrusted String name, @OsUntrusted String desc, @OsUntrusted T sink);

  /**
   * Register a callback interface for JMX events
   * @param callback  the callback object implementing the MBean interface.
   */
  public abstract void register(@OsUntrusted MetricsSystem this, @OsUntrusted Callback callback);

  /**
   * Requests an immediate publish of all metrics from sources to sinks.
   * 
   * This is a "soft" request: the expectation is that a best effort will be
   * done to synchronously snapshot the metrics from all the sources and put
   * them in all the sinks (including flushing the sinks) before returning to
   * the caller. If this can't be accomplished in reasonable time it's OK to
   * return to the caller before everything is done. 
   */
  public abstract void publishMetricsNow(@OsUntrusted MetricsSystem this);

  /**
   * Shutdown the metrics system completely (usually during server shutdown.)
   * The MetricsSystemMXBean will be unregistered.
   * @return true if shutdown completed
   */
  public abstract @OsUntrusted boolean shutdown(@OsUntrusted MetricsSystem this);

  /**
   * The metrics system callback interface (needed for proxies.)
   */
  public interface Callback {
    /**
     * Called before start()
     */
    void preStart(MetricsSystem.@OsUntrusted Callback this);

    /**
     * Called after start()
     */
    void postStart(MetricsSystem.@OsUntrusted Callback this);

    /**
     * Called before stop()
     */
    void preStop(MetricsSystem.@OsUntrusted Callback this);

    /**
     * Called after stop()
     */
    void postStop(MetricsSystem.@OsUntrusted Callback this);
  }

  /**
   * Convenient abstract class for implementing callback interface
   */
  public static abstract class AbstractCallback implements @OsUntrusted Callback {
    @Override public void preStart(MetricsSystem.@OsUntrusted AbstractCallback this) {}
    @Override public void postStart(MetricsSystem.@OsUntrusted AbstractCallback this) {}
    @Override public void preStop(MetricsSystem.@OsUntrusted AbstractCallback this) {}
    @Override public void postStop(MetricsSystem.@OsUntrusted AbstractCallback this) {}
  }
}
