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
package org.apache.hadoop.ipc.metrics;

import ostrusted.quals.OsUntrusted;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * This class is for maintaining  the various RPC statistics
 * and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@Metrics(about="Aggregate RPC metrics", context="rpc")
public class RpcMetrics {

  static final @OsUntrusted Log LOG = LogFactory.getLog(RpcMetrics.class);
  final @OsUntrusted Server server;
  final @OsUntrusted MetricsRegistry registry;
  final @OsUntrusted String name;
  
  @OsUntrusted
  RpcMetrics(@OsUntrusted Server server) {
    @OsUntrusted
    String port = String.valueOf(server.getListenerAddress().getPort());
    name = "RpcActivityForPort"+ port;
    this.server = server;
    registry = new @OsUntrusted MetricsRegistry("rpc").tag("port", "RPC port", port);
    LOG.debug("Initialized "+ registry);
  }

  public @OsUntrusted String name(@OsUntrusted RpcMetrics this) { return name; }

  public static @OsUntrusted RpcMetrics create(@OsUntrusted Server server) {
    @OsUntrusted
    RpcMetrics m = new @OsUntrusted RpcMetrics(server);
    return DefaultMetricsSystem.instance().register(m.name, null, m);
  }

  @Metric("Number of received bytes") @OsUntrusted MutableCounterLong receivedBytes;
  @Metric("Number of sent bytes") @OsUntrusted MutableCounterLong sentBytes;
  @Metric("Queue time") @OsUntrusted MutableRate rpcQueueTime;
  @Metric("Processsing time") @OsUntrusted MutableRate rpcProcessingTime;
  @Metric("Number of authentication failures")
  @OsUntrusted
  MutableCounterInt rpcAuthenticationFailures;
  @Metric("Number of authentication successes")
  @OsUntrusted
  MutableCounterInt rpcAuthenticationSuccesses;
  @Metric("Number of authorization failures")
  @OsUntrusted
  MutableCounterInt rpcAuthorizationFailures;
  @Metric("Number of authorization sucesses")
  @OsUntrusted
  MutableCounterInt rpcAuthorizationSuccesses;

  @Metric("Number of open connections") public @OsUntrusted int numOpenConnections(@OsUntrusted RpcMetrics this) {
    return server.getNumOpenConnections();
  }

  @Metric("Length of the call queue") public @OsUntrusted int callQueueLength(@OsUntrusted RpcMetrics this) {
    return server.getCallQueueLen();
  }

  // Public instrumentation methods that could be extracted to an
  // abstract class if we decide to do custom instrumentation classes a la
  // JobTrackerInstrumenation. The methods with //@Override comment are
  // candidates for abstract methods in a abstract instrumentation class.

  /**
   * One authentication failure event
   */
  //@Override
  public void incrAuthenticationFailures(@OsUntrusted RpcMetrics this) {
    rpcAuthenticationFailures.incr();
  }

  /**
   * One authentication success event
   */
  //@Override
  public void incrAuthenticationSuccesses(@OsUntrusted RpcMetrics this) {
    rpcAuthenticationSuccesses.incr();
  }

  /**
   * One authorization success event
   */
  //@Override
  public void incrAuthorizationSuccesses(@OsUntrusted RpcMetrics this) {
    rpcAuthorizationSuccesses.incr();
  }

  /**
   * One authorization failure event
   */
  //@Override
  public void incrAuthorizationFailures(@OsUntrusted RpcMetrics this) {
    rpcAuthorizationFailures.incr();
  }

  /**
   * Shutdown the instrumentation for the process
   */
  //@Override
  public void shutdown(@OsUntrusted RpcMetrics this) {}

  /**
   * Increment sent bytes by count
   * @param count to increment
   */
  //@Override
  public void incrSentBytes(@OsUntrusted RpcMetrics this, @OsUntrusted int count) {
    sentBytes.incr(count);
  }

  /**
   * Increment received bytes by count
   * @param count to increment
   */
  //@Override
  public void incrReceivedBytes(@OsUntrusted RpcMetrics this, @OsUntrusted int count) {
    receivedBytes.incr(count);
  }

  /**
   * Add an RPC queue time sample
   * @param qTime the queue time
   */
  //@Override
  public void addRpcQueueTime(@OsUntrusted RpcMetrics this, @OsUntrusted int qTime) {
    rpcQueueTime.add(qTime);
  }

  /**
   * Add an RPC processing time sample
   * @param processingTime the processing time
   */
  //@Override
  public void addRpcProcessingTime(@OsUntrusted RpcMetrics this, @OsUntrusted int processingTime) {
    rpcProcessingTime.add(processingTime);
  }
}
