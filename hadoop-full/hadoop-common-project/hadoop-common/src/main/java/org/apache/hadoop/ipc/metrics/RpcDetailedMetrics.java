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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRates;

/**
 * This class is for maintaining RPC method related statistics
 * and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@Metrics(about="Per method RPC metrics", context="rpcdetailed")
public class RpcDetailedMetrics {

  @Metric @OsUntrusted MutableRates rates;

  static final @OsUntrusted Log LOG = LogFactory.getLog(RpcDetailedMetrics.class);
  final @OsUntrusted MetricsRegistry registry;
  final @OsUntrusted String name;

  @OsUntrusted
  RpcDetailedMetrics(@OsUntrusted int port) {
    name = "RpcDetailedActivityForPort"+ port;
    registry = new @OsUntrusted MetricsRegistry("rpcdetailed")
        .tag("port", "RPC port", String.valueOf(port));
    LOG.debug(registry.info());
  }

  public @OsUntrusted String name(@OsUntrusted RpcDetailedMetrics this) { return name; }

  public static @OsUntrusted RpcDetailedMetrics create(@OsUntrusted int port) {
    @OsUntrusted
    RpcDetailedMetrics m = new @OsUntrusted RpcDetailedMetrics(port);
    return DefaultMetricsSystem.instance().register(m.name, null, m);
  }

  /**
   * Initialize the metrics for JMX with protocol methods
   * @param protocol the protocol class
   */
  public void init(@OsUntrusted RpcDetailedMetrics this, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protocol) {
    rates.init(protocol);
  }

  /**
   * Add an RPC processing time sample
   * @param name  of the RPC call
   * @param processingTime  the processing time
   */
  //@Override // some instrumentation interface
  public void addProcessingTime(@OsUntrusted RpcDetailedMetrics this, @OsUntrusted String name, @OsUntrusted int processingTime) {
    rates.add(name, processingTime);
  }

  /**
   * Shutdown the instrumentation for the process
   */
  //@Override // some instrumentation interface
  public void shutdown(@OsUntrusted RpcDetailedMetrics this) {}
}
