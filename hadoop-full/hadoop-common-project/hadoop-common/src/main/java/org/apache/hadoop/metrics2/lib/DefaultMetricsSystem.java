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

package org.apache.hadoop.metrics2.lib;

import ostrusted.quals.OsUntrusted;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.ObjectName;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;

import com.google.common.annotations.VisibleForTesting;

/**
 * The default metrics system singleton. This class is used by all the daemon
 * processes(such as NameNode, DataNode, JobTracker etc.). During daemon process
 * initialization the processes call {@link DefaultMetricsSystem#init(String)}
 * to initialize the {@link MetricsSystem}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum DefaultMetricsSystem {

@OsUntrusted  INSTANCE; // the singleton

  private @OsUntrusted AtomicReference<@OsUntrusted MetricsSystem> impl =
      new @OsUntrusted AtomicReference<@OsUntrusted MetricsSystem>(new @OsUntrusted MetricsSystemImpl());
  
  @VisibleForTesting
  volatile @OsUntrusted boolean miniClusterMode = false;
  
  transient final @OsUntrusted UniqueNames mBeanNames = new @OsUntrusted UniqueNames();
  transient final @OsUntrusted UniqueNames sourceNames = new @OsUntrusted UniqueNames();

  /**
   * Convenience method to initialize the metrics system
   * @param prefix  for the metrics system configuration
   * @return the metrics system instance
   */
  public static @OsUntrusted MetricsSystem initialize(@OsUntrusted String prefix) {
    return INSTANCE.init(prefix);
  }

  @OsUntrusted
  MetricsSystem init(@OsUntrusted DefaultMetricsSystem this, @OsUntrusted String prefix) {
    return impl.get().init(prefix);
  }

  /**
   * @return the metrics system object
   */
  public static @OsUntrusted MetricsSystem instance() {
    return INSTANCE.getImpl();
  }

  /**
   * Shutdown the metrics system
   */
  public static void shutdown() {
    INSTANCE.shutdownInstance();
  }

  void shutdownInstance(@OsUntrusted DefaultMetricsSystem this) {
    @OsUntrusted
    boolean last = impl.get().shutdown();
    if (last) synchronized(this) {
      mBeanNames.map.clear();
      sourceNames.map.clear();
    }
  }

  @InterfaceAudience.Private
  public static @OsUntrusted MetricsSystem setInstance(@OsUntrusted MetricsSystem ms) {
    return INSTANCE.setImpl(ms);
  }

  @OsUntrusted
  MetricsSystem setImpl(@OsUntrusted DefaultMetricsSystem this, @OsUntrusted MetricsSystem ms) {
    return impl.getAndSet(ms);
  }

  @OsUntrusted
  MetricsSystem getImpl(@OsUntrusted DefaultMetricsSystem this) { return impl.get(); }

  @VisibleForTesting
  public static void setMiniClusterMode(@OsUntrusted boolean choice) {
    INSTANCE.miniClusterMode = choice;
  }

  @VisibleForTesting
  public static @OsUntrusted boolean inMiniClusterMode() {
    return INSTANCE.miniClusterMode;
  }

  @InterfaceAudience.Private
  public static @OsUntrusted ObjectName newMBeanName(@OsUntrusted String name) {
    return INSTANCE.newObjectName(name);
  }

  @InterfaceAudience.Private
  public static @OsUntrusted String sourceName(@OsUntrusted String name, @OsUntrusted boolean dupOK) {
    return INSTANCE.newSourceName(name, dupOK);
  }

  synchronized @OsUntrusted ObjectName newObjectName(@OsUntrusted DefaultMetricsSystem this, @OsUntrusted String name) {
    try {
      if (mBeanNames.map.containsKey(name) && !miniClusterMode) {
        throw new @OsUntrusted MetricsException(name +" already exists!");
      }
      return new @OsUntrusted ObjectName(mBeanNames.uniqueName(name));
    } catch (@OsUntrusted Exception e) {
      throw new @OsUntrusted MetricsException(e);
    }
  }

  synchronized @OsUntrusted String newSourceName(@OsUntrusted DefaultMetricsSystem this, @OsUntrusted String name, @OsUntrusted boolean dupOK) {
    if (sourceNames.map.containsKey(name)) {
      if (dupOK) {
        return name;
      } else if (!miniClusterMode) {
        throw new @OsUntrusted MetricsException("Metrics source "+ name +" already exists!");
      }
    }
    return sourceNames.uniqueName(name);
  }
}
