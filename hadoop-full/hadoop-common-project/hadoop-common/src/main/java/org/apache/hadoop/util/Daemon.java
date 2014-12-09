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

package org.apache.hadoop.util;

import ostrusted.quals.OsUntrusted;
import java.util.concurrent.ThreadFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** A thread that has called {@link Thread#setDaemon(boolean) } with true.*/
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class Daemon extends @OsUntrusted Thread {

  {
    setDaemon(true);                              // always a daemon
  }

  /**
   * Provide a factory for named daemon threads,
   * for use in ExecutorServices constructors
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  public static class DaemonFactory extends @OsUntrusted Daemon implements @OsUntrusted ThreadFactory {

    @Override
    public @OsUntrusted Thread newThread(Daemon.@OsUntrusted DaemonFactory this, @OsUntrusted Runnable runnable) {
      return new @OsUntrusted Daemon(runnable);
    }

  }

  @OsUntrusted
  Runnable runnable = null;
  /** Construct a daemon thread. */
  public @OsUntrusted Daemon() {
    super();
  }

  /** Construct a daemon thread. */
  public @OsUntrusted Daemon(@OsUntrusted Runnable runnable) {
    super(runnable);
    this.runnable = runnable;
    this.setName(((@OsUntrusted Object)runnable).toString());
  }

  /** Construct a daemon thread to be part of a specified thread group. */
  public @OsUntrusted Daemon(@OsUntrusted ThreadGroup group, @OsUntrusted Runnable runnable) {
    super(group, runnable);
    this.runnable = runnable;
    this.setName(((@OsUntrusted Object)runnable).toString());
  }

  public @OsUntrusted Runnable getRunnable(@OsUntrusted Daemon this) {
    return runnable;
  }
}
