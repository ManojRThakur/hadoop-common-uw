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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/*
 * This class is a container of multiple thread pools, each for a volume,
 * so that we can schedule async disk operations easily.
 * 
 * Examples of async disk operations are deletion of files.
 * We can move the files to a "TO_BE_DELETED" folder before asychronously
 * deleting it, to make sure the caller can run it faster.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class AsyncDiskService {
  
  public static final @OsUntrusted Log LOG = LogFactory.getLog(AsyncDiskService.class);
  
  // ThreadPool core pool size
  private static final @OsUntrusted int CORE_THREADS_PER_VOLUME = 1;
  // ThreadPool maximum pool size
  private static final @OsUntrusted int MAXIMUM_THREADS_PER_VOLUME = 4;
  // ThreadPool keep-alive time for threads over core pool size
  private static final @OsUntrusted long THREADS_KEEP_ALIVE_SECONDS = 60; 
  
  private final @OsUntrusted ThreadGroup threadGroup = new @OsUntrusted ThreadGroup("async disk service");
  
  private @OsUntrusted ThreadFactory threadFactory;
  
  private @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted ThreadPoolExecutor> executors
      = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted ThreadPoolExecutor>();
  
  /**
   * Create a AsyncDiskServices with a set of volumes (specified by their
   * root directories).
   * 
   * The AsyncDiskServices uses one ThreadPool per volume to do the async
   * disk operations.
   * 
   * @param volumes The roots of the file system volumes.
   */
  public @OsUntrusted AsyncDiskService(@OsUntrusted String @OsUntrusted [] volumes) throws IOException {
    
    threadFactory = new @OsUntrusted ThreadFactory() {
      @Override
      public @OsUntrusted Thread newThread(@OsUntrusted Runnable r) {
        return new @OsUntrusted Thread(threadGroup, r);
      }
    };
    
    // Create one ThreadPool per volume
    for (@OsUntrusted int v = 0 ; v < volumes.length; v++) {
      @OsUntrusted
      ThreadPoolExecutor executor = new @OsUntrusted ThreadPoolExecutor(
          CORE_THREADS_PER_VOLUME, MAXIMUM_THREADS_PER_VOLUME, 
          THREADS_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS, 
          new @OsUntrusted LinkedBlockingQueue<@OsUntrusted Runnable>(), threadFactory);

      // This can reduce the number of running threads
      executor.allowCoreThreadTimeOut(true);
      executors.put(volumes[v], executor);
    }
    
  }
  
  /**
   * Execute the task sometime in the future, using ThreadPools.
   */
  public synchronized void execute(@OsUntrusted AsyncDiskService this, @OsUntrusted String root, @OsUntrusted Runnable task) {
    @OsUntrusted
    ThreadPoolExecutor executor = executors.get(root);
    if (executor == null) {
      throw new @OsUntrusted RuntimeException("Cannot find root " + root
          + " for execution of task " + task);
    } else {
      executor.execute(task);
    }
  }
  
  /**
   * Gracefully start the shut down of all ThreadPools.
   */
  public synchronized void shutdown(@OsUntrusted AsyncDiskService this) {
    
    LOG.info("Shutting down all AsyncDiskService threads...");
    
    for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted ThreadPoolExecutor> e
        : executors.entrySet()) {
      e.getValue().shutdown();
    }
  }

  /**
   * Wait for the termination of the thread pools.
   * 
   * @param milliseconds  The number of milliseconds to wait
   * @return   true if all thread pools are terminated without time limit
   * @throws InterruptedException 
   */
  public synchronized @OsUntrusted boolean awaitTermination(@OsUntrusted AsyncDiskService this, @OsUntrusted long milliseconds) 
      throws InterruptedException {

    @OsUntrusted
    long end = Time.now() + milliseconds;
    for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted ThreadPoolExecutor> e:
        executors.entrySet()) {
      @OsUntrusted
      ThreadPoolExecutor executor = e.getValue();
      if (!executor.awaitTermination(
          Math.max(end - Time.now(), 0),
          TimeUnit.MILLISECONDS)) {
        LOG.warn("AsyncDiskService awaitTermination timeout.");
        return false;
      }
    }
    LOG.info("All AsyncDiskService threads are terminated.");
    return true;
  }
  
  /**
   * Shut down all ThreadPools immediately.
   */
  public synchronized @OsUntrusted List<@OsUntrusted Runnable> shutdownNow(@OsUntrusted AsyncDiskService this) {
    
    LOG.info("Shutting down all AsyncDiskService threads immediately...");
    
    @OsUntrusted
    List<@OsUntrusted Runnable> list = new @OsUntrusted ArrayList<@OsUntrusted Runnable>();
    for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted ThreadPoolExecutor> e
        : executors.entrySet()) {
      list.addAll(e.getValue().shutdownNow());
    }
    return list;
  }
  
}
