/*
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
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Class which sets up a simple thread which runs in a loop sleeping
 * for a short interval of time. If the sleep takes significantly longer
 * than its target time, it implies that the JVM or host machine has
 * paused processing, which may cause other problems. If such a pause is
 * detected, the thread logs a message.
 */
@InterfaceAudience.Private
public class JvmPauseMonitor {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(
      JvmPauseMonitor.class);

  /** The target sleep time */
  private static final @OsUntrusted long SLEEP_INTERVAL_MS = 500;
  
  /** log WARN if we detect a pause longer than this threshold */
  private final @OsUntrusted long warnThresholdMs;
  private static final @OsUntrusted String WARN_THRESHOLD_KEY =
      "jvm.pause.warn-threshold.ms";
  private static final @OsUntrusted long WARN_THRESHOLD_DEFAULT = 10000;
  
  /** log INFO if we detect a pause longer than this threshold */
  private final @OsUntrusted long infoThresholdMs;
  private static final @OsUntrusted String INFO_THRESHOLD_KEY =
      "jvm.pause.info-threshold.ms";
  private static final @OsUntrusted long INFO_THRESHOLD_DEFAULT = 1000;

  
  private @OsUntrusted Thread monitorThread;
  private volatile @OsUntrusted boolean shouldRun = true;
  
  public @OsUntrusted JvmPauseMonitor(@OsUntrusted Configuration conf) {
    this.warnThresholdMs = conf.getLong(WARN_THRESHOLD_KEY, WARN_THRESHOLD_DEFAULT);
    this.infoThresholdMs = conf.getLong(INFO_THRESHOLD_KEY, INFO_THRESHOLD_DEFAULT);
  }
  
  public void start(@OsUntrusted JvmPauseMonitor this) {
    Preconditions.checkState(monitorThread == null,
        "Already started");
    monitorThread = new @OsUntrusted Daemon(new @OsUntrusted Monitor());
    monitorThread.start();
  }
  
  public void stop(@OsUntrusted JvmPauseMonitor this) {
    shouldRun = false;
    monitorThread.interrupt();
    try {
      monitorThread.join();
    } catch (@OsUntrusted InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
  
  private @OsUntrusted String formatMessage(@OsUntrusted JvmPauseMonitor this, @OsUntrusted long extraSleepTime,
      @OsUntrusted
      Map<@OsUntrusted String, @OsUntrusted GcTimes> gcTimesAfterSleep,
      @OsUntrusted
      Map<@OsUntrusted String, @OsUntrusted GcTimes> gcTimesBeforeSleep) {
    
    @OsUntrusted
    Set<@OsUntrusted String> gcBeanNames = Sets.intersection(
        gcTimesAfterSleep.keySet(),
        gcTimesBeforeSleep.keySet());
    @OsUntrusted
    List<@OsUntrusted String> gcDiffs = Lists.newArrayList();
    for (@OsUntrusted String name : gcBeanNames) {
      @OsUntrusted
      GcTimes diff = gcTimesAfterSleep.get(name).subtract(
          gcTimesBeforeSleep.get(name));
      if (diff.gcCount != 0) {
        gcDiffs.add("GC pool '" + name + "' had collection(s): " +
            diff.toString());
      }
    }
    
    @OsUntrusted
    String ret = "Detected pause in JVM or host machine (eg GC): " +
        "pause of approximately " + extraSleepTime + "ms\n";
    if (gcDiffs.isEmpty()) {
      ret += "No GCs detected";
    } else {
      ret += Joiner.on("\n").join(gcDiffs);
    }
    return ret;
  }
  
  private @OsUntrusted Map<@OsUntrusted String, @OsUntrusted GcTimes> getGcTimes(@OsUntrusted JvmPauseMonitor this) {
    @OsUntrusted
    Map<@OsUntrusted String, @OsUntrusted GcTimes> map = Maps.newHashMap();
    @OsUntrusted
    List<@OsUntrusted GarbageCollectorMXBean> gcBeans =
        ManagementFactory.getGarbageCollectorMXBeans();
    for (@OsUntrusted GarbageCollectorMXBean gcBean : gcBeans) {
      map.put(gcBean.getName(), new @OsUntrusted GcTimes(gcBean));
    }
    return map;
  }
  
  private static class GcTimes {
    private @OsUntrusted GcTimes(@OsUntrusted GarbageCollectorMXBean gcBean) {
      gcCount = gcBean.getCollectionCount();
      gcTimeMillis = gcBean.getCollectionTime();
    }
    
    private @OsUntrusted GcTimes(@OsUntrusted long count, @OsUntrusted long time) {
      this.gcCount = count;
      this.gcTimeMillis = time;
    }

    private @OsUntrusted GcTimes subtract(JvmPauseMonitor.@OsUntrusted GcTimes this, @OsUntrusted GcTimes other) {
      return new @OsUntrusted GcTimes(this.gcCount - other.gcCount,
          this.gcTimeMillis - other.gcTimeMillis);
    }
    
    @Override
    public @OsUntrusted String toString(JvmPauseMonitor.@OsUntrusted GcTimes this) {
      return "count=" + gcCount + " time=" + gcTimeMillis + "ms";
    }
    
    private @OsUntrusted long gcCount;
    private @OsUntrusted long gcTimeMillis;
  }

  private class Monitor implements @OsUntrusted Runnable {
    @Override
    public void run(@OsUntrusted JvmPauseMonitor.Monitor this) {
      @OsUntrusted
      Stopwatch sw = new @OsUntrusted Stopwatch();
      @OsUntrusted
      Map<@OsUntrusted String, @OsUntrusted GcTimes> gcTimesBeforeSleep = getGcTimes();
      while (shouldRun) {
        sw.reset().start();
        try {
          Thread.sleep(SLEEP_INTERVAL_MS);
        } catch (@OsUntrusted InterruptedException ie) {
          return;
        }
        @OsUntrusted
        long extraSleepTime = sw.elapsedMillis() - SLEEP_INTERVAL_MS;
        @OsUntrusted
        Map<@OsUntrusted String, @OsUntrusted GcTimes> gcTimesAfterSleep = getGcTimes();

        if (extraSleepTime > warnThresholdMs) {
          LOG.warn(formatMessage(
              extraSleepTime, gcTimesAfterSleep, gcTimesBeforeSleep));
        } else if (extraSleepTime > infoThresholdMs) {
          LOG.info(formatMessage(
              extraSleepTime, gcTimesAfterSleep, gcTimesBeforeSleep));
        }
        
        gcTimesBeforeSleep = gcTimesAfterSleep;
      }
    }
  }
  
  /**
   * Simple 'main' to facilitate manual testing of the pause monitor.
   * 
   * This main function just leaks memory into a list. Running this class
   * with a 1GB heap will very quickly go into "GC hell" and result in
   * log messages about the GC pauses.
   */
  public static void main(@OsUntrusted String @OsUntrusted []args) throws Exception {
    new @OsUntrusted JvmPauseMonitor(new @OsUntrusted Configuration()).start();
    @OsUntrusted
    List<@OsUntrusted String> list = Lists.newArrayList();
    @OsUntrusted
    int i = 0;
    while (true) {
      list.add(String.valueOf(i++));
    }
  }
}
