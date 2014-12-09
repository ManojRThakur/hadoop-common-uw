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

package org.apache.hadoop.metrics2.impl;

import ostrusted.quals.OsUntrusted;
import java.util.Random;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import static org.apache.hadoop.metrics2.util.Contracts.*;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.util.Time;

/**
 * An adapter class for metrics sink and associated filters
 */
class MetricsSinkAdapter implements SinkQueue.@OsUntrusted Consumer<@OsUntrusted MetricsBuffer> {

  private final @OsUntrusted Log LOG = LogFactory.getLog(MetricsSinkAdapter.class);
  private final @OsUntrusted String name;
  private final @OsUntrusted String description;
  private final @OsUntrusted String context;
  private final @OsUntrusted MetricsSink sink;
  private final @OsUntrusted MetricsFilter sourceFilter;
  private final @OsUntrusted MetricsFilter recordFilter;
  private final @OsUntrusted MetricsFilter metricFilter;
  private final @OsUntrusted SinkQueue<@OsUntrusted MetricsBuffer> queue;
  private final @OsUntrusted Thread sinkThread;
  private volatile @OsUntrusted boolean stopping = false;
  private volatile @OsUntrusted boolean inError = false;
  private final @OsUntrusted int period;
  private final @OsUntrusted int firstRetryDelay;
  private final @OsUntrusted int retryCount;
  private final @OsUntrusted long oobPutTimeout;
  private final @OsUntrusted float retryBackoff;
  private final @OsUntrusted MetricsRegistry registry = new @OsUntrusted MetricsRegistry("sinkadapter");
  private final @OsUntrusted MutableStat latency;
  private final @OsUntrusted MutableCounterInt dropped;
  private final @OsUntrusted MutableGaugeInt qsize;

  @OsUntrusted
  MetricsSinkAdapter(@OsUntrusted String name, @OsUntrusted String description, @OsUntrusted MetricsSink sink,
                     @OsUntrusted
                     String context, @OsUntrusted MetricsFilter sourceFilter,
                     @OsUntrusted
                     MetricsFilter recordFilter, @OsUntrusted MetricsFilter metricFilter,
                     @OsUntrusted
                     int period, @OsUntrusted int queueCapacity, @OsUntrusted int retryDelay,
                     @OsUntrusted
                     float retryBackoff, @OsUntrusted int retryCount) {
    this.name = checkNotNull(name, "name");
    this.description = description;
    this.sink = checkNotNull(sink, "sink object");
    this.context = context;
    this.sourceFilter = sourceFilter;
    this.recordFilter = recordFilter;
    this.metricFilter = metricFilter;
    this.period = checkArg(period, period > 0, "period");
    firstRetryDelay = checkArg(retryDelay, retryDelay > 0, "retry delay");
    this.retryBackoff = checkArg(retryBackoff, retryBackoff>1, "retry backoff");
    oobPutTimeout = (@OsUntrusted long)
        (firstRetryDelay * Math.pow(retryBackoff, retryCount) * 1000);
    this.retryCount = retryCount;
    this.queue = new @OsUntrusted SinkQueue<@OsUntrusted MetricsBuffer>(checkArg(queueCapacity,
        queueCapacity > 0, "queue capacity"));
    latency = registry.newRate("Sink_"+ name, "Sink end to end latency", false);
    dropped = registry.newCounter("Sink_"+ name +"Dropped",
                                  "Dropped updates per sink", 0);
    qsize = registry.newGauge("Sink_"+ name + "Qsize", "Queue size", 0);

    sinkThread = new @OsUntrusted Thread() {
      @Override public void run() {
        publishMetricsFromQueue();
      }
    };
    sinkThread.setName(name);
    sinkThread.setDaemon(true);
  }

  @OsUntrusted
  boolean putMetrics(@OsUntrusted MetricsSinkAdapter this, @OsUntrusted MetricsBuffer buffer, @OsUntrusted long logicalTime) {
    if (logicalTime % period == 0) {
      LOG.debug("enqueue, logicalTime="+ logicalTime);
      if (queue.enqueue(buffer)) return true;
      dropped.incr();
      return false;
    }
    return true; // OK
  }
  
  public @OsUntrusted boolean putMetricsImmediate(@OsUntrusted MetricsSinkAdapter this, @OsUntrusted MetricsBuffer buffer) {
    @OsUntrusted
    WaitableMetricsBuffer waitableBuffer =
        new @OsUntrusted WaitableMetricsBuffer(buffer);
    if (!queue.enqueue(waitableBuffer)) {
      LOG.warn(name + " has a full queue and can't consume the given metrics.");
      dropped.incr();
      return false;
    }
    if (!waitableBuffer.waitTillNotified(oobPutTimeout)) {
      LOG.warn(name +
          " couldn't fulfill an immediate putMetrics request in time." +
          " Abandoning.");
      return false;
    }
    return true;
  }

  void publishMetricsFromQueue(@OsUntrusted MetricsSinkAdapter this) {
    @OsUntrusted
    int retryDelay = firstRetryDelay;
    @OsUntrusted
    int n = retryCount;
    @OsUntrusted
    int minDelay = Math.min(500, retryDelay * 1000); // millis
    @OsUntrusted
    Random rng = new @OsUntrusted Random(System.nanoTime());
    while (!stopping) {
      try {
        queue.consumeAll(this);
        retryDelay = firstRetryDelay;
        n = retryCount;
        inError = false;
      } catch (@OsUntrusted InterruptedException e) {
        LOG.info(name +" thread interrupted.");
      } catch (@OsUntrusted Exception e) {
        if (n > 0) {
          @OsUntrusted
          int retryWindow = Math.max(0, 1000 / 2 * retryDelay - minDelay);
          @OsUntrusted
          int awhile = rng.nextInt(retryWindow) + minDelay;
          if (!inError) {
            LOG.error("Got sink exception, retry in "+ awhile +"ms", e);
          }
          retryDelay *= retryBackoff;
          try { Thread.sleep(awhile); }
          catch (@OsUntrusted InterruptedException e2) {
            LOG.info(name +" thread interrupted while waiting for retry", e2);
          }
          --n;
        } else {
          if (!inError) {
            LOG.error("Got sink exception and over retry limit, "+
                      "suppressing further error messages", e);
          }
          queue.clear();
          inError = true; // Don't keep complaining ad infinitum
        }
      }
    }
  }

  @Override
  public void consume(@OsUntrusted MetricsSinkAdapter this, @OsUntrusted MetricsBuffer buffer) {
    @OsUntrusted
    long ts = 0;
    for (MetricsBuffer.@OsUntrusted Entry entry : buffer) {
      if (sourceFilter == null || sourceFilter.accepts(entry.name())) {
        for (@OsUntrusted MetricsRecordImpl record : entry.records()) {
          if ((context == null || context.equals(record.context())) &&
              (recordFilter == null || recordFilter.accepts(record))) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Pushing record "+ entry.name() +"."+ record.context() +
                        "."+ record.name() +" to "+ name);
            }
            sink.putMetrics(metricFilter == null
                ? record
                : new @OsUntrusted MetricsRecordFiltered(record, metricFilter));
            if (ts == 0) ts = record.timestamp();
          }
        }
      }
    }
    if (ts > 0) {
      sink.flush();
      latency.add(Time.now() - ts);
    }
    if (buffer instanceof @OsUntrusted WaitableMetricsBuffer) {
      ((@OsUntrusted WaitableMetricsBuffer)buffer).notifyAnyWaiters();
    }
    LOG.debug("Done");
  }

  void start(@OsUntrusted MetricsSinkAdapter this) {
    sinkThread.start();
    LOG.info("Sink "+ name +" started");
  }

  void stop(@OsUntrusted MetricsSinkAdapter this) {
    stopping = true;
    sinkThread.interrupt();
    try {
      sinkThread.join();
    } catch (@OsUntrusted InterruptedException e) {
      LOG.warn("Stop interrupted", e);
    }
  }

  @OsUntrusted
  String name(@OsUntrusted MetricsSinkAdapter this) {
    return name;
  }

  @OsUntrusted
  String description(@OsUntrusted MetricsSinkAdapter this) {
    return description;
  }

  void snapshot(@OsUntrusted MetricsSinkAdapter this, @OsUntrusted MetricsRecordBuilder rb, @OsUntrusted boolean all) {
    registry.snapshot(rb, all);
  }

  @OsUntrusted
  MetricsSink sink(@OsUntrusted MetricsSinkAdapter this) {
    return sink;
  }

  static class WaitableMetricsBuffer extends @OsUntrusted MetricsBuffer {
    private final @OsUntrusted Semaphore notificationSemaphore =
        new @OsUntrusted Semaphore(0);

    public @OsUntrusted WaitableMetricsBuffer(@OsUntrusted MetricsBuffer metricsBuffer) {
      super(metricsBuffer);
    }

    public @OsUntrusted boolean waitTillNotified(MetricsSinkAdapter.@OsUntrusted WaitableMetricsBuffer this, @OsUntrusted long millisecondsToWait) {
      try {
        return notificationSemaphore.tryAcquire(millisecondsToWait,
            TimeUnit.MILLISECONDS);
      } catch (@OsUntrusted InterruptedException e) {
        return false;
      }
    }

    public void notifyAnyWaiters(MetricsSinkAdapter.@OsUntrusted WaitableMetricsBuffer this) {
      notificationSemaphore.release();
    }
  }
}
