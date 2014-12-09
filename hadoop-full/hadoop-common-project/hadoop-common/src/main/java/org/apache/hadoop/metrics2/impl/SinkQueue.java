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
import java.util.ConcurrentModificationException;

/**
 * A half-blocking (nonblocking for producers, blocking for consumers) queue
 * for metrics sinks.
 *
 * New elements are dropped when the queue is full to preserve "interesting"
 * elements at the onset of queue filling events
 */
class SinkQueue<@OsUntrusted T extends java.lang.@OsUntrusted Object> {

  interface Consumer<@OsUntrusted T extends java.lang.@OsUntrusted Object> {
    void consume(SinkQueue.@OsUntrusted Consumer<T> this, @OsUntrusted T object) throws InterruptedException;
  }

  // A fixed size circular buffer to minimize garbage
  private final @OsUntrusted T @OsUntrusted [] data;
  private @OsUntrusted int head; // head position
  private @OsUntrusted int tail; // tail position
  private @OsUntrusted int size; // number of elements
  private @OsUntrusted Thread currentConsumer = null;

  @SuppressWarnings("unchecked")
  @OsUntrusted
  SinkQueue(@OsUntrusted int capacity) {
    this.data = (T @OsUntrusted []) new @OsUntrusted Object @OsUntrusted [Math.max(1, capacity)];
    head = tail = size = 0;
  }

  synchronized @OsUntrusted boolean enqueue(@OsUntrusted SinkQueue<T> this, @OsUntrusted T e) {
    if (data.length == size) {
      return false;
    }
    ++size;
    tail = (tail + 1) % data.length;
    data[tail] = e;
    notify();
    return true;
  }

  /**
   * Consume one element, will block if queue is empty
   * Only one consumer at a time is allowed
   * @param consumer  the consumer callback object
   */
  void consume(@OsUntrusted SinkQueue<T> this, @OsUntrusted Consumer<@OsUntrusted T> consumer) throws InterruptedException {
    T e = waitForData();

    try {
      consumer.consume(e);  // can take forever
      _dequeue();
    }
    finally {
      clearConsumerLock();
    }
  }

  /**
   * Consume all the elements, will block if queue is empty
   * @param consumer  the consumer callback object
   * @throws InterruptedException
   */
  void consumeAll(@OsUntrusted SinkQueue<T> this, @OsUntrusted Consumer<@OsUntrusted T> consumer) throws InterruptedException {
    waitForData();

    try {
      for (@OsUntrusted int i = size(); i-- > 0; ) {
        consumer.consume(front()); // can take forever
        _dequeue();
      }
    }
    finally {
      clearConsumerLock();
    }
  }

  /**
   * Dequeue one element from head of the queue, will block if queue is empty
   * @return  the first element
   * @throws InterruptedException
   */
  synchronized @OsUntrusted T dequeue(@OsUntrusted SinkQueue<T> this) throws InterruptedException {
    checkConsumer();

    while (0 == size) {
      wait();
    }
    return _dequeue();
  }

  private synchronized @OsUntrusted T waitForData(@OsUntrusted SinkQueue<T> this) throws InterruptedException {
    checkConsumer();

    while (0 == size) {
      wait();
    }
    setConsumerLock();
    return front();
  }

  private synchronized void checkConsumer(@OsUntrusted SinkQueue<T> this) {
    if (currentConsumer != null) {
      throw new @OsUntrusted ConcurrentModificationException("The "+
          currentConsumer.getName() +" thread is consuming the queue.");
    }
  }

  private synchronized void setConsumerLock(@OsUntrusted SinkQueue<T> this) {
    currentConsumer = Thread.currentThread();
  }

  private synchronized void clearConsumerLock(@OsUntrusted SinkQueue<T> this) {
    currentConsumer = null;
  }

  private synchronized @OsUntrusted T _dequeue(@OsUntrusted SinkQueue<T> this) {
    if (0 == size) {
      throw new @OsUntrusted IllegalStateException("Size must > 0 here.");
    }
    --size;
    head = (head + 1) % data.length;
    T ret = data[head];
    data[head] = null;  // hint to gc
    return ret;
  }

  synchronized @OsUntrusted T front(@OsUntrusted SinkQueue<T> this) {
    return data[(head + 1) % data.length];
  }

  synchronized @OsUntrusted T back(@OsUntrusted SinkQueue<T> this) {
    return data[tail];
  }

  synchronized void clear(@OsUntrusted SinkQueue<T> this) {
    checkConsumer();

    for (@OsUntrusted int i = data.length; i-- > 0; ) {
      data[i] = null;
    }
    size = 0;
  }

  synchronized @OsUntrusted int size(@OsUntrusted SinkQueue<T> this) {
    return size;
  }

  @OsUntrusted
  int capacity(@OsUntrusted SinkQueue<T> this) {
    return data.length;
  }
}
