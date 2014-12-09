/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/** A PriorityQueue maintains a partial ordering of its elements such that the
  least element can always be found in constant time.  Put()'s and pop()'s
  require log(size) time. */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class PriorityQueue<@OsUntrusted T extends java.lang.@OsUntrusted Object> {
  private @OsUntrusted T @OsUntrusted [] heap;
  private @OsUntrusted int size;
  private @OsUntrusted int maxSize;

  /** Determines the ordering of objects in this priority queue.  Subclasses
      must define this one method. */
  protected abstract @OsUntrusted boolean lessThan(@OsUntrusted PriorityQueue<T> this, @OsUntrusted Object a, @OsUntrusted Object b);

  /** Subclass constructors must call this. */
  @SuppressWarnings("unchecked")
  protected final void initialize(@OsUntrusted PriorityQueue<T> this, @OsUntrusted int maxSize) {
    size = 0;
    @OsUntrusted
    int heapSize = maxSize + 1;
    heap = (T @OsUntrusted []) new @OsUntrusted Object @OsUntrusted [heapSize];
    this.maxSize = maxSize;
  }

  /**
   * Adds an Object to a PriorityQueue in log(size) time.
   * If one tries to add more objects than maxSize from initialize
   * a RuntimeException (ArrayIndexOutOfBound) is thrown.
   */
  public final void put(@OsUntrusted PriorityQueue<T> this, @OsUntrusted T element) {
    size++;
    heap[size] = element;
    upHeap();
  }

  /**
   * Adds element to the PriorityQueue in log(size) time if either
   * the PriorityQueue is not full, or not lessThan(element, top()).
   * @param element
   * @return true if element is added, false otherwise.
   */
  public @OsUntrusted boolean insert(@OsUntrusted PriorityQueue<T> this, @OsUntrusted T element){
    if (size < maxSize){
      put(element);
      return true;
    }
    else if (size > 0 && !lessThan(element, top())){
      heap[1] = element;
      adjustTop();
      return true;
    }
    else
      return false;
  }

  /** Returns the least element of the PriorityQueue in constant time. */
  public final @OsUntrusted T top(@OsUntrusted PriorityQueue<T> this) {
    if (size > 0)
      return heap[1];
    else
      return null;
  }

  /** Removes and returns the least element of the PriorityQueue in log(size)
      time. */
  public final @OsUntrusted T pop(@OsUntrusted PriorityQueue<T> this) {
    if (size > 0) {
      T result = heap[1];			  // save first value
      heap[1] = heap[size];			  // move last to first
      heap[size] = null;			  // permit GC of objects
      size--;
      downHeap();				  // adjust heap
      return result;
    } else
      return null;
  }

  /** Should be called when the Object at top changes values.  Still log(n)
   * worst case, but it's at least twice as fast to <pre>
   *  { pq.top().change(); pq.adjustTop(); }
   * </pre> instead of <pre>
   *  { o = pq.pop(); o.change(); pq.push(o); }
   * </pre>
   */
  public final void adjustTop(@OsUntrusted PriorityQueue<T> this) {
    downHeap();
  }


  /** Returns the number of elements currently stored in the PriorityQueue. */
  public final @OsUntrusted int size(@OsUntrusted PriorityQueue<T> this) {
    return size;
  }

  /** Removes all entries from the PriorityQueue. */
  public final void clear(@OsUntrusted PriorityQueue<T> this) {
    for (@OsUntrusted int i = 0; i <= size; i++)
      heap[i] = null;
    size = 0;
  }

  private final void upHeap(@OsUntrusted PriorityQueue<T> this) {
    @OsUntrusted
    int i = size;
    T node = heap[i];			  // save bottom node
    @OsUntrusted
    int j = i >>> 1;
    while (j > 0 && lessThan(node, heap[j])) {
      heap[i] = heap[j];			  // shift parents down
      i = j;
      j = j >>> 1;
    }
    heap[i] = node;				  // install saved node
  }

  private final void downHeap(@OsUntrusted PriorityQueue<T> this) {
    @OsUntrusted
    int i = 1;
    T node = heap[i];			  // save top node
    @OsUntrusted
    int j = i << 1;				  // find smaller child
    @OsUntrusted
    int k = j + 1;
    if (k <= size && lessThan(heap[k], heap[j])) {
      j = k;
    }
    while (j <= size && lessThan(heap[j], node)) {
      heap[i] = heap[j];			  // shift up child
      i = j;
      j = i << 1;
      k = j + 1;
      if (k <= size && lessThan(heap[k], heap[j])) {
	j = k;
      }
    }
    heap[i] = node;				  // install saved node
  }
}
