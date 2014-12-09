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
import java.util.Iterator;
import java.util.Collection;

import com.google.common.collect.AbstractIterator;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;

class MetricsRecordFiltered extends @OsUntrusted AbstractMetricsRecord {
  private final @OsUntrusted MetricsRecord delegate;
  private final @OsUntrusted MetricsFilter filter;

  @OsUntrusted
  MetricsRecordFiltered(@OsUntrusted MetricsRecord delegate, @OsUntrusted MetricsFilter filter) {
    this.delegate = delegate;
    this.filter = filter;
  }

  @Override public @OsUntrusted long timestamp(@OsUntrusted MetricsRecordFiltered this) {
    return delegate.timestamp();
  }

  @Override public @OsUntrusted String name(@OsUntrusted MetricsRecordFiltered this) {
    return delegate.name();
  }

  @Override public @OsUntrusted String description(@OsUntrusted MetricsRecordFiltered this) {
    return delegate.description();
  }

  @Override public @OsUntrusted String context(@OsUntrusted MetricsRecordFiltered this) {
    return delegate.context();
  }

  @Override public @OsUntrusted Collection<@OsUntrusted MetricsTag> tags(@OsUntrusted MetricsRecordFiltered this) {
    return delegate.tags();
  }

  @Override public @OsUntrusted Iterable<@OsUntrusted AbstractMetric> metrics(@OsUntrusted MetricsRecordFiltered this) {
    return new @OsUntrusted Iterable<@OsUntrusted AbstractMetric>() {
      final @OsUntrusted Iterator<@OsUntrusted AbstractMetric> it = delegate.metrics().iterator();
      @Override public @OsUntrusted Iterator<@OsUntrusted AbstractMetric> iterator() {
        return new @OsUntrusted AbstractIterator<@OsUntrusted AbstractMetric>() {
          @Override public AbstractMetric computeNext() {
            while (it.hasNext()) {
              AbstractMetric next = it.next();
              if (filter.accepts(next.name())) {
                return next;
              }
            }
            return endOfData();
          }
        };
      }
    };
  }
}
