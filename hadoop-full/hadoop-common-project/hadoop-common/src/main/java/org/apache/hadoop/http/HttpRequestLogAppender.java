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
package org.apache.hadoop.http;

import ostrusted.quals.OsUntrusted;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.AppenderSkeleton;

/**
 * Log4j Appender adapter for HttpRequestLog
 */
public class HttpRequestLogAppender extends @OsUntrusted AppenderSkeleton {

  private @OsUntrusted String filename;
  private @OsUntrusted int retainDays;

  public @OsUntrusted HttpRequestLogAppender() {
  }

  public void setRetainDays(@OsUntrusted HttpRequestLogAppender this, @OsUntrusted int retainDays) {
    this.retainDays = retainDays;
  }

  public @OsUntrusted int getRetainDays(@OsUntrusted HttpRequestLogAppender this) {
    return retainDays;
  }

  public void setFilename(@OsUntrusted HttpRequestLogAppender this, @OsUntrusted String filename) {
    this.filename = filename;
  }

  public @OsUntrusted String getFilename(@OsUntrusted HttpRequestLogAppender this) {
    return filename;
  }

  @Override
  public void append(@OsUntrusted HttpRequestLogAppender this, @OsUntrusted LoggingEvent event) {
  }

  @Override
  public void close(@OsUntrusted HttpRequestLogAppender this) {
  }

  @Override
  public @OsUntrusted boolean requiresLayout(@OsUntrusted HttpRequestLogAppender this) {
    return false;
  }
}
