/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.io.file.tfile;

import ostrusted.quals.OsUntrusted;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A simplified BufferedOutputStream with borrowed buffer, and allow users to
 * see how much data have been buffered.
 */
class SimpleBufferedOutputStream extends @OsUntrusted FilterOutputStream {
  protected @OsUntrusted byte buf @OsUntrusted []; // the borrowed buffer
  protected @OsUntrusted int count = 0; // bytes used in buffer.

  // Constructor
  public @OsUntrusted SimpleBufferedOutputStream(@OsUntrusted OutputStream out, @OsUntrusted byte @OsUntrusted [] buf) {
    super(out);
    this.buf = buf;
  }

  private void flushBuffer(@OsUntrusted SimpleBufferedOutputStream this) throws IOException {
    if (count > 0) {
      out.write(buf, 0, count);
      count = 0;
    }
  }

  @Override
  public void write(@OsUntrusted SimpleBufferedOutputStream this, @OsUntrusted int b) throws IOException {
    if (count >= buf.length) {
      flushBuffer();
    }
    buf[count++] = (@OsUntrusted byte) b;
  }

  @Override
  public void write(@OsUntrusted SimpleBufferedOutputStream this, @OsUntrusted byte b @OsUntrusted [], @OsUntrusted int off, @OsUntrusted int len) throws IOException {
    if (len >= buf.length) {
      flushBuffer();
      out.write(b, off, len);
      return;
    }
    if (len > buf.length - count) {
      flushBuffer();
    }
    System.arraycopy(b, off, buf, count, len);
    count += len;
  }

  @Override
  public synchronized void flush(@OsUntrusted SimpleBufferedOutputStream this) throws IOException {
    flushBuffer();
    out.flush();
  }

  // Get the size of internal buffer being used.
  public @OsUntrusted int size(@OsUntrusted SimpleBufferedOutputStream this) {
    return count;
  }
}
