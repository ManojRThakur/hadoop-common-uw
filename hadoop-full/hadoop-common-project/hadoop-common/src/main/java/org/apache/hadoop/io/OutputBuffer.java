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

package org.apache.hadoop.io;

import ostrusted.quals.OsUntrusted;
import java.io.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** A reusable {@link OutputStream} implementation that writes to an in-memory
 * buffer.
 *
 * <p>This saves memory over creating a new OutputStream and
 * ByteArrayOutputStream each time data is written.
 *
 * <p>Typical usage is something like the following:<pre>
 *
 * OutputBuffer buffer = new OutputBuffer();
 * while (... loop condition ...) {
 *   buffer.reset();
 *   ... write buffer using OutputStream methods ...
 *   byte[] data = buffer.getData();
 *   int dataLength = buffer.getLength();
 *   ... write data to its ultimate destination ...
 * }
 * </pre>
 * @see DataOutputBuffer
 * @see InputBuffer
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class OutputBuffer extends @OsUntrusted FilterOutputStream {

  private static class Buffer extends @OsUntrusted ByteArrayOutputStream {
    public @OsUntrusted byte @OsUntrusted [] getData(OutputBuffer.@OsUntrusted Buffer this) { return buf; }
    public @OsUntrusted int getLength(OutputBuffer.@OsUntrusted Buffer this) { return count; }
    @Override
    public void reset(OutputBuffer.@OsUntrusted Buffer this) { count = 0; }

    public void write(OutputBuffer.@OsUntrusted Buffer this, @OsUntrusted InputStream in, @OsUntrusted int len) throws IOException {
      @OsUntrusted
      int newcount = count + len;
      if (newcount > buf.length) {
        @OsUntrusted
        byte newbuf @OsUntrusted [] = new @OsUntrusted byte @OsUntrusted [Math.max(buf.length << 1, newcount)];
        System.arraycopy(buf, 0, newbuf, 0, count);
        buf = newbuf;
      }
      IOUtils.readFully(in, buf, count, len);
      count = newcount;
    }
  }

  private @OsUntrusted Buffer buffer;
  
  /** Constructs a new empty buffer. */
  public @OsUntrusted OutputBuffer() {
    this(new @OsUntrusted Buffer());
  }
  
  private @OsUntrusted OutputBuffer(@OsUntrusted Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  /** Returns the current contents of the buffer.
   *  Data is only valid to {@link #getLength()}.
   */
  public @OsUntrusted byte @OsUntrusted [] getData(@OsUntrusted OutputBuffer this) { return buffer.getData(); }

  /** Returns the length of the valid data currently in the buffer. */
  public @OsUntrusted int getLength(@OsUntrusted OutputBuffer this) { return buffer.getLength(); }

  /** Resets the buffer to empty. */
  public @OsUntrusted OutputBuffer reset(@OsUntrusted OutputBuffer this) {
    buffer.reset();
    return this;
  }

  /** Writes bytes from a InputStream directly into the buffer. */
  public void write(@OsUntrusted OutputBuffer this, @OsUntrusted InputStream in, @OsUntrusted int length) throws IOException {
    buffer.write(in, length);
  }
}
