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

/** A reusable {@link DataOutput} implementation that writes to an in-memory
 * buffer.
 *
 * <p>This saves memory over creating a new DataOutputStream and
 * ByteArrayOutputStream each time data is written.
 *
 * <p>Typical usage is something like the following:<pre>
 *
 * DataOutputBuffer buffer = new DataOutputBuffer();
 * while (... loop condition ...) {
 *   buffer.reset();
 *   ... write buffer using DataOutput methods ...
 *   byte[] data = buffer.getData();
 *   int dataLength = buffer.getLength();
 *   ... write data to its ultimate destination ...
 * }
 * </pre>
 *  
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class DataOutputBuffer extends @OsUntrusted DataOutputStream {

  private static class Buffer extends @OsUntrusted ByteArrayOutputStream {
    public @OsUntrusted byte @OsUntrusted [] getData(DataOutputBuffer.@OsUntrusted Buffer this) { return buf; }
    public @OsUntrusted int getLength(DataOutputBuffer.@OsUntrusted Buffer this) { return count; }

    public @OsUntrusted Buffer() {
      super();
    }
    
    public @OsUntrusted Buffer(@OsUntrusted int size) {
      super(size);
    }
    
    public void write(DataOutputBuffer.@OsUntrusted Buffer this, @OsUntrusted DataInput in, @OsUntrusted int len) throws IOException {
      @OsUntrusted
      int newcount = count + len;
      if (newcount > buf.length) {
        @OsUntrusted
        byte newbuf @OsUntrusted [] = new @OsUntrusted byte @OsUntrusted [Math.max(buf.length << 1, newcount)];
        System.arraycopy(buf, 0, newbuf, 0, count);
        buf = newbuf;
      }
      in.readFully(buf, count, len);
      count = newcount;
    }
  }

  private @OsUntrusted Buffer buffer;
  
  /** Constructs a new empty buffer. */
  public @OsUntrusted DataOutputBuffer() {
    this(new @OsUntrusted Buffer());
  }
  
  public @OsUntrusted DataOutputBuffer(@OsUntrusted int size) {
    this(new @OsUntrusted Buffer(size));
  }
  
  private @OsUntrusted DataOutputBuffer(@OsUntrusted Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  /** Returns the current contents of the buffer.
   *  Data is only valid to {@link #getLength()}.
   */
  public @OsUntrusted byte @OsUntrusted [] getData(@OsUntrusted DataOutputBuffer this) { return buffer.getData(); }

  /** Returns the length of the valid data currently in the buffer. */
  public @OsUntrusted int getLength(@OsUntrusted DataOutputBuffer this) { return buffer.getLength(); }

  /** Resets the buffer to empty. */
  public @OsUntrusted DataOutputBuffer reset(@OsUntrusted DataOutputBuffer this) {
    this.written = 0;
    buffer.reset();
    return this;
  }

  /** Writes bytes from a DataInput directly into the buffer. */
  public void write(@OsUntrusted DataOutputBuffer this, @OsUntrusted DataInput in, @OsUntrusted int length) throws IOException {
    buffer.write(in, length);
  }

  /** Write to a file stream */
  public void writeTo(@OsUntrusted DataOutputBuffer this, @OsUntrusted OutputStream out) throws IOException {
    buffer.writeTo(out);
  }
}
