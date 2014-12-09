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


/** A reusable {@link InputStream} implementation that reads from an in-memory
 * buffer.
 *
 * <p>This saves memory over creating a new InputStream and
 * ByteArrayInputStream each time data is read.
 *
 * <p>Typical usage is something like the following:<pre>
 *
 * InputBuffer buffer = new InputBuffer();
 * while (... loop condition ...) {
 *   byte[] data = ... get data ...;
 *   int dataLength = ... get data length ...;
 *   buffer.reset(data, dataLength);
 *   ... read buffer using InputStream methods ...
 * }
 * </pre>
 * @see DataInputBuffer
 * @see DataOutput
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class InputBuffer extends @OsUntrusted FilterInputStream {

  private static class Buffer extends @OsUntrusted ByteArrayInputStream {
    public @OsUntrusted Buffer() {
      super(new @OsUntrusted byte @OsUntrusted [] {});
    }

    public void reset(InputBuffer.@OsUntrusted Buffer this, @OsUntrusted byte @OsUntrusted [] input, @OsUntrusted int start, @OsUntrusted int length) {
      this.buf = input;
      this.count = start+length;
      this.mark = start;
      this.pos = start;
    }

    public @OsUntrusted int getPosition(InputBuffer.@OsUntrusted Buffer this) { return pos; }
    public @OsUntrusted int getLength(InputBuffer.@OsUntrusted Buffer this) { return count; }
  }

  private @OsUntrusted Buffer buffer;
  
  /** Constructs a new empty buffer. */
  public @OsUntrusted InputBuffer() {
    this(new @OsUntrusted Buffer());
  }

  private @OsUntrusted InputBuffer(@OsUntrusted Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  /** Resets the data that the buffer reads. */
  public void reset(@OsUntrusted InputBuffer this, @OsUntrusted byte @OsUntrusted [] input, @OsUntrusted int length) {
    buffer.reset(input, 0, length);
  }

  /** Resets the data that the buffer reads. */
  public void reset(@OsUntrusted InputBuffer this, @OsUntrusted byte @OsUntrusted [] input, @OsUntrusted int start, @OsUntrusted int length) {
    buffer.reset(input, start, length);
  }

  /** Returns the current position in the input. */
  public @OsUntrusted int getPosition(@OsUntrusted InputBuffer this) { return buffer.getPosition(); }

  /** Returns the length of the input. */
  public @OsUntrusted int getLength(@OsUntrusted InputBuffer this) { return buffer.getLength(); }

}
