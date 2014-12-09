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

package org.apache.hadoop.fs;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.ByteBufferPool;

import com.google.common.base.Preconditions;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ByteBufferUtil {

  /**
   * Determine if a stream can do a byte buffer read via read(ByteBuffer buf)
   */
  private static @OsUntrusted boolean streamHasByteBufferRead(@OsUntrusted InputStream stream) {
    if (!(stream instanceof @OsUntrusted ByteBufferReadable)) {
      return false;
    }
    if (!(stream instanceof @OsUntrusted FSDataInputStream)) {
      return true;
    }
    return ((@OsUntrusted FSDataInputStream)stream).getWrappedStream() 
        instanceof @OsUntrusted ByteBufferReadable;
  }

  /**
   * Perform a fallback read.
   */
  public static @OsUntrusted ByteBuffer fallbackRead(
      @OsUntrusted
      InputStream stream, @OsUntrusted ByteBufferPool bufferPool, @OsUntrusted int maxLength)
          throws IOException {
    if (bufferPool == null) {
      throw new @OsUntrusted UnsupportedOperationException("zero-copy reads " +
          "were not available, and you did not provide a fallback " +
          "ByteBufferPool.");
    }
    @OsUntrusted
    boolean useDirect = streamHasByteBufferRead(stream);
    @OsUntrusted
    ByteBuffer buffer = bufferPool.getBuffer(useDirect, maxLength);
    if (buffer == null) {
      throw new @OsUntrusted UnsupportedOperationException("zero-copy reads " +
          "were not available, and the ByteBufferPool did not provide " +
          "us with " + (useDirect ? "a direct" : "an indirect") +
          "buffer.");
    }
    Preconditions.checkState(buffer.capacity() > 0);
    Preconditions.checkState(buffer.isDirect() == useDirect);
    maxLength = Math.min(maxLength, buffer.capacity());
    @OsUntrusted
    boolean success = false;
    try {
      if (useDirect) {
        buffer.clear();
        buffer.limit(maxLength);
        @OsUntrusted
        ByteBufferReadable readable = (@OsUntrusted ByteBufferReadable)stream;
        @OsUntrusted
        int totalRead = 0;
        while (true) {
          if (totalRead >= maxLength) {
            success = true;
            break;
          }
          @OsUntrusted
          int nRead = readable.read(buffer);
          if (nRead < 0) {
            if (totalRead > 0) {
              success = true;
            }
            break;
          }
          totalRead += nRead;
        }
        buffer.flip();
      } else {
        buffer.clear();
        @OsUntrusted
        int nRead = stream.read(buffer.array(),
            buffer.arrayOffset(), maxLength);
        if (nRead >= 0) {
          buffer.limit(nRead);
          success = true;
        }
      }
    } finally {
      if (!success) {
        // If we got an error while reading, or if we are at EOF, we 
        // don't need the buffer any more.  We can give it back to the
        // bufferPool.
        bufferPool.putBuffer(buffer);
        buffer = null;
      }
    }
    return buffer;
  }
}
