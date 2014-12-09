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
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class DataInputByteBuffer extends @OsUntrusted DataInputStream {

  private static class Buffer extends @OsUntrusted InputStream {
    private final @OsUntrusted byte @OsUntrusted [] scratch = new @OsUntrusted byte @OsUntrusted [1];
    @OsUntrusted
    ByteBuffer @OsUntrusted [] buffers = new @OsUntrusted ByteBuffer @OsUntrusted [0];
    @OsUntrusted
    int bidx;
    @OsUntrusted
    int pos;
    @OsUntrusted
    int length;
    @Override
    public @OsUntrusted int read(DataInputByteBuffer.@OsUntrusted Buffer this) {
      if (-1 == read(scratch, 0, 1)) {
        return -1;
      }
      return scratch[0] & 0xFF;
    }
    @Override
    public @OsUntrusted int read(DataInputByteBuffer.@OsUntrusted Buffer this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
      if (bidx >= buffers.length) {
        return -1;
      }
      @OsUntrusted
      int cur = 0;
      do {
        @OsUntrusted
        int rem = Math.min(len, buffers[bidx].remaining());
        buffers[bidx].get(b, off, rem);
        cur += rem;
        off += rem;
        len -= rem;
      } while (len > 0 && ++bidx < buffers.length);
      pos += cur;
      return cur;
    }
    public void reset(DataInputByteBuffer.@OsUntrusted Buffer this, @OsUntrusted ByteBuffer @OsUntrusted [] buffers) {
      bidx = pos = length = 0;
      this.buffers = buffers;
      for (@OsUntrusted ByteBuffer b : buffers) {
        length += b.remaining();
      }
    }
    public @OsUntrusted int getPosition(DataInputByteBuffer.@OsUntrusted Buffer this) {
      return pos;
    }
    public @OsUntrusted int getLength(DataInputByteBuffer.@OsUntrusted Buffer this) {
      return length;
    }
    public @OsUntrusted ByteBuffer @OsUntrusted [] getData(DataInputByteBuffer.@OsUntrusted Buffer this) {
      return buffers;
    }
  }

  private @OsUntrusted Buffer buffers;

  public @OsUntrusted DataInputByteBuffer() {
    this(new @OsUntrusted Buffer());
  }

  private @OsUntrusted DataInputByteBuffer(@OsUntrusted Buffer buffers) {
    super(buffers);
    this.buffers = buffers;
  }

  public void reset(@OsUntrusted DataInputByteBuffer this, @OsUntrusted ByteBuffer @OsUntrusted ... input) {
    buffers.reset(input);
  }

  public @OsUntrusted ByteBuffer @OsUntrusted [] getData(@OsUntrusted DataInputByteBuffer this) {
    return buffers.getData();
  }

  public @OsUntrusted int getPosition(@OsUntrusted DataInputByteBuffer this) {
    return buffers.getPosition();
  }

  public @OsUntrusted int getLength(@OsUntrusted DataInputByteBuffer this) {
    return buffers.getLength();
  }
}
