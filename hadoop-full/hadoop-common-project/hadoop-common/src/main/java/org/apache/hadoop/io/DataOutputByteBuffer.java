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
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;

public class DataOutputByteBuffer extends @OsUntrusted DataOutputStream {

   static class Buffer extends @OsUntrusted OutputStream {

    final @OsUntrusted byte @OsUntrusted [] b = new @OsUntrusted byte @OsUntrusted [1];
    final @OsUntrusted boolean direct;
    final @OsUntrusted List<@OsUntrusted ByteBuffer> active = new @OsUntrusted ArrayList<@OsUntrusted ByteBuffer>();
    final @OsUntrusted List<@OsUntrusted ByteBuffer> inactive = new @OsUntrusted LinkedList<@OsUntrusted ByteBuffer>();
    @OsUntrusted
    int size;
    @OsUntrusted
    int length;
    @OsUntrusted
    ByteBuffer current;

    @OsUntrusted
    Buffer(@OsUntrusted int size, @OsUntrusted boolean direct) {
      this.direct = direct;
      this.size = size;
      current = direct
          ? ByteBuffer.allocateDirect(size)
          : ByteBuffer.allocate(size);
    }
    @Override
    public void write(DataOutputByteBuffer.@OsUntrusted Buffer this, @OsUntrusted int b) {
      this.b[0] = (@OsUntrusted byte)(b & 0xFF);
      write(this.b);
    }
    @Override
    public void write(DataOutputByteBuffer.@OsUntrusted Buffer this, @OsUntrusted byte @OsUntrusted [] b) {
      write(b, 0, b.length);
    }
    @Override
    public void write(DataOutputByteBuffer.@OsUntrusted Buffer this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
      @OsUntrusted
      int rem = current.remaining();
      while (len > rem) {
        current.put(b, off, rem);
        length += rem;
        current.flip();
        active.add(current);
        off += rem;
        len -= rem;
        rem = getBuffer(len);
      }
      current.put(b, off, len);
      length += len;
    }
    @OsUntrusted
    int getBuffer(DataOutputByteBuffer.@OsUntrusted Buffer this, @OsUntrusted int newsize) {
      if (inactive.isEmpty()) {
        size = Math.max(size << 1, newsize);
        current = direct
            ? ByteBuffer.allocateDirect(size)
            : ByteBuffer.allocate(size);
      } else {
        current = inactive.remove(0);
      }
      return current.remaining();
    }
    @OsUntrusted
    ByteBuffer @OsUntrusted [] getData(DataOutputByteBuffer.@OsUntrusted Buffer this) {
      @OsUntrusted
      ByteBuffer @OsUntrusted [] ret = active.toArray(new @OsUntrusted ByteBuffer @OsUntrusted [active.size() + 1]);
      @OsUntrusted
      ByteBuffer tmp = current.duplicate();
      tmp.flip();
      ret[ret.length - 1] = tmp.slice();
      return ret;
    }
    @OsUntrusted
    int getLength(DataOutputByteBuffer.@OsUntrusted Buffer this) {
      return length;
    }
    void reset(DataOutputByteBuffer.@OsUntrusted Buffer this) {
      length = 0;
      current.rewind();
      inactive.add(0, current);
      for (@OsUntrusted int i = active.size() - 1; i >= 0; --i) {
        @OsUntrusted
        ByteBuffer b = active.remove(i);
        b.rewind();
        inactive.add(0, b);
      }
      current = inactive.remove(0);
    }
  }

  private final @OsUntrusted Buffer buffers;

  public @OsUntrusted DataOutputByteBuffer() {
    this(32);
  }

  public @OsUntrusted DataOutputByteBuffer(@OsUntrusted int size) {
    this(size, false);
  }

  public @OsUntrusted DataOutputByteBuffer(@OsUntrusted int size, @OsUntrusted boolean direct) {
    this(new @OsUntrusted Buffer(size, direct));
  }

  private @OsUntrusted DataOutputByteBuffer(@OsUntrusted Buffer buffers) {
    super(buffers);
    this.buffers = buffers;
  }

  public @OsUntrusted ByteBuffer @OsUntrusted [] getData(@OsUntrusted DataOutputByteBuffer this) {
    return buffers.getData();
  }

  public @OsUntrusted int getLength(@OsUntrusted DataOutputByteBuffer this) {
    return buffers.getLength();
  }

  public void reset(@OsUntrusted DataOutputByteBuffer this) {
    this.written = 0;
    buffers.reset();
  }

}
