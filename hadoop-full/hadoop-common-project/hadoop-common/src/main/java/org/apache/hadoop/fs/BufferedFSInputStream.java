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
import java.io.BufferedInputStream;
import java.io.FileDescriptor;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/**
 * A class optimizes reading from FSInputStream by bufferring
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class BufferedFSInputStream extends @OsUntrusted BufferedInputStream
implements @OsUntrusted Seekable, @OsUntrusted PositionedReadable, @OsUntrusted HasFileDescriptor {
  /**
   * Creates a <code>BufferedFSInputStream</code>
   * with the specified buffer size,
   * and saves its  argument, the input stream
   * <code>in</code>, for later use.  An internal
   * buffer array of length  <code>size</code>
   * is created and stored in <code>buf</code>.
   *
   * @param   in     the underlying input stream.
   * @param   size   the buffer size.
   * @exception IllegalArgumentException if size <= 0.
   */
  public @OsUntrusted BufferedFSInputStream(@OsUntrusted FSInputStream in, @OsUntrusted int size) {
    super(in, size);
  }

  @Override
  public @OsUntrusted long getPos(@OsUntrusted BufferedFSInputStream this) throws IOException {
    return ((@OsUntrusted FSInputStream)in).getPos()-(count-pos);
  }

  @Override
  public @OsUntrusted long skip(@OsUntrusted BufferedFSInputStream this, @OsUntrusted long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    seek(getPos()+n);
    return n;
  }

  @Override
  public void seek(@OsUntrusted BufferedFSInputStream this, @OsUntrusted long pos) throws IOException {
    if( pos<0 ) {
      return;
    }
    if (this.pos != this.count) {
      // optimize: check if the pos is in the buffer
      // This optimization only works if pos != count -- if they are
      // equal, it's possible that the previous reads were just
      // longer than the total buffer size, and hence skipped the buffer.
      @OsUntrusted
      long end = ((@OsUntrusted FSInputStream)in).getPos();
      @OsUntrusted
      long start = end - count;
      if( pos>=start && pos<end) {
        this.pos = (@OsUntrusted int)(pos-start);
        return;
      }
    }

    // invalidate buffer
    this.pos = 0;
    this.count = 0;

    ((@OsUntrusted FSInputStream)in).seek(pos);
  }

  @Override
  public @OsUntrusted boolean seekToNewSource(@OsUntrusted BufferedFSInputStream this, @OsUntrusted long targetPos) throws IOException {
    pos = 0;
    count = 0;
    return ((@OsUntrusted FSInputStream)in).seekToNewSource(targetPos);
  }

  @Override
  public @OsUntrusted int read(@OsUntrusted BufferedFSInputStream this, @OsUntrusted long position, @OsUntrusted byte @OsUntrusted [] buffer, @OsUntrusted int offset, @OsUntrusted int length) throws IOException {
    return ((@OsUntrusted FSInputStream)in).read(position, buffer, offset, length) ;
  }

  @Override
  public void readFully(@OsUntrusted BufferedFSInputStream this, @OsUntrusted long position, @OsUntrusted byte @OsUntrusted [] buffer, @OsUntrusted int offset, @OsUntrusted int length) throws IOException {
    ((@OsUntrusted FSInputStream)in).readFully(position, buffer, offset, length);
  }

  @Override
  public void readFully(@OsUntrusted BufferedFSInputStream this, @OsUntrusted long position, @OsUntrusted byte @OsUntrusted [] buffer) throws IOException {
    ((@OsUntrusted FSInputStream)in).readFully(position, buffer);
  }

  @Override
  public @OsUntrusted FileDescriptor getFileDescriptor(@OsUntrusted BufferedFSInputStream this) throws IOException {
    if (in instanceof @OsUntrusted HasFileDescriptor) {
      return ((@OsUntrusted HasFileDescriptor) in).getFileDescriptor();
    } else {
      return null;
    }
  }
}
