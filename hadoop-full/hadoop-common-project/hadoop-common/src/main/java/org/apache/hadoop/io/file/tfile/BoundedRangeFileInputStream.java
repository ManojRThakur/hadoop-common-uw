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
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 * BoundedRangeFIleInputStream abstracts a contiguous region of a Hadoop
 * FSDataInputStream as a regular input stream. One can create multiple
 * BoundedRangeFileInputStream on top of the same FSDataInputStream and they
 * would not interfere with each other.
 */
class BoundedRangeFileInputStream extends @OsUntrusted InputStream {

  private @OsUntrusted FSDataInputStream in;
  private @OsUntrusted long pos;
  private @OsUntrusted long end;
  private @OsUntrusted long mark;
  private final @OsUntrusted byte @OsUntrusted [] oneByte = new @OsUntrusted byte @OsUntrusted [1];

  /**
   * Constructor
   * 
   * @param in
   *          The FSDataInputStream we connect to.
   * @param offset
   *          Begining offset of the region.
   * @param length
   *          Length of the region.
   * 
   *          The actual length of the region may be smaller if (off_begin +
   *          length) goes beyond the end of FS input stream.
   */
  public @OsUntrusted BoundedRangeFileInputStream(@OsUntrusted FSDataInputStream in, @OsUntrusted long offset,
      @OsUntrusted
      long length) {
    if (offset < 0 || length < 0) {
      throw new @OsUntrusted IndexOutOfBoundsException("Invalid offset/length: " + offset
          + "/" + length);
    }

    this.in = in;
    this.pos = offset;
    this.end = offset + length;
    this.mark = -1;
  }

  @Override
  public @OsUntrusted int available(@OsUntrusted BoundedRangeFileInputStream this) throws IOException {
    @OsUntrusted
    int avail = in.available();
    if (pos + avail > end) {
      avail = (@OsUntrusted int) (end - pos);
    }

    return avail;
  }

  @Override
  public @OsUntrusted int read(@OsUntrusted BoundedRangeFileInputStream this) throws IOException {
    @OsUntrusted
    int ret = read(oneByte);
    if (ret == 1) return oneByte[0] & 0xff;
    return -1;
  }

  @Override
  public @OsUntrusted int read(@OsUntrusted BoundedRangeFileInputStream this, @OsUntrusted byte @OsUntrusted [] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public @OsUntrusted int read(@OsUntrusted BoundedRangeFileInputStream this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) throws IOException {
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new @OsUntrusted IndexOutOfBoundsException();
    }

    @OsUntrusted
    int n = (@OsUntrusted int) Math.min(Integer.MAX_VALUE, Math.min(len, (end - pos)));
    if (n == 0) return -1;
    @OsUntrusted
    int ret = 0;
    synchronized (in) {
      in.seek(pos);
      ret = in.read(b, off, n);
    }
    if (ret < 0) {
      end = pos;
      return -1;
    }
    pos += ret;
    return ret;
  }

  @Override
  /*
   * We may skip beyond the end of the file.
   */
  public @OsUntrusted long skip(@OsUntrusted BoundedRangeFileInputStream this, @OsUntrusted long n) throws IOException {
    @OsUntrusted
    long len = Math.min(n, end - pos);
    pos += len;
    return len;
  }

  @Override
  public synchronized void mark(@OsUntrusted BoundedRangeFileInputStream this, @OsUntrusted int readlimit) {
    mark = pos;
  }

  @Override
  public synchronized void reset(@OsUntrusted BoundedRangeFileInputStream this) throws IOException {
    if (mark < 0) throw new @OsUntrusted IOException("Resetting to invalid mark");
    pos = mark;
  }

  @Override
  public @OsUntrusted boolean markSupported(@OsUntrusted BoundedRangeFileInputStream this) {
    return true;
  }

  @Override
  public void close(@OsUntrusted BoundedRangeFileInputStream this) {
    // Invalidate the state of the stream.
    in = null;
    pos = end;
    mark = -1;
  }
}
