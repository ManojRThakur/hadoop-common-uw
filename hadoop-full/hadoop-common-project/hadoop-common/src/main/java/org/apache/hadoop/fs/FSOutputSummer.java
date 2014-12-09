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
import java.io.OutputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is a generic output stream for generating checksums for
 * data before it is written to the underlying stream
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
abstract public class FSOutputSummer extends @OsUntrusted OutputStream {
  // data checksum
  private @OsUntrusted Checksum sum;
  // internal buffer for storing data before it is checksumed
  private @OsUntrusted byte buf @OsUntrusted [];
  // internal buffer for storing checksum
  private @OsUntrusted byte checksum @OsUntrusted [];
  // The number of valid bytes in the buffer.
  private @OsUntrusted int count;
  
  protected @OsUntrusted FSOutputSummer(@OsUntrusted Checksum sum, @OsUntrusted int maxChunkSize, @OsUntrusted int checksumSize) {
    this.sum = sum;
    this.buf = new @OsUntrusted byte @OsUntrusted [maxChunkSize];
    this.checksum = new @OsUntrusted byte @OsUntrusted [checksumSize];
    this.count = 0;
  }
  
  /* write the data chunk in <code>b</code> staring at <code>offset</code> with
   * a length of <code>len</code>, and its checksum
   */
  protected abstract void writeChunk(@OsUntrusted FSOutputSummer this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int offset, @OsUntrusted int len, @OsUntrusted byte @OsUntrusted [] checksum)
  throws IOException;
  
  /**
   * Check if the implementing OutputStream is closed and should no longer
   * accept writes. Implementations should do nothing if this stream is not
   * closed, and should throw an {@link IOException} if it is closed.
   * 
   * @throws IOException if this stream is already closed.
   */
  protected abstract void checkClosed(@OsUntrusted FSOutputSummer this) throws IOException;

  /** Write one byte */
  @Override
  public synchronized void write(@OsUntrusted FSOutputSummer this, @OsUntrusted int b) throws IOException {
    sum.update(b);
    buf[count++] = (@OsUntrusted byte)b;
    if(count == buf.length) {
      flushBuffer();
    }
  }

  /**
   * Writes <code>len</code> bytes from the specified byte array 
   * starting at offset <code>off</code> and generate a checksum for
   * each data chunk.
   *
   * <p> This method stores bytes from the given array into this
   * stream's buffer before it gets checksumed. The buffer gets checksumed 
   * and flushed to the underlying output stream when all data 
   * in a checksum chunk are in the buffer.  If the buffer is empty and
   * requested length is at least as large as the size of next checksum chunk
   * size, this method will checksum and write the chunk directly 
   * to the underlying output stream.  Thus it avoids uneccessary data copy.
   *
   * @param      b     the data.
   * @param      off   the start offset in the data.
   * @param      len   the number of bytes to write.
   * @exception  IOException  if an I/O error occurs.
   */
  @Override
  public synchronized void write(@OsUntrusted FSOutputSummer this, @OsUntrusted byte b @OsUntrusted [], @OsUntrusted int off, @OsUntrusted int len)
      throws IOException {
    
    checkClosed();
    
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @OsUntrusted ArrayIndexOutOfBoundsException();
    }

    for (@OsUntrusted int n=0;n<len;n+=write1(b, off+n, len-n)) {
    }
  }
  
  /**
   * Write a portion of an array, flushing to the underlying
   * stream at most once if necessary.
   */
  private @OsUntrusted int write1(@OsUntrusted FSOutputSummer this, @OsUntrusted byte b @OsUntrusted [], @OsUntrusted int off, @OsUntrusted int len) throws IOException {
    if(count==0 && len>=buf.length) {
      // local buffer is empty and user data has one chunk
      // checksum and output data
      final @OsUntrusted int length = buf.length;
      sum.update(b, off, length);
      writeChecksumChunk(b, off, length, false);
      return length;
    }
    
    // copy user data to local buffer
    @OsUntrusted
    int bytesToCopy = buf.length-count;
    bytesToCopy = (len<bytesToCopy) ? len : bytesToCopy;
    sum.update(b, off, bytesToCopy);
    System.arraycopy(b, off, buf, count, bytesToCopy);
    count += bytesToCopy;
    if (count == buf.length) {
      // local buffer is full
      flushBuffer();
    } 
    return bytesToCopy;
  }

  /* Forces any buffered output bytes to be checksumed and written out to
   * the underlying output stream. 
   */
  protected synchronized void flushBuffer(@OsUntrusted FSOutputSummer this) throws IOException {
    flushBuffer(false);
  }

  /* Forces any buffered output bytes to be checksumed and written out to
   * the underlying output stream.  If keep is true, then the state of 
   * this object remains intact.
   */
  protected synchronized void flushBuffer(@OsUntrusted FSOutputSummer this, @OsUntrusted boolean keep) throws IOException {
    if (count != 0) {
      @OsUntrusted
      int chunkLen = count;
      count = 0;
      writeChecksumChunk(buf, 0, chunkLen, keep);
      if (keep) {
        count = chunkLen;
      }
    }
  }

  /**
   * Return the number of valid bytes currently in the buffer.
   */
  protected synchronized @OsUntrusted int getBufferedDataSize(@OsUntrusted FSOutputSummer this) {
    return count;
  }
  
  /** Generate checksum for the data chunk and output data chunk & checksum
   * to the underlying output stream. If keep is true then keep the
   * current checksum intact, do not reset it.
   */
  private void writeChecksumChunk(@OsUntrusted FSOutputSummer this, @OsUntrusted byte b @OsUntrusted [], @OsUntrusted int off, @OsUntrusted int len, @OsUntrusted boolean keep)
  throws IOException {
    @OsUntrusted
    int tempChecksum = (@OsUntrusted int)sum.getValue();
    if (!keep) {
      sum.reset();
    }
    int2byte(tempChecksum, checksum);
    writeChunk(b, off, len, checksum);
  }

  /**
   * Converts a checksum integer value to a byte stream
   */
  static public @OsUntrusted byte @OsUntrusted [] convertToByteStream(@OsUntrusted Checksum sum, @OsUntrusted int checksumSize) {
    return int2byte((@OsUntrusted int)sum.getValue(), new @OsUntrusted byte @OsUntrusted [checksumSize]);
  }

  static @OsUntrusted byte @OsUntrusted [] int2byte(@OsUntrusted int integer, @OsUntrusted byte @OsUntrusted [] bytes) {
    bytes[0] = (@OsUntrusted byte)((integer >>> 24) & 0xFF);
    bytes[1] = (@OsUntrusted byte)((integer >>> 16) & 0xFF);
    bytes[2] = (@OsUntrusted byte)((integer >>>  8) & 0xFF);
    bytes[3] = (@OsUntrusted byte)((integer >>>  0) & 0xFF);
    return bytes;
  }

  /**
   * Resets existing buffer with a new one of the specified size.
   */
  protected synchronized void resetChecksumChunk(@OsUntrusted FSOutputSummer this, @OsUntrusted int size) {
    sum.reset();
    this.buf = new @OsUntrusted byte @OsUntrusted [size];
    this.count = 0;
  }
}
