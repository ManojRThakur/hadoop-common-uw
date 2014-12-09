/*
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

package org.apache.hadoop.io.compress.bzip2;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.compress.Decompressor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@link Decompressor} based on the popular 
 * bzip2 compression algorithm.
 * http://www.bzip2.org/
 * 
 */
public class Bzip2Decompressor implements @OsUntrusted Decompressor {
  private static final @OsUntrusted int DEFAULT_DIRECT_BUFFER_SIZE = 64*1024;
  
  private static final @OsUntrusted Log LOG = LogFactory.getLog(Bzip2Decompressor.class);

  // HACK - Use this as a global lock in the JNI layer.
  private static @OsUntrusted Class<@OsUntrusted Bzip2Decompressor> clazz = Bzip2Decompressor.class;
  
  private @OsUntrusted long stream;
  private @OsUntrusted boolean conserveMemory;
  private @OsUntrusted int directBufferSize;
  private @OsUntrusted Buffer compressedDirectBuf = null;
  private @OsUntrusted int compressedDirectBufOff;
  private @OsUntrusted int compressedDirectBufLen;
  private @OsUntrusted Buffer uncompressedDirectBuf = null;
  private @OsUntrusted byte @OsUntrusted [] userBuf = null;
  private @OsUntrusted int userBufOff = 0;
  private @OsUntrusted int userBufLen = 0;
  private @OsUntrusted boolean finished;

  /**
   * Creates a new decompressor.
   */
  public @OsUntrusted Bzip2Decompressor(@OsUntrusted boolean conserveMemory, @OsUntrusted int directBufferSize) {
    this.conserveMemory = conserveMemory;
    this.directBufferSize = directBufferSize;
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    
    stream = init(conserveMemory ? 1 : 0);
  }
  
  public @OsUntrusted Bzip2Decompressor() {
    this(false, DEFAULT_DIRECT_BUFFER_SIZE);
  }

  @Override
  public synchronized void setInput(@OsUntrusted Bzip2Decompressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
    if (b == null) {
      throw new @OsUntrusted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @OsUntrusted ArrayIndexOutOfBoundsException();
    }
  
    this.userBuf = b;
    this.userBufOff = off;
    this.userBufLen = len;
    
    setInputFromSavedData();
    
    // Reinitialize bzip2's output direct buffer.
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
  }
  
  synchronized void setInputFromSavedData(@OsUntrusted Bzip2Decompressor this) {
    compressedDirectBufOff = 0;
    compressedDirectBufLen = userBufLen;
    if (compressedDirectBufLen > directBufferSize) {
      compressedDirectBufLen = directBufferSize;
    }

    // Reinitialize bzip2's input direct buffer.
    compressedDirectBuf.rewind();
    ((@OsUntrusted ByteBuffer)compressedDirectBuf).put(userBuf, userBufOff, 
                                          compressedDirectBufLen);
    
    // Note how much data is being fed to bzip2.
    userBufOff += compressedDirectBufLen;
    userBufLen -= compressedDirectBufLen;
  }

  @Override
  public synchronized void setDictionary(@OsUntrusted Bzip2Decompressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public synchronized @OsUntrusted boolean needsInput(@OsUntrusted Bzip2Decompressor this) {
    // Consume remaining compressed data?
    if (uncompressedDirectBuf.remaining() > 0) {
      return false;
    }
    
    // Check if bzip2 has consumed all input.
    if (compressedDirectBufLen <= 0) {
      // Check if we have consumed all user-input.
      if (userBufLen <= 0) {
        return true;
      } else {
        setInputFromSavedData();
      }
    }
    
    return false;
  }

  @Override
  public synchronized @OsUntrusted boolean needsDictionary(@OsUntrusted Bzip2Decompressor this) {
    return false;
  }

  @Override
  public synchronized @OsUntrusted boolean finished(@OsUntrusted Bzip2Decompressor this) {
    // Check if bzip2 says it has finished and
    // all compressed data has been consumed.
    return (finished && uncompressedDirectBuf.remaining() == 0);
  }

  @Override
  public synchronized @OsUntrusted int decompress(@OsUntrusted Bzip2Decompressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) 
    throws IOException {
    if (b == null) {
      throw new @OsUntrusted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @OsUntrusted ArrayIndexOutOfBoundsException();
    }
    
    // Check if there is uncompressed data.
    @OsUntrusted
    int n = uncompressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((@OsUntrusted ByteBuffer)uncompressedDirectBuf).get(b, off, n);
      return n;
    }
    
    // Re-initialize bzip2's output direct buffer.
    uncompressedDirectBuf.rewind();
    uncompressedDirectBuf.limit(directBufferSize);

    // Decompress the data.
    n = finished ? 0 : inflateBytesDirect();
    uncompressedDirectBuf.limit(n);

    // Get at most 'len' bytes.
    n = Math.min(n, len);
    ((@OsUntrusted ByteBuffer)uncompressedDirectBuf).get(b, off, n);

    return n;
  }
  
  /**
   * Returns the total number of uncompressed bytes output so far.
   *
   * @return the total (non-negative) number of uncompressed bytes output so far
   */
  public synchronized @OsUntrusted long getBytesWritten(@OsUntrusted Bzip2Decompressor this) {
    checkStream();
    return getBytesWritten(stream);
  }

  /**
   * Returns the total number of compressed bytes input so far.</p>
   *
   * @return the total (non-negative) number of compressed bytes input so far
   */
  public synchronized @OsUntrusted long getBytesRead(@OsUntrusted Bzip2Decompressor this) {
    checkStream();
    return getBytesRead(stream);
  }

  /**
   * Returns the number of bytes remaining in the input buffers; normally
   * called when finished() is true to determine amount of post-gzip-stream
   * data.</p>
   *
   * @return the total (non-negative) number of unprocessed bytes in input
   */
  @Override
  public synchronized @OsUntrusted int getRemaining(@OsUntrusted Bzip2Decompressor this) {
    checkStream();
    return userBufLen + getRemaining(stream);  // userBuf + compressedDirectBuf
  }

  /**
   * Resets everything including the input buffers (user and direct).</p>
   */
  @Override
  public synchronized void reset(@OsUntrusted Bzip2Decompressor this) {
    checkStream();
    end(stream);
    stream = init(conserveMemory ? 1 : 0);
    finished = false;
    compressedDirectBufOff = compressedDirectBufLen = 0;
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;
  }

  @Override
  public synchronized void end(@OsUntrusted Bzip2Decompressor this) {
    if (stream != 0) {
      end(stream);
      stream = 0;
    }
  }

  static void initSymbols(@OsUntrusted String libname) {
    initIDs(libname);
  }

  private void checkStream(@OsUntrusted Bzip2Decompressor this) {
    if (stream == 0)
      throw new @OsUntrusted NullPointerException();
  }
  
  private native static void initIDs(@OsUntrusted String libname);
  private native static @OsUntrusted long init(@OsUntrusted int conserveMemory);
  private native @OsUntrusted int inflateBytesDirect(@OsUntrusted Bzip2Decompressor this);
  private native static @OsUntrusted long getBytesRead(@OsUntrusted long strm);
  private native static @OsUntrusted long getBytesWritten(@OsUntrusted long strm);
  private native static @OsUntrusted int getRemaining(@OsUntrusted long strm);
  private native static void end(@OsUntrusted long strm);
}
