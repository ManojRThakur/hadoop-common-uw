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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@link Compressor} based on the popular 
 * bzip2 compression algorithm.
 * http://www.bzip2.org/
 * 
 */
public class Bzip2Compressor implements @OsUntrusted Compressor {
  private static final @OsUntrusted int DEFAULT_DIRECT_BUFFER_SIZE = 64*1024;

  // The default values for the block size and work factor are the same 
  // those in Julian Seward's original bzip2 implementation.
  static final @OsUntrusted int DEFAULT_BLOCK_SIZE = 9;
  static final @OsUntrusted int DEFAULT_WORK_FACTOR = 30;

  private static final @OsUntrusted Log LOG = LogFactory.getLog(Bzip2Compressor.class);

  // HACK - Use this as a global lock in the JNI layer.
  private static @OsUntrusted Class<@OsUntrusted Bzip2Compressor> clazz = Bzip2Compressor.class;

  private @OsUntrusted long stream;
  private @OsUntrusted int blockSize;
  private @OsUntrusted int workFactor;
  private @OsUntrusted int directBufferSize;
  private @OsUntrusted byte @OsUntrusted [] userBuf = null;
  private @OsUntrusted int userBufOff = 0, userBufLen = 0;
  private @OsUntrusted Buffer uncompressedDirectBuf = null;
  private @OsUntrusted int uncompressedDirectBufOff = 0, uncompressedDirectBufLen = 0;
  private @OsUntrusted boolean keepUncompressedBuf = false;
  private @OsUntrusted Buffer compressedDirectBuf = null;
  private @OsUntrusted boolean finish, finished;

  /**
   * Creates a new compressor with a default values for the
   * compression block size and work factor.  Compressed data will be
   * generated in bzip2 format.
   */
  public @OsUntrusted Bzip2Compressor() {
    this(DEFAULT_BLOCK_SIZE, DEFAULT_WORK_FACTOR, DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /**
   * Creates a new compressor, taking settings from the configuration.
   */
  public @OsUntrusted Bzip2Compressor(@OsUntrusted Configuration conf) {
    this(Bzip2Factory.getBlockSize(conf),
         Bzip2Factory.getWorkFactor(conf),
         DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /** 
   * Creates a new compressor using the specified block size.
   * Compressed data will be generated in bzip2 format.
   * 
   * @param blockSize The block size to be used for compression.  This is
   *        an integer from 1 through 9, which is multiplied by 100,000 to 
   *        obtain the actual block size in bytes.
   * @param workFactor This parameter is a threshold that determines when a 
   *        fallback algorithm is used for pathological data.  It ranges from
   *        0 to 250.
   * @param directBufferSize Size of the direct buffer to be used.
   */
  public @OsUntrusted Bzip2Compressor(@OsUntrusted int blockSize, @OsUntrusted int workFactor, 
                         @OsUntrusted
                         int directBufferSize) {
    this.blockSize = blockSize;
    this.workFactor = workFactor;
    this.directBufferSize = directBufferSize;
    stream = init(blockSize, workFactor);
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
  }

  /**
   * Prepare the compressor to be used in a new stream with settings defined in
   * the given Configuration. It will reset the compressor's block size and
   * and work factor.
   * 
   * @param conf Configuration storing new settings
   */
  @Override
  public synchronized void reinit(@OsUntrusted Bzip2Compressor this, @OsUntrusted Configuration conf) {
    reset();
    end(stream);
    if (conf == null) {
      stream = init(blockSize, workFactor);
      return;
    }
    blockSize = Bzip2Factory.getBlockSize(conf);
    workFactor = Bzip2Factory.getWorkFactor(conf);
    stream = init(blockSize, workFactor);
    if(LOG.isDebugEnabled()) {
      LOG.debug("Reinit compressor with new compression configuration");
    }
  }

  @Override
  public synchronized void setInput(@OsUntrusted Bzip2Compressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
    if (b == null) {
      throw new @OsUntrusted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @OsUntrusted ArrayIndexOutOfBoundsException();
    }
    
    this.userBuf = b;
    this.userBufOff = off;
    this.userBufLen = len;
    uncompressedDirectBufOff = 0;
    setInputFromSavedData();
    
    // Reinitialize bzip2's output direct buffer.
    compressedDirectBuf.limit(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
  }
  
  // Copy enough data from userBuf to uncompressedDirectBuf.
  synchronized void setInputFromSavedData(@OsUntrusted Bzip2Compressor this) {
    @OsUntrusted
    int len = Math.min(userBufLen, uncompressedDirectBuf.remaining());
    ((@OsUntrusted ByteBuffer)uncompressedDirectBuf).put(userBuf, userBufOff, len);
    userBufLen -= len;
    userBufOff += len;
    uncompressedDirectBufLen = uncompressedDirectBuf.position();
  }

  @Override
  public synchronized void setDictionary(@OsUntrusted Bzip2Compressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public synchronized @OsUntrusted boolean needsInput(@OsUntrusted Bzip2Compressor this) {
    // Compressed data still available?
    if (compressedDirectBuf.remaining() > 0) {
      return false;
    }

    // Uncompressed data available in either the direct buffer or user buffer?
    if (keepUncompressedBuf && uncompressedDirectBufLen > 0)
      return false;
    
    if (uncompressedDirectBuf.remaining() > 0) {
      // Check if we have consumed all data in the user buffer.
      if (userBufLen <= 0) {
        return true;
      } else {
        // Copy enough data from userBuf to uncompressedDirectBuf.
        setInputFromSavedData();
        return uncompressedDirectBuf.remaining() > 0;
      }
    }
    
    return false;
  }
  
  @Override
  public synchronized void finish(@OsUntrusted Bzip2Compressor this) {
    finish = true;
  }
  
  @Override
  public synchronized @OsUntrusted boolean finished(@OsUntrusted Bzip2Compressor this) {
    // Check if bzip2 says it has finished and
    // all compressed data has been consumed.
    return (finished && compressedDirectBuf.remaining() == 0);
  }

  @Override
  public synchronized @OsUntrusted int compress(@OsUntrusted Bzip2Compressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) 
    throws IOException {
    if (b == null) {
      throw new @OsUntrusted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @OsUntrusted ArrayIndexOutOfBoundsException();
    }
    
    // Check if there is compressed data.
    @OsUntrusted
    int n = compressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((@OsUntrusted ByteBuffer)compressedDirectBuf).get(b, off, n);
      return n;
    }

    // Re-initialize bzip2's output direct buffer.
    compressedDirectBuf.rewind();
    compressedDirectBuf.limit(directBufferSize);

    // Compress the data.
    n = deflateBytesDirect();
    compressedDirectBuf.limit(n);
    
    // Check if bzip2 has consumed the entire input buffer.
    // Set keepUncompressedBuf properly.
    if (uncompressedDirectBufLen <= 0) { // bzip2 consumed all input
      keepUncompressedBuf = false;
      uncompressedDirectBuf.clear();
      uncompressedDirectBufOff = 0;
      uncompressedDirectBufLen = 0;
    } else {
      keepUncompressedBuf = true;
    }

    // Get at most 'len' bytes.
    n = Math.min(n, len);
    ((@OsUntrusted ByteBuffer)compressedDirectBuf).get(b, off, n);

    return n;
  }

  /**
   * Returns the total number of compressed bytes output so far.
   *
   * @return the total (non-negative) number of compressed bytes output so far
   */
  @Override
  public synchronized @OsUntrusted long getBytesWritten(@OsUntrusted Bzip2Compressor this) {
    checkStream();
    return getBytesWritten(stream);
  }

  /**
   * Returns the total number of uncompressed bytes input so far.</p>
   *
   * @return the total (non-negative) number of uncompressed bytes input so far
   */
  @Override
  public synchronized @OsUntrusted long getBytesRead(@OsUntrusted Bzip2Compressor this) {
    checkStream();
    return getBytesRead(stream);
  }

  @Override
  public synchronized void reset(@OsUntrusted Bzip2Compressor this) {
    checkStream();
    end(stream);
    stream = init(blockSize, workFactor);
    finish = false;
    finished = false;
    uncompressedDirectBuf.rewind();
    uncompressedDirectBufOff = uncompressedDirectBufLen = 0;
    keepUncompressedBuf = false;
    compressedDirectBuf.limit(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;
  }
  
  @Override
  public synchronized void end(@OsUntrusted Bzip2Compressor this) {
    if (stream != 0) {
      end(stream);
      stream = 0;
    }
  }
  
  static void initSymbols(@OsUntrusted String libname) {
    initIDs(libname);
  }

  private void checkStream(@OsUntrusted Bzip2Compressor this) {
    if (stream == 0)
      throw new @OsUntrusted NullPointerException();
  }
  
  private native static void initIDs(@OsUntrusted String libname);
  private native static @OsUntrusted long init(@OsUntrusted int blockSize, @OsUntrusted int workFactor);
  private native @OsUntrusted int deflateBytesDirect(@OsUntrusted Bzip2Compressor this);
  private native static @OsUntrusted long getBytesRead(@OsUntrusted long strm);
  private native static @OsUntrusted long getBytesWritten(@OsUntrusted long strm);
  private native static void end(@OsUntrusted long strm);

  public native static @OsUntrusted String getLibraryName();
}
