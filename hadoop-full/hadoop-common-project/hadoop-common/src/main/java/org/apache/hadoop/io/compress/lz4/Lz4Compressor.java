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

package org.apache.hadoop.io.compress.lz4;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * A {@link Compressor} based on the lz4 compression algorithm.
 * http://code.google.com/p/lz4/
 */
public class Lz4Compressor implements @OsUntrusted Compressor {
  private static final @OsUntrusted Log LOG =
      LogFactory.getLog(Lz4Compressor.class.getName());
  private static final @OsUntrusted int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

  // HACK - Use this as a global lock in the JNI layer
  @SuppressWarnings({"unchecked", "unused"})
  private static @OsUntrusted Class clazz = Lz4Compressor.class;

  private @OsUntrusted int directBufferSize;
  private @OsUntrusted Buffer compressedDirectBuf = null;
  private @OsUntrusted int uncompressedDirectBufLen;
  private @OsUntrusted Buffer uncompressedDirectBuf = null;
  private @OsUntrusted byte @OsUntrusted [] userBuf = null;
  private @OsUntrusted int userBufOff = 0;
  private @OsUntrusted int userBufLen = 0;
  private @OsUntrusted boolean finish;
  private @OsUntrusted boolean  finished;

  private @OsUntrusted long bytesRead = 0L;
  private @OsUntrusted long bytesWritten = 0L;

  private final @OsUntrusted boolean useLz4HC;

  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      // Initialize the native library
      try {
        initIDs();
      } catch (@OsUntrusted Throwable t) {
        // Ignore failure to load/initialize lz4
        LOG.warn(t.toString());
      }
    } else {
      LOG.error("Cannot load " + Lz4Compressor.class.getName() +
          " without native hadoop library!");
    }
  }

  /**
   * Creates a new compressor.
   *
   * @param directBufferSize size of the direct buffer to be used.
   * @param useLz4HC use high compression ratio version of lz4, 
   *                 which trades CPU for compression ratio.
   */
  public @OsUntrusted Lz4Compressor(@OsUntrusted int directBufferSize, @OsUntrusted boolean useLz4HC) {
    this.useLz4HC = useLz4HC;
    this.directBufferSize = directBufferSize;

    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
  }

  /**
   * Creates a new compressor.
   *
   * @param directBufferSize size of the direct buffer to be used.
   */
  public @OsUntrusted Lz4Compressor(@OsUntrusted int directBufferSize) {
    this(directBufferSize, false);
  }

  /**
   * Creates a new compressor with the default buffer size.
   */
  public @OsUntrusted Lz4Compressor() {
    this(DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /**
   * Sets input data for compression.
   * This should be called whenever #needsInput() returns
   * <code>true</code> indicating that more input data is required.
   *
   * @param b   Input data
   * @param off Start offset
   * @param len Length
   */
  @Override
  public synchronized void setInput(@OsUntrusted Lz4Compressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
    if (b == null) {
      throw new @OsUntrusted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @OsUntrusted ArrayIndexOutOfBoundsException();
    }
    finished = false;

    if (len > uncompressedDirectBuf.remaining()) {
      // save data; now !needsInput
      this.userBuf = b;
      this.userBufOff = off;
      this.userBufLen = len;
    } else {
      ((@OsUntrusted ByteBuffer) uncompressedDirectBuf).put(b, off, len);
      uncompressedDirectBufLen = uncompressedDirectBuf.position();
    }

    bytesRead += len;
  }

  /**
   * If a write would exceed the capacity of the direct buffers, it is set
   * aside to be loaded by this function while the compressed data are
   * consumed.
   */
  synchronized void setInputFromSavedData(@OsUntrusted Lz4Compressor this) {
    if (0 >= userBufLen) {
      return;
    }
    finished = false;

    uncompressedDirectBufLen = Math.min(userBufLen, directBufferSize);
    ((@OsUntrusted ByteBuffer) uncompressedDirectBuf).put(userBuf, userBufOff,
        uncompressedDirectBufLen);

    // Note how much data is being fed to lz4
    userBufOff += uncompressedDirectBufLen;
    userBufLen -= uncompressedDirectBufLen;
  }

  /**
   * Does nothing.
   */
  @Override
  public synchronized void setDictionary(@OsUntrusted Lz4Compressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
    // do nothing
  }

  /**
   * Returns true if the input data buffer is empty and
   * #setInput() should be called to provide more input.
   *
   * @return <code>true</code> if the input data buffer is empty and
   *         #setInput() should be called in order to provide more input.
   */
  @Override
  public synchronized @OsUntrusted boolean needsInput(@OsUntrusted Lz4Compressor this) {
    return !(compressedDirectBuf.remaining() > 0
        || uncompressedDirectBuf.remaining() == 0 || userBufLen > 0);
  }

  /**
   * When called, indicates that compression should end
   * with the current contents of the input buffer.
   */
  @Override
  public synchronized void finish(@OsUntrusted Lz4Compressor this) {
    finish = true;
  }

  /**
   * Returns true if the end of the compressed
   * data output stream has been reached.
   *
   * @return <code>true</code> if the end of the compressed
   *         data output stream has been reached.
   */
  @Override
  public synchronized @OsUntrusted boolean finished(@OsUntrusted Lz4Compressor this) {
    // Check if all uncompressed data has been consumed
    return (finish && finished && compressedDirectBuf.remaining() == 0);
  }

  /**
   * Fills specified buffer with compressed data. Returns actual number
   * of bytes of compressed data. A return value of 0 indicates that
   * needsInput() should be called in order to determine if more input
   * data is required.
   *
   * @param b   Buffer for the compressed data
   * @param off Start offset of the data
   * @param len Size of the buffer
   * @return The actual number of bytes of compressed data.
   */
  @Override
  public synchronized @OsUntrusted int compress(@OsUntrusted Lz4Compressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len)
      throws IOException {
    if (b == null) {
      throw new @OsUntrusted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @OsUntrusted ArrayIndexOutOfBoundsException();
    }

    // Check if there is compressed data
    @OsUntrusted
    int n = compressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((@OsUntrusted ByteBuffer) compressedDirectBuf).get(b, off, n);
      bytesWritten += n;
      return n;
    }

    // Re-initialize the lz4's output direct-buffer
    compressedDirectBuf.clear();
    compressedDirectBuf.limit(0);
    if (0 == uncompressedDirectBuf.position()) {
      // No compressed data, so we should have !needsInput or !finished
      setInputFromSavedData();
      if (0 == uncompressedDirectBuf.position()) {
        // Called without data; write nothing
        finished = true;
        return 0;
      }
    }

    // Compress data
    n = useLz4HC ? compressBytesDirectHC() : compressBytesDirect();
    compressedDirectBuf.limit(n);
    uncompressedDirectBuf.clear(); // lz4 consumes all buffer input

    // Set 'finished' if snapy has consumed all user-data
    if (0 == userBufLen) {
      finished = true;
    }

    // Get atmost 'len' bytes
    n = Math.min(n, len);
    bytesWritten += n;
    ((@OsUntrusted ByteBuffer) compressedDirectBuf).get(b, off, n);

    return n;
  }

  /**
   * Resets compressor so that a new set of input data can be processed.
   */
  @Override
  public synchronized void reset(@OsUntrusted Lz4Compressor this) {
    finish = false;
    finished = false;
    uncompressedDirectBuf.clear();
    uncompressedDirectBufLen = 0;
    compressedDirectBuf.clear();
    compressedDirectBuf.limit(0);
    userBufOff = userBufLen = 0;
    bytesRead = bytesWritten = 0L;
  }

  /**
   * Prepare the compressor to be used in a new stream with settings defined in
   * the given Configuration
   *
   * @param conf Configuration from which new setting are fetched
   */
  @Override
  public synchronized void reinit(@OsUntrusted Lz4Compressor this, @OsUntrusted Configuration conf) {
    reset();
  }

  /**
   * Return number of bytes given to this compressor since last reset.
   */
  @Override
  public synchronized @OsUntrusted long getBytesRead(@OsUntrusted Lz4Compressor this) {
    return bytesRead;
  }

  /**
   * Return number of bytes consumed by callers of compress since last reset.
   */
  @Override
  public synchronized @OsUntrusted long getBytesWritten(@OsUntrusted Lz4Compressor this) {
    return bytesWritten;
  }

  /**
   * Closes the compressor and discards any unprocessed input.
   */
  @Override
  public synchronized void end(@OsUntrusted Lz4Compressor this) {
  }

  private native static void initIDs();

  private native @OsUntrusted int compressBytesDirect(@OsUntrusted Lz4Compressor this);

  private native @OsUntrusted int compressBytesDirectHC(@OsUntrusted Lz4Compressor this);

  public native static @OsUntrusted String getLibraryName();
}
