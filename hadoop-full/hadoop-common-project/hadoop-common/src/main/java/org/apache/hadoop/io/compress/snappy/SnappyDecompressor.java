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

package org.apache.hadoop.io.compress.snappy;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * A {@link Decompressor} based on the snappy compression algorithm.
 * http://code.google.com/p/snappy/
 */
public class SnappyDecompressor implements @OsUntrusted Decompressor {
  private static final @OsUntrusted Log LOG =
      LogFactory.getLog(SnappyCompressor.class.getName());
  private static final @OsUntrusted int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

  // HACK - Use this as a global lock in the JNI layer
  @SuppressWarnings({"unchecked", "unused"})
  private static @OsUntrusted Class clazz = SnappyDecompressor.class;

  private @OsUntrusted int directBufferSize;
  private @OsUntrusted Buffer compressedDirectBuf = null;
  private @OsUntrusted int compressedDirectBufLen;
  private @OsUntrusted Buffer uncompressedDirectBuf = null;
  private @OsUntrusted byte @OsUntrusted [] userBuf = null;
  private @OsUntrusted int userBufOff = 0, userBufLen = 0;
  private @OsUntrusted boolean finished;

  private static @OsUntrusted boolean nativeSnappyLoaded = false;

  static {
    if (NativeCodeLoader.isNativeCodeLoaded() &&
        NativeCodeLoader.buildSupportsSnappy()) {
      try {
        initIDs();
        nativeSnappyLoaded = true;
      } catch (@OsUntrusted Throwable t) {
        LOG.error("failed to load SnappyDecompressor", t);
      }
    }
  }
  
  public static @OsUntrusted boolean isNativeCodeLoaded() {
    return nativeSnappyLoaded;
  }
  
  /**
   * Creates a new compressor.
   *
   * @param directBufferSize size of the direct buffer to be used.
   */
  public @OsUntrusted SnappyDecompressor(@OsUntrusted int directBufferSize) {
    this.directBufferSize = directBufferSize;

    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);

  }

  /**
   * Creates a new decompressor with the default buffer size.
   */
  public @OsUntrusted SnappyDecompressor() {
    this(DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /**
   * Sets input data for decompression.
   * This should be called if and only if {@link #needsInput()} returns
   * <code>true</code> indicating that more input data is required.
   * (Both native and non-native versions of various Decompressors require
   * that the data passed in via <code>b[]</code> remain unmodified until
   * the caller is explicitly notified--via {@link #needsInput()}--that the
   * buffer may be safely modified.  With this requirement, an extra
   * buffer-copy can be avoided.)
   *
   * @param b   Input data
   * @param off Start offset
   * @param len Length
   */
  @Override
  public synchronized void setInput(@OsUntrusted SnappyDecompressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
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

    // Reinitialize snappy's output direct-buffer
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
  }

  /**
   * If a write would exceed the capacity of the direct buffers, it is set
   * aside to be loaded by this function while the compressed data are
   * consumed.
   */
  synchronized void setInputFromSavedData(@OsUntrusted SnappyDecompressor this) {
    compressedDirectBufLen = Math.min(userBufLen, directBufferSize);

    // Reinitialize snappy's input direct buffer
    compressedDirectBuf.rewind();
    ((@OsUntrusted ByteBuffer) compressedDirectBuf).put(userBuf, userBufOff,
        compressedDirectBufLen);

    // Note how much data is being fed to snappy
    userBufOff += compressedDirectBufLen;
    userBufLen -= compressedDirectBufLen;
  }

  /**
   * Does nothing.
   */
  @Override
  public synchronized void setDictionary(@OsUntrusted SnappyDecompressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
    // do nothing
  }

  /**
   * Returns true if the input data buffer is empty and
   * {@link #setInput(byte[], int, int)} should be called to
   * provide more input.
   *
   * @return <code>true</code> if the input data buffer is empty and
   *         {@link #setInput(byte[], int, int)} should be called in
   *         order to provide more input.
   */
  @Override
  public synchronized @OsUntrusted boolean needsInput(@OsUntrusted SnappyDecompressor this) {
    // Consume remaining compressed data?
    if (uncompressedDirectBuf.remaining() > 0) {
      return false;
    }

    // Check if snappy has consumed all input
    if (compressedDirectBufLen <= 0) {
      // Check if we have consumed all user-input
      if (userBufLen <= 0) {
        return true;
      } else {
        setInputFromSavedData();
      }
    }

    return false;
  }

  /**
   * Returns <code>false</code>.
   *
   * @return <code>false</code>.
   */
  @Override
  public synchronized @OsUntrusted boolean needsDictionary(@OsUntrusted SnappyDecompressor this) {
    return false;
  }

  /**
   * Returns true if the end of the decompressed
   * data output stream has been reached.
   *
   * @return <code>true</code> if the end of the decompressed
   *         data output stream has been reached.
   */
  @Override
  public synchronized @OsUntrusted boolean finished(@OsUntrusted SnappyDecompressor this) {
    return (finished && uncompressedDirectBuf.remaining() == 0);
  }

  /**
   * Fills specified buffer with uncompressed data. Returns actual number
   * of bytes of uncompressed data. A return value of 0 indicates that
   * {@link #needsInput()} should be called in order to determine if more
   * input data is required.
   *
   * @param b   Buffer for the compressed data
   * @param off Start offset of the data
   * @param len Size of the buffer
   * @return The actual number of bytes of compressed data.
   * @throws IOException
   */
  @Override
  public synchronized @OsUntrusted int decompress(@OsUntrusted SnappyDecompressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len)
      throws IOException {
    if (b == null) {
      throw new @OsUntrusted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @OsUntrusted ArrayIndexOutOfBoundsException();
    }

    @OsUntrusted
    int n = 0;

    // Check if there is uncompressed data
    n = uncompressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((@OsUntrusted ByteBuffer) uncompressedDirectBuf).get(b, off, n);
      return n;
    }
    if (compressedDirectBufLen > 0) {
      // Re-initialize the snappy's output direct buffer
      uncompressedDirectBuf.rewind();
      uncompressedDirectBuf.limit(directBufferSize);

      // Decompress data
      n = decompressBytesDirect();
      uncompressedDirectBuf.limit(n);

      if (userBufLen <= 0) {
        finished = true;
      }

      // Get atmost 'len' bytes
      n = Math.min(n, len);
      ((@OsUntrusted ByteBuffer) uncompressedDirectBuf).get(b, off, n);
    }

    return n;
  }

  /**
   * Returns <code>0</code>.
   *
   * @return <code>0</code>.
   */
  @Override
  public synchronized @OsUntrusted int getRemaining(@OsUntrusted SnappyDecompressor this) {
    // Never use this function in BlockDecompressorStream.
    return 0;
  }

  @Override
  public synchronized void reset(@OsUntrusted SnappyDecompressor this) {
    finished = false;
    compressedDirectBufLen = 0;
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;
  }

  /**
   * Resets decompressor and input and output buffers so that a new set of
   * input data can be processed.
   */
  @Override
  public synchronized void end(@OsUntrusted SnappyDecompressor this) {
    // do nothing
  }

  private native static void initIDs();

  private native @OsUntrusted int decompressBytesDirect(@OsUntrusted SnappyDecompressor this);
}
