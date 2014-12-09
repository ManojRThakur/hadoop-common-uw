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

package org.apache.hadoop.io.compress.zlib;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.NativeCodeLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@link Compressor} based on the popular 
 * zlib compression algorithm.
 * http://www.zlib.net/
 * 
 */
public class ZlibCompressor implements @OsUntrusted Compressor {

  private static final @OsUntrusted Log LOG = LogFactory.getLog(ZlibCompressor.class);

  private static final @OsUntrusted int DEFAULT_DIRECT_BUFFER_SIZE = 64*1024;

  // HACK - Use this as a global lock in the JNI layer
  private static @OsUntrusted Class clazz = ZlibCompressor.class;

  private @OsUntrusted long stream;
  private @OsUntrusted CompressionLevel level;
  private @OsUntrusted CompressionStrategy strategy;
  private final @OsUntrusted CompressionHeader windowBits;
  private @OsUntrusted int directBufferSize;
  private @OsUntrusted byte @OsUntrusted [] userBuf = null;
  private @OsUntrusted int userBufOff = 0;
  private @OsUntrusted int userBufLen = 0;
  private @OsUntrusted Buffer uncompressedDirectBuf = null;
  private @OsUntrusted int uncompressedDirectBufOff = 0;
  private @OsUntrusted int uncompressedDirectBufLen = 0;
  private @OsUntrusted boolean keepUncompressedBuf = false;
  private @OsUntrusted Buffer compressedDirectBuf = null;
  private @OsUntrusted boolean finish;
  private @OsUntrusted boolean finished;

  /**
   * The compression level for zlib library.
   */
  public static enum CompressionLevel {
    /**
     * Compression level for no compression.
     */

@OsUntrusted  NO_COMPRESSION (0),
    
    /**
     * Compression level for fastest compression.
     */

@OsUntrusted  BEST_SPEED (1),
    
    /**
     * Compression level for best compression.
     */

@OsUntrusted  BEST_COMPRESSION (9),
    
    /**
     * Default compression level.
     */

@OsUntrusted  DEFAULT_COMPRESSION (-1);
    
    
    private final @OsUntrusted int compressionLevel;
    
    @OsUntrusted
    CompressionLevel(@OsUntrusted int level) {
      compressionLevel = level;
    }
    
    @OsUntrusted
    int compressionLevel(ZlibCompressor.@OsUntrusted CompressionLevel this) {
      return compressionLevel;
    }
  };
  
  /**
   * The compression level for zlib library.
   */
  public static enum CompressionStrategy {
    /**
     * Compression strategy best used for data consisting mostly of small
     * values with a somewhat random distribution. Forces more Huffman coding
     * and less string matching.
     */

@OsTrusted  FILTERED (1),
    
    /**
     * Compression strategy for Huffman coding only.
     */

@OsTrusted  HUFFMAN_ONLY (2),
    
    /**
     * Compression strategy to limit match distances to one
     * (run-length encoding).
     */

@OsTrusted  RLE (3),

    /**
     * Compression strategy to prevent the use of dynamic Huffman codes, 
     * allowing for a simpler decoder for special applications.
     */

@OsTrusted  FIXED (4),

    /**
     * Default compression strategy.
     */

@OsTrusted  DEFAULT_STRATEGY (0);
    
    
    private final @OsUntrusted int compressionStrategy;
    
    @OsUntrusted
    CompressionStrategy(@OsUntrusted int strategy) {
      compressionStrategy = strategy;
    }
    
    @OsUntrusted
    int compressionStrategy(ZlibCompressor.@OsUntrusted CompressionStrategy this) {
      return compressionStrategy;
    }
  };

  /**
   * The type of header for compressed data.
   */
  public static enum CompressionHeader {
    /**
     * No headers/trailers/checksums.
     */

@OsUntrusted  NO_HEADER (-15),
    
    /**
     * Default headers/trailers/checksums.
     */

@OsUntrusted  DEFAULT_HEADER (15),
    
    /**
     * Simple gzip headers/trailers.
     */

@OsUntrusted  GZIP_FORMAT (31);

    private final @OsUntrusted int windowBits;
    
    @OsUntrusted
    CompressionHeader(@OsUntrusted int windowBits) {
      this.windowBits = windowBits;
    }
    
    public @OsUntrusted int windowBits(ZlibCompressor.@OsUntrusted CompressionHeader this) {
      return windowBits;
    }
  }
  
  private static @OsUntrusted boolean nativeZlibLoaded = false;
  
  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      try {
        // Initialize the native library
        initIDs();
        nativeZlibLoaded = true;
      } catch (@OsUntrusted Throwable t) {
        // Ignore failure to load/initialize native-zlib
      }
    }
  }
  
  static @OsUntrusted boolean isNativeZlibLoaded() {
    return nativeZlibLoaded;
  }

  protected final void construct(@OsUntrusted ZlibCompressor this, @OsUntrusted CompressionLevel level, @OsUntrusted CompressionStrategy strategy,
      @OsUntrusted
      CompressionHeader header, @OsUntrusted int directBufferSize) {
  }

  /**
   * Creates a new compressor with the default compression level.
   * Compressed data will be generated in ZLIB format.
   */
  public @OsUntrusted ZlibCompressor() {
    this(CompressionLevel.DEFAULT_COMPRESSION,
         CompressionStrategy.DEFAULT_STRATEGY,
         CompressionHeader.DEFAULT_HEADER,
         DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /**
   * Creates a new compressor, taking settings from the configuration.
   */
  public @OsUntrusted ZlibCompressor(@OsUntrusted Configuration conf) {
    this(ZlibFactory.getCompressionLevel(conf),
         ZlibFactory.getCompressionStrategy(conf),
         CompressionHeader.DEFAULT_HEADER,
         DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /** 
   * Creates a new compressor using the specified compression level.
   * Compressed data will be generated in ZLIB format.
   * 
   * @param level Compression level #CompressionLevel
   * @param strategy Compression strategy #CompressionStrategy
   * @param header Compression header #CompressionHeader
   * @param directBufferSize Size of the direct buffer to be used.
   */
  public @OsUntrusted ZlibCompressor(@OsUntrusted CompressionLevel level, @OsUntrusted CompressionStrategy strategy, 
                        @OsUntrusted
                        CompressionHeader header, @OsUntrusted int directBufferSize) {
    this.level = level;
    this.strategy = strategy;
    this.windowBits = header;
    stream = init(this.level.compressionLevel(), 
                  this.strategy.compressionStrategy(), 
                  this.windowBits.windowBits());

    this.directBufferSize = directBufferSize;
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
  }

  /**
   * Prepare the compressor to be used in a new stream with settings defined in
   * the given Configuration. It will reset the compressor's compression level
   * and compression strategy.
   * 
   * @param conf Configuration storing new settings
   */
  @Override
  public synchronized void reinit(@OsUntrusted ZlibCompressor this, @OsUntrusted Configuration conf) {
    reset();
    if (conf == null) {
      return;
    }
    end(stream);
    level = ZlibFactory.getCompressionLevel(conf);
    strategy = ZlibFactory.getCompressionStrategy(conf);
    stream = init(level.compressionLevel(), 
                  strategy.compressionStrategy(), 
                  windowBits.windowBits());
    if(LOG.isDebugEnabled()) {
      LOG.debug("Reinit compressor with new compression configuration");
    }
  }

  @Override
  public synchronized void setInput(@OsUntrusted ZlibCompressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
    if (b== null) {
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
    
    // Reinitialize zlib's output direct buffer 
    compressedDirectBuf.limit(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
  }
  
  //copy enough data from userBuf to uncompressedDirectBuf
  synchronized void setInputFromSavedData(@OsUntrusted ZlibCompressor this) {
    @OsUntrusted
    int len = Math.min(userBufLen, uncompressedDirectBuf.remaining());
    ((@OsUntrusted ByteBuffer)uncompressedDirectBuf).put(userBuf, userBufOff, len);
    userBufLen -= len;
    userBufOff += len;
    uncompressedDirectBufLen = uncompressedDirectBuf.position();
  }

  @Override
  public synchronized void setDictionary(@OsUntrusted ZlibCompressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
    if (stream == 0 || b == null) {
      throw new @OsUntrusted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @OsUntrusted ArrayIndexOutOfBoundsException();
    }
    setDictionary(stream, b, off, len);
  }

  @Override
  public synchronized @OsUntrusted boolean needsInput(@OsUntrusted ZlibCompressor this) {
    // Consume remaining compressed data?
    if (compressedDirectBuf.remaining() > 0) {
      return false;
    }

    // Check if zlib has consumed all input
    // compress should be invoked if keepUncompressedBuf true
    if (keepUncompressedBuf && uncompressedDirectBufLen > 0)
      return false;
    
    if (uncompressedDirectBuf.remaining() > 0) {
      // Check if we have consumed all user-input
      if (userBufLen <= 0) {
        return true;
      } else {
        // copy enough data from userBuf to uncompressedDirectBuf
        setInputFromSavedData();
        if (uncompressedDirectBuf.remaining() > 0) // uncompressedDirectBuf is not full
          return true;
        else 
          return false;
      }
    }
    
    return false;
  }
  
  @Override
  public synchronized void finish(@OsUntrusted ZlibCompressor this) {
    finish = true;
  }
  
  @Override
  public synchronized @OsUntrusted boolean finished(@OsUntrusted ZlibCompressor this) {
    // Check if 'zlib' says its 'finished' and
    // all compressed data has been consumed
    return (finished && compressedDirectBuf.remaining() == 0);
  }

  @Override
  public synchronized @OsUntrusted int compress(@OsUntrusted ZlibCompressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) 
    throws IOException {
    if (b == null) {
      throw new @OsUntrusted NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new @OsUntrusted ArrayIndexOutOfBoundsException();
    }
    
    @OsUntrusted
    int n = 0;
    
    // Check if there is compressed data
    n = compressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((@OsUntrusted ByteBuffer)compressedDirectBuf).get(b, off, n);
      return n;
    }

    // Re-initialize the zlib's output direct buffer
    compressedDirectBuf.rewind();
    compressedDirectBuf.limit(directBufferSize);

    // Compress data
    n = deflateBytesDirect();
    compressedDirectBuf.limit(n);
    
    // Check if zlib consumed all input buffer
    // set keepUncompressedBuf properly
    if (uncompressedDirectBufLen <= 0) { // zlib consumed all input buffer
      keepUncompressedBuf = false;
      uncompressedDirectBuf.clear();
      uncompressedDirectBufOff = 0;
      uncompressedDirectBufLen = 0;
    } else { // zlib did not consume all input buffer
      keepUncompressedBuf = true;
    }
    
    // Get atmost 'len' bytes
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
  public synchronized @OsUntrusted long getBytesWritten(@OsUntrusted ZlibCompressor this) {
    checkStream();
    return getBytesWritten(stream);
  }

  /**
   * Returns the total number of uncompressed bytes input so far.</p>
   *
   * @return the total (non-negative) number of uncompressed bytes input so far
   */
  @Override
  public synchronized @OsUntrusted long getBytesRead(@OsUntrusted ZlibCompressor this) {
    checkStream();
    return getBytesRead(stream);
  }

  @Override
  public synchronized void reset(@OsUntrusted ZlibCompressor this) {
    checkStream();
    reset(stream);
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
  public synchronized void end(@OsUntrusted ZlibCompressor this) {
    if (stream != 0) {
      end(stream);
      stream = 0;
    }
  }
  
  private void checkStream(@OsUntrusted ZlibCompressor this) {
    if (stream == 0)
      throw new @OsUntrusted NullPointerException();
  }
  
  private native static void initIDs();
  private native static @OsUntrusted long init(@OsUntrusted int level, @OsUntrusted int strategy, @OsUntrusted int windowBits);
  private native static void setDictionary(@OsUntrusted long strm, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off,
                                           @OsUntrusted
                                           int len);
  private native @OsUntrusted int deflateBytesDirect(@OsUntrusted ZlibCompressor this);
  private native static @OsUntrusted long getBytesRead(@OsUntrusted long strm);
  private native static @OsUntrusted long getBytesWritten(@OsUntrusted long strm);
  private native static void reset(@OsUntrusted long strm);
  private native static void end(@OsUntrusted long strm);

  public native static @OsUntrusted String getLibraryName();
}
