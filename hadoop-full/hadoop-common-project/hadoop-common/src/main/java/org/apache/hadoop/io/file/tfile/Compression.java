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
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Compression related stuff.
 */
final class Compression {
  static final @OsUntrusted Log LOG = LogFactory.getLog(Compression.class);

  /**
   * Prevent the instantiation of class.
   */
  private @OsUntrusted Compression() {
    // nothing
  }

  static class FinishOnFlushCompressionStream extends @OsUntrusted FilterOutputStream {
    public @OsUntrusted FinishOnFlushCompressionStream(@OsUntrusted CompressionOutputStream cout) {
      super(cout);
    }

    @Override
    public void write(Compression.@OsUntrusted FinishOnFlushCompressionStream this, @OsUntrusted byte b @OsUntrusted [], @OsUntrusted int off, @OsUntrusted int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void flush(Compression.@OsUntrusted FinishOnFlushCompressionStream this) throws IOException {
      @OsUntrusted
      CompressionOutputStream cout = (@OsUntrusted CompressionOutputStream) out;
      cout.finish();
      cout.flush();
      cout.resetState();
    }
  }

  /**
   * Compression algorithms.
   */
  static enum Algorithm {

@OsUntrusted  LZO(TFile.COMPRESSION_LZO) {
      private transient boolean checked = false;
      private static final String defaultClazz =
          "org.apache.hadoop.io.compress.LzoCodec";
      private transient CompressionCodec codec = null;

      @Override
      public synchronized boolean isSupported() {
        if (!checked) {
          checked = true;
          String extClazz =
              (conf.get(CONF_LZO_CLASS) == null ? System
                  .getProperty(CONF_LZO_CLASS) : null);
          String clazz = (extClazz != null) ? extClazz : defaultClazz;
          try {
            LOG.info("Trying to load Lzo codec class: " + clazz);
            codec =
                (CompressionCodec) ReflectionUtils.newInstance(Class
                    .forName(clazz), conf);
          } catch (ClassNotFoundException e) {
            // that is okay
          }
        }
        return codec != null;
      }

      @Override
      CompressionCodec getCodec() throws IOException {
        if (!isSupported()) {
          throw new IOException(
              "LZO codec class not specified. Did you forget to set property "
                  + CONF_LZO_CLASS + "?");
        }

        return codec;
      }

      @Override
      public synchronized InputStream createDecompressionStream(
          InputStream downStream, Decompressor decompressor,
          int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException(
              "LZO codec class not specified. Did you forget to set property "
                  + CONF_LZO_CLASS + "?");
        }
        InputStream bis1 = null;
        if (downStreamBufferSize > 0) {
          bis1 = new BufferedInputStream(downStream, downStreamBufferSize);
        } else {
          bis1 = downStream;
        }
        conf.setInt("io.compression.codec.lzo.buffersize", 64 * 1024);
        CompressionInputStream cis =
            codec.createInputStream(bis1, decompressor);
        BufferedInputStream bis2 = new BufferedInputStream(cis, DATA_IBUF_SIZE);
        return bis2;
      }

      @Override
      public synchronized OutputStream createCompressionStream(
          OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException(
              "LZO codec class not specified. Did you forget to set property "
                  + CONF_LZO_CLASS + "?");
        }
        OutputStream bos1 = null;
        if (downStreamBufferSize > 0) {
          bos1 = new BufferedOutputStream(downStream, downStreamBufferSize);
        } else {
          bos1 = downStream;
        }
        conf.setInt("io.compression.codec.lzo.buffersize", 64 * 1024);
        CompressionOutputStream cos =
            codec.createOutputStream(bos1, compressor);
        BufferedOutputStream bos2 =
            new BufferedOutputStream(new FinishOnFlushCompressionStream(cos),
                DATA_OBUF_SIZE);
        return bos2;
      }
    },


@OsUntrusted  GZ(TFile.COMPRESSION_GZ) {
      private transient DefaultCodec codec;

      @Override
      CompressionCodec getCodec() {
        if (codec == null) {
          codec = new DefaultCodec();
          codec.setConf(conf);
        }

        return codec;
      }

      @Override
      public synchronized InputStream createDecompressionStream(
          InputStream downStream, Decompressor decompressor,
          int downStreamBufferSize) throws IOException {
        // Set the internal buffer size to read from down stream.
        if (downStreamBufferSize > 0) {
          codec.getConf().setInt("io.file.buffer.size", downStreamBufferSize);
        }
        CompressionInputStream cis =
            codec.createInputStream(downStream, decompressor);
        BufferedInputStream bis2 = new BufferedInputStream(cis, DATA_IBUF_SIZE);
        return bis2;
      }

      @Override
      public synchronized OutputStream createCompressionStream(
          OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        OutputStream bos1 = null;
        if (downStreamBufferSize > 0) {
          bos1 = new BufferedOutputStream(downStream, downStreamBufferSize);
        } else {
          bos1 = downStream;
        }
        codec.getConf().setInt("io.file.buffer.size", 32 * 1024);
        CompressionOutputStream cos =
            codec.createOutputStream(bos1, compressor);
        BufferedOutputStream bos2 =
            new BufferedOutputStream(new FinishOnFlushCompressionStream(cos),
                DATA_OBUF_SIZE);
        return bos2;
      }

      @Override
      public boolean isSupported() {
        return true;
      }
    },


@OsUntrusted  NONE(TFile.COMPRESSION_NONE) {
      @Override
      CompressionCodec getCodec() {
        return null;
      }

      @Override
      public synchronized InputStream createDecompressionStream(
          InputStream downStream, Decompressor decompressor,
          int downStreamBufferSize) throws IOException {
        if (downStreamBufferSize > 0) {
          return new BufferedInputStream(downStream, downStreamBufferSize);
        }
        return downStream;
      }

      @Override
      public synchronized OutputStream createCompressionStream(
          OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        if (downStreamBufferSize > 0) {
          return new BufferedOutputStream(downStream, downStreamBufferSize);
        }

        return downStream;
      }

      @Override
      public boolean isSupported() {
        return true;
      }
    };

    // We require that all compression related settings are configured
    // statically in the Configuration object.
    protected static final @OsUntrusted Configuration conf = new @OsUntrusted Configuration();
    private final @OsUntrusted String compressName;
    // data input buffer size to absorb small reads from application.
    private static final @OsUntrusted int DATA_IBUF_SIZE = 1 * 1024;
    // data output buffer size to absorb small writes from application.
    private static final @OsUntrusted int DATA_OBUF_SIZE = 4 * 1024;
    public static final @OsUntrusted String CONF_LZO_CLASS =
        "io.compression.codec.lzo.class";

    @OsUntrusted
    Algorithm(@OsUntrusted String name) {
      this.compressName = name;
    }

    abstract @OsUntrusted CompressionCodec getCodec(Compression.@OsUntrusted Algorithm this) throws IOException;

    public abstract @OsUntrusted InputStream createDecompressionStream(
        Compression.@OsUntrusted Algorithm this, @OsUntrusted
        InputStream downStream, @OsUntrusted Decompressor decompressor,
        @OsUntrusted
        int downStreamBufferSize) throws IOException;

    public abstract @OsUntrusted OutputStream createCompressionStream(
        Compression.@OsUntrusted Algorithm this, @OsUntrusted
        OutputStream downStream, @OsUntrusted Compressor compressor, @OsUntrusted int downStreamBufferSize)
        throws IOException;

    public abstract @OsUntrusted boolean isSupported(Compression.@OsUntrusted Algorithm this);

    public @OsUntrusted Compressor getCompressor(Compression.@OsUntrusted Algorithm this) throws IOException {
      @OsUntrusted
      CompressionCodec codec = getCodec();
      if (codec != null) {
        @OsUntrusted
        Compressor compressor = CodecPool.getCompressor(codec);
        if (compressor != null) {
          if (compressor.finished()) {
            // Somebody returns the compressor to CodecPool but is still using
            // it.
            LOG.warn("Compressor obtained from CodecPool already finished()");
          } else {
            if(LOG.isDebugEnabled()) {
              LOG.debug("Got a compressor: " + compressor.hashCode());
            }
          }
          /**
           * Following statement is necessary to get around bugs in 0.18 where a
           * compressor is referenced after returned back to the codec pool.
           */
          compressor.reset();
        }
        return compressor;
      }
      return null;
    }

    public void returnCompressor(Compression.@OsUntrusted Algorithm this, @OsUntrusted Compressor compressor) {
      if (compressor != null) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Return a compressor: " + compressor.hashCode());
        }
        CodecPool.returnCompressor(compressor);
      }
    }

    public @OsUntrusted Decompressor getDecompressor(Compression.@OsUntrusted Algorithm this) throws IOException {
      @OsUntrusted
      CompressionCodec codec = getCodec();
      if (codec != null) {
        @OsUntrusted
        Decompressor decompressor = CodecPool.getDecompressor(codec);
        if (decompressor != null) {
          if (decompressor.finished()) {
            // Somebody returns the decompressor to CodecPool but is still using
            // it.
            LOG.warn("Deompressor obtained from CodecPool already finished()");
          } else {
            if(LOG.isDebugEnabled()) {
              LOG.debug("Got a decompressor: " + decompressor.hashCode());
            }
          }
          /**
           * Following statement is necessary to get around bugs in 0.18 where a
           * decompressor is referenced after returned back to the codec pool.
           */
          decompressor.reset();
        }
        return decompressor;
      }

      return null;
    }

    public void returnDecompressor(Compression.@OsUntrusted Algorithm this, @OsUntrusted Decompressor decompressor) {
      if (decompressor != null) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Returned a decompressor: " + decompressor.hashCode());
        }
        CodecPool.returnDecompressor(decompressor);
      }
    }

    public @OsUntrusted String getName(Compression.@OsUntrusted Algorithm this) {
      return compressName;
    }
  }

  static @OsUntrusted Algorithm getCompressionAlgorithmByName(@OsUntrusted String compressName) {
    @OsUntrusted
    Algorithm @OsUntrusted [] algos = Algorithm.class.getEnumConstants();

    for (@OsUntrusted Algorithm a : algos) {
      if (a.getName().equals(compressName)) {
        return a;
      }
    }

    throw new @OsUntrusted IllegalArgumentException(
        "Unsupported compression algorithm name: " + compressName);
  }

  static @OsUntrusted String @OsUntrusted [] getSupportedAlgorithms() {
    @OsUntrusted
    Algorithm @OsUntrusted [] algos = Algorithm.class.getEnumConstants();

    @OsUntrusted
    ArrayList<@OsUntrusted String> ret = new @OsUntrusted ArrayList<@OsUntrusted String>();
    for (@OsUntrusted Algorithm a : algos) {
      if (a.isSupported()) {
        ret.add(a.getName());
      }
    }
    return ret.toArray(new @OsUntrusted String @OsUntrusted [ret.size()]);
  }
}
