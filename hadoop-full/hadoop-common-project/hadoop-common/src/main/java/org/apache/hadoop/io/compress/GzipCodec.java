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

package org.apache.hadoop.io.compress;

import ostrusted.quals.OsUntrusted;
import java.io.*;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.zlib.*;
import static org.apache.hadoop.util.PlatformName.IBM_JAVA;

/**
 * This class creates gzip compressors/decompressors. 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class GzipCodec extends @OsUntrusted DefaultCodec {
  /**
   * A bridge that wraps around a DeflaterOutputStream to make it 
   * a CompressionOutputStream.
   */
  @InterfaceStability.Evolving
  protected static class GzipOutputStream extends @OsUntrusted CompressorStream {

    private static class ResetableGZIPOutputStream extends @OsUntrusted GZIPOutputStream {
      private static final @OsUntrusted int TRAILER_SIZE = 8;
      public static final @OsUntrusted String JVMVersion= System.getProperty("java.version");
      private static final @OsUntrusted boolean HAS_BROKEN_FINISH =
          (IBM_JAVA && JVMVersion.contains("1.6.0"));

      public @OsUntrusted ResetableGZIPOutputStream(@OsUntrusted OutputStream out) throws IOException {
        super(out);
      }

      public void resetState(GzipCodec.GzipOutputStream.@OsUntrusted ResetableGZIPOutputStream this) throws IOException {
        def.reset();
      }

      /**
       * Override this method for HADOOP-8419.
       * Override because IBM implementation calls def.end() which
       * causes problem when reseting the stream for reuse.
       *
       */
      @Override
      public void finish(GzipCodec.GzipOutputStream.@OsUntrusted ResetableGZIPOutputStream this) throws IOException {
        if (HAS_BROKEN_FINISH) {
          if (!def.finished()) {
            def.finish();
            while (!def.finished()) {
              @OsUntrusted
              int i = def.deflate(this.buf, 0, this.buf.length);
              if ((def.finished()) && (i <= this.buf.length - TRAILER_SIZE)) {
                writeTrailer(this.buf, i);
                i += TRAILER_SIZE;
                out.write(this.buf, 0, i);

                return;
              }
              if (i > 0) {
                out.write(this.buf, 0, i);
              }
            }

            @OsUntrusted
            byte @OsUntrusted [] arrayOfByte = new @OsUntrusted byte @OsUntrusted [TRAILER_SIZE];
            writeTrailer(arrayOfByte, 0);
            out.write(arrayOfByte);
          }
        } else {
          super.finish();
        }
      }

      /** re-implement for HADOOP-8419 because the relative method in jdk is invisible */
      private void writeTrailer(GzipCodec.GzipOutputStream.@OsUntrusted ResetableGZIPOutputStream this, @OsUntrusted byte @OsUntrusted [] paramArrayOfByte, @OsUntrusted int paramInt)
        throws IOException {
        writeInt((@OsUntrusted int)this.crc.getValue(), paramArrayOfByte, paramInt);
        writeInt(this.def.getTotalIn(), paramArrayOfByte, paramInt + 4);
      }

      /** re-implement for HADOOP-8419 because the relative method in jdk is invisible */
      private void writeInt(GzipCodec.GzipOutputStream.@OsUntrusted ResetableGZIPOutputStream this, @OsUntrusted int paramInt1, @OsUntrusted byte @OsUntrusted [] paramArrayOfByte, @OsUntrusted int paramInt2)
        throws IOException {
        writeShort(paramInt1 & 0xFFFF, paramArrayOfByte, paramInt2);
        writeShort(paramInt1 >> 16 & 0xFFFF, paramArrayOfByte, paramInt2 + 2);
      }

      /** re-implement for HADOOP-8419 because the relative method in jdk is invisible */
      private void writeShort(GzipCodec.GzipOutputStream.@OsUntrusted ResetableGZIPOutputStream this, @OsUntrusted int paramInt1, @OsUntrusted byte @OsUntrusted [] paramArrayOfByte, @OsUntrusted int paramInt2)
        throws IOException {
        paramArrayOfByte[paramInt2] = (@OsUntrusted byte)(paramInt1 & 0xFF);
        paramArrayOfByte[(paramInt2 + 1)] = (@OsUntrusted byte)(paramInt1 >> 8 & 0xFF);
      }
    }

    public @OsUntrusted GzipOutputStream(@OsUntrusted OutputStream out) throws IOException {
      super(new @OsUntrusted ResetableGZIPOutputStream(out));
    }
    
    /**
     * Allow children types to put a different type in here.
     * @param out the Deflater stream to use
     */
    protected @OsUntrusted GzipOutputStream(@OsUntrusted CompressorStream out) {
      super(out);
    }
    
    @Override
    public void close(GzipCodec.@OsUntrusted GzipOutputStream this) throws IOException {
      out.close();
    }
    
    @Override
    public void flush(GzipCodec.@OsUntrusted GzipOutputStream this) throws IOException {
      out.flush();
    }
    
    @Override
    public void write(GzipCodec.@OsUntrusted GzipOutputStream this, @OsUntrusted int b) throws IOException {
      out.write(b);
    }
    
    @Override
    public void write(GzipCodec.@OsUntrusted GzipOutputStream this, @OsUntrusted byte @OsUntrusted [] data, @OsUntrusted int offset, @OsUntrusted int length) 
      throws IOException {
      out.write(data, offset, length);
    }
    
    @Override
    public void finish(GzipCodec.@OsUntrusted GzipOutputStream this) throws IOException {
      ((@OsUntrusted ResetableGZIPOutputStream) out).finish();
    }

    @Override
    public void resetState(GzipCodec.@OsUntrusted GzipOutputStream this) throws IOException {
      ((@OsUntrusted ResetableGZIPOutputStream) out).resetState();
    }
  }

  @Override
  public @OsUntrusted CompressionOutputStream createOutputStream(@OsUntrusted GzipCodec this, @OsUntrusted OutputStream out) 
    throws IOException {
    return (ZlibFactory.isNativeZlibLoaded(conf)) ?
               new @OsUntrusted CompressorStream(out, createCompressor(),
                                    conf.getInt("io.file.buffer.size", 4*1024)) :
               new @OsUntrusted GzipOutputStream(out);
  }
  
  @Override
  public @OsUntrusted CompressionOutputStream createOutputStream(@OsUntrusted GzipCodec this, @OsUntrusted OutputStream out, 
                                                    @OsUntrusted
                                                    Compressor compressor) 
  throws IOException {
    return (compressor != null) ?
               new @OsUntrusted CompressorStream(out, compressor,
                                    conf.getInt("io.file.buffer.size", 
                                                4*1024)) :
               createOutputStream(out);
  }

  @Override
  public @OsUntrusted Compressor createCompressor(@OsUntrusted GzipCodec this) {
    return (ZlibFactory.isNativeZlibLoaded(conf))
      ? new @OsUntrusted GzipZlibCompressor(conf)
      : null;
  }

  @Override
  public @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Compressor> getCompressorType(@OsUntrusted GzipCodec this) {
    return ZlibFactory.isNativeZlibLoaded(conf)
      ? GzipZlibCompressor.class
      : null;
  }

  @Override
  public @OsUntrusted CompressionInputStream createInputStream(@OsUntrusted GzipCodec this, @OsUntrusted InputStream in)
  throws IOException {
    return createInputStream(in, null);
  }

  @Override
  public @OsUntrusted CompressionInputStream createInputStream(@OsUntrusted GzipCodec this, @OsUntrusted InputStream in,
                                                  @OsUntrusted
                                                  Decompressor decompressor)
  throws IOException {
    if (decompressor == null) {
      decompressor = createDecompressor();  // always succeeds (or throws)
    }
    return new @OsUntrusted DecompressorStream(in, decompressor,
                                  conf.getInt("io.file.buffer.size", 4*1024));
  }

  @Override
  public @OsUntrusted Decompressor createDecompressor(@OsUntrusted GzipCodec this) {
    return (ZlibFactory.isNativeZlibLoaded(conf))
      ? new @OsUntrusted GzipZlibDecompressor()
      : new @OsUntrusted BuiltInGzipDecompressor();
  }

  @Override
  public @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Decompressor> getDecompressorType(@OsUntrusted GzipCodec this) {
    return ZlibFactory.isNativeZlibLoaded(conf)
      ? GzipZlibDecompressor.class
      : BuiltInGzipDecompressor.class;
  }

  @Override
  public @OsUntrusted String getDefaultExtension(@OsUntrusted GzipCodec this) {
    return ".gz";
  }

  static final class GzipZlibCompressor extends @OsUntrusted ZlibCompressor {
    public @OsUntrusted GzipZlibCompressor() {
      super(ZlibCompressor.CompressionLevel.DEFAULT_COMPRESSION,
          ZlibCompressor.CompressionStrategy.DEFAULT_STRATEGY,
          ZlibCompressor.CompressionHeader.GZIP_FORMAT, 64*1024);
    }
    
    public @OsUntrusted GzipZlibCompressor(@OsUntrusted Configuration conf) {
      super(ZlibFactory.getCompressionLevel(conf),
           ZlibFactory.getCompressionStrategy(conf),
           ZlibCompressor.CompressionHeader.GZIP_FORMAT,
           64 * 1024);
    }
  }

  static final class GzipZlibDecompressor extends @OsUntrusted ZlibDecompressor {
    public @OsUntrusted GzipZlibDecompressor() {
      super(ZlibDecompressor.CompressionHeader.AUTODETECT_GZIP_ZLIB, 64*1024);
    }
  }

}
