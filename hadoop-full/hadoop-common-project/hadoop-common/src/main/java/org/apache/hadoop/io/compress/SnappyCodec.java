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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * This class creates snappy compressors/decompressors.
 */
public class SnappyCodec implements @OsUntrusted Configurable, @OsUntrusted CompressionCodec {
  @OsUntrusted
  Configuration conf;

  /**
   * Set the configuration to be used by this object.
   *
   * @param conf the configuration object.
   */
  @Override
  public void setConf(@OsUntrusted SnappyCodec this, @OsUntrusted Configuration conf) {
    this.conf = conf;
  }

  /**
   * Return the configuration used by this object.
   *
   * @return the configuration object used by this objec.
   */
  @Override
  public @OsUntrusted Configuration getConf(@OsUntrusted SnappyCodec this) {
    return conf;
  }

  /**
   * Are the native snappy libraries loaded & initialized?
   */
  public static void checkNativeCodeLoaded() {
      if (!NativeCodeLoader.buildSupportsSnappy()) {
        throw new @OsUntrusted RuntimeException("native snappy library not available: " +
            "this version of libhadoop was built without " +
            "snappy support.");
      }
      if (!SnappyCompressor.isNativeCodeLoaded()) {
        throw new @OsUntrusted RuntimeException("native snappy library not available: " +
            "SnappyCompressor has not been loaded.");
      }
      if (!SnappyDecompressor.isNativeCodeLoaded()) {
        throw new @OsUntrusted RuntimeException("native snappy library not available: " +
            "SnappyDecompressor has not been loaded.");
      }
  }
  
  public static @OsUntrusted boolean isNativeCodeLoaded() {
    return SnappyCompressor.isNativeCodeLoaded() && 
        SnappyDecompressor.isNativeCodeLoaded();
  }

  public static @OsUntrusted String getLibraryName() {
    return SnappyCompressor.getLibraryName();
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream}.
   *
   * @param out the location for the final output stream
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  @Override
  public @OsUntrusted CompressionOutputStream createOutputStream(@OsUntrusted SnappyCodec this, @OsUntrusted OutputStream out)
      throws IOException {
    return createOutputStream(out, createCompressor());
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream} with the given {@link Compressor}.
   *
   * @param out        the location for the final output stream
   * @param compressor compressor to use
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  @Override
  public @OsUntrusted CompressionOutputStream createOutputStream(@OsUntrusted SnappyCodec this, @OsUntrusted OutputStream out,
                                                    @OsUntrusted
                                                    Compressor compressor)
      throws IOException {
    checkNativeCodeLoaded();
    @OsUntrusted
    int bufferSize = conf.getInt(
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY,
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT);

    @OsUntrusted
    int compressionOverhead = (bufferSize / 6) + 32;

    return new @OsUntrusted BlockCompressorStream(out, compressor, bufferSize,
        compressionOverhead);
  }

  /**
   * Get the type of {@link Compressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of compressor needed by this codec.
   */
  @Override
  public @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Compressor> getCompressorType(@OsUntrusted SnappyCodec this) {
    checkNativeCodeLoaded();
    return SnappyCompressor.class;
  }

  /**
   * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
   *
   * @return a new compressor for use by this codec
   */
  @Override
  public @OsUntrusted Compressor createCompressor(@OsUntrusted SnappyCodec this) {
    checkNativeCodeLoaded();
    @OsUntrusted
    int bufferSize = conf.getInt(
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY,
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT);
    return new @OsUntrusted SnappyCompressor(bufferSize);
  }

  /**
   * Create a {@link CompressionInputStream} that will read from the given
   * input stream.
   *
   * @param in the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  @Override
  public @OsUntrusted CompressionInputStream createInputStream(@OsUntrusted SnappyCodec this, @OsUntrusted InputStream in)
      throws IOException {
    return createInputStream(in, createDecompressor());
  }

  /**
   * Create a {@link CompressionInputStream} that will read from the given
   * {@link InputStream} with the given {@link Decompressor}.
   *
   * @param in           the stream to read compressed bytes from
   * @param decompressor decompressor to use
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  @Override
  public @OsUntrusted CompressionInputStream createInputStream(@OsUntrusted SnappyCodec this, @OsUntrusted InputStream in,
                                                  @OsUntrusted
                                                  Decompressor decompressor)
      throws IOException {
    checkNativeCodeLoaded();
    return new @OsUntrusted BlockDecompressorStream(in, decompressor, conf.getInt(
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY,
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT));
  }

  /**
   * Get the type of {@link Decompressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of decompressor needed by this codec.
   */
  @Override
  public @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Decompressor> getDecompressorType(@OsUntrusted SnappyCodec this) {
    checkNativeCodeLoaded();
    return SnappyDecompressor.class;
  }

  /**
   * Create a new {@link Decompressor} for use by this {@link CompressionCodec}.
   *
   * @return a new decompressor for use by this codec
   */
  @Override
  public @OsUntrusted Decompressor createDecompressor(@OsUntrusted SnappyCodec this) {
    checkNativeCodeLoaded();
    @OsUntrusted
    int bufferSize = conf.getInt(
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY,
        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT);
    return new @OsUntrusted SnappyDecompressor(bufferSize);
  }

  /**
   * Get the default filename extension for this kind of compression.
   *
   * @return <code>.snappy</code>.
   */
  @Override
  public @OsUntrusted String getDefaultExtension(@OsUntrusted SnappyCodec this) {
    return ".snappy";
  }
}
