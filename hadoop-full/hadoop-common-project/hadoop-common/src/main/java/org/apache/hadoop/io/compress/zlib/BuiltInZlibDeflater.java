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
import java.io.IOException;
import java.util.zip.Deflater;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A wrapper around java.util.zip.Deflater to make it conform 
 * to org.apache.hadoop.io.compress.Compressor interface.
 * 
 */
public class BuiltInZlibDeflater extends @OsUntrusted Deflater implements @OsUntrusted Compressor {

  private static final @OsUntrusted Log LOG = LogFactory.getLog(BuiltInZlibDeflater.class);

  public @OsUntrusted BuiltInZlibDeflater(@OsUntrusted int level, @OsUntrusted boolean nowrap) {
    super(level, nowrap);
  }

  public @OsUntrusted BuiltInZlibDeflater(@OsUntrusted int level) {
    super(level);
  }

  public @OsUntrusted BuiltInZlibDeflater() {
    super();
  }

  @Override
  public synchronized @OsUntrusted int compress(@OsUntrusted BuiltInZlibDeflater this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) 
    throws IOException {
    return super.deflate(b, off, len);
  }

  /**
   * reinit the compressor with the given configuration. It will reset the
   * compressor's compression level and compression strategy. Different from
   * <tt>ZlibCompressor</tt>, <tt>BuiltInZlibDeflater</tt> only support three
   * kind of compression strategy: FILTERED, HUFFMAN_ONLY and DEFAULT_STRATEGY.
   * It will use DEFAULT_STRATEGY as default if the configured compression
   * strategy is not supported.
   */
  @Override
  public void reinit(@OsUntrusted BuiltInZlibDeflater this, @OsUntrusted Configuration conf) {
    reset();
    if (conf == null) {
      return;
    }
    setLevel(ZlibFactory.getCompressionLevel(conf).compressionLevel());
    final ZlibCompressor.@OsUntrusted CompressionStrategy strategy =
      ZlibFactory.getCompressionStrategy(conf);
    try {
      setStrategy(strategy.compressionStrategy());
    } catch (@OsUntrusted IllegalArgumentException ill) {
      LOG.warn(strategy + " not supported by BuiltInZlibDeflater.");
      setStrategy(DEFAULT_STRATEGY);
    }
    if(LOG.isDebugEnabled()) {
      LOG.debug("Reinit compressor with new compression configuration");
    }
  }
}
