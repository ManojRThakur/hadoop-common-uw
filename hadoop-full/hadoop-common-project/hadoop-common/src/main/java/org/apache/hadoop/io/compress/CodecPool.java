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
package org.apache.hadoop.io.compress;

import ostrusted.quals.OsUntrusted;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * A global compressor/decompressor pool used to save and reuse 
 * (possibly native) compression/decompression codecs.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CodecPool {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(CodecPool.class);
  
  /**
   * A global compressor pool used to save the expensive 
   * construction/destruction of (possibly native) decompression codecs.
   */
  private static final @OsUntrusted Map<@OsUntrusted Class<@OsUntrusted Compressor>, @OsUntrusted List<@OsUntrusted Compressor>> compressorPool = 
    new @OsUntrusted HashMap<@OsUntrusted Class<@OsUntrusted Compressor>, @OsUntrusted List<@OsUntrusted Compressor>>();
  
  /**
   * A global decompressor pool used to save the expensive 
   * construction/destruction of (possibly native) decompression codecs.
   */
  private static final @OsUntrusted Map<@OsUntrusted Class<@OsUntrusted Decompressor>, @OsUntrusted List<@OsUntrusted Decompressor>> decompressorPool = 
    new @OsUntrusted HashMap<@OsUntrusted Class<@OsUntrusted Decompressor>, @OsUntrusted List<@OsUntrusted Decompressor>>();

  private static <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted LoadingCache<@OsUntrusted Class<@OsUntrusted T>, @OsUntrusted AtomicInteger> createCache(
      @OsUntrusted
      Class<@OsUntrusted T> klass) {
    return CacheBuilder.newBuilder().build(
        new @OsUntrusted CacheLoader<@OsUntrusted Class<@OsUntrusted T>, @OsUntrusted AtomicInteger>() {
          @Override
          public @OsUntrusted AtomicInteger load(@OsUntrusted Class<@OsUntrusted T> key) throws Exception {
            return new @OsUntrusted AtomicInteger();
          }
        });
  }

  /**
   * Map to track the number of leased compressors
   */
  private static final @OsUntrusted LoadingCache<@OsUntrusted Class<@OsUntrusted Compressor>, @OsUntrusted AtomicInteger> compressorCounts =
      createCache(Compressor.class);

   /**
   * Map to tracks the number of leased decompressors
   */
  private static final @OsUntrusted LoadingCache<@OsUntrusted Class<@OsUntrusted Decompressor>, @OsUntrusted AtomicInteger> decompressorCounts =
      createCache(Decompressor.class);

  private static <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted T borrow(@OsUntrusted Map<@OsUntrusted Class<@OsUntrusted T>, @OsUntrusted List<@OsUntrusted T>> pool,
                             @OsUntrusted
                             Class<@OsUntrusted ? extends @OsUntrusted T> codecClass) {
    @OsUntrusted
    T codec = null;
    
    // Check if an appropriate codec is available
    synchronized (pool) {
      if (pool.containsKey(codecClass)) {
        @OsUntrusted
        List<@OsUntrusted T> codecList = pool.get(codecClass);
        
        if (codecList != null) {
          synchronized (codecList) {
            if (!codecList.isEmpty()) {
              codec = codecList.remove(codecList.size()-1);
            }
          }
        }
      }
    }
    
    return codec;
  }

  private static <@OsUntrusted T extends java.lang.@OsUntrusted Object> void payback(@OsUntrusted Map<@OsUntrusted Class<@OsUntrusted T>, @OsUntrusted List<@OsUntrusted T>> pool, @OsUntrusted T codec) {
    if (codec != null) {
      @OsUntrusted
      Class<@OsUntrusted T> codecClass = ReflectionUtils.getClass(codec);
      synchronized (pool) {
        if (!pool.containsKey(codecClass)) {
          pool.put(codecClass, new @OsUntrusted ArrayList<@OsUntrusted T>());
        }

        @OsUntrusted
        List<@OsUntrusted T> codecList = pool.get(codecClass);
        synchronized (codecList) {
          codecList.add(codec);
        }
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  private static <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted int getLeaseCount(
      @OsUntrusted
      LoadingCache<@OsUntrusted Class<@OsUntrusted T>, @OsUntrusted AtomicInteger> usageCounts,
      @OsUntrusted
      Class<@OsUntrusted ? extends @OsUntrusted T> codecClass) {
    return usageCounts.getUnchecked((@OsUntrusted Class<@OsUntrusted T>) codecClass).get();
  }

  private static <@OsUntrusted T extends java.lang.@OsUntrusted Object> void updateLeaseCount(
      @OsUntrusted
      LoadingCache<@OsUntrusted Class<@OsUntrusted T>, @OsUntrusted AtomicInteger> usageCounts, @OsUntrusted T codec, @OsUntrusted int delta) {
    if (codec != null) {
      @OsUntrusted
      Class<@OsUntrusted T> codecClass = ReflectionUtils.getClass(codec);
      usageCounts.getUnchecked(codecClass).addAndGet(delta);
    }
  }

  /**
   * Get a {@link Compressor} for the given {@link CompressionCodec} from the 
   * pool or a new one.
   *
   * @param codec the <code>CompressionCodec</code> for which to get the 
   *              <code>Compressor</code>
   * @param conf the <code>Configuration</code> object which contains confs for creating or reinit the compressor
   * @return <code>Compressor</code> for the given 
   *         <code>CompressionCodec</code> from the pool or a new one
   */
  public static @OsUntrusted Compressor getCompressor(@OsUntrusted CompressionCodec codec, @OsUntrusted Configuration conf) {
    @OsUntrusted
    Compressor compressor = borrow(compressorPool, codec.getCompressorType());
    if (compressor == null) {
      compressor = codec.createCompressor();
      LOG.info("Got brand-new compressor ["+codec.getDefaultExtension()+"]");
    } else {
      compressor.reinit(conf);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Got recycled compressor");
      }
    }
    updateLeaseCount(compressorCounts, compressor, 1);
    return compressor;
  }
  
  public static @OsUntrusted Compressor getCompressor(@OsUntrusted CompressionCodec codec) {
    return getCompressor(codec, null);
  }
  
  /**
   * Get a {@link Decompressor} for the given {@link CompressionCodec} from the
   * pool or a new one.
   *  
   * @param codec the <code>CompressionCodec</code> for which to get the 
   *              <code>Decompressor</code>
   * @return <code>Decompressor</code> for the given 
   *         <code>CompressionCodec</code> the pool or a new one
   */
  public static @OsUntrusted Decompressor getDecompressor(@OsUntrusted CompressionCodec codec) {
    @OsUntrusted
    Decompressor decompressor = borrow(decompressorPool, codec.getDecompressorType());
    if (decompressor == null) {
      decompressor = codec.createDecompressor();
      LOG.info("Got brand-new decompressor ["+codec.getDefaultExtension()+"]");
    } else {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Got recycled decompressor");
      }
    }
    updateLeaseCount(decompressorCounts, decompressor, 1);
    return decompressor;
  }
  
  /**
   * Return the {@link Compressor} to the pool.
   * 
   * @param compressor the <code>Compressor</code> to be returned to the pool
   */
  public static void returnCompressor(@OsUntrusted Compressor compressor) {
    if (compressor == null) {
      return;
    }
    // if the compressor can't be reused, don't pool it.
    if (compressor.getClass().isAnnotationPresent(DoNotPool.class)) {
      return;
    }
    compressor.reset();
    payback(compressorPool, compressor);
    updateLeaseCount(compressorCounts, compressor, -1);
  }
  
  /**
   * Return the {@link Decompressor} to the pool.
   * 
   * @param decompressor the <code>Decompressor</code> to be returned to the 
   *                     pool
   */
  public static void returnDecompressor(@OsUntrusted Decompressor decompressor) {
    if (decompressor == null) {
      return;
    }
    // if the decompressor can't be reused, don't pool it.
    if (decompressor.getClass().isAnnotationPresent(DoNotPool.class)) {
      return;
    }
    decompressor.reset();
    payback(decompressorPool, decompressor);
    updateLeaseCount(decompressorCounts, decompressor, -1);
  }

  /**
   * Return the number of leased {@link Compressor}s for this
   * {@link CompressionCodec}
   */
  public static @OsUntrusted int getLeasedCompressorsCount(@OsUntrusted CompressionCodec codec) {
    return (codec == null) ? 0 : getLeaseCount(compressorCounts,
        codec.getCompressorType());
  }

  /**
   * Return the number of leased {@link Decompressor}s for this
   * {@link CompressionCodec}
   */
  public static @OsUntrusted int getLeasedDecompressorsCount(@OsUntrusted CompressionCodec codec) {
    return (codec == null) ? 0 : getLeaseCount(decompressorCounts,
        codec.getDecompressorType());
  }
}
