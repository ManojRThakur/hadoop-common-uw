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

package org.apache.hadoop.io;

import ostrusted.quals.OsUntrusted;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.bloom.DynamicBloomFilter;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * This class extends {@link MapFile} and provides very much the same
 * functionality. However, it uses dynamic Bloom filters to provide
 * quick membership test for keys, and it offers a fast version of 
 * {@link Reader#get(WritableComparable, Writable)} operation, especially in
 * case of sparsely populated MapFile-s.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BloomMapFile {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(BloomMapFile.class);
  public static final @OsUntrusted String BLOOM_FILE_NAME = "bloom";
  public static final @OsUntrusted int HASH_COUNT = 5;
  
  public static void delete(@OsUntrusted FileSystem fs, @OsUntrusted String name) throws IOException {
    @OsUntrusted
    Path dir = new @OsUntrusted Path(name);
    @OsUntrusted
    Path data = new @OsUntrusted Path(dir, MapFile.DATA_FILE_NAME);
    @OsUntrusted
    Path index = new @OsUntrusted Path(dir, MapFile.INDEX_FILE_NAME);
    @OsUntrusted
    Path bloom = new @OsUntrusted Path(dir, BLOOM_FILE_NAME);

    fs.delete(data, true);
    fs.delete(index, true);
    fs.delete(bloom, true);
    fs.delete(dir, true);
  }

  private static @OsUntrusted byte @OsUntrusted [] byteArrayForBloomKey(@OsUntrusted DataOutputBuffer buf) {
    @OsUntrusted
    int cleanLength = buf.getLength();
    @OsUntrusted
    byte @OsUntrusted [] ba = buf.getData();
    if (cleanLength != ba.length) {
      ba = new @OsUntrusted byte @OsUntrusted [cleanLength];
      System.arraycopy(buf.getData(), 0, ba, 0, cleanLength);
    }
    return ba;
  }
  
  public static class Writer extends MapFile.@OsUntrusted Writer {
    private @OsUntrusted DynamicBloomFilter bloomFilter;
    private @OsUntrusted int numKeys;
    private @OsUntrusted int vectorSize;
    private @OsUntrusted Key bloomKey = new @OsUntrusted Key();
    private @OsUntrusted DataOutputBuffer buf = new @OsUntrusted DataOutputBuffer();
    private @OsUntrusted FileSystem fs;
    private @OsUntrusted Path dir;
    
    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
        @OsUntrusted
        Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> keyClass,
        @OsUntrusted
        Class<@OsUntrusted ? extends @OsUntrusted Writable> valClass, @OsUntrusted CompressionType compress,
        @OsUntrusted
        CompressionCodec codec, @OsUntrusted Progressable progress) throws IOException {
      this(conf, new @OsUntrusted Path(dirName), keyClass(keyClass), valueClass(valClass), 
           compression(compress, codec), progressable(progress));
    }

    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
        @OsUntrusted
        Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> keyClass,
        @OsUntrusted
        Class valClass, @OsUntrusted CompressionType compress,
        @OsUntrusted
        Progressable progress) throws IOException {
      this(conf, new @OsUntrusted Path(dirName), keyClass(keyClass), valueClass(valClass), 
           compression(compress), progressable(progress));
    }

    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
        @OsUntrusted
        Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> keyClass,
        @OsUntrusted
        Class valClass, @OsUntrusted CompressionType compress)
        throws IOException {
      this(conf, new @OsUntrusted Path(dirName), keyClass(keyClass), valueClass(valClass), 
           compression(compress));
    }

    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
        @OsUntrusted
        WritableComparator comparator, @OsUntrusted Class valClass,
        @OsUntrusted
        CompressionType compress, @OsUntrusted CompressionCodec codec, @OsUntrusted Progressable progress)
        throws IOException {
      this(conf, new @OsUntrusted Path(dirName), comparator(comparator), 
           valueClass(valClass), compression(compress, codec), 
           progressable(progress));
    }

    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
        @OsUntrusted
        WritableComparator comparator, @OsUntrusted Class valClass,
        @OsUntrusted
        CompressionType compress, @OsUntrusted Progressable progress) throws IOException {
      this(conf, new @OsUntrusted Path(dirName), comparator(comparator), 
           valueClass(valClass), compression(compress),
           progressable(progress));
    }

    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
        @OsUntrusted
        WritableComparator comparator, @OsUntrusted Class valClass, @OsUntrusted CompressionType compress)
        throws IOException {
      this(conf, new @OsUntrusted Path(dirName), comparator(comparator), 
           valueClass(valClass), compression(compress));
    }

    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
        @OsUntrusted
        WritableComparator comparator, @OsUntrusted Class valClass) throws IOException {
      this(conf, new @OsUntrusted Path(dirName), comparator(comparator), 
           valueClass(valClass));
    }

    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
                  @OsUntrusted
                  Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> keyClass,
                  @OsUntrusted
                  Class valClass) throws IOException {
      this(conf, new @OsUntrusted Path(dirName), keyClass(keyClass), valueClass(valClass));
    }

    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted Path dir, 
                  SequenceFile.Writer.@OsUntrusted Option @OsUntrusted ... options) throws IOException {
      super(conf, dir, options);
      this.fs = dir.getFileSystem(conf);
      this.dir = dir;
      initBloomFilter(conf);
    }

    private synchronized void initBloomFilter(BloomMapFile.@OsUntrusted Writer this, @OsUntrusted Configuration conf) {
      numKeys = conf.getInt("io.mapfile.bloom.size", 1024 * 1024);
      // vector size should be <code>-kn / (ln(1 - c^(1/k)))</code> bits for
      // single key, where <code> is the number of hash functions,
      // <code>n</code> is the number of keys and <code>c</code> is the desired
      // max. error rate.
      // Our desired error rate is by default 0.005, i.e. 0.5%
      @OsUntrusted
      float errorRate = conf.getFloat("io.mapfile.bloom.error.rate", 0.005f);
      vectorSize = (@OsUntrusted int)Math.ceil((double)(-HASH_COUNT * numKeys) /
          Math.log(1.0 - Math.pow(errorRate, 1.0/HASH_COUNT)));
      bloomFilter = new @OsUntrusted DynamicBloomFilter(vectorSize, HASH_COUNT,
          Hash.getHashType(conf), numKeys);
    }

    @Override
    public synchronized void append(BloomMapFile.@OsUntrusted Writer this, @OsUntrusted WritableComparable key, @OsUntrusted Writable val)
        throws IOException {
      super.append(key, val);
      buf.reset();
      key.write(buf);
      bloomKey.set(byteArrayForBloomKey(buf), 1.0);
      bloomFilter.add(bloomKey);
    }

    @Override
    public synchronized void close(BloomMapFile.@OsUntrusted Writer this) throws IOException {
      super.close();
      @OsUntrusted
      DataOutputStream out = fs.create(new @OsUntrusted Path(dir, BLOOM_FILE_NAME), true);
      try {
        bloomFilter.write(out);
        out.flush();
        out.close();
        out = null;
      } finally {
        IOUtils.closeStream(out);
      }
    }

  }
  
  public static class Reader extends MapFile.@OsUntrusted Reader {
    private @OsUntrusted DynamicBloomFilter bloomFilter;
    private @OsUntrusted DataOutputBuffer buf = new @OsUntrusted DataOutputBuffer();
    private @OsUntrusted Key bloomKey = new @OsUntrusted Key();

    public @OsUntrusted Reader(@OsUntrusted Path dir, @OsUntrusted Configuration conf,
                  SequenceFile.Reader.@OsUntrusted Option @OsUntrusted ... options) throws IOException {
      super(dir, conf, options);
      initBloomFilter(dir, conf);
    }

    @Deprecated
    public @OsUntrusted Reader(@OsUntrusted FileSystem fs, @OsUntrusted String dirName, @OsUntrusted Configuration conf)
        throws IOException {
      this(new @OsUntrusted Path(dirName), conf);
    }

    @Deprecated
    public @OsUntrusted Reader(@OsUntrusted FileSystem fs, @OsUntrusted String dirName, @OsUntrusted WritableComparator comparator,
        @OsUntrusted
        Configuration conf, @OsUntrusted boolean open) throws IOException {
      this(new @OsUntrusted Path(dirName), conf, comparator(comparator));
    }

    @Deprecated
    public @OsUntrusted Reader(@OsUntrusted FileSystem fs, @OsUntrusted String dirName, @OsUntrusted WritableComparator comparator,
        @OsUntrusted
        Configuration conf) throws IOException {
      this(new @OsUntrusted Path(dirName), conf, comparator(comparator));
    }
    
    private void initBloomFilter(BloomMapFile.@OsUntrusted Reader this, @OsUntrusted Path dirName, 
                                 @OsUntrusted
                                 Configuration conf) {
      
      @OsUntrusted
      DataInputStream in = null;
      try {
        @OsUntrusted
        FileSystem fs = dirName.getFileSystem(conf);
        in = fs.open(new @OsUntrusted Path(dirName, BLOOM_FILE_NAME));
        bloomFilter = new @OsUntrusted DynamicBloomFilter();
        bloomFilter.readFields(in);
        in.close();
        in = null;
      } catch (@OsUntrusted IOException ioe) {
        LOG.warn("Can't open BloomFilter: " + ioe + " - fallback to MapFile.");
        bloomFilter = null;
      } finally {
        IOUtils.closeStream(in);
      }
    }
    
    /**
     * Checks if this MapFile has the indicated key. The membership test is
     * performed using a Bloom filter, so the result has always non-zero
     * probability of false positives.
     * @param key key to check
     * @return  false iff key doesn't exist, true if key probably exists.
     * @throws IOException
     */
    public @OsUntrusted boolean probablyHasKey(BloomMapFile.@OsUntrusted Reader this, @OsUntrusted WritableComparable key) throws IOException {
      if (bloomFilter == null) {
        return true;
      }
      buf.reset();
      key.write(buf);
      bloomKey.set(byteArrayForBloomKey(buf), 1.0);
      return bloomFilter.membershipTest(bloomKey);
    }
    
    /**
     * Fast version of the
     * {@link MapFile.Reader#get(WritableComparable, Writable)} method. First
     * it checks the Bloom filter for the existence of the key, and only if
     * present it performs the real get operation. This yields significant
     * performance improvements for get operations on sparsely populated files.
     */
    @Override
    public synchronized @OsUntrusted Writable get(BloomMapFile.@OsUntrusted Reader this, @OsUntrusted WritableComparable key, @OsUntrusted Writable val)
        throws IOException {
      if (!probablyHasKey(key)) {
        return null;
      }
      return super.get(key, val);
    }
    
    /**
     * Retrieve the Bloom filter used by this instance of the Reader.
     * @return a Bloom filter (see {@link Filter})
     */
    public @OsUntrusted Filter getBloomFilter(BloomMapFile.@OsUntrusted Reader this) {
      return bloomFilter;
    }
  }
}
