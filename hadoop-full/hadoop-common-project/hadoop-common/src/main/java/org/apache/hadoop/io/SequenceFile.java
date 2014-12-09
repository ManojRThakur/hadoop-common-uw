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
import ostrusted.quals.OsTrusted;
import java.io.*;
import java.util.*;
import java.rmi.server.UID;
import java.security.MessageDigest;
import org.apache.commons.logging.*;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.MergeSort;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.hadoop.util.Time;

/** 
 * <code>SequenceFile</code>s are flat files consisting of binary key/value 
 * pairs.
 * 
 * <p><code>SequenceFile</code> provides {@link Writer}, {@link Reader} and
 * {@link Sorter} classes for writing, reading and sorting respectively.</p>
 * 
 * There are three <code>SequenceFile</code> <code>Writer</code>s based on the 
 * {@link CompressionType} used to compress key/value pairs:
 * <ol>
 *   <li>
 *   <code>Writer</code> : Uncompressed records.
 *   </li>
 *   <li>
 *   <code>RecordCompressWriter</code> : Record-compressed files, only compress 
 *                                       values.
 *   </li>
 *   <li>
 *   <code>BlockCompressWriter</code> : Block-compressed files, both keys & 
 *                                      values are collected in 'blocks' 
 *                                      separately and compressed. The size of 
 *                                      the 'block' is configurable.
 * </ol>
 * 
 * <p>The actual compression algorithm used to compress key and/or values can be
 * specified by using the appropriate {@link CompressionCodec}.</p>
 * 
 * <p>The recommended way is to use the static <tt>createWriter</tt> methods
 * provided by the <code>SequenceFile</code> to chose the preferred format.</p>
 *
 * <p>The {@link Reader} acts as the bridge and can read any of the above 
 * <code>SequenceFile</code> formats.</p>
 *
 * <h4 id="Formats">SequenceFile Formats</h4>
 * 
 * <p>Essentially there are 3 different formats for <code>SequenceFile</code>s
 * depending on the <code>CompressionType</code> specified. All of them share a
 * <a href="#Header">common header</a> described below.
 * 
 * <h5 id="Header">SequenceFile Header</h5>
 * <ul>
 *   <li>
 *   version - 3 bytes of magic header <b>SEQ</b>, followed by 1 byte of actual 
 *             version number (e.g. SEQ4 or SEQ6)
 *   </li>
 *   <li>
 *   keyClassName -key class
 *   </li>
 *   <li>
 *   valueClassName - value class
 *   </li>
 *   <li>
 *   compression - A boolean which specifies if compression is turned on for 
 *                 keys/values in this file.
 *   </li>
 *   <li>
 *   blockCompression - A boolean which specifies if block-compression is 
 *                      turned on for keys/values in this file.
 *   </li>
 *   <li>
 *   compression codec - <code>CompressionCodec</code> class which is used for  
 *                       compression of keys and/or values (if compression is 
 *                       enabled).
 *   </li>
 *   <li>
 *   metadata - {@link Metadata} for this file.
 *   </li>
 *   <li>
 *   sync - A sync marker to denote end of the header.
 *   </li>
 * </ul>
 * 
 * <h5 id="#UncompressedFormat">Uncompressed SequenceFile Format</h5>
 * <ul>
 * <li>
 * <a href="#Header">Header</a>
 * </li>
 * <li>
 * Record
 *   <ul>
 *     <li>Record length</li>
 *     <li>Key length</li>
 *     <li>Key</li>
 *     <li>Value</li>
 *   </ul>
 * </li>
 * <li>
 * A sync-marker every few <code>100</code> bytes or so.
 * </li>
 * </ul>
 *
 * <h5 id="#RecordCompressedFormat">Record-Compressed SequenceFile Format</h5>
 * <ul>
 * <li>
 * <a href="#Header">Header</a>
 * </li>
 * <li>
 * Record
 *   <ul>
 *     <li>Record length</li>
 *     <li>Key length</li>
 *     <li>Key</li>
 *     <li><i>Compressed</i> Value</li>
 *   </ul>
 * </li>
 * <li>
 * A sync-marker every few <code>100</code> bytes or so.
 * </li>
 * </ul>
 * 
 * <h5 id="#BlockCompressedFormat">Block-Compressed SequenceFile Format</h5>
 * <ul>
 * <li>
 * <a href="#Header">Header</a>
 * </li>
 * <li>
 * Record <i>Block</i>
 *   <ul>
 *     <li>Uncompressed number of records in the block</li>
 *     <li>Compressed key-lengths block-size</li>
 *     <li>Compressed key-lengths block</li>
 *     <li>Compressed keys block-size</li>
 *     <li>Compressed keys block</li>
 *     <li>Compressed value-lengths block-size</li>
 *     <li>Compressed value-lengths block</li>
 *     <li>Compressed values block-size</li>
 *     <li>Compressed values block</li>
 *   </ul>
 * </li>
 * <li>
 * A sync-marker every block.
 * </li>
 * </ul>
 * 
 * <p>The compressed blocks of key lengths and value lengths consist of the 
 * actual lengths of individual keys/values encoded in ZeroCompressedInteger 
 * format.</p>
 * 
 * @see CompressionCodec
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFile {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(SequenceFile.class);

  private @OsUntrusted SequenceFile() {}                         // no public ctor

  private static final @OsUntrusted byte BLOCK_COMPRESS_VERSION = (@OsUntrusted byte)4;
  private static final @OsUntrusted byte CUSTOM_COMPRESS_VERSION = (@OsUntrusted byte)5;
  private static final @OsUntrusted byte VERSION_WITH_METADATA = (@OsUntrusted byte)6;
  private static @OsUntrusted byte @OsUntrusted [] VERSION = new @OsUntrusted byte @OsUntrusted [] {
    (@OsUntrusted byte)'S', (@OsUntrusted byte)'E', (@OsUntrusted byte)'Q', VERSION_WITH_METADATA
  };

  private static final @OsUntrusted int SYNC_ESCAPE = -1;      // "length" of sync entries
  private static final @OsUntrusted int SYNC_HASH_SIZE = 16;   // number of bytes in hash 
  private static final @OsUntrusted int SYNC_SIZE = 4+SYNC_HASH_SIZE; // escape + hash

  /** The number of bytes between sync points.*/
  public static final @OsUntrusted int SYNC_INTERVAL = 100*SYNC_SIZE; 

  /** 
   * The compression type used to compress key/value pairs in the 
   * {@link SequenceFile}.
   * 
   * @see SequenceFile.Writer
   */
  public static enum CompressionType {
    /** Do not compress records. */

@OsUntrusted  NONE,
    /** Compress values only, each separately. */

@OsUntrusted  RECORD,
    /** Compress sequences of records together in blocks. */

@OsUntrusted  BLOCK
  }

  /**
   * Get the compression type for the reduce outputs
   * @param job the job config to look in
   * @return the kind of compression to use
   */
  static public @OsUntrusted CompressionType getDefaultCompressionType(@OsUntrusted Configuration job) {
    @OsUntrusted
    String name = job.get("io.seqfile.compression.type");
    return name == null ? CompressionType.RECORD : 
      CompressionType.valueOf(name);
  }
  
  /**
   * Set the default compression type for sequence files.
   * @param job the configuration to modify
   * @param val the new compression type (none, block, record)
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  static public void setDefaultCompressionType(@OsUntrusted Configuration job, 
                                               @OsTrusted CompressionType val) {
    job.set("io.seqfile.compression.type", (@OsTrusted String) val.toString());
  }

  /**
   * Create a new Writer with the given options.
   * @param conf the configuration to use
   * @param opts the options to create the file with
   * @return a new Writer
   * @throws IOException
   */
  public static @OsUntrusted Writer createWriter(@OsUntrusted Configuration conf, Writer.@OsUntrusted Option @OsUntrusted ... opts
                                    ) throws IOException {
    Writer.@OsUntrusted CompressionOption compressionOption = 
      Options.getOption(Writer.CompressionOption.class, opts);
    @OsUntrusted
    CompressionType kind;
    if (compressionOption != null) {
      kind = compressionOption.getValue();
    } else {
      kind = getDefaultCompressionType(conf);
      opts = Options.prependOptions(opts, Writer.compression(kind));
    }
    switch (kind) {
      default:
      case NONE:
        return new @OsUntrusted Writer(conf, opts);
      case RECORD:
        return new @OsUntrusted RecordCompressWriter(conf, opts);
      case BLOCK:
        return new @OsUntrusted BlockCompressWriter(conf, opts);
    }
  }

  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem. 
   * @param conf The configuration.
   * @param name The name of the file. 
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  public static @OsUntrusted Writer 
    createWriter(@OsUntrusted FileSystem fs, @OsUntrusted Configuration conf, @OsUntrusted Path name, 
                 @OsUntrusted
                 Class keyClass, @OsUntrusted Class valClass) throws IOException {
    return createWriter(conf, Writer.filesystem(fs),
                        Writer.file(name), Writer.keyClass(keyClass),
                        Writer.valueClass(valClass));
  }
  
  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem. 
   * @param conf The configuration.
   * @param name The name of the file. 
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  public static @OsUntrusted Writer 
    createWriter(@OsUntrusted FileSystem fs, @OsUntrusted Configuration conf, @OsUntrusted Path name, 
                 @OsUntrusted
                 Class keyClass, @OsUntrusted Class valClass, 
                 @OsUntrusted
                 CompressionType compressionType) throws IOException {
    return createWriter(conf, Writer.filesystem(fs),
                        Writer.file(name), Writer.keyClass(keyClass),
                        Writer.valueClass(valClass), 
                        Writer.compression(compressionType));
  }
  
  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem. 
   * @param conf The configuration.
   * @param name The name of the file. 
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @param progress The Progressable object to track progress.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  public static @OsUntrusted Writer
    createWriter(@OsUntrusted FileSystem fs, @OsUntrusted Configuration conf, @OsUntrusted Path name, 
                 @OsUntrusted
                 Class keyClass, @OsUntrusted Class valClass, @OsUntrusted CompressionType compressionType,
                 @OsUntrusted
                 Progressable progress) throws IOException {
    return createWriter(conf, Writer.file(name),
                        Writer.filesystem(fs),
                        Writer.keyClass(keyClass),
                        Writer.valueClass(valClass), 
                        Writer.compression(compressionType),
                        Writer.progressable(progress));
  }

  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem. 
   * @param conf The configuration.
   * @param name The name of the file. 
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @param codec The compression codec.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  public static @OsUntrusted Writer 
    createWriter(@OsUntrusted FileSystem fs, @OsUntrusted Configuration conf, @OsUntrusted Path name, 
                 @OsUntrusted
                 Class keyClass, @OsUntrusted Class valClass, @OsUntrusted CompressionType compressionType, 
                 @OsUntrusted
                 CompressionCodec codec) throws IOException {
    return createWriter(conf, Writer.file(name),
                        Writer.filesystem(fs),
                        Writer.keyClass(keyClass),
                        Writer.valueClass(valClass), 
                        Writer.compression(compressionType, codec));
  }
  
  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem. 
   * @param conf The configuration.
   * @param name The name of the file. 
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @param codec The compression codec.
   * @param progress The Progressable object to track progress.
   * @param metadata The metadata of the file.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  public static @OsUntrusted Writer
    createWriter(@OsUntrusted FileSystem fs, @OsUntrusted Configuration conf, @OsUntrusted Path name, 
                 @OsUntrusted
                 Class keyClass, @OsUntrusted Class valClass, 
                 @OsUntrusted
                 CompressionType compressionType, @OsUntrusted CompressionCodec codec,
                 @OsUntrusted
                 Progressable progress, @OsUntrusted Metadata metadata) throws IOException {
    return createWriter(conf, Writer.file(name),
                        Writer.filesystem(fs),
                        Writer.keyClass(keyClass),
                        Writer.valueClass(valClass),
                        Writer.compression(compressionType, codec),
                        Writer.progressable(progress),
                        Writer.metadata(metadata));
  }

  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem.
   * @param conf The configuration.
   * @param name The name of the file.
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param bufferSize buffer size for the underlaying outputstream.
   * @param replication replication factor for the file.
   * @param blockSize block size for the file.
   * @param compressionType The compression type.
   * @param codec The compression codec.
   * @param progress The Progressable object to track progress.
   * @param metadata The metadata of the file.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  public static @OsUntrusted Writer
    createWriter(@OsUntrusted FileSystem fs, @OsUntrusted Configuration conf, @OsUntrusted Path name,
                 @OsUntrusted
                 Class keyClass, @OsUntrusted Class valClass, @OsUntrusted int bufferSize,
                 @OsUntrusted
                 short replication, @OsUntrusted long blockSize,
                 @OsUntrusted
                 CompressionType compressionType, @OsUntrusted CompressionCodec codec,
                 @OsUntrusted
                 Progressable progress, @OsUntrusted Metadata metadata) throws IOException {
    return createWriter(conf, Writer.file(name),
                        Writer.filesystem(fs),
                        Writer.keyClass(keyClass),
                        Writer.valueClass(valClass), 
                        Writer.bufferSize(bufferSize), 
                        Writer.replication(replication),
                        Writer.blockSize(blockSize),
                        Writer.compression(compressionType, codec),
                        Writer.progressable(progress),
                        Writer.metadata(metadata));
  }

  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem.
   * @param conf The configuration.
   * @param name The name of the file.
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param bufferSize buffer size for the underlaying outputstream.
   * @param replication replication factor for the file.
   * @param blockSize block size for the file.
   * @param createParent create parent directory if non-existent
   * @param compressionType The compression type.
   * @param codec The compression codec.
   * @param metadata The metadata of the file.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   */
  @Deprecated
  public static @OsUntrusted Writer
  createWriter(@OsUntrusted FileSystem fs, @OsUntrusted Configuration conf, @OsUntrusted Path name,
               @OsUntrusted
               Class keyClass, @OsUntrusted Class valClass, @OsUntrusted int bufferSize,
               @OsUntrusted
               short replication, @OsUntrusted long blockSize, @OsUntrusted boolean createParent,
               @OsUntrusted
               CompressionType compressionType, @OsUntrusted CompressionCodec codec,
               @OsUntrusted
               Metadata metadata) throws IOException {
    return createWriter(FileContext.getFileContext(fs.getUri(), conf),
        conf, name, keyClass, valClass, compressionType, codec,
        metadata, EnumSet.of(CreateFlag.CREATE,CreateFlag.OVERWRITE),
        CreateOpts.bufferSize(bufferSize),
        createParent ? CreateOpts.createParent()
                     : CreateOpts.donotCreateParent(),
        CreateOpts.repFac(replication),
        CreateOpts.blockSize(blockSize)
      );
  }

  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fc The context for the specified file.
   * @param conf The configuration.
   * @param name The name of the file.
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @param codec The compression codec.
   * @param metadata The metadata of the file.
   * @param createFlag gives the semantics of create: overwrite, append etc.
   * @param opts file creation options; see {@link CreateOpts}.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   */
  public static @OsUntrusted Writer
  createWriter(@OsUntrusted FileContext fc, @OsUntrusted Configuration conf, @OsUntrusted Path name,
               @OsUntrusted
               Class keyClass, @OsUntrusted Class valClass,
               @OsUntrusted
               CompressionType compressionType, @OsUntrusted CompressionCodec codec,
               @OsUntrusted
               Metadata metadata,
               final @OsUntrusted EnumSet<@OsUntrusted CreateFlag> createFlag, @OsUntrusted CreateOpts @OsUntrusted ... opts)
               throws IOException {
    return createWriter(conf, fc.create(name, createFlag, opts),
          keyClass, valClass, compressionType, codec, metadata).ownStream();
  }

  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem. 
   * @param conf The configuration.
   * @param name The name of the file. 
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @param codec The compression codec.
   * @param progress The Progressable object to track progress.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  public static @OsUntrusted Writer
    createWriter(@OsUntrusted FileSystem fs, @OsUntrusted Configuration conf, @OsUntrusted Path name, 
                 @OsUntrusted
                 Class keyClass, @OsUntrusted Class valClass, 
                 @OsUntrusted
                 CompressionType compressionType, @OsUntrusted CompressionCodec codec,
                 @OsUntrusted
                 Progressable progress) throws IOException {
    return createWriter(conf, Writer.file(name),
                        Writer.filesystem(fs),
                        Writer.keyClass(keyClass),
                        Writer.valueClass(valClass),
                        Writer.compression(compressionType, codec),
                        Writer.progressable(progress));
  }

  /**
   * Construct the preferred type of 'raw' SequenceFile Writer.
   * @param conf The configuration.
   * @param out The stream on top which the writer is to be constructed.
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @param codec The compression codec.
   * @param metadata The metadata of the file.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  public static @OsUntrusted Writer
    createWriter(@OsUntrusted Configuration conf, @OsUntrusted FSDataOutputStream out, 
                 @OsUntrusted
                 Class keyClass, @OsUntrusted Class valClass,
                 @OsUntrusted
                 CompressionType compressionType,
                 @OsUntrusted
                 CompressionCodec codec, @OsUntrusted Metadata metadata) throws IOException {
    return createWriter(conf, Writer.stream(out), Writer.keyClass(keyClass),
                        Writer.valueClass(valClass), 
                        Writer.compression(compressionType, codec),
                        Writer.metadata(metadata));
  }
  
  /**
   * Construct the preferred type of 'raw' SequenceFile Writer.
   * @param conf The configuration.
   * @param out The stream on top which the writer is to be constructed.
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @param codec The compression codec.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  public static @OsUntrusted Writer
    createWriter(@OsUntrusted Configuration conf, @OsUntrusted FSDataOutputStream out, 
                 @OsUntrusted
                 Class keyClass, @OsUntrusted Class valClass, @OsUntrusted CompressionType compressionType,
                 @OsUntrusted
                 CompressionCodec codec) throws IOException {
    return createWriter(conf, Writer.stream(out), Writer.keyClass(keyClass),
                        Writer.valueClass(valClass),
                        Writer.compression(compressionType, codec));
  }
  

  /** The interface to 'raw' values of SequenceFiles. */
  public static interface ValueBytes {

    /** Writes the uncompressed bytes to the outStream.
     * @param outStream : Stream to write uncompressed bytes into.
     * @throws IOException
     */
    public void writeUncompressedBytes(SequenceFile.@OsUntrusted ValueBytes this, @OsUntrusted DataOutputStream outStream)
      throws IOException;

    /** Write compressed bytes to outStream. 
     * Note: that it will NOT compress the bytes if they are not compressed.
     * @param outStream : Stream to write compressed bytes into.
     */
    public void writeCompressedBytes(SequenceFile.@OsUntrusted ValueBytes this, @OsUntrusted DataOutputStream outStream) 
      throws IllegalArgumentException, IOException;

    /**
     * Size of stored data.
     */
    public @OsUntrusted int getSize(SequenceFile.@OsUntrusted ValueBytes this);
  }
  
  private static class UncompressedBytes implements @OsUntrusted ValueBytes {
    private @OsUntrusted int dataSize;
    private @OsUntrusted byte @OsUntrusted [] data;
    
    private @OsUntrusted UncompressedBytes() {
      data = null;
      dataSize = 0;
    }
    
    private void reset(SequenceFile.@OsUntrusted UncompressedBytes this, @OsUntrusted DataInputStream in, @OsUntrusted int length) throws IOException {
      if (data == null) {
        data = new @OsUntrusted byte @OsUntrusted [length];
      } else if (length > data.length) {
        data = new @OsUntrusted byte @OsUntrusted [Math.max(length, data.length * 2)];
      }
      dataSize = -1;
      in.readFully(data, 0, length);
      dataSize = length;
    }
    
    @Override
    public @OsUntrusted int getSize(SequenceFile.@OsUntrusted UncompressedBytes this) {
      return dataSize;
    }
    
    @Override
    public void writeUncompressedBytes(SequenceFile.@OsUntrusted UncompressedBytes this, @OsUntrusted DataOutputStream outStream)
      throws IOException {
      outStream.write(data, 0, dataSize);
    }

    @Override
    public void writeCompressedBytes(SequenceFile.@OsUntrusted UncompressedBytes this, @OsUntrusted DataOutputStream outStream) 
      throws IllegalArgumentException, IOException {
      throw 
        new @OsUntrusted IllegalArgumentException("UncompressedBytes cannot be compressed!");
    }

  } // UncompressedBytes
  
  private static class CompressedBytes implements @OsUntrusted ValueBytes {
    private @OsUntrusted int dataSize;
    private @OsUntrusted byte @OsUntrusted [] data;
    @OsUntrusted
    DataInputBuffer rawData = null;
    @OsUntrusted
    CompressionCodec codec = null;
    @OsUntrusted
    CompressionInputStream decompressedStream = null;

    private @OsUntrusted CompressedBytes(@OsUntrusted CompressionCodec codec) {
      data = null;
      dataSize = 0;
      this.codec = codec;
    }

    private void reset(SequenceFile.@OsUntrusted CompressedBytes this, @OsUntrusted DataInputStream in, @OsUntrusted int length) throws IOException {
      if (data == null) {
        data = new @OsUntrusted byte @OsUntrusted [length];
      } else if (length > data.length) {
        data = new @OsUntrusted byte @OsUntrusted [Math.max(length, data.length * 2)];
      } 
      dataSize = -1;
      in.readFully(data, 0, length);
      dataSize = length;
    }
    
    @Override
    public @OsUntrusted int getSize(SequenceFile.@OsUntrusted CompressedBytes this) {
      return dataSize;
    }
    
    @Override
    public void writeUncompressedBytes(SequenceFile.@OsUntrusted CompressedBytes this, @OsUntrusted DataOutputStream outStream)
      throws IOException {
      if (decompressedStream == null) {
        rawData = new @OsUntrusted DataInputBuffer();
        decompressedStream = codec.createInputStream(rawData);
      } else {
        decompressedStream.resetState();
      }
      rawData.reset(data, 0, dataSize);

      @OsUntrusted
      byte @OsUntrusted [] buffer = new @OsUntrusted byte @OsUntrusted [8192];
      @OsUntrusted
      int bytesRead = 0;
      while ((bytesRead = decompressedStream.read(buffer, 0, 8192)) != -1) {
        outStream.write(buffer, 0, bytesRead);
      }
    }

    @Override
    public void writeCompressedBytes(SequenceFile.@OsUntrusted CompressedBytes this, @OsUntrusted DataOutputStream outStream) 
      throws IllegalArgumentException, IOException {
      outStream.write(data, 0, dataSize);
    }

  } // CompressedBytes
  
  /**
   * The class encapsulating with the metadata of a file.
   * The metadata of a file is a list of attribute name/value
   * pairs of Text type.
   *
   */
  public static class Metadata implements @OsUntrusted Writable {

    private @OsUntrusted TreeMap<@OsUntrusted Text, @OsUntrusted Text> theMetadata;
    
    public @OsUntrusted Metadata() {
      this(new @OsUntrusted TreeMap<@OsUntrusted Text, @OsUntrusted Text>());
    }
    
    public @OsUntrusted Metadata(@OsUntrusted TreeMap<@OsUntrusted Text, @OsUntrusted Text> arg) {
      if (arg == null) {
        this.theMetadata = new @OsUntrusted TreeMap<@OsUntrusted Text, @OsUntrusted Text>();
      } else {
        this.theMetadata = arg;
      }
    }
    
    public @OsUntrusted Text get(SequenceFile.@OsUntrusted Metadata this, @OsUntrusted Text name) {
      return this.theMetadata.get(name);
    }
    
    public void set(SequenceFile.@OsUntrusted Metadata this, @OsUntrusted Text name, @OsUntrusted Text value) {
      this.theMetadata.put(name, value);
    }
    
    public @OsUntrusted TreeMap<@OsUntrusted Text, @OsUntrusted Text> getMetadata(SequenceFile.@OsUntrusted Metadata this) {
      return new @OsUntrusted TreeMap<@OsUntrusted Text, @OsUntrusted Text>(this.theMetadata);
    }
    
    @Override
    public void write(SequenceFile.@OsUntrusted Metadata this, @OsUntrusted DataOutput out) throws IOException {
      out.writeInt(this.theMetadata.size());
      @OsUntrusted
      Iterator<Map.@OsUntrusted Entry<@OsUntrusted Text, @OsUntrusted Text>> iter =
        this.theMetadata.entrySet().iterator();
      while (iter.hasNext()) {
        Map.@OsUntrusted Entry<@OsUntrusted Text, @OsUntrusted Text> en = iter.next();
        en.getKey().write(out);
        en.getValue().write(out);
      }
    }

    @Override
    public void readFields(SequenceFile.@OsUntrusted Metadata this, @OsUntrusted DataInput in) throws IOException {
      @OsUntrusted
      int sz = in.readInt();
      if (sz < 0) throw new @OsUntrusted IOException("Invalid size: " + sz + " for file metadata object");
      this.theMetadata = new @OsUntrusted TreeMap<@OsUntrusted Text, @OsUntrusted Text>();
      for (@OsUntrusted int i = 0; i < sz; i++) {
        @OsUntrusted
        Text key = new @OsUntrusted Text();
        @OsUntrusted
        Text val = new @OsUntrusted Text();
        key.readFields(in);
        val.readFields(in);
        this.theMetadata.put(key, val);
      }    
    }

    @Override
    public @OsUntrusted boolean equals(SequenceFile.@OsUntrusted Metadata this, @OsUntrusted Object other) {
      if (other == null) {
        return false;
      }
      if (other.getClass() != this.getClass()) {
        return false;
      } else {
        return equals((@OsUntrusted Metadata)other);
      }
    }
    
    public @OsUntrusted boolean equals(SequenceFile.@OsUntrusted Metadata this, @OsUntrusted Metadata other) {
      if (other == null) return false;
      if (this.theMetadata.size() != other.theMetadata.size()) {
        return false;
      }
      @OsUntrusted
      Iterator<Map.@OsUntrusted Entry<@OsUntrusted Text, @OsUntrusted Text>> iter1 =
        this.theMetadata.entrySet().iterator();
      @OsUntrusted
      Iterator<Map.@OsUntrusted Entry<@OsUntrusted Text, @OsUntrusted Text>> iter2 =
        other.theMetadata.entrySet().iterator();
      while (iter1.hasNext() && iter2.hasNext()) {
        Map.@OsUntrusted Entry<@OsUntrusted Text, @OsUntrusted Text> en1 = iter1.next();
        Map.@OsUntrusted Entry<@OsUntrusted Text, @OsUntrusted Text> en2 = iter2.next();
        if (!en1.getKey().equals(en2.getKey())) {
          return false;
        }
        if (!en1.getValue().equals(en2.getValue())) {
          return false;
        }
      }
      if (iter1.hasNext() || iter2.hasNext()) {
        return false;
      }
      return true;
    }

    @Override
    public @OsUntrusted int hashCode(SequenceFile.@OsUntrusted Metadata this) {
      assert false : "hashCode not designed";
      return 42; // any arbitrary constant will do 
    }
    
    @Override
    public @OsUntrusted String toString(SequenceFile.@OsUntrusted Metadata this) {
      @OsUntrusted
      StringBuilder sb = new @OsUntrusted StringBuilder();
      sb.append("size: ").append(this.theMetadata.size()).append("\n");
      @OsUntrusted
      Iterator<Map.@OsUntrusted Entry<@OsUntrusted Text, @OsUntrusted Text>> iter =
        this.theMetadata.entrySet().iterator();
      while (iter.hasNext()) {
        Map.@OsUntrusted Entry<@OsUntrusted Text, @OsUntrusted Text> en = iter.next();
        sb.append("\t").append(en.getKey().toString()).append("\t").append(en.getValue().toString());
        sb.append("\n");
      }
      return sb.toString();
    }
  }
  
  /** Write key/value pairs to a sequence-format file. */
  public static class Writer implements java.io.Closeable, @OsUntrusted Syncable {
    private @OsUntrusted Configuration conf;
    @OsUntrusted
    FSDataOutputStream out;
    @OsUntrusted
    boolean ownOutputStream = true;
    @OsUntrusted
    DataOutputBuffer buffer = new @OsUntrusted DataOutputBuffer();

    @OsUntrusted
    Class keyClass;
    @OsUntrusted
    Class valClass;

    private final @OsUntrusted CompressionType compress;
    @OsUntrusted
    CompressionCodec codec = null;
    @OsUntrusted
    CompressionOutputStream deflateFilter = null;
    @OsUntrusted
    DataOutputStream deflateOut = null;
    @OsUntrusted
    Metadata metadata = null;
    @OsUntrusted
    Compressor compressor = null;
    
    protected @OsUntrusted Serializer keySerializer;
    protected @OsUntrusted Serializer uncompressedValSerializer;
    protected @OsUntrusted Serializer compressedValSerializer;
    
    // Insert a globally unique 16-byte value every few entries, so that one
    // can seek into the middle of a file and then synchronize with record
    // starts and ends by scanning for this value.
    @OsUntrusted
    long lastSyncPos;                     // position of last sync
    @OsUntrusted
    byte @OsUntrusted [] sync;                          // 16 random bytes
    {
      try {                                       
        MessageDigest digester = MessageDigest.getInstance("MD5");
        long time = Time.now();
        digester.update((new UID()+"@"+time).getBytes());
        sync = digester.digest();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public static interface Option {}
    
    static class FileOption extends Options.@OsUntrusted PathOption 
                                    implements @OsUntrusted Option {
      @OsUntrusted
      FileOption(@OsUntrusted Path path) {
        super(path);
      }
    }

    /**
     * @deprecated only used for backwards-compatibility in the createWriter methods
     * that take FileSystem.
     */
    @Deprecated
    private static class FileSystemOption implements @OsUntrusted Option {
      private final @OsUntrusted FileSystem value;
      protected @OsUntrusted FileSystemOption(@OsUntrusted FileSystem value) {
        this.value = value;
      }
      public @OsUntrusted FileSystem getValue(SequenceFile.Writer.@OsUntrusted FileSystemOption this) {
        return value;
      }
    }

    static class StreamOption extends Options.@OsUntrusted FSDataOutputStreamOption 
                              implements @OsUntrusted Option {
      @OsUntrusted
      StreamOption(@OsUntrusted FSDataOutputStream stream) {
        super(stream);
      }
    }

    static class BufferSizeOption extends Options.@OsUntrusted IntegerOption
                                  implements @OsUntrusted Option {
      @OsUntrusted
      BufferSizeOption(@OsUntrusted int value) {
        super(value);
      }
    }
    
    static class BlockSizeOption extends Options.@OsUntrusted LongOption implements @OsUntrusted Option {
      @OsUntrusted
      BlockSizeOption(@OsUntrusted long value) {
        super(value);
      }
    }

    static class ReplicationOption extends Options.@OsUntrusted IntegerOption
                                   implements @OsUntrusted Option {
      @OsUntrusted
      ReplicationOption(@OsUntrusted int value) {
        super(value);
      }
    }

    static class KeyClassOption extends Options.@OsUntrusted ClassOption implements @OsUntrusted Option {
      @OsUntrusted
      KeyClassOption(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> value) {
        super(value);
      }
    }

    static class ValueClassOption extends Options.@OsUntrusted ClassOption
                                          implements @OsUntrusted Option {
      @OsUntrusted
      ValueClassOption(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> value) {
        super(value);
      }
    }

    static class MetadataOption implements @OsUntrusted Option {
      private final @OsUntrusted Metadata value;
      @OsUntrusted
      MetadataOption(@OsUntrusted Metadata value) {
        this.value = value;
      }
      @OsUntrusted
      Metadata getValue(SequenceFile.Writer.@OsUntrusted MetadataOption this) {
        return value;
      }
    }

    static class ProgressableOption extends Options.@OsUntrusted ProgressableOption
                                    implements @OsUntrusted Option {
      @OsUntrusted
      ProgressableOption(@OsUntrusted Progressable value) {
        super(value);
      }
    }

    private static class CompressionOption implements @OsUntrusted Option {
      private final @OsUntrusted CompressionType value;
      private final @OsUntrusted CompressionCodec codec;
      @OsUntrusted
      CompressionOption(@OsUntrusted CompressionType value) {
        this(value, null);
      }
      @OsUntrusted
      CompressionOption(@OsUntrusted CompressionType value, @OsUntrusted CompressionCodec codec) {
        this.value = value;
        this.codec = (CompressionType.NONE != value && null == codec)
          ? new @OsUntrusted DefaultCodec()
          : codec;
      }
      @OsUntrusted
      CompressionType getValue(SequenceFile.Writer.@OsUntrusted CompressionOption this) {
        return value;
      }
      @OsUntrusted
      CompressionCodec getCodec(SequenceFile.Writer.@OsUntrusted CompressionOption this) {
        return codec;
      }
    }
    
    public static @OsUntrusted Option file(@OsUntrusted Path value) {
      return new @OsUntrusted FileOption(value);
    }

    /**
     * @deprecated only used for backwards-compatibility in the createWriter methods
     * that take FileSystem.
     */
    @Deprecated
    private static @OsUntrusted Option filesystem(@OsUntrusted FileSystem fs) {
      return new SequenceFile.Writer.@OsUntrusted FileSystemOption(fs);
    }
    
    public static @OsUntrusted Option bufferSize(@OsUntrusted int value) {
      return new @OsUntrusted BufferSizeOption(value);
    }
    
    public static @OsUntrusted Option stream(@OsUntrusted FSDataOutputStream value) {
      return new @OsUntrusted StreamOption(value);
    }
    
    public static @OsUntrusted Option replication(@OsUntrusted short value) {
      return new @OsUntrusted ReplicationOption(value);
    }
    
    public static @OsUntrusted Option blockSize(@OsUntrusted long value) {
      return new @OsUntrusted BlockSizeOption(value);
    }
    
    public static @OsUntrusted Option progressable(@OsUntrusted Progressable value) {
      return new @OsUntrusted ProgressableOption(value);
    }

    public static @OsUntrusted Option keyClass(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> value) {
      return new @OsUntrusted KeyClassOption(value);
    }
    
    public static @OsUntrusted Option valueClass(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> value) {
      return new @OsUntrusted ValueClassOption(value);
    }
    
    public static @OsUntrusted Option metadata(@OsUntrusted Metadata value) {
      return new @OsUntrusted MetadataOption(value);
    }

    public static @OsUntrusted Option compression(@OsUntrusted CompressionType value) {
      return new @OsUntrusted CompressionOption(value);
    }

    public static @OsUntrusted Option compression(@OsUntrusted CompressionType value,
        @OsUntrusted
        CompressionCodec codec) {
      return new @OsUntrusted CompressionOption(value, codec);
    }
    
    /**
     * Construct a uncompressed writer from a set of options.
     * @param conf the configuration to use
     * @param options the options used when creating the writer
     * @throws IOException if it fails
     */
    @OsUntrusted
    Writer(@OsUntrusted Configuration conf, 
           @OsUntrusted
           Option @OsUntrusted ... opts) throws IOException {
      @OsUntrusted
      BlockSizeOption blockSizeOption = 
        Options.getOption(BlockSizeOption.class, opts);
      @OsUntrusted
      BufferSizeOption bufferSizeOption = 
        Options.getOption(BufferSizeOption.class, opts);
      @OsUntrusted
      ReplicationOption replicationOption = 
        Options.getOption(ReplicationOption.class, opts);
      @OsUntrusted
      ProgressableOption progressOption = 
        Options.getOption(ProgressableOption.class, opts);
      @OsUntrusted
      FileOption fileOption = Options.getOption(FileOption.class, opts);
      @OsUntrusted
      FileSystemOption fsOption = Options.getOption(FileSystemOption.class, opts);
      @OsUntrusted
      StreamOption streamOption = Options.getOption(StreamOption.class, opts);
      @OsUntrusted
      KeyClassOption keyClassOption = 
        Options.getOption(KeyClassOption.class, opts);
      @OsUntrusted
      ValueClassOption valueClassOption = 
        Options.getOption(ValueClassOption.class, opts);
      @OsUntrusted
      MetadataOption metadataOption = 
        Options.getOption(MetadataOption.class, opts);
      @OsUntrusted
      CompressionOption compressionTypeOption =
        Options.getOption(CompressionOption.class, opts);
      // check consistency of options
      if ((fileOption == null) == (streamOption == null)) {
        throw new @OsUntrusted IllegalArgumentException("file or stream must be specified");
      }
      if (fileOption == null && (blockSizeOption != null ||
                                 bufferSizeOption != null ||
                                 replicationOption != null ||
                                 progressOption != null)) {
        throw new @OsUntrusted IllegalArgumentException("file modifier options not " +
                                           "compatible with stream");
      }

      @OsUntrusted
      FSDataOutputStream out;
      @OsUntrusted
      boolean ownStream = fileOption != null;
      if (ownStream) {
        @OsUntrusted
        Path p = fileOption.getValue();
        @OsUntrusted
        FileSystem fs;
        if (fsOption != null) {
          fs = fsOption.getValue();
        } else {
          fs = p.getFileSystem(conf);
        }
        @OsUntrusted
        int bufferSize = bufferSizeOption == null ? getBufferSize(conf) :
          bufferSizeOption.getValue();
        @OsUntrusted
        short replication = replicationOption == null ? 
          fs.getDefaultReplication(p) :
          (@OsUntrusted short) replicationOption.getValue();
        @OsUntrusted
        long blockSize = blockSizeOption == null ? fs.getDefaultBlockSize(p) :
          blockSizeOption.getValue();
        @OsUntrusted
        Progressable progress = progressOption == null ? null :
          progressOption.getValue();
        out = fs.create(p, true, bufferSize, replication, blockSize, progress);
      } else {
        out = streamOption.getValue();
      }
      @OsUntrusted
      Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> keyClass = keyClassOption == null ?
          Object.class : keyClassOption.getValue();
      @OsUntrusted
      Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> valueClass = valueClassOption == null ?
          Object.class : valueClassOption.getValue();
      @OsUntrusted
      Metadata metadata = metadataOption == null ?
          new @OsUntrusted Metadata() : metadataOption.getValue();
      this.compress = compressionTypeOption.getValue();
      final @OsUntrusted CompressionCodec codec = compressionTypeOption.getCodec();
      if (codec != null &&
          (codec instanceof @OsUntrusted GzipCodec) &&
          !NativeCodeLoader.isNativeCodeLoaded() &&
          !ZlibFactory.isNativeZlibLoaded(conf)) {
        throw new @OsUntrusted IllegalArgumentException("SequenceFile doesn't work with " +
                                           "GzipCodec without native-hadoop " +
                                           "code!");
      }
      init(conf, out, ownStream, keyClass, valueClass, codec, metadata);
    }

    /** Create the named file.
     * @deprecated Use 
     *   {@link SequenceFile#createWriter(Configuration, Writer.Option...)} 
     *   instead.
     */
    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted FileSystem fs, @OsUntrusted Configuration conf, @OsUntrusted Path name, 
                  @OsUntrusted
                  Class keyClass, @OsUntrusted Class valClass) throws IOException {
      this.compress = CompressionType.NONE;
      init(conf, fs.create(name), true, keyClass, valClass, null, 
           new @OsUntrusted Metadata());
    }
    
    /** Create the named file with write-progress reporter.
     * @deprecated Use 
     *   {@link SequenceFile#createWriter(Configuration, Writer.Option...)} 
     *   instead.
     */
    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted FileSystem fs, @OsUntrusted Configuration conf, @OsUntrusted Path name, 
                  @OsUntrusted
                  Class keyClass, @OsUntrusted Class valClass,
                  @OsUntrusted
                  Progressable progress, @OsUntrusted Metadata metadata) throws IOException {
      this.compress = CompressionType.NONE;
      init(conf, fs.create(name, progress), true, keyClass, valClass,
           null, metadata);
    }
    
    /** Create the named file with write-progress reporter. 
     * @deprecated Use 
     *   {@link SequenceFile#createWriter(Configuration, Writer.Option...)} 
     *   instead.
     */
    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted FileSystem fs, @OsUntrusted Configuration conf, @OsUntrusted Path name,
                  @OsUntrusted
                  Class keyClass, @OsUntrusted Class valClass,
                  @OsUntrusted
                  int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize,
                  @OsUntrusted
                  Progressable progress, @OsUntrusted Metadata metadata) throws IOException {
      this.compress = CompressionType.NONE;
      init(conf,
           fs.create(name, true, bufferSize, replication, blockSize, progress),
           true, keyClass, valClass, null, metadata);
    }

    @OsUntrusted
    boolean isCompressed(SequenceFile.@OsUntrusted Writer this) { return compress != CompressionType.NONE; }
    @OsUntrusted
    boolean isBlockCompressed(SequenceFile.@OsUntrusted Writer this) { return compress == CompressionType.BLOCK; }
    
    @OsUntrusted
    Writer ownStream(SequenceFile.@OsUntrusted Writer this) { this.ownOutputStream = true; return this;  }

    /** Write and flush the file header. */
    private void writeFileHeader(SequenceFile.@OsUntrusted Writer this) 
      throws IOException {
      out.write(VERSION);
      Text.writeString(out, keyClass.getName());
      Text.writeString(out, valClass.getName());
      
      out.writeBoolean(this.isCompressed());
      out.writeBoolean(this.isBlockCompressed());
      
      if (this.isCompressed()) {
        Text.writeString(out, (codec.getClass()).getName());
      }
      this.metadata.write(out);
      out.write(sync);                       // write the sync bytes
      out.flush();                           // flush header
    }
    
    /** Initialize. */
    @SuppressWarnings("unchecked")
    void init(SequenceFile.@OsUntrusted Writer this, @OsUntrusted Configuration conf, @OsUntrusted FSDataOutputStream out, @OsUntrusted boolean ownStream,
              @OsUntrusted
              Class keyClass, @OsUntrusted Class valClass,
              @OsUntrusted
              CompressionCodec codec, @OsUntrusted Metadata metadata) 
      throws IOException {
      this.conf = conf;
      this.out = out;
      this.ownOutputStream = ownStream;
      this.keyClass = keyClass;
      this.valClass = valClass;
      this.codec = codec;
      this.metadata = metadata;
      @OsUntrusted
      SerializationFactory serializationFactory = new @OsUntrusted SerializationFactory(conf);
      this.keySerializer = serializationFactory.getSerializer(keyClass);
      if (this.keySerializer == null) {
        throw new @OsUntrusted IOException(
            "Could not find a serializer for the Key class: '"
                + keyClass.getCanonicalName() + "'. "
                + "Please ensure that the configuration '" +
                CommonConfigurationKeys.IO_SERIALIZATIONS_KEY + "' is "
                + "properly configured, if you're using"
                + "custom serialization.");
      }
      this.keySerializer.open(buffer);
      this.uncompressedValSerializer = serializationFactory.getSerializer(valClass);
      if (this.uncompressedValSerializer == null) {
        throw new @OsUntrusted IOException(
            "Could not find a serializer for the Value class: '"
                + valClass.getCanonicalName() + "'. "
                + "Please ensure that the configuration '" +
                CommonConfigurationKeys.IO_SERIALIZATIONS_KEY + "' is "
                + "properly configured, if you're using"
                + "custom serialization.");
      }
      this.uncompressedValSerializer.open(buffer);
      if (this.codec != null) {
        ReflectionUtils.setConf(this.codec, this.conf);
        this.compressor = CodecPool.getCompressor(this.codec);
        this.deflateFilter = this.codec.createOutputStream(buffer, compressor);
        this.deflateOut = 
          new @OsUntrusted DataOutputStream(new @OsUntrusted BufferedOutputStream(deflateFilter));
        this.compressedValSerializer = serializationFactory.getSerializer(valClass);
        if (this.compressedValSerializer == null) {
          throw new @OsUntrusted IOException(
              "Could not find a serializer for the Value class: '"
                  + valClass.getCanonicalName() + "'. "
                  + "Please ensure that the configuration '" +
                  CommonConfigurationKeys.IO_SERIALIZATIONS_KEY + "' is "
                  + "properly configured, if you're using"
                  + "custom serialization.");
        }
        this.compressedValSerializer.open(deflateOut);
      }
      writeFileHeader();
    }
    
    /** Returns the class of keys in this file. */
    public @OsUntrusted Class getKeyClass(SequenceFile.@OsUntrusted Writer this) { return keyClass; }

    /** Returns the class of values in this file. */
    public @OsUntrusted Class getValueClass(SequenceFile.@OsUntrusted Writer this) { return valClass; }

    /** Returns the compression codec of data in this file. */
    public @OsUntrusted CompressionCodec getCompressionCodec(SequenceFile.@OsUntrusted Writer this) { return codec; }
    
    /** create a sync point */
    public void sync(SequenceFile.@OsUntrusted Writer this) throws IOException {
      if (sync != null && lastSyncPos != out.getPos()) {
        out.writeInt(SYNC_ESCAPE);                // mark the start of the sync
        out.write(sync);                          // write sync
        lastSyncPos = out.getPos();               // update lastSyncPos
      }
    }

    /**
     * flush all currently written data to the file system
     * @deprecated Use {@link #hsync()} or {@link #hflush()} instead
     */
    @Deprecated
    public void syncFs(SequenceFile.@OsUntrusted Writer this) throws IOException {
      if (out != null) {
        out.hflush();  // flush contents to file system
      }
    }

    @Override
    public void hsync(SequenceFile.@OsUntrusted Writer this) throws IOException {
      if (out != null) {
        out.hsync();
      }
    }

    @Override
    public void hflush(SequenceFile.@OsUntrusted Writer this) throws IOException {
      if (out != null) {
        out.hflush();
      }
    }
    
    /** Returns the configuration of this file. */
    @OsUntrusted
    Configuration getConf(SequenceFile.@OsUntrusted Writer this) { return conf; }
    
    /** Close the file. */
    @Override
    public synchronized void close(SequenceFile.@OsUntrusted Writer this) throws IOException {
      keySerializer.close();
      uncompressedValSerializer.close();
      if (compressedValSerializer != null) {
        compressedValSerializer.close();
      }

      CodecPool.returnCompressor(compressor);
      compressor = null;
      
      if (out != null) {
        
        // Close the underlying stream iff we own it...
        if (ownOutputStream) {
          out.close();
        } else {
          out.flush();
        }
        out = null;
      }
    }

    synchronized void checkAndWriteSync(SequenceFile.@OsUntrusted Writer this) throws IOException {
      if (sync != null &&
          out.getPos() >= lastSyncPos+SYNC_INTERVAL) { // time to emit sync
        sync();
      }
    }

    /** Append a key/value pair. */
    public void append(SequenceFile.@OsUntrusted Writer this, @OsUntrusted Writable key, @OsUntrusted Writable val)
      throws IOException {
      append((@OsUntrusted Object) key, (@OsUntrusted Object) val);
    }

    /** Append a key/value pair. */
    @SuppressWarnings("unchecked")
    public synchronized void append(SequenceFile.@OsUntrusted Writer this, @OsUntrusted Object key, @OsUntrusted Object val)
      throws IOException {
      if (key.getClass() != keyClass)
        throw new @OsUntrusted IOException("wrong key class: "+key.getClass().getName()
                              +" is not "+keyClass);
      if (val.getClass() != valClass)
        throw new @OsUntrusted IOException("wrong value class: "+val.getClass().getName()
                              +" is not "+valClass);

      buffer.reset();

      // Append the 'key'
      keySerializer.serialize(key);
      @OsUntrusted
      int keyLength = buffer.getLength();
      if (keyLength < 0)
        throw new @OsUntrusted IOException("negative length keys not allowed: " + key);

      // Append the 'value'
      if (compress == CompressionType.RECORD) {
        deflateFilter.resetState();
        compressedValSerializer.serialize(val);
        deflateOut.flush();
        deflateFilter.finish();
      } else {
        uncompressedValSerializer.serialize(val);
      }

      // Write the record out
      checkAndWriteSync();                                // sync
      out.writeInt(buffer.getLength());                   // total record length
      out.writeInt(keyLength);                            // key portion length
      out.write(buffer.getData(), 0, buffer.getLength()); // data
    }

    public synchronized void appendRaw(SequenceFile.@OsUntrusted Writer this, @OsUntrusted byte @OsUntrusted [] keyData, @OsUntrusted int keyOffset,
        @OsUntrusted
        int keyLength, @OsUntrusted ValueBytes val) throws IOException {
      if (keyLength < 0)
        throw new @OsUntrusted IOException("negative length keys not allowed: " + keyLength);

      @OsUntrusted
      int valLength = val.getSize();

      checkAndWriteSync();
      
      out.writeInt(keyLength+valLength);          // total record length
      out.writeInt(keyLength);                    // key portion length
      out.write(keyData, keyOffset, keyLength);   // key
      val.writeUncompressedBytes(out);            // value
    }

    /** Returns the current length of the output file.
     *
     * <p>This always returns a synchronized position.  In other words,
     * immediately after calling {@link SequenceFile.Reader#seek(long)} with a position
     * returned by this method, {@link SequenceFile.Reader#next(Writable)} may be called.  However
     * the key may be earlier in the file than key last written when this
     * method was called (e.g., with block-compression, it may be the first key
     * in the block that was being written when this method was called).
     */
    public synchronized @OsUntrusted long getLength(SequenceFile.@OsUntrusted Writer this) throws IOException {
      return out.getPos();
    }

  } // class Writer

  /** Write key/compressed-value pairs to a sequence-format file. */
  static class RecordCompressWriter extends @OsUntrusted Writer {
    
    @OsUntrusted
    RecordCompressWriter(@OsUntrusted Configuration conf, 
                         @OsUntrusted
                         Option @OsUntrusted ... options) throws IOException {
      super(conf, options);
    }

    /** Append a key/value pair. */
    @Override
    @SuppressWarnings("unchecked")
    public synchronized void append(SequenceFile.@OsUntrusted RecordCompressWriter this, @OsUntrusted Object key, @OsUntrusted Object val)
      throws IOException {
      if (key.getClass() != keyClass)
        throw new @OsUntrusted IOException("wrong key class: "+key.getClass().getName()
                              +" is not "+keyClass);
      if (val.getClass() != valClass)
        throw new @OsUntrusted IOException("wrong value class: "+val.getClass().getName()
                              +" is not "+valClass);

      buffer.reset();

      // Append the 'key'
      keySerializer.serialize(key);
      @OsUntrusted
      int keyLength = buffer.getLength();
      if (keyLength < 0)
        throw new @OsUntrusted IOException("negative length keys not allowed: " + key);

      // Compress 'value' and append it
      deflateFilter.resetState();
      compressedValSerializer.serialize(val);
      deflateOut.flush();
      deflateFilter.finish();

      // Write the record out
      checkAndWriteSync();                                // sync
      out.writeInt(buffer.getLength());                   // total record length
      out.writeInt(keyLength);                            // key portion length
      out.write(buffer.getData(), 0, buffer.getLength()); // data
    }

    /** Append a key/value pair. */
    @Override
    public synchronized void appendRaw(SequenceFile.@OsUntrusted RecordCompressWriter this, @OsUntrusted byte @OsUntrusted [] keyData, @OsUntrusted int keyOffset,
        @OsUntrusted
        int keyLength, @OsUntrusted ValueBytes val) throws IOException {

      if (keyLength < 0)
        throw new @OsUntrusted IOException("negative length keys not allowed: " + keyLength);

      @OsUntrusted
      int valLength = val.getSize();
      
      checkAndWriteSync();                        // sync
      out.writeInt(keyLength+valLength);          // total record length
      out.writeInt(keyLength);                    // key portion length
      out.write(keyData, keyOffset, keyLength);   // 'key' data
      val.writeCompressedBytes(out);              // 'value' data
    }
    
  } // RecordCompressionWriter

  /** Write compressed key/value blocks to a sequence-format file. */
  static class BlockCompressWriter extends @OsUntrusted Writer {
    
    private @OsUntrusted int noBufferedRecords = 0;
    
    private @OsUntrusted DataOutputBuffer keyLenBuffer = new @OsUntrusted DataOutputBuffer();
    private @OsUntrusted DataOutputBuffer keyBuffer = new @OsUntrusted DataOutputBuffer();

    private @OsUntrusted DataOutputBuffer valLenBuffer = new @OsUntrusted DataOutputBuffer();
    private @OsUntrusted DataOutputBuffer valBuffer = new @OsUntrusted DataOutputBuffer();

    private final @OsUntrusted int compressionBlockSize;
    
    @OsUntrusted
    BlockCompressWriter(@OsUntrusted Configuration conf,
                        @OsUntrusted
                        Option @OsUntrusted ... options) throws IOException {
      super(conf, options);
      compressionBlockSize = 
        conf.getInt("io.seqfile.compress.blocksize", 1000000);
      keySerializer.close();
      keySerializer.open(keyBuffer);
      uncompressedValSerializer.close();
      uncompressedValSerializer.open(valBuffer);
    }

    /** Workhorse to check and write out compressed data/lengths */
    private synchronized 
      void writeBuffer(SequenceFile.@OsUntrusted BlockCompressWriter this, @OsUntrusted DataOutputBuffer uncompressedDataBuffer) 
      throws IOException {
      deflateFilter.resetState();
      buffer.reset();
      deflateOut.write(uncompressedDataBuffer.getData(), 0, 
                       uncompressedDataBuffer.getLength());
      deflateOut.flush();
      deflateFilter.finish();
      
      WritableUtils.writeVInt(out, buffer.getLength());
      out.write(buffer.getData(), 0, buffer.getLength());
    }
    
    /** Compress and flush contents to dfs */
    @Override
    public synchronized void sync(SequenceFile.@OsUntrusted BlockCompressWriter this) throws IOException {
      if (noBufferedRecords > 0) {
        super.sync();
        
        // No. of records
        WritableUtils.writeVInt(out, noBufferedRecords);
        
        // Write 'keys' and lengths
        writeBuffer(keyLenBuffer);
        writeBuffer(keyBuffer);
        
        // Write 'values' and lengths
        writeBuffer(valLenBuffer);
        writeBuffer(valBuffer);
        
        // Flush the file-stream
        out.flush();
        
        // Reset internal states
        keyLenBuffer.reset();
        keyBuffer.reset();
        valLenBuffer.reset();
        valBuffer.reset();
        noBufferedRecords = 0;
      }
      
    }
    
    /** Close the file. */
    @Override
    public synchronized void close(SequenceFile.@OsUntrusted BlockCompressWriter this) throws IOException {
      if (out != null) {
        sync();
      }
      super.close();
    }

    /** Append a key/value pair. */
    @Override
    @SuppressWarnings("unchecked")
    public synchronized void append(SequenceFile.@OsUntrusted BlockCompressWriter this, @OsUntrusted Object key, @OsUntrusted Object val)
      throws IOException {
      if (key.getClass() != keyClass)
        throw new @OsUntrusted IOException("wrong key class: "+key+" is not "+keyClass);
      if (val.getClass() != valClass)
        throw new @OsUntrusted IOException("wrong value class: "+val+" is not "+valClass);

      // Save key/value into respective buffers 
      @OsUntrusted
      int oldKeyLength = keyBuffer.getLength();
      keySerializer.serialize(key);
      @OsUntrusted
      int keyLength = keyBuffer.getLength() - oldKeyLength;
      if (keyLength < 0)
        throw new @OsUntrusted IOException("negative length keys not allowed: " + key);
      WritableUtils.writeVInt(keyLenBuffer, keyLength);

      @OsUntrusted
      int oldValLength = valBuffer.getLength();
      uncompressedValSerializer.serialize(val);
      @OsUntrusted
      int valLength = valBuffer.getLength() - oldValLength;
      WritableUtils.writeVInt(valLenBuffer, valLength);
      
      // Added another key/value pair
      ++noBufferedRecords;
      
      // Compress and flush?
      @OsUntrusted
      int currentBlockSize = keyBuffer.getLength() + valBuffer.getLength();
      if (currentBlockSize >= compressionBlockSize) {
        sync();
      }
    }
    
    /** Append a key/value pair. */
    @Override
    public synchronized void appendRaw(SequenceFile.@OsUntrusted BlockCompressWriter this, @OsUntrusted byte @OsUntrusted [] keyData, @OsUntrusted int keyOffset,
        @OsUntrusted
        int keyLength, @OsUntrusted ValueBytes val) throws IOException {
      
      if (keyLength < 0)
        throw new @OsUntrusted IOException("negative length keys not allowed");

      @OsUntrusted
      int valLength = val.getSize();
      
      // Save key/value data in relevant buffers
      WritableUtils.writeVInt(keyLenBuffer, keyLength);
      keyBuffer.write(keyData, keyOffset, keyLength);
      WritableUtils.writeVInt(valLenBuffer, valLength);
      val.writeUncompressedBytes(valBuffer);

      // Added another key/value pair
      ++noBufferedRecords;

      // Compress and flush?
      @OsUntrusted
      int currentBlockSize = keyBuffer.getLength() + valBuffer.getLength(); 
      if (currentBlockSize >= compressionBlockSize) {
        sync();
      }
    }
  
  } // BlockCompressionWriter

  /** Get the configured buffer size */
  private static @OsUntrusted int getBufferSize(@OsUntrusted Configuration conf) {
    return conf.getInt("io.file.buffer.size", 4096);
  }

  /** Reads key/value pairs from a sequence-format file. */
  public static class Reader implements java.io.Closeable {
    private @OsUntrusted String filename;
    private @OsUntrusted FSDataInputStream in;
    private @OsUntrusted DataOutputBuffer outBuf = new @OsUntrusted DataOutputBuffer();

    private @OsUntrusted byte version;

    private @OsUntrusted String keyClassName;
    private @OsUntrusted String valClassName;
    private @OsUntrusted Class keyClass;
    private @OsUntrusted Class valClass;

    private @OsUntrusted CompressionCodec codec = null;
    private @OsUntrusted Metadata metadata = null;
    
    private @OsUntrusted byte @OsUntrusted [] sync = new @OsUntrusted byte @OsUntrusted [SYNC_HASH_SIZE];
    private @OsUntrusted byte @OsUntrusted [] syncCheck = new @OsUntrusted byte @OsUntrusted [SYNC_HASH_SIZE];
    private @OsUntrusted boolean syncSeen;

    private @OsUntrusted long headerEnd;
    private @OsUntrusted long end;
    private @OsUntrusted int keyLength;
    private @OsUntrusted int recordLength;

    private @OsUntrusted boolean decompress;
    private @OsUntrusted boolean blockCompressed;
    
    private @OsUntrusted Configuration conf;

    private @OsUntrusted int noBufferedRecords = 0;
    private @OsUntrusted boolean lazyDecompress = true;
    private @OsUntrusted boolean valuesDecompressed = true;
    
    private @OsUntrusted int noBufferedKeys = 0;
    private @OsUntrusted int noBufferedValues = 0;
    
    private @OsUntrusted DataInputBuffer keyLenBuffer = null;
    private @OsUntrusted CompressionInputStream keyLenInFilter = null;
    private @OsUntrusted DataInputStream keyLenIn = null;
    private @OsUntrusted Decompressor keyLenDecompressor = null;
    private @OsUntrusted DataInputBuffer keyBuffer = null;
    private @OsUntrusted CompressionInputStream keyInFilter = null;
    private @OsUntrusted DataInputStream keyIn = null;
    private @OsUntrusted Decompressor keyDecompressor = null;

    private @OsUntrusted DataInputBuffer valLenBuffer = null;
    private @OsUntrusted CompressionInputStream valLenInFilter = null;
    private @OsUntrusted DataInputStream valLenIn = null;
    private @OsUntrusted Decompressor valLenDecompressor = null;
    private @OsUntrusted DataInputBuffer valBuffer = null;
    private @OsUntrusted CompressionInputStream valInFilter = null;
    private @OsUntrusted DataInputStream valIn = null;
    private @OsUntrusted Decompressor valDecompressor = null;
    
    private @OsUntrusted Deserializer keyDeserializer;
    private @OsUntrusted Deserializer valDeserializer;

    /**
     * A tag interface for all of the Reader options
     */
    public static interface Option {}
    
    /**
     * Create an option to specify the path name of the sequence file.
     * @param value the path to read
     * @return a new option
     */
    public static @OsUntrusted Option file(@OsUntrusted Path value) {
      return new @OsUntrusted FileOption(value);
    }
    
    /**
     * Create an option to specify the stream with the sequence file.
     * @param value the stream to read.
     * @return a new option
     */
    public static @OsUntrusted Option stream(@OsUntrusted FSDataInputStream value) {
      return new @OsUntrusted InputStreamOption(value);
    }
    
    /**
     * Create an option to specify the starting byte to read.
     * @param value the number of bytes to skip over
     * @return a new option
     */
    public static @OsUntrusted Option start(@OsUntrusted long value) {
      return new @OsUntrusted StartOption(value);
    }
    
    /**
     * Create an option to specify the number of bytes to read.
     * @param value the number of bytes to read
     * @return a new option
     */
    public static @OsUntrusted Option length(@OsUntrusted long value) {
      return new @OsUntrusted LengthOption(value);
    }
    
    /**
     * Create an option with the buffer size for reading the given pathname.
     * @param value the number of bytes to buffer
     * @return a new option
     */
    public static @OsUntrusted Option bufferSize(@OsUntrusted int value) {
      return new @OsUntrusted BufferSizeOption(value);
    }

    private static class FileOption extends Options.@OsUntrusted PathOption 
                                    implements @OsUntrusted Option {
      private @OsUntrusted FileOption(@OsUntrusted Path value) {
        super(value);
      }
    }
    
    private static class InputStreamOption
        extends Options.@OsUntrusted FSDataInputStreamOption 
        implements @OsUntrusted Option {
      private @OsUntrusted InputStreamOption(@OsUntrusted FSDataInputStream value) {
        super(value);
      }
    }

    private static class StartOption extends Options.@OsUntrusted LongOption
                                     implements @OsUntrusted Option {
      private @OsUntrusted StartOption(@OsUntrusted long value) {
        super(value);
      }
    }

    private static class LengthOption extends Options.@OsUntrusted LongOption
                                      implements @OsUntrusted Option {
      private @OsUntrusted LengthOption(@OsUntrusted long value) {
        super(value);
      }
    }

    private static class BufferSizeOption extends Options.@OsUntrusted IntegerOption
                                      implements @OsUntrusted Option {
      private @OsUntrusted BufferSizeOption(@OsUntrusted int value) {
        super(value);
      }
    }

    // only used directly
    private static class OnlyHeaderOption extends Options.@OsUntrusted BooleanOption 
                                          implements @OsUntrusted Option {
      private @OsUntrusted OnlyHeaderOption() {
        super(true);
      }
    }

    public @OsUntrusted Reader(@OsUntrusted Configuration conf, @OsUntrusted Option @OsUntrusted ... opts) throws IOException {
      // Look up the options, these are null if not set
      @OsUntrusted
      FileOption fileOpt = Options.getOption(FileOption.class, opts);
      @OsUntrusted
      InputStreamOption streamOpt = 
        Options.getOption(InputStreamOption.class, opts);
      @OsUntrusted
      StartOption startOpt = Options.getOption(StartOption.class, opts);
      @OsUntrusted
      LengthOption lenOpt = Options.getOption(LengthOption.class, opts);
      @OsUntrusted
      BufferSizeOption bufOpt = Options.getOption(BufferSizeOption.class,opts);
      @OsUntrusted
      OnlyHeaderOption headerOnly = 
        Options.getOption(OnlyHeaderOption.class, opts);
      // check for consistency
      if ((fileOpt == null) == (streamOpt == null)) {
        throw new 
          @OsUntrusted IllegalArgumentException("File or stream option must be specified");
      }
      if (fileOpt == null && bufOpt != null) {
        throw new @OsUntrusted IllegalArgumentException("buffer size can only be set when" +
                                           " a file is specified.");
      }
      // figure out the real values
      @OsUntrusted
      Path filename = null;
      @OsUntrusted
      FSDataInputStream file;
      final @OsUntrusted long len;
      if (fileOpt != null) {
        filename = fileOpt.getValue();
        @OsUntrusted
        FileSystem fs = filename.getFileSystem(conf);
        @OsUntrusted
        int bufSize = bufOpt == null ? getBufferSize(conf): bufOpt.getValue();
        len = null == lenOpt
          ? fs.getFileStatus(filename).getLen()
          : lenOpt.getValue();
        file = openFile(fs, filename, bufSize, len);
      } else {
        len = null == lenOpt ? Long.MAX_VALUE : lenOpt.getValue();
        file = streamOpt.getValue();
      }
      @OsUntrusted
      long start = startOpt == null ? 0 : startOpt.getValue();
      // really set up
      initialize(filename, file, start, len, conf, headerOnly != null);
    }

    /**
     * Construct a reader by opening a file from the given file system.
     * @param fs The file system used to open the file.
     * @param file The file being read.
     * @param conf Configuration
     * @throws IOException
     * @deprecated Use Reader(Configuration, Option...) instead.
     */
    @Deprecated
    public @OsUntrusted Reader(@OsUntrusted FileSystem fs, @OsUntrusted Path file, 
                  @OsUntrusted
                  Configuration conf) throws IOException {
      this(conf, file(file.makeQualified(fs)));
    }

    /**
     * Construct a reader by the given input stream.
     * @param in An input stream.
     * @param buffersize unused
     * @param start The starting position.
     * @param length The length being read.
     * @param conf Configuration
     * @throws IOException
     * @deprecated Use Reader(Configuration, Reader.Option...) instead.
     */
    @Deprecated
    public @OsUntrusted Reader(@OsUntrusted FSDataInputStream in, @OsUntrusted int buffersize,
        @OsUntrusted
        long start, @OsUntrusted long length, @OsUntrusted Configuration conf) throws IOException {
      this(conf, stream(in), start(start), length(length));
    }

    /** Common work of the constructors. */
    private void initialize(SequenceFile.@OsUntrusted Reader this, @OsUntrusted Path filename, @OsUntrusted FSDataInputStream in,
                            @OsUntrusted
                            long start, @OsUntrusted long length, @OsUntrusted Configuration conf,
                            @OsUntrusted
                            boolean tempReader) throws IOException {
      if (in == null) {
        throw new @OsUntrusted IllegalArgumentException("in == null");
      }
      this.filename = filename == null ? "<unknown>" : filename.toString();
      this.in = in;
      this.conf = conf;
      @OsUntrusted
      boolean succeeded = false;
      try {
        seek(start);
        this.end = this.in.getPos() + length;
        // if it wrapped around, use the max
        if (end < length) {
          end = Long.MAX_VALUE;
        }
        init(tempReader);
        succeeded = true;
      } finally {
        if (!succeeded) {
          IOUtils.cleanup(LOG, this.in);
        }
      }
    }

    /**
     * Override this method to specialize the type of
     * {@link FSDataInputStream} returned.
     * @param fs The file system used to open the file.
     * @param file The file being read.
     * @param bufferSize The buffer size used to read the file.
     * @param length The length being read if it is >= 0.  Otherwise,
     *               the length is not available.
     * @return The opened stream.
     * @throws IOException
     */
    protected @OsUntrusted FSDataInputStream openFile(SequenceFile.@OsUntrusted Reader this, @OsUntrusted FileSystem fs, @OsUntrusted Path file,
        @OsUntrusted
        int bufferSize, @OsUntrusted long length) throws IOException {
      return fs.open(file, bufferSize);
    }
    
    /**
     * Initialize the {@link Reader}
     * @param tmpReader <code>true</code> if we are constructing a temporary
     *                  reader {@link SequenceFile.Sorter.cloneFileAttributes}, 
     *                  and hence do not initialize every component; 
     *                  <code>false</code> otherwise.
     * @throws IOException
     */
    private void init(SequenceFile.@OsUntrusted Reader this, @OsUntrusted boolean tempReader) throws IOException {
      @OsUntrusted
      byte @OsUntrusted [] versionBlock = new @OsUntrusted byte @OsUntrusted [VERSION.length];
      in.readFully(versionBlock);

      if ((versionBlock[0] != VERSION[0]) ||
          (versionBlock[1] != VERSION[1]) ||
          (versionBlock[2] != VERSION[2]))
        throw new @OsUntrusted IOException(this + " not a SequenceFile");

      // Set 'version'
      version = versionBlock[3];
      if (version > VERSION[3])
        throw new @OsUntrusted VersionMismatchException(VERSION[3], version);

      if (version < BLOCK_COMPRESS_VERSION) {
        @OsUntrusted
        UTF8 className = new @OsUntrusted UTF8();

        className.readFields(in);
        keyClassName = className.toStringChecked(); // key class name

        className.readFields(in);
        valClassName = className.toStringChecked(); // val class name
      } else {
        keyClassName = Text.readString(in);
        valClassName = Text.readString(in);
      }

      if (version > 2) {                          // if version > 2
        this.decompress = in.readBoolean();       // is compressed?
      } else {
        decompress = false;
      }

      if (version >= BLOCK_COMPRESS_VERSION) {    // if version >= 4
        this.blockCompressed = in.readBoolean();  // is block-compressed?
      } else {
        blockCompressed = false;
      }
      
      // if version >= 5
      // setup the compression codec
      if (decompress) {
        if (version >= CUSTOM_COMPRESS_VERSION) {
          @OsUntrusted
          String codecClassname = Text.readString(in);
          try {
            @OsUntrusted
            Class<@OsUntrusted ? extends @OsUntrusted CompressionCodec> codecClass
              = conf.getClassByName(codecClassname).asSubclass(CompressionCodec.class);
            this.codec = ReflectionUtils.newInstance(codecClass, conf);
          } catch (@OsUntrusted ClassNotFoundException cnfe) {
            throw new @OsUntrusted IllegalArgumentException("Unknown codec: " + 
                                               codecClassname, cnfe);
          }
        } else {
          codec = new @OsUntrusted DefaultCodec();
          ((@OsUntrusted Configurable)codec).setConf(conf);
        }
      }
      
      this.metadata = new @OsUntrusted Metadata();
      if (version >= VERSION_WITH_METADATA) {    // if version >= 6
        this.metadata.readFields(in);
      }
      
      if (version > 1) {                          // if version > 1
        in.readFully(sync);                       // read sync bytes
        headerEnd = in.getPos();                  // record end of header
      }
      
      // Initialize... *not* if this we are constructing a temporary Reader
      if (!tempReader) {
        valBuffer = new @OsUntrusted DataInputBuffer();
        if (decompress) {
          valDecompressor = CodecPool.getDecompressor(codec);
          valInFilter = codec.createInputStream(valBuffer, valDecompressor);
          valIn = new @OsUntrusted DataInputStream(valInFilter);
        } else {
          valIn = valBuffer;
        }

        if (blockCompressed) {
          keyLenBuffer = new @OsUntrusted DataInputBuffer();
          keyBuffer = new @OsUntrusted DataInputBuffer();
          valLenBuffer = new @OsUntrusted DataInputBuffer();

          keyLenDecompressor = CodecPool.getDecompressor(codec);
          keyLenInFilter = codec.createInputStream(keyLenBuffer, 
                                                   keyLenDecompressor);
          keyLenIn = new @OsUntrusted DataInputStream(keyLenInFilter);

          keyDecompressor = CodecPool.getDecompressor(codec);
          keyInFilter = codec.createInputStream(keyBuffer, keyDecompressor);
          keyIn = new @OsUntrusted DataInputStream(keyInFilter);

          valLenDecompressor = CodecPool.getDecompressor(codec);
          valLenInFilter = codec.createInputStream(valLenBuffer, 
                                                   valLenDecompressor);
          valLenIn = new @OsUntrusted DataInputStream(valLenInFilter);
        }
        
        @OsUntrusted
        SerializationFactory serializationFactory =
          new @OsUntrusted SerializationFactory(conf);
        this.keyDeserializer =
          getDeserializer(serializationFactory, getKeyClass());
        if (this.keyDeserializer == null) {
          throw new @OsUntrusted IOException(
              "Could not find a deserializer for the Key class: '"
                  + getKeyClass().getCanonicalName() + "'. "
                  + "Please ensure that the configuration '" +
                  CommonConfigurationKeys.IO_SERIALIZATIONS_KEY + "' is "
                  + "properly configured, if you're using "
                  + "custom serialization.");
        }
        if (!blockCompressed) {
          this.keyDeserializer.open(valBuffer);
        } else {
          this.keyDeserializer.open(keyIn);
        }
        this.valDeserializer =
          getDeserializer(serializationFactory, getValueClass());
        if (this.valDeserializer == null) {
          throw new @OsUntrusted IOException(
              "Could not find a deserializer for the Value class: '"
                  + getValueClass().getCanonicalName() + "'. "
                  + "Please ensure that the configuration '" +
                  CommonConfigurationKeys.IO_SERIALIZATIONS_KEY + "' is "
                  + "properly configured, if you're using "
                  + "custom serialization.");
        }
        this.valDeserializer.open(valIn);
      }
    }
    
    @SuppressWarnings("unchecked")
    private @OsUntrusted Deserializer getDeserializer(SequenceFile.@OsUntrusted Reader this, @OsUntrusted SerializationFactory sf, @OsUntrusted Class c) {
      return sf.getDeserializer(c);
    }
    
    /** Close the file. */
    @Override
    public synchronized void close(SequenceFile.@OsUntrusted Reader this) throws IOException {
      // Return the decompressors to the pool
      CodecPool.returnDecompressor(keyLenDecompressor);
      CodecPool.returnDecompressor(keyDecompressor);
      CodecPool.returnDecompressor(valLenDecompressor);
      CodecPool.returnDecompressor(valDecompressor);
      keyLenDecompressor = keyDecompressor = null;
      valLenDecompressor = valDecompressor = null;
      
      if (keyDeserializer != null) {
    	keyDeserializer.close();
      }
      if (valDeserializer != null) {
        valDeserializer.close();
      }
      
      // Close the input-stream
      in.close();
    }

    /** Returns the name of the key class. */
    public @OsUntrusted String getKeyClassName(SequenceFile.@OsUntrusted Reader this) {
      return keyClassName;
    }

    /** Returns the class of keys in this file. */
    public synchronized @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> getKeyClass(SequenceFile.@OsUntrusted Reader this) {
      if (null == keyClass) {
        try {
          keyClass = WritableName.getClass(getKeyClassName(), conf);
        } catch (@OsUntrusted IOException e) {
          throw new @OsUntrusted RuntimeException(e);
        }
      }
      return keyClass;
    }

    /** Returns the name of the value class. */
    public @OsUntrusted String getValueClassName(SequenceFile.@OsUntrusted Reader this) {
      return valClassName;
    }

    /** Returns the class of values in this file. */
    public synchronized @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> getValueClass(SequenceFile.@OsUntrusted Reader this) {
      if (null == valClass) {
        try {
          valClass = WritableName.getClass(getValueClassName(), conf);
        } catch (@OsUntrusted IOException e) {
          throw new @OsUntrusted RuntimeException(e);
        }
      }
      return valClass;
    }

    /** Returns true if values are compressed. */
    public @OsUntrusted boolean isCompressed(SequenceFile.@OsUntrusted Reader this) { return decompress; }
    
    /** Returns true if records are block-compressed. */
    public @OsUntrusted boolean isBlockCompressed(SequenceFile.@OsUntrusted Reader this) { return blockCompressed; }
    
    /** Returns the compression codec of data in this file. */
    public @OsUntrusted CompressionCodec getCompressionCodec(SequenceFile.@OsUntrusted Reader this) { return codec; }
    
    /**
     * Get the compression type for this file.
     * @return the compression type
     */
    public @OsUntrusted CompressionType getCompressionType(SequenceFile.@OsUntrusted Reader this) {
      if (decompress) {
        return blockCompressed ? CompressionType.BLOCK : CompressionType.RECORD;
      } else {
        return CompressionType.NONE;
      }
    }

    /** Returns the metadata object of the file */
    public @OsUntrusted Metadata getMetadata(SequenceFile.@OsUntrusted Reader this) {
      return this.metadata;
    }
    
    /** Returns the configuration used for this file. */
    @OsUntrusted
    Configuration getConf(SequenceFile.@OsUntrusted Reader this) { return conf; }
    
    /** Read a compressed buffer */
    private synchronized void readBuffer(SequenceFile.@OsUntrusted Reader this, @OsUntrusted DataInputBuffer buffer, 
                                         @OsUntrusted
                                         CompressionInputStream filter) throws IOException {
      // Read data into a temporary buffer
      @OsUntrusted
      DataOutputBuffer dataBuffer = new @OsUntrusted DataOutputBuffer();

      try {
        @OsUntrusted
        int dataBufferLength = WritableUtils.readVInt(in);
        dataBuffer.write(in, dataBufferLength);
      
        // Set up 'buffer' connected to the input-stream
        buffer.reset(dataBuffer.getData(), 0, dataBuffer.getLength());
      } finally {
        dataBuffer.close();
      }

      // Reset the codec
      filter.resetState();
    }
    
    /** Read the next 'compressed' block */
    private synchronized void readBlock(SequenceFile.@OsUntrusted Reader this) throws IOException {
      // Check if we need to throw away a whole block of 
      // 'values' due to 'lazy decompression' 
      if (lazyDecompress && !valuesDecompressed) {
        in.seek(WritableUtils.readVInt(in)+in.getPos());
        in.seek(WritableUtils.readVInt(in)+in.getPos());
      }
      
      // Reset internal states
      noBufferedKeys = 0; noBufferedValues = 0; noBufferedRecords = 0;
      valuesDecompressed = false;

      //Process sync
      if (sync != null) {
        in.readInt();
        in.readFully(syncCheck);                // read syncCheck
        if (!Arrays.equals(sync, syncCheck))    // check it
          throw new @OsUntrusted IOException("File is corrupt!");
      }
      syncSeen = true;

      // Read number of records in this block
      noBufferedRecords = WritableUtils.readVInt(in);
      
      // Read key lengths and keys
      readBuffer(keyLenBuffer, keyLenInFilter);
      readBuffer(keyBuffer, keyInFilter);
      noBufferedKeys = noBufferedRecords;
      
      // Read value lengths and values
      if (!lazyDecompress) {
        readBuffer(valLenBuffer, valLenInFilter);
        readBuffer(valBuffer, valInFilter);
        noBufferedValues = noBufferedRecords;
        valuesDecompressed = true;
      }
    }

    /** 
     * Position valLenIn/valIn to the 'value' 
     * corresponding to the 'current' key 
     */
    private synchronized void seekToCurrentValue(SequenceFile.@OsUntrusted Reader this) throws IOException {
      if (!blockCompressed) {
        if (decompress) {
          valInFilter.resetState();
        }
        valBuffer.reset();
      } else {
        // Check if this is the first value in the 'block' to be read
        if (lazyDecompress && !valuesDecompressed) {
          // Read the value lengths and values
          readBuffer(valLenBuffer, valLenInFilter);
          readBuffer(valBuffer, valInFilter);
          noBufferedValues = noBufferedRecords;
          valuesDecompressed = true;
        }
        
        // Calculate the no. of bytes to skip
        // Note: 'current' key has already been read!
        @OsUntrusted
        int skipValBytes = 0;
        @OsUntrusted
        int currentKey = noBufferedKeys + 1;          
        for (@OsUntrusted int i=noBufferedValues; i > currentKey; --i) {
          skipValBytes += WritableUtils.readVInt(valLenIn);
          --noBufferedValues;
        }
        
        // Skip to the 'val' corresponding to 'current' key
        if (skipValBytes > 0) {
          if (valIn.skipBytes(skipValBytes) != skipValBytes) {
            throw new @OsUntrusted IOException("Failed to seek to " + currentKey + 
                                  "(th) value!");
          }
        }
      }
    }

    /**
     * Get the 'value' corresponding to the last read 'key'.
     * @param val : The 'value' to be read.
     * @throws IOException
     */
    public synchronized void getCurrentValue(SequenceFile.@OsUntrusted Reader this, @OsUntrusted Writable val) 
      throws IOException {
      if (val instanceof @OsUntrusted Configurable) {
        ((@OsUntrusted Configurable) val).setConf(this.conf);
      }

      // Position stream to 'current' value
      seekToCurrentValue();

      if (!blockCompressed) {
        val.readFields(valIn);
        
        if (valIn.read() > 0) {
          LOG.info("available bytes: " + valIn.available());
          throw new @OsUntrusted IOException(val+" read "+(valBuffer.getPosition()-keyLength)
                                + " bytes, should read " +
                                (valBuffer.getLength()-keyLength));
        }
      } else {
        // Get the value
        @OsUntrusted
        int valLength = WritableUtils.readVInt(valLenIn);
        val.readFields(valIn);
        
        // Read another compressed 'value'
        --noBufferedValues;
        
        // Sanity check
        if ((valLength < 0) && LOG.isDebugEnabled()) {
          LOG.debug(val + " is a zero-length value");
        }
      }

    }
    
    /**
     * Get the 'value' corresponding to the last read 'key'.
     * @param val : The 'value' to be read.
     * @throws IOException
     */
    public synchronized @OsUntrusted Object getCurrentValue(SequenceFile.@OsUntrusted Reader this, @OsUntrusted Object val) 
      throws IOException {
      if (val instanceof @OsUntrusted Configurable) {
        ((@OsUntrusted Configurable) val).setConf(this.conf);
      }

      // Position stream to 'current' value
      seekToCurrentValue();

      if (!blockCompressed) {
        val = deserializeValue(val);
        
        if (valIn.read() > 0) {
          LOG.info("available bytes: " + valIn.available());
          throw new @OsUntrusted IOException(val+" read "+(valBuffer.getPosition()-keyLength)
                                + " bytes, should read " +
                                (valBuffer.getLength()-keyLength));
        }
      } else {
        // Get the value
        @OsUntrusted
        int valLength = WritableUtils.readVInt(valLenIn);
        val = deserializeValue(val);
        
        // Read another compressed 'value'
        --noBufferedValues;
        
        // Sanity check
        if ((valLength < 0) && LOG.isDebugEnabled()) {
          LOG.debug(val + " is a zero-length value");
        }
      }
      return val;

    }

    @SuppressWarnings("unchecked")
    private @OsUntrusted Object deserializeValue(SequenceFile.@OsUntrusted Reader this, @OsUntrusted Object val) throws IOException {
      return valDeserializer.deserialize(val);
    }
    
    /** Read the next key in the file into <code>key</code>, skipping its
     * value.  True if another entry exists, and false at end of file. */
    public synchronized @OsUntrusted boolean next(SequenceFile.@OsUntrusted Reader this, @OsUntrusted Writable key) throws IOException {
      if (key.getClass() != getKeyClass())
        throw new @OsUntrusted IOException("wrong key class: "+key.getClass().getName()
                              +" is not "+keyClass);

      if (!blockCompressed) {
        outBuf.reset();
        
        keyLength = next(outBuf);
        if (keyLength < 0)
          return false;
        
        valBuffer.reset(outBuf.getData(), outBuf.getLength());
        
        key.readFields(valBuffer);
        valBuffer.mark(0);
        if (valBuffer.getPosition() != keyLength)
          throw new @OsUntrusted IOException(key + " read " + valBuffer.getPosition()
                                + " bytes, should read " + keyLength);
      } else {
        //Reset syncSeen
        syncSeen = false;
        
        if (noBufferedKeys == 0) {
          try {
            readBlock();
          } catch (@OsUntrusted EOFException eof) {
            return false;
          }
        }
        
        @OsUntrusted
        int keyLength = WritableUtils.readVInt(keyLenIn);
        
        // Sanity check
        if (keyLength < 0) {
          return false;
        }
        
        //Read another compressed 'key'
        key.readFields(keyIn);
        --noBufferedKeys;
      }

      return true;
    }

    /** Read the next key/value pair in the file into <code>key</code> and
     * <code>val</code>.  Returns true if such a pair exists and false when at
     * end of file */
    public synchronized @OsUntrusted boolean next(SequenceFile.@OsUntrusted Reader this, @OsUntrusted Writable key, @OsUntrusted Writable val)
      throws IOException {
      if (val.getClass() != getValueClass())
        throw new @OsUntrusted IOException("wrong value class: "+val+" is not "+valClass);

      @OsUntrusted
      boolean more = next(key);
      
      if (more) {
        getCurrentValue(val);
      }

      return more;
    }
    
    /**
     * Read and return the next record length, potentially skipping over 
     * a sync block.
     * @return the length of the next record or -1 if there is no next record
     * @throws IOException
     */
    private synchronized @OsUntrusted int readRecordLength(SequenceFile.@OsUntrusted Reader this) throws IOException {
      if (in.getPos() >= end) {
        return -1;
      }      
      @OsUntrusted
      int length = in.readInt();
      if (version > 1 && sync != null &&
          length == SYNC_ESCAPE) {              // process a sync entry
        in.readFully(syncCheck);                // read syncCheck
        if (!Arrays.equals(sync, syncCheck))    // check it
          throw new @OsUntrusted IOException("File is corrupt!");
        syncSeen = true;
        if (in.getPos() >= end) {
          return -1;
        }
        length = in.readInt();                  // re-read length
      } else {
        syncSeen = false;
      }
      
      return length;
    }
    
    /** Read the next key/value pair in the file into <code>buffer</code>.
     * Returns the length of the key read, or -1 if at end of file.  The length
     * of the value may be computed by calling buffer.getLength() before and
     * after calls to this method. */
    /** @deprecated Call {@link #nextRaw(DataOutputBuffer,SequenceFile.ValueBytes)}. */
    @Deprecated
    synchronized @OsUntrusted int next(SequenceFile.@OsUntrusted Reader this, @OsUntrusted DataOutputBuffer buffer) throws IOException {
      // Unsupported for block-compressed sequence files
      if (blockCompressed) {
        throw new @OsUntrusted IOException("Unsupported call for block-compressed" +
                              " SequenceFiles - use SequenceFile.Reader.next(DataOutputStream, ValueBytes)");
      }
      try {
        @OsUntrusted
        int length = readRecordLength();
        if (length == -1) {
          return -1;
        }
        @OsUntrusted
        int keyLength = in.readInt();
        buffer.write(in, length);
        return keyLength;
      } catch (@OsUntrusted ChecksumException e) {             // checksum failure
        handleChecksumException(e);
        return next(buffer);
      }
    }

    public @OsUntrusted ValueBytes createValueBytes(SequenceFile.@OsUntrusted Reader this) {
      @OsUntrusted
      ValueBytes val = null;
      if (!decompress || blockCompressed) {
        val = new @OsUntrusted UncompressedBytes();
      } else {
        val = new @OsUntrusted CompressedBytes(codec);
      }
      return val;
    }

    /**
     * Read 'raw' records.
     * @param key - The buffer into which the key is read
     * @param val - The 'raw' value
     * @return Returns the total record length or -1 for end of file
     * @throws IOException
     */
    public synchronized @OsUntrusted int nextRaw(SequenceFile.@OsUntrusted Reader this, @OsUntrusted DataOutputBuffer key, @OsUntrusted ValueBytes val) 
      throws IOException {
      if (!blockCompressed) {
        @OsUntrusted
        int length = readRecordLength();
        if (length == -1) {
          return -1;
        }
        @OsUntrusted
        int keyLength = in.readInt();
        @OsUntrusted
        int valLength = length - keyLength;
        key.write(in, keyLength);
        if (decompress) {
          @OsUntrusted
          CompressedBytes value = (@OsUntrusted CompressedBytes)val;
          value.reset(in, valLength);
        } else {
          @OsUntrusted
          UncompressedBytes value = (@OsUntrusted UncompressedBytes)val;
          value.reset(in, valLength);
        }
        
        return length;
      } else {
        //Reset syncSeen
        syncSeen = false;
        
        // Read 'key'
        if (noBufferedKeys == 0) {
          if (in.getPos() >= end) 
            return -1;

          try { 
            readBlock();
          } catch (@OsUntrusted EOFException eof) {
            return -1;
          }
        }
        @OsUntrusted
        int keyLength = WritableUtils.readVInt(keyLenIn);
        if (keyLength < 0) {
          throw new @OsUntrusted IOException("zero length key found!");
        }
        key.write(keyIn, keyLength);
        --noBufferedKeys;
        
        // Read raw 'value'
        seekToCurrentValue();
        @OsUntrusted
        int valLength = WritableUtils.readVInt(valLenIn);
        @OsUntrusted
        UncompressedBytes rawValue = (@OsUntrusted UncompressedBytes)val;
        rawValue.reset(valIn, valLength);
        --noBufferedValues;
        
        return (keyLength+valLength);
      }
      
    }

    /**
     * Read 'raw' keys.
     * @param key - The buffer into which the key is read
     * @return Returns the key length or -1 for end of file
     * @throws IOException
     */
    public synchronized @OsUntrusted int nextRawKey(SequenceFile.@OsUntrusted Reader this, @OsUntrusted DataOutputBuffer key) 
      throws IOException {
      if (!blockCompressed) {
        recordLength = readRecordLength();
        if (recordLength == -1) {
          return -1;
        }
        keyLength = in.readInt();
        key.write(in, keyLength);
        return keyLength;
      } else {
        //Reset syncSeen
        syncSeen = false;
        
        // Read 'key'
        if (noBufferedKeys == 0) {
          if (in.getPos() >= end) 
            return -1;

          try { 
            readBlock();
          } catch (@OsUntrusted EOFException eof) {
            return -1;
          }
        }
        @OsUntrusted
        int keyLength = WritableUtils.readVInt(keyLenIn);
        if (keyLength < 0) {
          throw new @OsUntrusted IOException("zero length key found!");
        }
        key.write(keyIn, keyLength);
        --noBufferedKeys;
        
        return keyLength;
      }
      
    }

    /** Read the next key in the file, skipping its
     * value.  Return null at end of file. */
    public synchronized @OsUntrusted Object next(SequenceFile.@OsUntrusted Reader this, @OsUntrusted Object key) throws IOException {
      if (key != null && key.getClass() != getKeyClass()) {
        throw new @OsUntrusted IOException("wrong key class: "+key.getClass().getName()
                              +" is not "+keyClass);
      }

      if (!blockCompressed) {
        outBuf.reset();
        
        keyLength = next(outBuf);
        if (keyLength < 0)
          return null;
        
        valBuffer.reset(outBuf.getData(), outBuf.getLength());
        
        key = deserializeKey(key);
        valBuffer.mark(0);
        if (valBuffer.getPosition() != keyLength)
          throw new @OsUntrusted IOException(key + " read " + valBuffer.getPosition()
                                + " bytes, should read " + keyLength);
      } else {
        //Reset syncSeen
        syncSeen = false;
        
        if (noBufferedKeys == 0) {
          try {
            readBlock();
          } catch (@OsUntrusted EOFException eof) {
            return null;
          }
        }
        
        @OsUntrusted
        int keyLength = WritableUtils.readVInt(keyLenIn);
        
        // Sanity check
        if (keyLength < 0) {
          return null;
        }
        
        //Read another compressed 'key'
        key = deserializeKey(key);
        --noBufferedKeys;
      }

      return key;
    }

    @SuppressWarnings("unchecked")
    private @OsUntrusted Object deserializeKey(SequenceFile.@OsUntrusted Reader this, @OsUntrusted Object key) throws IOException {
      return keyDeserializer.deserialize(key);
    }

    /**
     * Read 'raw' values.
     * @param val - The 'raw' value
     * @return Returns the value length
     * @throws IOException
     */
    public synchronized @OsUntrusted int nextRawValue(SequenceFile.@OsUntrusted Reader this, @OsUntrusted ValueBytes val) 
      throws IOException {
      
      // Position stream to current value
      seekToCurrentValue();
 
      if (!blockCompressed) {
        @OsUntrusted
        int valLength = recordLength - keyLength;
        if (decompress) {
          @OsUntrusted
          CompressedBytes value = (@OsUntrusted CompressedBytes)val;
          value.reset(in, valLength);
        } else {
          @OsUntrusted
          UncompressedBytes value = (@OsUntrusted UncompressedBytes)val;
          value.reset(in, valLength);
        }
         
        return valLength;
      } else {
        @OsUntrusted
        int valLength = WritableUtils.readVInt(valLenIn);
        @OsUntrusted
        UncompressedBytes rawValue = (@OsUntrusted UncompressedBytes)val;
        rawValue.reset(valIn, valLength);
        --noBufferedValues;
        return valLength;
      }
      
    }

    private void handleChecksumException(SequenceFile.@OsUntrusted Reader this, @OsUntrusted ChecksumException e)
      throws IOException {
      if (this.conf.getBoolean("io.skip.checksum.errors", false)) {
        LOG.warn("Bad checksum at "+getPosition()+". Skipping entries.");
        sync(getPosition()+this.conf.getInt("io.bytes.per.checksum", 512));
      } else {
        throw e;
      }
    }

    /** disables sync. often invoked for tmp files */
    synchronized void ignoreSync(SequenceFile.@OsUntrusted Reader this) {
      sync = null;
    }
    
    /** Set the current byte position in the input file.
     *
     * <p>The position passed must be a position returned by {@link
     * SequenceFile.Writer#getLength()} when writing this file.  To seek to an arbitrary
     * position, use {@link SequenceFile.Reader#sync(long)}.
     */
    public synchronized void seek(SequenceFile.@OsUntrusted Reader this, @OsUntrusted long position) throws IOException {
      in.seek(position);
      if (blockCompressed) {                      // trigger block read
        noBufferedKeys = 0;
        valuesDecompressed = true;
      }
    }

    /** Seek to the next sync mark past a given position.*/
    public synchronized void sync(SequenceFile.@OsUntrusted Reader this, @OsUntrusted long position) throws IOException {
      if (position+SYNC_SIZE >= end) {
        seek(end);
        return;
      }

      if (position < headerEnd) {
        // seek directly to first record
        in.seek(headerEnd);
        // note the sync marker "seen" in the header
        syncSeen = true;
        return;
      }

      try {
        seek(position+4);                         // skip escape
        in.readFully(syncCheck);
        @OsUntrusted
        int syncLen = sync.length;
        for (@OsUntrusted int i = 0; in.getPos() < end; i++) {
          @OsUntrusted
          int j = 0;
          for (; j < syncLen; j++) {
            if (sync[j] != syncCheck[(i+j)%syncLen])
              break;
          }
          if (j == syncLen) {
            in.seek(in.getPos() - SYNC_SIZE);     // position before sync
            return;
          }
          syncCheck[i%syncLen] = in.readByte();
        }
      } catch (@OsUntrusted ChecksumException e) {             // checksum failure
        handleChecksumException(e);
      }
    }

    /** Returns true iff the previous call to next passed a sync mark.*/
    public synchronized @OsUntrusted boolean syncSeen(SequenceFile.@OsUntrusted Reader this) { return syncSeen; }

    /** Return the current byte position in the input file. */
    public synchronized @OsUntrusted long getPosition(SequenceFile.@OsUntrusted Reader this) throws IOException {
      return in.getPos();
    }

    /** Returns the name of the file. */
    @Override
    public @OsUntrusted String toString(SequenceFile.@OsUntrusted Reader this) {
      return filename;
    }

  }

  /** Sorts key/value pairs in a sequence-format file.
   *
   * <p>For best performance, applications should make sure that the {@link
   * Writable#readFields(DataInput)} implementation of their keys is
   * very efficient.  In particular, it should avoid allocating memory.
   */
  public static class Sorter {

    private @OsUntrusted RawComparator comparator;

    private @OsUntrusted MergeSort mergeSort; //the implementation of merge sort
    
    private @OsUntrusted Path @OsUntrusted [] inFiles;                     // when merging or sorting

    private @OsUntrusted Path outFile;

    private @OsUntrusted int memory; // bytes
    private @OsUntrusted int factor; // merged per pass

    private @OsUntrusted FileSystem fs = null;

    private @OsUntrusted Class keyClass;
    private @OsUntrusted Class valClass;

    private @OsUntrusted Configuration conf;
    private @OsUntrusted Metadata metadata;
    
    private @OsUntrusted Progressable progressable = null;

    /** Sort and merge files containing the named classes. */
    public @OsUntrusted Sorter(@OsUntrusted FileSystem fs, @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> keyClass,
                  @OsUntrusted
                  Class valClass, @OsUntrusted Configuration conf)  {
      this(fs, WritableComparator.get(keyClass), keyClass, valClass, conf);
    }

    /** Sort and merge using an arbitrary {@link RawComparator}. */
    public @OsUntrusted Sorter(@OsUntrusted FileSystem fs, @OsUntrusted RawComparator comparator, @OsUntrusted Class keyClass, 
                  @OsUntrusted
                  Class valClass, @OsUntrusted Configuration conf) {
      this(fs, comparator, keyClass, valClass, conf, new @OsUntrusted Metadata());
    }

    /** Sort and merge using an arbitrary {@link RawComparator}. */
    public @OsUntrusted Sorter(@OsUntrusted FileSystem fs, @OsUntrusted RawComparator comparator, @OsUntrusted Class keyClass,
                  @OsUntrusted
                  Class valClass, @OsUntrusted Configuration conf, @OsUntrusted Metadata metadata) {
      this.fs = fs;
      this.comparator = comparator;
      this.keyClass = keyClass;
      this.valClass = valClass;
      this.memory = conf.getInt("io.sort.mb", 100) * 1024 * 1024;
      this.factor = conf.getInt("io.sort.factor", 100);
      this.conf = conf;
      this.metadata = metadata;
    }

    /** Set the number of streams to merge at once.*/
    public void setFactor(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted int factor) { this.factor = factor; }

    /** Get the number of streams to merge at once.*/
    public @OsUntrusted int getFactor(SequenceFile.@OsUntrusted Sorter this) { return factor; }

    /** Set the total amount of buffer memory, in bytes.*/
    public void setMemory(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted int memory) { this.memory = memory; }

    /** Get the total amount of buffer memory, in bytes.*/
    public @OsUntrusted int getMemory(SequenceFile.@OsUntrusted Sorter this) { return memory; }

    /** Set the progressable object in order to report progress. */
    public void setProgressable(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted Progressable progressable) {
      this.progressable = progressable;
    }
    
    /** 
     * Perform a file sort from a set of input files into an output file.
     * @param inFiles the files to be sorted
     * @param outFile the sorted output file
     * @param deleteInput should the input files be deleted as they are read?
     */
    public void sort(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted Path @OsUntrusted [] inFiles, @OsUntrusted Path outFile,
                     @OsUntrusted
                     boolean deleteInput) throws IOException {
      if (fs.exists(outFile)) {
        throw new @OsUntrusted IOException("already exists: " + outFile);
      }

      this.inFiles = inFiles;
      this.outFile = outFile;

      @OsUntrusted
      int segments = sortPass(deleteInput);
      if (segments > 1) {
        mergePass(outFile.getParent());
      }
    }

    /** 
     * Perform a file sort from a set of input files and return an iterator.
     * @param inFiles the files to be sorted
     * @param tempDir the directory where temp files are created during sort
     * @param deleteInput should the input files be deleted as they are read?
     * @return iterator the RawKeyValueIterator
     */
    public @OsUntrusted RawKeyValueIterator sortAndIterate(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted Path @OsUntrusted [] inFiles, @OsUntrusted Path tempDir, 
                                              @OsUntrusted
                                              boolean deleteInput) throws IOException {
      @OsUntrusted
      Path outFile = new @OsUntrusted Path(tempDir + Path.SEPARATOR + "all.2");
      if (fs.exists(outFile)) {
        throw new @OsUntrusted IOException("already exists: " + outFile);
      }
      this.inFiles = inFiles;
      //outFile will basically be used as prefix for temp files in the cases
      //where sort outputs multiple sorted segments. For the single segment
      //case, the outputFile itself will contain the sorted data for that
      //segment
      this.outFile = outFile;

      @OsUntrusted
      int segments = sortPass(deleteInput);
      if (segments > 1)
        return merge(outFile.suffix(".0"), outFile.suffix(".0.index"), 
                     tempDir);
      else if (segments == 1)
        return merge(new @OsUntrusted Path @OsUntrusted []{outFile}, true, tempDir);
      else return null;
    }

    /**
     * The backwards compatible interface to sort.
     * @param inFile the input file to sort
     * @param outFile the sorted output file
     */
    public void sort(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted Path inFile, @OsUntrusted Path outFile) throws IOException {
      sort(new @OsUntrusted Path @OsUntrusted []{inFile}, outFile, false);
    }
    
    private @OsUntrusted int sortPass(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted boolean deleteInput) throws IOException {
      if(LOG.isDebugEnabled()) {
        LOG.debug("running sort pass");
      }
      @OsUntrusted
      SortPass sortPass = new @OsUntrusted SortPass();         // make the SortPass
      sortPass.setProgressable(progressable);
      mergeSort = new @OsUntrusted MergeSort(sortPass.new @OsUntrusted SeqFileComparator());
      try {
        return sortPass.run(deleteInput);         // run it
      } finally {
        sortPass.close();                         // close it
      }
    }

    private class SortPass {
      private @OsUntrusted int memoryLimit = memory/4;
      private @OsUntrusted int recordLimit = 1000000;
      
      private @OsUntrusted DataOutputBuffer rawKeys = new @OsUntrusted DataOutputBuffer();
      private @OsUntrusted byte @OsUntrusted [] rawBuffer;

      private @OsUntrusted int @OsUntrusted [] keyOffsets = new @OsUntrusted int @OsUntrusted [1024];
      private @OsUntrusted int @OsUntrusted [] pointers = new @OsUntrusted int @OsUntrusted [keyOffsets.length];
      private @OsUntrusted int @OsUntrusted [] pointersCopy = new @OsUntrusted int @OsUntrusted [keyOffsets.length];
      private @OsUntrusted int @OsUntrusted [] keyLengths = new @OsUntrusted int @OsUntrusted [keyOffsets.length];
      private @OsUntrusted ValueBytes @OsUntrusted [] rawValues = new @OsUntrusted ValueBytes @OsUntrusted [keyOffsets.length];
      
      private @OsUntrusted ArrayList segmentLengths = new @OsUntrusted ArrayList();
      
      private @OsUntrusted Reader in = null;
      private @OsUntrusted FSDataOutputStream out = null;
      private @OsUntrusted FSDataOutputStream indexOut = null;
      private @OsUntrusted Path outName;

      private @OsUntrusted Progressable progressable = null;

      public @OsUntrusted int run(SequenceFile.@OsUntrusted Sorter.SortPass this, @OsUntrusted boolean deleteInput) throws IOException {
        @OsUntrusted
        int segments = 0;
        @OsUntrusted
        int currentFile = 0;
        @OsUntrusted
        boolean atEof = (currentFile >= inFiles.length);
        @OsUntrusted
        CompressionType compressionType;
        @OsUntrusted
        CompressionCodec codec = null;
        segmentLengths.clear();
        if (atEof) {
          return 0;
        }
        
        // Initialize
        in = new @OsUntrusted Reader(fs, inFiles[currentFile], conf);
        compressionType = in.getCompressionType();
        codec = in.getCompressionCodec();
        
        for (@OsUntrusted int i=0; i < rawValues.length; ++i) {
          rawValues[i] = null;
        }
        
        while (!atEof) {
          @OsUntrusted
          int count = 0;
          @OsUntrusted
          int bytesProcessed = 0;
          rawKeys.reset();
          while (!atEof && 
                 bytesProcessed < memoryLimit && count < recordLimit) {

            // Read a record into buffer
            // Note: Attempt to re-use 'rawValue' as far as possible
            @OsUntrusted
            int keyOffset = rawKeys.getLength();       
            @OsUntrusted
            ValueBytes rawValue = 
              (count == keyOffsets.length || rawValues[count] == null) ? 
              in.createValueBytes() : 
              rawValues[count];
            @OsUntrusted
            int recordLength = in.nextRaw(rawKeys, rawValue);
            if (recordLength == -1) {
              in.close();
              if (deleteInput) {
                fs.delete(inFiles[currentFile], true);
              }
              currentFile += 1;
              atEof = currentFile >= inFiles.length;
              if (!atEof) {
                in = new @OsUntrusted Reader(fs, inFiles[currentFile], conf);
              } else {
                in = null;
              }
              continue;
            }

            @OsUntrusted
            int keyLength = rawKeys.getLength() - keyOffset;

            if (count == keyOffsets.length)
              grow();

            keyOffsets[count] = keyOffset;                // update pointers
            pointers[count] = count;
            keyLengths[count] = keyLength;
            rawValues[count] = rawValue;

            bytesProcessed += recordLength; 
            count++;
          }

          // buffer is full -- sort & flush it
          if(LOG.isDebugEnabled()) {
            LOG.debug("flushing segment " + segments);
          }
          rawBuffer = rawKeys.getData();
          sort(count);
          // indicate we're making progress
          if (progressable != null) {
            progressable.progress();
          }
          flush(count, bytesProcessed, compressionType, codec, 
                segments==0 && atEof);
          segments++;
        }
        return segments;
      }

      public void close(SequenceFile.@OsUntrusted Sorter.SortPass this) throws IOException {
        if (in != null) {
          in.close();
        }
        if (out != null) {
          out.close();
        }
        if (indexOut != null) {
          indexOut.close();
        }
      }

      private void grow(SequenceFile.@OsUntrusted Sorter.SortPass this) {
        @OsUntrusted
        int newLength = keyOffsets.length * 3 / 2;
        keyOffsets = grow(keyOffsets, newLength);
        pointers = grow(pointers, newLength);
        pointersCopy = new @OsUntrusted int @OsUntrusted [newLength];
        keyLengths = grow(keyLengths, newLength);
        rawValues = grow(rawValues, newLength);
      }

      private @OsUntrusted int @OsUntrusted [] grow(SequenceFile.@OsUntrusted Sorter.SortPass this, @OsUntrusted int @OsUntrusted [] old, @OsUntrusted int newLength) {
        @OsUntrusted
        int @OsUntrusted [] result = new @OsUntrusted int @OsUntrusted [newLength];
        System.arraycopy(old, 0, result, 0, old.length);
        return result;
      }
      
      private @OsUntrusted ValueBytes @OsUntrusted [] grow(SequenceFile.@OsUntrusted Sorter.SortPass this, @OsUntrusted ValueBytes @OsUntrusted [] old, @OsUntrusted int newLength) {
        @OsUntrusted
        ValueBytes @OsUntrusted [] result = new @OsUntrusted ValueBytes @OsUntrusted [newLength];
        System.arraycopy(old, 0, result, 0, old.length);
        for (@OsUntrusted int i=old.length; i < newLength; ++i) {
          result[i] = null;
        }
        return result;
      }

      private void flush(SequenceFile.@OsUntrusted Sorter.SortPass this, @OsUntrusted int count, @OsUntrusted int bytesProcessed, 
                         @OsUntrusted
                         CompressionType compressionType, 
                         @OsUntrusted
                         CompressionCodec codec, 
                         @OsUntrusted
                         boolean done) throws IOException {
        if (out == null) {
          outName = done ? outFile : outFile.suffix(".0");
          out = fs.create(outName);
          if (!done) {
            indexOut = fs.create(outName.suffix(".index"));
          }
        }

        @OsUntrusted
        long segmentStart = out.getPos();
        @OsUntrusted
        Writer writer = createWriter(conf, Writer.stream(out), 
            Writer.keyClass(keyClass), Writer.valueClass(valClass),
            Writer.compression(compressionType, codec),
            Writer.metadata(done ? metadata : new @OsUntrusted Metadata()));
        
        if (!done) {
          writer.sync = null;                     // disable sync on temp files
        }

        for (@OsUntrusted int i = 0; i < count; i++) {         // write in sorted order
          @OsUntrusted
          int p = pointers[i];
          writer.appendRaw(rawBuffer, keyOffsets[p], keyLengths[p], rawValues[p]);
        }
        writer.close();
        
        if (!done) {
          // Save the segment length
          WritableUtils.writeVLong(indexOut, segmentStart);
          WritableUtils.writeVLong(indexOut, (out.getPos()-segmentStart));
          indexOut.flush();
        }
      }

      private void sort(SequenceFile.@OsUntrusted Sorter.SortPass this, @OsUntrusted int count) {
        System.arraycopy(pointers, 0, pointersCopy, 0, count);
        mergeSort.mergeSort(pointersCopy, pointers, 0, count);
      }
      class SeqFileComparator implements @OsUntrusted Comparator<@OsUntrusted IntWritable> {
        @Override
        public @OsUntrusted int compare(SequenceFile.@OsUntrusted Sorter.SortPass.SeqFileComparator this, @OsUntrusted IntWritable I, @OsUntrusted IntWritable J) {
          return comparator.compare(rawBuffer, keyOffsets[I.get()], 
                                    keyLengths[I.get()], rawBuffer, 
                                    keyOffsets[J.get()], keyLengths[J.get()]);
        }
      }
      
      /** set the progressable object in order to report progress */
      public void setProgressable(SequenceFile.@OsUntrusted Sorter.SortPass this, @OsUntrusted Progressable progressable)
      {
        this.progressable = progressable;
      }
      
    } // SequenceFile.Sorter.SortPass

    /** The interface to iterate over raw keys/values of SequenceFiles. */
    public static interface RawKeyValueIterator {
      /** Gets the current raw key
       * @return DataOutputBuffer
       * @throws IOException
       */
      @OsUntrusted
      DataOutputBuffer getKey(SequenceFile.Sorter.@OsUntrusted RawKeyValueIterator this) throws IOException; 
      /** Gets the current raw value
       * @return ValueBytes 
       * @throws IOException
       */
      @OsUntrusted
      ValueBytes getValue(SequenceFile.Sorter.@OsUntrusted RawKeyValueIterator this) throws IOException; 
      /** Sets up the current key and value (for getKey and getValue)
       * @return true if there exists a key/value, false otherwise 
       * @throws IOException
       */
      @OsUntrusted
      boolean next(SequenceFile.Sorter.@OsUntrusted RawKeyValueIterator this) throws IOException;
      /** closes the iterator so that the underlying streams can be closed
       * @throws IOException
       */
      void close(SequenceFile.Sorter.@OsUntrusted RawKeyValueIterator this) throws IOException;
      /** Gets the Progress object; this has a float (0.0 - 1.0) 
       * indicating the bytes processed by the iterator so far
       */
      @OsUntrusted
      Progress getProgress(SequenceFile.Sorter.@OsUntrusted RawKeyValueIterator this);
    }    
    
    /**
     * Merges the list of segments of type <code>SegmentDescriptor</code>
     * @param segments the list of SegmentDescriptors
     * @param tmpDir the directory to write temporary files into
     * @return RawKeyValueIterator
     * @throws IOException
     */
    public @OsUntrusted RawKeyValueIterator merge(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted List <@OsUntrusted SegmentDescriptor> segments, 
                                     @OsUntrusted
                                     Path tmpDir) 
      throws IOException {
      // pass in object to report progress, if present
      @OsUntrusted
      MergeQueue mQueue = new @OsUntrusted MergeQueue(segments, tmpDir, progressable);
      return mQueue.merge();
    }

    /**
     * Merges the contents of files passed in Path[] using a max factor value
     * that is already set
     * @param inNames the array of path names
     * @param deleteInputs true if the input files should be deleted when 
     * unnecessary
     * @param tmpDir the directory to write temporary files into
     * @return RawKeyValueIteratorMergeQueue
     * @throws IOException
     */
    public @OsUntrusted RawKeyValueIterator merge(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted Path @OsUntrusted [] inNames, @OsUntrusted boolean deleteInputs,
                                     @OsUntrusted
                                     Path tmpDir) 
      throws IOException {
      return merge(inNames, deleteInputs, 
                   (inNames.length < factor) ? inNames.length : factor,
                   tmpDir);
    }

    /**
     * Merges the contents of files passed in Path[]
     * @param inNames the array of path names
     * @param deleteInputs true if the input files should be deleted when 
     * unnecessary
     * @param factor the factor that will be used as the maximum merge fan-in
     * @param tmpDir the directory to write temporary files into
     * @return RawKeyValueIteratorMergeQueue
     * @throws IOException
     */
    public @OsUntrusted RawKeyValueIterator merge(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted Path @OsUntrusted [] inNames, @OsUntrusted boolean deleteInputs,
                                     @OsUntrusted
                                     int factor, @OsUntrusted Path tmpDir) 
      throws IOException {
      //get the segments from inNames
      @OsUntrusted
      ArrayList <@OsUntrusted SegmentDescriptor> a = new @OsUntrusted ArrayList <@OsUntrusted SegmentDescriptor>();
      for (@OsUntrusted int i = 0; i < inNames.length; i++) {
        @OsUntrusted
        SegmentDescriptor s = new @OsUntrusted SegmentDescriptor(0,
            fs.getFileStatus(inNames[i]).getLen(), inNames[i]);
        s.preserveInput(!deleteInputs);
        s.doSync();
        a.add(s);
      }
      this.factor = factor;
      @OsUntrusted
      MergeQueue mQueue = new @OsUntrusted MergeQueue(a, tmpDir, progressable);
      return mQueue.merge();
    }

    /**
     * Merges the contents of files passed in Path[]
     * @param inNames the array of path names
     * @param tempDir the directory for creating temp files during merge
     * @param deleteInputs true if the input files should be deleted when 
     * unnecessary
     * @return RawKeyValueIteratorMergeQueue
     * @throws IOException
     */
    public @OsUntrusted RawKeyValueIterator merge(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted Path @OsUntrusted [] inNames, @OsUntrusted Path tempDir, 
                                     @OsUntrusted
                                     boolean deleteInputs) 
      throws IOException {
      //outFile will basically be used as prefix for temp files for the
      //intermediate merge outputs           
      this.outFile = new @OsUntrusted Path(tempDir + Path.SEPARATOR + "merged");
      //get the segments from inNames
      @OsUntrusted
      ArrayList <@OsUntrusted SegmentDescriptor> a = new @OsUntrusted ArrayList <@OsUntrusted SegmentDescriptor>();
      for (@OsUntrusted int i = 0; i < inNames.length; i++) {
        @OsUntrusted
        SegmentDescriptor s = new @OsUntrusted SegmentDescriptor(0,
            fs.getFileStatus(inNames[i]).getLen(), inNames[i]);
        s.preserveInput(!deleteInputs);
        s.doSync();
        a.add(s);
      }
      factor = (inNames.length < factor) ? inNames.length : factor;
      // pass in object to report progress, if present
      @OsUntrusted
      MergeQueue mQueue = new @OsUntrusted MergeQueue(a, tempDir, progressable);
      return mQueue.merge();
    }

    /**
     * Clones the attributes (like compression of the input file and creates a 
     * corresponding Writer
     * @param inputFile the path of the input file whose attributes should be 
     * cloned
     * @param outputFile the path of the output file 
     * @param prog the Progressable to report status during the file write
     * @return Writer
     * @throws IOException
     */
    public @OsUntrusted Writer cloneFileAttributes(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted Path inputFile, @OsUntrusted Path outputFile, 
                                      @OsUntrusted
                                      Progressable prog) throws IOException {
      @OsUntrusted
      Reader reader = new @OsUntrusted Reader(conf,
                                 Reader.file(inputFile),
                                 new Reader.@OsUntrusted OnlyHeaderOption());
      @OsUntrusted
      CompressionType compress = reader.getCompressionType();
      @OsUntrusted
      CompressionCodec codec = reader.getCompressionCodec();
      reader.close();

      @OsUntrusted
      Writer writer = createWriter(conf, 
                                   Writer.file(outputFile), 
                                   Writer.keyClass(keyClass), 
                                   Writer.valueClass(valClass), 
                                   Writer.compression(compress, codec), 
                                   Writer.progressable(prog));
      return writer;
    }

    /**
     * Writes records from RawKeyValueIterator into a file represented by the 
     * passed writer
     * @param records the RawKeyValueIterator
     * @param writer the Writer created earlier 
     * @throws IOException
     */
    public void writeFile(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted RawKeyValueIterator records, @OsUntrusted Writer writer) 
      throws IOException {
      while(records.next()) {
        writer.appendRaw(records.getKey().getData(), 0, 
                         records.getKey().getLength(), records.getValue());
      }
      writer.sync();
    }
        
    /** Merge the provided files.
     * @param inFiles the array of input path names
     * @param outFile the final output file
     * @throws IOException
     */
    public void merge(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted Path @OsUntrusted [] inFiles, @OsUntrusted Path outFile) throws IOException {
      if (fs.exists(outFile)) {
        throw new @OsUntrusted IOException("already exists: " + outFile);
      }
      @OsUntrusted
      RawKeyValueIterator r = merge(inFiles, false, outFile.getParent());
      @OsUntrusted
      Writer writer = cloneFileAttributes(inFiles[0], outFile, null);
      
      writeFile(r, writer);

      writer.close();
    }

    /** sort calls this to generate the final merged output */
    private @OsUntrusted int mergePass(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted Path tmpDir) throws IOException {
      if(LOG.isDebugEnabled()) {
        LOG.debug("running merge pass");
      }
      @OsUntrusted
      Writer writer = cloneFileAttributes(
                                          outFile.suffix(".0"), outFile, null);
      @OsUntrusted
      RawKeyValueIterator r = merge(outFile.suffix(".0"), 
                                    outFile.suffix(".0.index"), tmpDir);
      writeFile(r, writer);

      writer.close();
      return 0;
    }

    /** Used by mergePass to merge the output of the sort
     * @param inName the name of the input file containing sorted segments
     * @param indexIn the offsets of the sorted segments
     * @param tmpDir the relative directory to store intermediate results in
     * @return RawKeyValueIterator
     * @throws IOException
     */
    private @OsUntrusted RawKeyValueIterator merge(SequenceFile.@OsUntrusted Sorter this, @OsUntrusted Path inName, @OsUntrusted Path indexIn, @OsUntrusted Path tmpDir) 
      throws IOException {
      //get the segments from indexIn
      //we create a SegmentContainer so that we can track segments belonging to
      //inName and delete inName as soon as we see that we have looked at all
      //the contained segments during the merge process & hence don't need 
      //them anymore
      @OsUntrusted
      SegmentContainer container = new @OsUntrusted SegmentContainer(inName, indexIn);
      @OsUntrusted
      MergeQueue mQueue = new @OsUntrusted MergeQueue(container.getSegmentList(), tmpDir, progressable);
      return mQueue.merge();
    }
    
    /** This class implements the core of the merge logic */
    private class MergeQueue extends @OsUntrusted PriorityQueue 
      implements @OsUntrusted RawKeyValueIterator {
      private @OsUntrusted boolean compress;
      private @OsUntrusted boolean blockCompress;
      private @OsUntrusted DataOutputBuffer rawKey = new @OsUntrusted DataOutputBuffer();
      private @OsUntrusted ValueBytes rawValue;
      private @OsUntrusted long totalBytesProcessed;
      private @OsUntrusted float progPerByte;
      private @OsUntrusted Progress mergeProgress = new @OsUntrusted Progress();
      private @OsUntrusted Path tmpDir;
      private @OsUntrusted Progressable progress = null; //handle to the progress reporting object
      private @OsUntrusted SegmentDescriptor minSegment;
      
      //a TreeMap used to store the segments sorted by size (segment offset and
      //segment path name is used to break ties between segments of same sizes)
      private @OsUntrusted Map<@OsUntrusted SegmentDescriptor, @OsUntrusted Void> sortedSegmentSizes =
        new @OsUntrusted TreeMap<@OsUntrusted SegmentDescriptor, @OsUntrusted Void>();
            
      @SuppressWarnings("unchecked")
      public void put(SequenceFile.@OsUntrusted Sorter.MergeQueue this, @OsUntrusted SegmentDescriptor stream) throws IOException {
        if (size() == 0) {
          compress = stream.in.isCompressed();
          blockCompress = stream.in.isBlockCompressed();
        } else if (compress != stream.in.isCompressed() || 
                   blockCompress != stream.in.isBlockCompressed()) {
          throw new @OsUntrusted IOException("All merged files must be compressed or not.");
        } 
        super.put(stream);
      }
      
      /**
       * A queue of file segments to merge
       * @param segments the file segments to merge
       * @param tmpDir a relative local directory to save intermediate files in
       * @param progress the reference to the Progressable object
       */
      public @OsUntrusted MergeQueue(@OsUntrusted List <@OsUntrusted SegmentDescriptor> segments,
          @OsUntrusted
          Path tmpDir, @OsUntrusted Progressable progress) {
        @OsUntrusted
        int size = segments.size();
        for (@OsUntrusted int i = 0; i < size; i++) {
          sortedSegmentSizes.put(segments.get(i), null);
        }
        this.tmpDir = tmpDir;
        this.progress = progress;
      }
      @Override
      protected @OsUntrusted boolean lessThan(SequenceFile.@OsUntrusted Sorter.MergeQueue this, @OsUntrusted Object a, @OsUntrusted Object b) {
        // indicate we're making progress
        if (progress != null) {
          progress.progress();
        }
        @OsUntrusted
        SegmentDescriptor msa = (@OsUntrusted SegmentDescriptor)a;
        @OsUntrusted
        SegmentDescriptor msb = (@OsUntrusted SegmentDescriptor)b;
        return comparator.compare(msa.getKey().getData(), 0, 
                                  msa.getKey().getLength(), msb.getKey().getData(), 0, 
                                  msb.getKey().getLength()) < 0;
      }
      @Override
      public void close(SequenceFile.@OsUntrusted Sorter.MergeQueue this) throws IOException {
        @OsUntrusted
        SegmentDescriptor ms;                           // close inputs
        while ((ms = (@OsUntrusted SegmentDescriptor)pop()) != null) {
          ms.cleanup();
        }
        minSegment = null;
      }
      @Override
      public @OsUntrusted DataOutputBuffer getKey(SequenceFile.@OsUntrusted Sorter.MergeQueue this) throws IOException {
        return rawKey;
      }
      @Override
      public @OsUntrusted ValueBytes getValue(SequenceFile.@OsUntrusted Sorter.MergeQueue this) throws IOException {
        return rawValue;
      }
      @Override
      public @OsUntrusted boolean next(SequenceFile.@OsUntrusted Sorter.MergeQueue this) throws IOException {
        if (size() == 0)
          return false;
        if (minSegment != null) {
          //minSegment is non-null for all invocations of next except the first
          //one. For the first invocation, the priority queue is ready for use
          //but for the subsequent invocations, first adjust the queue 
          adjustPriorityQueue(minSegment);
          if (size() == 0) {
            minSegment = null;
            return false;
          }
        }
        minSegment = (@OsUntrusted SegmentDescriptor)top();
        @OsUntrusted
        long startPos = minSegment.in.getPosition(); // Current position in stream
        //save the raw key reference
        rawKey = minSegment.getKey();
        //load the raw value. Re-use the existing rawValue buffer
        if (rawValue == null) {
          rawValue = minSegment.in.createValueBytes();
        }
        minSegment.nextRawValue(rawValue);
        @OsUntrusted
        long endPos = minSegment.in.getPosition(); // End position after reading value
        updateProgress(endPos - startPos);
        return true;
      }
      
      @Override
      public @OsUntrusted Progress getProgress(SequenceFile.@OsUntrusted Sorter.MergeQueue this) {
        return mergeProgress; 
      }

      private void adjustPriorityQueue(SequenceFile.@OsUntrusted Sorter.MergeQueue this, @OsUntrusted SegmentDescriptor ms) throws IOException{
        @OsUntrusted
        long startPos = ms.in.getPosition(); // Current position in stream
        @OsUntrusted
        boolean hasNext = ms.nextRawKey();
        @OsUntrusted
        long endPos = ms.in.getPosition(); // End position after reading key
        updateProgress(endPos - startPos);
        if (hasNext) {
          adjustTop();
        } else {
          pop();
          ms.cleanup();
        }
      }

      private void updateProgress(SequenceFile.@OsUntrusted Sorter.MergeQueue this, @OsUntrusted long bytesProcessed) {
        totalBytesProcessed += bytesProcessed;
        if (progPerByte > 0) {
          mergeProgress.set(totalBytesProcessed * progPerByte);
        }
      }
      
      /** This is the single level merge that is called multiple times 
       * depending on the factor size and the number of segments
       * @return RawKeyValueIterator
       * @throws IOException
       */
      public @OsUntrusted RawKeyValueIterator merge(SequenceFile.@OsUntrusted Sorter.MergeQueue this) throws IOException {
        //create the MergeStreams from the sorted map created in the constructor
        //and dump the final output to a file
        @OsUntrusted
        int numSegments = sortedSegmentSizes.size();
        @OsUntrusted
        int origFactor = factor;
        @OsUntrusted
        int passNo = 1;
        @OsUntrusted
        LocalDirAllocator lDirAlloc = new @OsUntrusted LocalDirAllocator("io.seqfile.local.dir");
        do {
          //get the factor for this pass of merge
          factor = getPassFactor(passNo, numSegments);
          @OsUntrusted
          List<@OsUntrusted SegmentDescriptor> segmentsToMerge =
            new @OsUntrusted ArrayList<@OsUntrusted SegmentDescriptor>();
          @OsUntrusted
          int segmentsConsidered = 0;
          @OsUntrusted
          int numSegmentsToConsider = factor;
          while (true) {
            //extract the smallest 'factor' number of segment pointers from the 
            //TreeMap. Call cleanup on the empty segments (no key/value data)
            @OsUntrusted
            SegmentDescriptor @OsUntrusted [] mStream = 
              getSegmentDescriptors(numSegmentsToConsider);
            for (@OsUntrusted int i = 0; i < mStream.length; i++) {
              if (mStream[i].nextRawKey()) {
                segmentsToMerge.add(mStream[i]);
                segmentsConsidered++;
                // Count the fact that we read some bytes in calling nextRawKey()
                updateProgress(mStream[i].in.getPosition());
              }
              else {
                mStream[i].cleanup();
                numSegments--; //we ignore this segment for the merge
              }
            }
            //if we have the desired number of segments
            //or looked at all available segments, we break
            if (segmentsConsidered == factor || 
                sortedSegmentSizes.size() == 0) {
              break;
            }
              
            numSegmentsToConsider = factor - segmentsConsidered;
          }
          //feed the streams to the priority queue
          initialize(segmentsToMerge.size()); clear();
          for (@OsUntrusted int i = 0; i < segmentsToMerge.size(); i++) {
            put(segmentsToMerge.get(i));
          }
          //if we have lesser number of segments remaining, then just return the
          //iterator, else do another single level merge
          if (numSegments <= factor) {
            //calculate the length of the remaining segments. Required for 
            //calculating the merge progress
            @OsUntrusted
            long totalBytes = 0;
            for (@OsUntrusted int i = 0; i < segmentsToMerge.size(); i++) {
              totalBytes += segmentsToMerge.get(i).segmentLength;
            }
            if (totalBytes != 0) //being paranoid
              progPerByte = 1.0f / (@OsUntrusted float)totalBytes;
            //reset factor to what it originally was
            factor = origFactor;
            return this;
          } else {
            //we want to spread the creation of temp files on multiple disks if 
            //available under the space constraints
            @OsUntrusted
            long approxOutputSize = 0; 
            for (@OsUntrusted SegmentDescriptor s : segmentsToMerge) {
              approxOutputSize += s.segmentLength + 
                                  ChecksumFileSystem.getApproxChkSumLength(
                                  s.segmentLength);
            }
            @OsUntrusted
            Path tmpFilename = 
              new @OsUntrusted Path(tmpDir, "intermediate").suffix("." + passNo);

            @OsUntrusted
            Path outputFile =  lDirAlloc.getLocalPathForWrite(
                                                tmpFilename.toString(),
                                                approxOutputSize, conf);
            if(LOG.isDebugEnabled()) { 
              LOG.debug("writing intermediate results to " + outputFile);
            }
            @OsUntrusted
            Writer writer = cloneFileAttributes(
                                                fs.makeQualified(segmentsToMerge.get(0).segmentPathName), 
                                                fs.makeQualified(outputFile), null);
            writer.sync = null; //disable sync for temp files
            writeFile(this, writer);
            writer.close();
            
            //we finished one single level merge; now clean up the priority 
            //queue
            this.close();
            
            @OsUntrusted
            SegmentDescriptor tempSegment = 
              new @OsUntrusted SegmentDescriptor(0,
                  fs.getFileStatus(outputFile).getLen(), outputFile);
            //put the segment back in the TreeMap
            sortedSegmentSizes.put(tempSegment, null);
            numSegments = sortedSegmentSizes.size();
            passNo++;
          }
          //we are worried about only the first pass merge factor. So reset the 
          //factor to what it originally was
          factor = origFactor;
        } while(true);
      }
  
      //Hadoop-591
      public @OsUntrusted int getPassFactor(SequenceFile.@OsUntrusted Sorter.MergeQueue this, @OsUntrusted int passNo, @OsUntrusted int numSegments) {
        if (passNo > 1 || numSegments <= factor || factor == 1) 
          return factor;
        @OsUntrusted
        int mod = (numSegments - 1) % (factor - 1);
        if (mod == 0)
          return factor;
        return mod + 1;
      }
      
      /** Return (& remove) the requested number of segment descriptors from the
       * sorted map.
       */
      public @OsUntrusted SegmentDescriptor @OsUntrusted [] getSegmentDescriptors(SequenceFile.@OsUntrusted Sorter.MergeQueue this, @OsUntrusted int numDescriptors) {
        if (numDescriptors > sortedSegmentSizes.size())
          numDescriptors = sortedSegmentSizes.size();
        @OsUntrusted
        SegmentDescriptor @OsUntrusted [] SegmentDescriptors = 
          new @OsUntrusted SegmentDescriptor @OsUntrusted [numDescriptors];
        @OsUntrusted
        Iterator iter = sortedSegmentSizes.keySet().iterator();
        @OsUntrusted
        int i = 0;
        while (i < numDescriptors) {
          SegmentDescriptors[i++] = (@OsUntrusted SegmentDescriptor)iter.next();
          iter.remove();
        }
        return SegmentDescriptors;
      }
    } // SequenceFile.Sorter.MergeQueue

    /** This class defines a merge segment. This class can be subclassed to 
     * provide a customized cleanup method implementation. In this 
     * implementation, cleanup closes the file handle and deletes the file 
     */
    public class SegmentDescriptor implements @OsUntrusted Comparable {
      
      @OsUntrusted
      long segmentOffset; //the start of the segment in the file
      @OsUntrusted
      long segmentLength; //the length of the segment
      @OsUntrusted
      Path segmentPathName; //the path name of the file containing the segment
      @OsUntrusted
      boolean ignoreSync = true; //set to true for temp files
      private @OsUntrusted Reader in = null; 
      private @OsUntrusted DataOutputBuffer rawKey = null; //this will hold the current key
      private @OsUntrusted boolean preserveInput = false; //delete input segment files?
      
      /** Constructs a segment
       * @param segmentOffset the offset of the segment in the file
       * @param segmentLength the length of the segment
       * @param segmentPathName the path name of the file containing the segment
       */
      public @OsUntrusted SegmentDescriptor (@OsUntrusted long segmentOffset, @OsUntrusted long segmentLength, 
                                @OsUntrusted
                                Path segmentPathName) {
        this.segmentOffset = segmentOffset;
        this.segmentLength = segmentLength;
        this.segmentPathName = segmentPathName;
      }
      
      /** Do the sync checks */
      public void doSync(SequenceFile.@OsUntrusted Sorter.SegmentDescriptor this) {ignoreSync = false;}
      
      /** Whether to delete the files when no longer needed */
      public void preserveInput(SequenceFile.@OsUntrusted Sorter.SegmentDescriptor this, @OsUntrusted boolean preserve) {
        preserveInput = preserve;
      }

      public @OsUntrusted boolean shouldPreserveInput(SequenceFile.@OsUntrusted Sorter.SegmentDescriptor this) {
        return preserveInput;
      }
      
      @Override
      public @OsUntrusted int compareTo(SequenceFile.@OsUntrusted Sorter.SegmentDescriptor this, @OsUntrusted Object o) {
        @OsUntrusted
        SegmentDescriptor that = (@OsUntrusted SegmentDescriptor)o;
        if (this.segmentLength != that.segmentLength) {
          return (this.segmentLength < that.segmentLength ? -1 : 1);
        }
        if (this.segmentOffset != that.segmentOffset) {
          return (this.segmentOffset < that.segmentOffset ? -1 : 1);
        }
        return (this.segmentPathName.toString()).
          compareTo(that.segmentPathName.toString());
      }

      @Override
      public @OsUntrusted boolean equals(SequenceFile.@OsUntrusted Sorter.SegmentDescriptor this, @OsUntrusted Object o) {
        if (!(o instanceof @OsUntrusted SegmentDescriptor)) {
          return false;
        }
        @OsUntrusted
        SegmentDescriptor that = (@OsUntrusted SegmentDescriptor)o;
        if (this.segmentLength == that.segmentLength &&
            this.segmentOffset == that.segmentOffset &&
            this.segmentPathName.toString().equals(
              that.segmentPathName.toString())) {
          return true;
        }
        return false;
      }

      @Override
      public @OsUntrusted int hashCode(SequenceFile.@OsUntrusted Sorter.SegmentDescriptor this) {
        return 37 * 17 + (@OsUntrusted int) (segmentOffset^(segmentOffset>>>32));
      }

      /** Fills up the rawKey object with the key returned by the Reader
       * @return true if there is a key returned; false, otherwise
       * @throws IOException
       */
      public @OsUntrusted boolean nextRawKey(SequenceFile.@OsUntrusted Sorter.SegmentDescriptor this) throws IOException {
        if (in == null) {
          @OsUntrusted
          int bufferSize = getBufferSize(conf); 
          @OsUntrusted
          Reader reader = new @OsUntrusted Reader(conf,
                                     Reader.file(segmentPathName), 
                                     Reader.bufferSize(bufferSize),
                                     Reader.start(segmentOffset), 
                                     Reader.length(segmentLength));
        
          //sometimes we ignore syncs especially for temp merge files
          if (ignoreSync) reader.ignoreSync();

          if (reader.getKeyClass() != keyClass)
            throw new @OsUntrusted IOException("wrong key class: " + reader.getKeyClass() +
                                  " is not " + keyClass);
          if (reader.getValueClass() != valClass)
            throw new @OsUntrusted IOException("wrong value class: "+reader.getValueClass()+
                                  " is not " + valClass);
          this.in = reader;
          rawKey = new @OsUntrusted DataOutputBuffer();
        }
        rawKey.reset();
        @OsUntrusted
        int keyLength = 
          in.nextRawKey(rawKey);
        return (keyLength >= 0);
      }

      /** Fills up the passed rawValue with the value corresponding to the key
       * read earlier
       * @param rawValue
       * @return the length of the value
       * @throws IOException
       */
      public @OsUntrusted int nextRawValue(SequenceFile.@OsUntrusted Sorter.SegmentDescriptor this, @OsUntrusted ValueBytes rawValue) throws IOException {
        @OsUntrusted
        int valLength = in.nextRawValue(rawValue);
        return valLength;
      }
      
      /** Returns the stored rawKey */
      public @OsUntrusted DataOutputBuffer getKey(SequenceFile.@OsUntrusted Sorter.SegmentDescriptor this) {
        return rawKey;
      }
      
      /** closes the underlying reader */
      private void close(SequenceFile.@OsUntrusted Sorter.SegmentDescriptor this) throws IOException {
        this.in.close();
        this.in = null;
      }

      /** The default cleanup. Subclasses can override this with a custom 
       * cleanup 
       */
      public void cleanup(SequenceFile.@OsUntrusted Sorter.SegmentDescriptor this) throws IOException {
        close();
        if (!preserveInput) {
          fs.delete(segmentPathName, true);
        }
      }
    } // SequenceFile.Sorter.SegmentDescriptor
    
    /** This class provisions multiple segments contained within a single
     *  file
     */
    private class LinkedSegmentsDescriptor extends @OsUntrusted SegmentDescriptor {

      @OsUntrusted
      SegmentContainer parentContainer = null;

      /** Constructs a segment
       * @param segmentOffset the offset of the segment in the file
       * @param segmentLength the length of the segment
       * @param segmentPathName the path name of the file containing the segment
       * @param parent the parent SegmentContainer that holds the segment
       */
      public @OsUntrusted LinkedSegmentsDescriptor (@OsUntrusted long segmentOffset, @OsUntrusted long segmentLength, 
                                       @OsUntrusted
                                       Path segmentPathName, @OsUntrusted SegmentContainer parent) {
        super(segmentOffset, segmentLength, segmentPathName);
        this.parentContainer = parent;
      }
      /** The default cleanup. Subclasses can override this with a custom 
       * cleanup 
       */
      @Override
      public void cleanup(SequenceFile.@OsUntrusted Sorter.LinkedSegmentsDescriptor this) throws IOException {
        super.close();
        if (super.shouldPreserveInput()) return;
        parentContainer.cleanup();
      }
      
      @Override
      public @OsUntrusted boolean equals(SequenceFile.@OsUntrusted Sorter.LinkedSegmentsDescriptor this, @OsUntrusted Object o) {
        if (!(o instanceof @OsUntrusted LinkedSegmentsDescriptor)) {
          return false;
        }
        return super.equals(o);
      }
    } //SequenceFile.Sorter.LinkedSegmentsDescriptor

    /** The class that defines a container for segments to be merged. Primarily
     * required to delete temp files as soon as all the contained segments
     * have been looked at */
    private class SegmentContainer {
      private @OsUntrusted int numSegmentsCleanedUp = 0; //track the no. of segment cleanups
      private @OsUntrusted int numSegmentsContained; //# of segments contained
      private @OsUntrusted Path inName; //input file from where segments are created
      
      //the list of segments read from the file
      private @OsUntrusted ArrayList <@OsUntrusted SegmentDescriptor> segments = 
        new @OsUntrusted ArrayList <@OsUntrusted SegmentDescriptor>();
      /** This constructor is there primarily to serve the sort routine that 
       * generates a single output file with an associated index file */
      public @OsUntrusted SegmentContainer(@OsUntrusted Path inName, @OsUntrusted Path indexIn) throws IOException {
        //get the segments from indexIn
        @OsUntrusted
        FSDataInputStream fsIndexIn = fs.open(indexIn);
        @OsUntrusted
        long end = fs.getFileStatus(indexIn).getLen();
        while (fsIndexIn.getPos() < end) {
          @OsUntrusted
          long segmentOffset = WritableUtils.readVLong(fsIndexIn);
          @OsUntrusted
          long segmentLength = WritableUtils.readVLong(fsIndexIn);
          @OsUntrusted
          Path segmentName = inName;
          segments.add(new @OsUntrusted LinkedSegmentsDescriptor(segmentOffset, 
                                                    segmentLength, segmentName, this));
        }
        fsIndexIn.close();
        fs.delete(indexIn, true);
        numSegmentsContained = segments.size();
        this.inName = inName;
      }

      public @OsUntrusted List <@OsUntrusted SegmentDescriptor> getSegmentList(SequenceFile.@OsUntrusted Sorter.SegmentContainer this) {
        return segments;
      }
      public void cleanup(SequenceFile.@OsUntrusted Sorter.SegmentContainer this) throws IOException {
        numSegmentsCleanedUp++;
        if (numSegmentsCleanedUp == numSegmentsContained) {
          fs.delete(inName, true);
        }
      }
    } //SequenceFile.Sorter.SegmentContainer

  } // SequenceFile.Sorter

} // SequenceFile
