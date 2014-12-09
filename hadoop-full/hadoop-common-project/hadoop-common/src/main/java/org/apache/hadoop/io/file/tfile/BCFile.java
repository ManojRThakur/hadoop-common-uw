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
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.file.tfile.CompareUtils.Scalar;
import org.apache.hadoop.io.file.tfile.CompareUtils.ScalarComparator;
import org.apache.hadoop.io.file.tfile.CompareUtils.ScalarLong;
import org.apache.hadoop.io.file.tfile.Compression.Algorithm;
import org.apache.hadoop.io.file.tfile.Utils.Version;

/**
 * Block Compressed file, the underlying physical storage layer for TFile.
 * BCFile provides the basic block level compression for the data block and meta
 * blocks. It is separated from TFile as it may be used for other
 * block-compressed file implementation.
 */
final class BCFile {
  // the current version of BCFile impl, increment them (major or minor) made
  // enough changes
  static final @OsUntrusted Version API_VERSION = new @OsUntrusted Version((@OsUntrusted short) 1, (@OsUntrusted short) 0);
  static final @OsUntrusted Log LOG = LogFactory.getLog(BCFile.class);

  /**
   * Prevent the instantiation of BCFile objects.
   */
  private @OsUntrusted BCFile() {
    // nothing
  }

  /**
   * BCFile writer, the entry point for creating a new BCFile.
   */
  static public class Writer implements @OsUntrusted Closeable {
    private final @OsUntrusted FSDataOutputStream out;
    private final @OsUntrusted Configuration conf;
    // the single meta block containing index of compressed data blocks
    final @OsUntrusted DataIndex dataIndex;
    // index for meta blocks
    final @OsUntrusted MetaIndex metaIndex;
    @OsUntrusted
    boolean blkInProgress = false;
    private @OsUntrusted boolean metaBlkSeen = false;
    private @OsUntrusted boolean closed = false;
    @OsUntrusted
    long errorCount = 0;
    // reusable buffers.
    private @OsUntrusted BytesWritable fsOutputBuffer;

    /**
     * Call-back interface to register a block after a block is closed.
     */
    private static interface BlockRegister {
      /**
       * Register a block that is fully closed.
       * 
       * @param raw
       *          The size of block in terms of uncompressed bytes.
       * @param offsetStart
       *          The start offset of the block.
       * @param offsetEnd
       *          One byte after the end of the block. Compressed block size is
       *          offsetEnd - offsetStart.
       */
      public void register(BCFile.Writer.@OsUntrusted BlockRegister this, @OsUntrusted long raw, @OsUntrusted long offsetStart, @OsUntrusted long offsetEnd);
    }

    /**
     * Intermediate class that maintain the state of a Writable Compression
     * Block.
     */
    private static final class WBlockState {
      private final @OsUntrusted Algorithm compressAlgo;
      private @OsUntrusted Compressor compressor; // !null only if using native
      // Hadoop compression
      private final @OsUntrusted FSDataOutputStream fsOut;
      private final @OsUntrusted long posStart;
      private final @OsUntrusted SimpleBufferedOutputStream fsBufferedOutput;
      private @OsUntrusted OutputStream out;

      /**
       * @param compressionAlgo
       *          The compression algorithm to be used to for compression.
       * @throws IOException
       */
      public @OsUntrusted WBlockState(@OsUntrusted Algorithm compressionAlgo, @OsUntrusted FSDataOutputStream fsOut,
          @OsUntrusted
          BytesWritable fsOutputBuffer, @OsUntrusted Configuration conf) throws IOException {
        this.compressAlgo = compressionAlgo;
        this.fsOut = fsOut;
        this.posStart = fsOut.getPos();

        fsOutputBuffer.setCapacity(TFile.getFSOutputBufferSize(conf));

        this.fsBufferedOutput =
            new @OsUntrusted SimpleBufferedOutputStream(this.fsOut, fsOutputBuffer.getBytes());
        this.compressor = compressAlgo.getCompressor();

        try {
          this.out =
              compressionAlgo.createCompressionStream(fsBufferedOutput,
                  compressor, 0);
        } catch (@OsUntrusted IOException e) {
          compressAlgo.returnCompressor(compressor);
          throw e;
        }
      }

      /**
       * Get the output stream for BlockAppender's consumption.
       * 
       * @return the output stream suitable for writing block data.
       */
      @OsUntrusted
      OutputStream getOutputStream(BCFile.Writer.@OsUntrusted WBlockState this) {
        return out;
      }

      /**
       * Get the current position in file.
       * 
       * @return The current byte offset in underlying file.
       * @throws IOException
       */
      @OsUntrusted
      long getCurrentPos(BCFile.Writer.@OsUntrusted WBlockState this) throws IOException {
        return fsOut.getPos() + fsBufferedOutput.size();
      }

      @OsUntrusted
      long getStartPos(BCFile.Writer.@OsUntrusted WBlockState this) {
        return posStart;
      }

      /**
       * Current size of compressed data.
       * 
       * @return
       * @throws IOException
       */
      @OsUntrusted
      long getCompressedSize(BCFile.Writer.@OsUntrusted WBlockState this) throws IOException {
        @OsUntrusted
        long ret = getCurrentPos() - posStart;
        return ret;
      }

      /**
       * Finishing up the current block.
       */
      public void finish(BCFile.Writer.@OsUntrusted WBlockState this) throws IOException {
        try {
          if (out != null) {
            out.flush();
            out = null;
          }
        } finally {
          compressAlgo.returnCompressor(compressor);
          compressor = null;
        }
      }
    }

    /**
     * Access point to stuff data into a block.
     * 
     * TODO: Change DataOutputStream to something else that tracks the size as
     * long instead of int. Currently, we will wrap around if the row block size
     * is greater than 4GB.
     */
    public class BlockAppender extends @OsUntrusted DataOutputStream {
      private final @OsUntrusted BlockRegister blockRegister;
      private final @OsUntrusted WBlockState wBlkState;
      @SuppressWarnings("hiding")
      private @OsUntrusted boolean closed = false;

      /**
       * Constructor
       * 
       * @param register
       *          the block register, which is called when the block is closed.
       * @param wbs
       *          The writable compression block state.
       */
      @OsUntrusted
      BlockAppender(@OsUntrusted BlockRegister register, @OsUntrusted WBlockState wbs) {
        super(wbs.getOutputStream());
        this.blockRegister = register;
        this.wBlkState = wbs;
      }

      /**
       * Get the raw size of the block.
       * 
       * @return the number of uncompressed bytes written through the
       *         BlockAppender so far.
       * @throws IOException
       */
      public @OsUntrusted long getRawSize(BCFile.@OsUntrusted Writer.BlockAppender this) throws IOException {
        /**
         * Expecting the size() of a block not exceeding 4GB. Assuming the
         * size() will wrap to negative integer if it exceeds 2GB.
         */
        return size() & 0x00000000ffffffffL;
      }

      /**
       * Get the compressed size of the block in progress.
       * 
       * @return the number of compressed bytes written to the underlying FS
       *         file. The size may be smaller than actual need to compress the
       *         all data written due to internal buffering inside the
       *         compressor.
       * @throws IOException
       */
      public @OsUntrusted long getCompressedSize(BCFile.@OsUntrusted Writer.BlockAppender this) throws IOException {
        return wBlkState.getCompressedSize();
      }

      @Override
      public void flush(BCFile.@OsUntrusted Writer.BlockAppender this) {
        // The down stream is a special kind of stream that finishes a
        // compression block upon flush. So we disable flush() here.
      }

      /**
       * Signaling the end of write to the block. The block register will be
       * called for registering the finished block.
       */
      @Override
      public void close(BCFile.@OsUntrusted Writer.BlockAppender this) throws IOException {
        if (closed == true) {
          return;
        }
        try {
          ++errorCount;
          wBlkState.finish();
          blockRegister.register(getRawSize(), wBlkState.getStartPos(),
              wBlkState.getCurrentPos());
          --errorCount;
        } finally {
          closed = true;
          blkInProgress = false;
        }
      }
    }

    /**
     * Constructor
     * 
     * @param fout
     *          FS output stream.
     * @param compressionName
     *          Name of the compression algorithm, which will be used for all
     *          data blocks.
     * @throws IOException
     * @see Compression#getSupportedAlgorithms
     */
    public @OsUntrusted Writer(@OsUntrusted FSDataOutputStream fout, @OsUntrusted String compressionName,
        @OsUntrusted
        Configuration conf) throws IOException {
      if (fout.getPos() != 0) {
        throw new @OsUntrusted IOException("Output file not at zero offset.");
      }

      this.out = fout;
      this.conf = conf;
      dataIndex = new @OsUntrusted DataIndex(compressionName);
      metaIndex = new @OsUntrusted MetaIndex();
      fsOutputBuffer = new @OsUntrusted BytesWritable();
      Magic.write(fout);
    }

    /**
     * Close the BCFile Writer. Attempting to use the Writer after calling
     * <code>close</code> is not allowed and may lead to undetermined results.
     */
    @Override
    public void close(BCFile.@OsUntrusted Writer this) throws IOException {
      if (closed == true) {
        return;
      }

      try {
        if (errorCount == 0) {
          if (blkInProgress == true) {
            throw new @OsUntrusted IllegalStateException(
                "Close() called with active block appender.");
          }

          // add metaBCFileIndex to metaIndex as the last meta block
          @OsUntrusted
          BlockAppender appender =
              prepareMetaBlock(DataIndex.BLOCK_NAME,
                  getDefaultCompressionAlgorithm());
          try {
            dataIndex.write(appender);
          } finally {
            appender.close();
          }

          @OsUntrusted
          long offsetIndexMeta = out.getPos();
          metaIndex.write(out);

          // Meta Index and the trailing section are written out directly.
          out.writeLong(offsetIndexMeta);

          API_VERSION.write(out);
          Magic.write(out);
          out.flush();
        }
      } finally {
        closed = true;
      }
    }

    private @OsUntrusted Algorithm getDefaultCompressionAlgorithm(BCFile.@OsUntrusted Writer this) {
      return dataIndex.getDefaultCompressionAlgorithm();
    }

    private @OsUntrusted BlockAppender prepareMetaBlock(BCFile.@OsUntrusted Writer this, @OsUntrusted String name, @OsUntrusted Algorithm compressAlgo)
        throws IOException, MetaBlockAlreadyExists {
      if (blkInProgress == true) {
        throw new @OsUntrusted IllegalStateException(
            "Cannot create Meta Block until previous block is closed.");
      }

      if (metaIndex.getMetaByName(name) != null) {
        throw new @OsUntrusted MetaBlockAlreadyExists("name=" + name);
      }

      @OsUntrusted
      MetaBlockRegister mbr = new @OsUntrusted MetaBlockRegister(name, compressAlgo);
      @OsUntrusted
      WBlockState wbs =
          new @OsUntrusted WBlockState(compressAlgo, out, fsOutputBuffer, conf);
      @OsUntrusted
      BlockAppender ba = new @OsUntrusted BlockAppender(mbr, wbs);
      blkInProgress = true;
      metaBlkSeen = true;
      return ba;
    }

    /**
     * Create a Meta Block and obtain an output stream for adding data into the
     * block. There can only be one BlockAppender stream active at any time.
     * Regular Blocks may not be created after the first Meta Blocks. The caller
     * must call BlockAppender.close() to conclude the block creation.
     * 
     * @param name
     *          The name of the Meta Block. The name must not conflict with
     *          existing Meta Blocks.
     * @param compressionName
     *          The name of the compression algorithm to be used.
     * @return The BlockAppender stream
     * @throws IOException
     * @throws MetaBlockAlreadyExists
     *           If the meta block with the name already exists.
     */
    public @OsUntrusted BlockAppender prepareMetaBlock(BCFile.@OsUntrusted Writer this, @OsUntrusted String name, @OsUntrusted String compressionName)
        throws IOException, MetaBlockAlreadyExists {
      return prepareMetaBlock(name, Compression
          .getCompressionAlgorithmByName(compressionName));
    }

    /**
     * Create a Meta Block and obtain an output stream for adding data into the
     * block. The Meta Block will be compressed with the same compression
     * algorithm as data blocks. There can only be one BlockAppender stream
     * active at any time. Regular Blocks may not be created after the first
     * Meta Blocks. The caller must call BlockAppender.close() to conclude the
     * block creation.
     * 
     * @param name
     *          The name of the Meta Block. The name must not conflict with
     *          existing Meta Blocks.
     * @return The BlockAppender stream
     * @throws MetaBlockAlreadyExists
     *           If the meta block with the name already exists.
     * @throws IOException
     */
    public @OsUntrusted BlockAppender prepareMetaBlock(BCFile.@OsUntrusted Writer this, @OsUntrusted String name) throws IOException,
        MetaBlockAlreadyExists {
      return prepareMetaBlock(name, getDefaultCompressionAlgorithm());
    }

    /**
     * Create a Data Block and obtain an output stream for adding data into the
     * block. There can only be one BlockAppender stream active at any time.
     * Data Blocks may not be created after the first Meta Blocks. The caller
     * must call BlockAppender.close() to conclude the block creation.
     * 
     * @return The BlockAppender stream
     * @throws IOException
     */
    public @OsUntrusted BlockAppender prepareDataBlock(BCFile.@OsUntrusted Writer this) throws IOException {
      if (blkInProgress == true) {
        throw new @OsUntrusted IllegalStateException(
            "Cannot create Data Block until previous block is closed.");
      }

      if (metaBlkSeen == true) {
        throw new @OsUntrusted IllegalStateException(
            "Cannot create Data Block after Meta Blocks.");
      }

      @OsUntrusted
      DataBlockRegister dbr = new @OsUntrusted DataBlockRegister();

      @OsUntrusted
      WBlockState wbs =
          new @OsUntrusted WBlockState(getDefaultCompressionAlgorithm(), out,
              fsOutputBuffer, conf);
      @OsUntrusted
      BlockAppender ba = new @OsUntrusted BlockAppender(dbr, wbs);
      blkInProgress = true;
      return ba;
    }

    /**
     * Callback to make sure a meta block is added to the internal list when its
     * stream is closed.
     */
    private class MetaBlockRegister implements @OsUntrusted BlockRegister {
      private final @OsUntrusted String name;
      private final @OsUntrusted Algorithm compressAlgo;

      @OsUntrusted
      MetaBlockRegister(@OsUntrusted String name, @OsUntrusted Algorithm compressAlgo) {
        this.name = name;
        this.compressAlgo = compressAlgo;
      }

      @Override
      public void register(BCFile.@OsUntrusted Writer.MetaBlockRegister this, @OsUntrusted long raw, @OsUntrusted long begin, @OsUntrusted long end) {
        metaIndex.addEntry(new @OsUntrusted MetaIndexEntry(name, compressAlgo,
            new @OsUntrusted BlockRegion(begin, end - begin, raw)));
      }
    }

    /**
     * Callback to make sure a data block is added to the internal list when
     * it's being closed.
     * 
     */
    private class DataBlockRegister implements @OsUntrusted BlockRegister {
      @OsUntrusted
      DataBlockRegister() {
        // do nothing
      }

      @Override
      public void register(BCFile.@OsUntrusted Writer.DataBlockRegister this, @OsUntrusted long raw, @OsUntrusted long begin, @OsUntrusted long end) {
        dataIndex.addBlockRegion(new @OsUntrusted BlockRegion(begin, end - begin, raw));
      }
    }
  }

  /**
   * BCFile Reader, interface to read the file's data and meta blocks.
   */
  static public class Reader implements @OsUntrusted Closeable {
    private final @OsUntrusted FSDataInputStream in;
    private final @OsUntrusted Configuration conf;
    final @OsUntrusted DataIndex dataIndex;
    // Index for meta blocks
    final @OsUntrusted MetaIndex metaIndex;
    final @OsUntrusted Version version;

    /**
     * Intermediate class that maintain the state of a Readable Compression
     * Block.
     */
    static private final class RBlockState {
      private final @OsUntrusted Algorithm compressAlgo;
      private @OsUntrusted Decompressor decompressor;
      private final @OsUntrusted BlockRegion region;
      private final @OsUntrusted InputStream in;

      public @OsUntrusted RBlockState(@OsUntrusted Algorithm compressionAlgo, @OsUntrusted FSDataInputStream fsin,
          @OsUntrusted
          BlockRegion region, @OsUntrusted Configuration conf) throws IOException {
        this.compressAlgo = compressionAlgo;
        this.region = region;
        this.decompressor = compressionAlgo.getDecompressor();

        try {
          this.in =
              compressAlgo
                  .createDecompressionStream(new @OsUntrusted BoundedRangeFileInputStream(
                      fsin, this.region.getOffset(), this.region
                          .getCompressedSize()), decompressor, TFile
                      .getFSInputBufferSize(conf));
        } catch (@OsUntrusted IOException e) {
          compressAlgo.returnDecompressor(decompressor);
          throw e;
        }
      }

      /**
       * Get the output stream for BlockAppender's consumption.
       * 
       * @return the output stream suitable for writing block data.
       */
      public @OsUntrusted InputStream getInputStream(BCFile.Reader.@OsUntrusted RBlockState this) {
        return in;
      }

      public @OsUntrusted String getCompressionName(BCFile.Reader.@OsUntrusted RBlockState this) {
        return compressAlgo.getName();
      }

      public @OsUntrusted BlockRegion getBlockRegion(BCFile.Reader.@OsUntrusted RBlockState this) {
        return region;
      }

      public void finish(BCFile.Reader.@OsUntrusted RBlockState this) throws IOException {
        try {
          in.close();
        } finally {
          compressAlgo.returnDecompressor(decompressor);
          decompressor = null;
        }
      }
    }

    /**
     * Access point to read a block.
     */
    public static class BlockReader extends @OsUntrusted DataInputStream {
      private final @OsUntrusted RBlockState rBlkState;
      private @OsUntrusted boolean closed = false;

      @OsUntrusted
      BlockReader(@OsUntrusted RBlockState rbs) {
        super(rbs.getInputStream());
        rBlkState = rbs;
      }

      /**
       * Finishing reading the block. Release all resources.
       */
      @Override
      public void close(BCFile.Reader.@OsUntrusted BlockReader this) throws IOException {
        if (closed == true) {
          return;
        }
        try {
          // Do not set rBlkState to null. People may access stats after calling
          // close().
          rBlkState.finish();
        } finally {
          closed = true;
        }
      }

      /**
       * Get the name of the compression algorithm used to compress the block.
       * 
       * @return name of the compression algorithm.
       */
      public @OsUntrusted String getCompressionName(BCFile.Reader.@OsUntrusted BlockReader this) {
        return rBlkState.getCompressionName();
      }

      /**
       * Get the uncompressed size of the block.
       * 
       * @return uncompressed size of the block.
       */
      public @OsUntrusted long getRawSize(BCFile.Reader.@OsUntrusted BlockReader this) {
        return rBlkState.getBlockRegion().getRawSize();
      }

      /**
       * Get the compressed size of the block.
       * 
       * @return compressed size of the block.
       */
      public @OsUntrusted long getCompressedSize(BCFile.Reader.@OsUntrusted BlockReader this) {
        return rBlkState.getBlockRegion().getCompressedSize();
      }

      /**
       * Get the starting position of the block in the file.
       * 
       * @return the starting position of the block in the file.
       */
      public @OsUntrusted long getStartPos(BCFile.Reader.@OsUntrusted BlockReader this) {
        return rBlkState.getBlockRegion().getOffset();
      }
    }

    /**
     * Constructor
     * 
     * @param fin
     *          FS input stream.
     * @param fileLength
     *          Length of the corresponding file
     * @throws IOException
     */
    public @OsUntrusted Reader(@OsUntrusted FSDataInputStream fin, @OsUntrusted long fileLength, @OsUntrusted Configuration conf)
        throws IOException {
      this.in = fin;
      this.conf = conf;

      // move the cursor to the beginning of the tail, containing: offset to the
      // meta block index, version and magic
      fin.seek(fileLength - Magic.size() - Version.size() - Long.SIZE
          / Byte.SIZE);
      @OsUntrusted
      long offsetIndexMeta = fin.readLong();
      version = new @OsUntrusted Version(fin);
      Magic.readAndVerify(fin);

      if (!version.compatibleWith(BCFile.API_VERSION)) {
        throw new @OsUntrusted RuntimeException("Incompatible BCFile fileBCFileVersion.");
      }

      // read meta index
      fin.seek(offsetIndexMeta);
      metaIndex = new @OsUntrusted MetaIndex(fin);

      // read data:BCFile.index, the data block index
      @OsUntrusted
      BlockReader blockR = getMetaBlock(DataIndex.BLOCK_NAME);
      try {
        dataIndex = new @OsUntrusted DataIndex(blockR);
      } finally {
        blockR.close();
      }
    }

    /**
     * Get the name of the default compression algorithm.
     * 
     * @return the name of the default compression algorithm.
     */
    public @OsUntrusted String getDefaultCompressionName(BCFile.@OsUntrusted Reader this) {
      return dataIndex.getDefaultCompressionAlgorithm().getName();
    }

    /**
     * Get version of BCFile file being read.
     * 
     * @return version of BCFile file being read.
     */
    public @OsUntrusted Version getBCFileVersion(BCFile.@OsUntrusted Reader this) {
      return version;
    }

    /**
     * Get version of BCFile API.
     * 
     * @return version of BCFile API.
     */
    public @OsUntrusted Version getAPIVersion(BCFile.@OsUntrusted Reader this) {
      return API_VERSION;
    }

    /**
     * Finishing reading the BCFile. Release all resources.
     */
    @Override
    public void close(BCFile.@OsUntrusted Reader this) {
      // nothing to be done now
    }

    /**
     * Get the number of data blocks.
     * 
     * @return the number of data blocks.
     */
    public @OsUntrusted int getBlockCount(BCFile.@OsUntrusted Reader this) {
      return dataIndex.getBlockRegionList().size();
    }

    /**
     * Stream access to a Meta Block.
     * 
     * @param name
     *          meta block name
     * @return BlockReader input stream for reading the meta block.
     * @throws IOException
     * @throws MetaBlockDoesNotExist
     *           The Meta Block with the given name does not exist.
     */
    public @OsUntrusted BlockReader getMetaBlock(BCFile.@OsUntrusted Reader this, @OsUntrusted String name) throws IOException,
        MetaBlockDoesNotExist {
      @OsUntrusted
      MetaIndexEntry imeBCIndex = metaIndex.getMetaByName(name);
      if (imeBCIndex == null) {
        throw new @OsUntrusted MetaBlockDoesNotExist("name=" + name);
      }

      @OsUntrusted
      BlockRegion region = imeBCIndex.getRegion();
      return createReader(imeBCIndex.getCompressionAlgorithm(), region);
    }

    /**
     * Stream access to a Data Block.
     * 
     * @param blockIndex
     *          0-based data block index.
     * @return BlockReader input stream for reading the data block.
     * @throws IOException
     */
    public @OsUntrusted BlockReader getDataBlock(BCFile.@OsUntrusted Reader this, @OsUntrusted int blockIndex) throws IOException {
      if (blockIndex < 0 || blockIndex >= getBlockCount()) {
        throw new @OsUntrusted IndexOutOfBoundsException(String.format(
            "blockIndex=%d, numBlocks=%d", blockIndex, getBlockCount()));
      }

      @OsUntrusted
      BlockRegion region = dataIndex.getBlockRegionList().get(blockIndex);
      return createReader(dataIndex.getDefaultCompressionAlgorithm(), region);
    }

    private @OsUntrusted BlockReader createReader(BCFile.@OsUntrusted Reader this, @OsUntrusted Algorithm compressAlgo, @OsUntrusted BlockRegion region)
        throws IOException {
      @OsUntrusted
      RBlockState rbs = new @OsUntrusted RBlockState(compressAlgo, in, region, conf);
      return new @OsUntrusted BlockReader(rbs);
    }

    /**
     * Find the smallest Block index whose starting offset is greater than or
     * equal to the specified offset.
     * 
     * @param offset
     *          User-specific offset.
     * @return the index to the data Block if such block exists; or -1
     *         otherwise.
     */
    public @OsUntrusted int getBlockIndexNear(BCFile.@OsUntrusted Reader this, @OsUntrusted long offset) {
      @OsUntrusted
      ArrayList<@OsUntrusted BlockRegion> list = dataIndex.getBlockRegionList();
      @OsUntrusted
      int idx =
          Utils
              .lowerBound(list, new @OsUntrusted ScalarLong(offset), new @OsUntrusted ScalarComparator());

      if (idx == list.size()) {
        return -1;
      }

      return idx;
    }
  }

  /**
   * Index for all Meta blocks.
   */
  static class MetaIndex {
    // use a tree map, for getting a meta block entry by name
    final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted MetaIndexEntry> index;

    // for write
    public @OsUntrusted MetaIndex() {
      index = new @OsUntrusted TreeMap<@OsUntrusted String, @OsUntrusted MetaIndexEntry>();
    }

    // for read, construct the map from the file
    public @OsUntrusted MetaIndex(@OsUntrusted DataInput in) throws IOException {
      @OsUntrusted
      int count = Utils.readVInt(in);
      index = new @OsUntrusted TreeMap<@OsUntrusted String, @OsUntrusted MetaIndexEntry>();

      for (@OsUntrusted int nx = 0; nx < count; nx++) {
        @OsUntrusted
        MetaIndexEntry indexEntry = new @OsUntrusted MetaIndexEntry(in);
        index.put(indexEntry.getMetaName(), indexEntry);
      }
    }

    public void addEntry(BCFile.@OsUntrusted MetaIndex this, @OsUntrusted MetaIndexEntry indexEntry) {
      index.put(indexEntry.getMetaName(), indexEntry);
    }

    public @OsUntrusted MetaIndexEntry getMetaByName(BCFile.@OsUntrusted MetaIndex this, @OsUntrusted String name) {
      return index.get(name);
    }

    public void write(BCFile.@OsUntrusted MetaIndex this, @OsUntrusted DataOutput out) throws IOException {
      Utils.writeVInt(out, index.size());

      for (@OsUntrusted MetaIndexEntry indexEntry : index.values()) {
        indexEntry.write(out);
      }
    }
  }

  /**
   * An entry describes a meta block in the MetaIndex.
   */
  static final class MetaIndexEntry {
    private final @OsUntrusted String metaName;
    private final @OsUntrusted Algorithm compressionAlgorithm;
    private final static @OsUntrusted String defaultPrefix = "data:";

    private final @OsUntrusted BlockRegion region;

    public @OsUntrusted MetaIndexEntry(@OsUntrusted DataInput in) throws IOException {
      @OsUntrusted
      String fullMetaName = Utils.readString(in);
      if (fullMetaName.startsWith(defaultPrefix)) {
        metaName =
            fullMetaName.substring(defaultPrefix.length(), fullMetaName
                .length());
      } else {
        throw new @OsUntrusted IOException("Corrupted Meta region Index");
      }

      compressionAlgorithm =
          Compression.getCompressionAlgorithmByName(Utils.readString(in));
      region = new @OsUntrusted BlockRegion(in);
    }

    public @OsUntrusted MetaIndexEntry(@OsUntrusted String metaName, @OsUntrusted Algorithm compressionAlgorithm,
        @OsUntrusted
        BlockRegion region) {
      this.metaName = metaName;
      this.compressionAlgorithm = compressionAlgorithm;
      this.region = region;
    }

    public @OsUntrusted String getMetaName(BCFile.@OsUntrusted MetaIndexEntry this) {
      return metaName;
    }

    public @OsUntrusted Algorithm getCompressionAlgorithm(BCFile.@OsUntrusted MetaIndexEntry this) {
      return compressionAlgorithm;
    }

    public @OsUntrusted BlockRegion getRegion(BCFile.@OsUntrusted MetaIndexEntry this) {
      return region;
    }

    public void write(BCFile.@OsUntrusted MetaIndexEntry this, @OsUntrusted DataOutput out) throws IOException {
      Utils.writeString(out, defaultPrefix + metaName);
      Utils.writeString(out, compressionAlgorithm.getName());

      region.write(out);
    }
  }

  /**
   * Index of all compressed data blocks.
   */
  static class DataIndex {
    final static @OsUntrusted String BLOCK_NAME = "BCFile.index";

    private final @OsUntrusted Algorithm defaultCompressionAlgorithm;

    // for data blocks, each entry specifies a block's offset, compressed size
    // and raw size
    private final @OsUntrusted ArrayList<@OsUntrusted BlockRegion> listRegions;

    // for read, deserialized from a file
    public @OsUntrusted DataIndex(@OsUntrusted DataInput in) throws IOException {
      defaultCompressionAlgorithm =
          Compression.getCompressionAlgorithmByName(Utils.readString(in));

      @OsUntrusted
      int n = Utils.readVInt(in);
      listRegions = new @OsUntrusted ArrayList<@OsUntrusted BlockRegion>(n);

      for (@OsUntrusted int i = 0; i < n; i++) {
        @OsUntrusted
        BlockRegion region = new @OsUntrusted BlockRegion(in);
        listRegions.add(region);
      }
    }

    // for write
    public @OsUntrusted DataIndex(@OsUntrusted String defaultCompressionAlgorithmName) {
      this.defaultCompressionAlgorithm =
          Compression
              .getCompressionAlgorithmByName(defaultCompressionAlgorithmName);
      listRegions = new @OsUntrusted ArrayList<@OsUntrusted BlockRegion>();
    }

    public @OsUntrusted Algorithm getDefaultCompressionAlgorithm(BCFile.@OsUntrusted DataIndex this) {
      return defaultCompressionAlgorithm;
    }

    public @OsUntrusted ArrayList<@OsUntrusted BlockRegion> getBlockRegionList(BCFile.@OsUntrusted DataIndex this) {
      return listRegions;
    }

    public void addBlockRegion(BCFile.@OsUntrusted DataIndex this, @OsUntrusted BlockRegion region) {
      listRegions.add(region);
    }

    public void write(BCFile.@OsUntrusted DataIndex this, @OsUntrusted DataOutput out) throws IOException {
      Utils.writeString(out, defaultCompressionAlgorithm.getName());

      Utils.writeVInt(out, listRegions.size());

      for (@OsUntrusted BlockRegion region : listRegions) {
        region.write(out);
      }
    }
  }

  /**
   * Magic number uniquely identifying a BCFile in the header/footer.
   */
  static final class Magic {
    private final static @OsUntrusted byte @OsUntrusted [] AB_MAGIC_BCFILE =
        new byte @OsUntrusted [] {
            // ... total of 16 bytes
            (@OsUntrusted byte) 0xd1, (@OsUntrusted byte) 0x11, (@OsUntrusted byte) 0xd3, (@OsUntrusted byte) 0x68, (@OsUntrusted byte) 0x91,
            (@OsUntrusted byte) 0xb5, (@OsUntrusted byte) 0xd7, (@OsUntrusted byte) 0xb6, (@OsUntrusted byte) 0x39, (@OsUntrusted byte) 0xdf,
            (@OsUntrusted byte) 0x41, (@OsUntrusted byte) 0x40, (@OsUntrusted byte) 0x92, (@OsUntrusted byte) 0xba, (@OsUntrusted byte) 0xe1,
            (@OsUntrusted byte) 0x50 };

    public static void readAndVerify(@OsUntrusted DataInput in) throws IOException {
      @OsUntrusted
      byte @OsUntrusted [] abMagic = new @OsUntrusted byte @OsUntrusted [size()];
      in.readFully(abMagic);

      // check against AB_MAGIC_BCFILE, if not matching, throw an
      // Exception
      if (!Arrays.equals(abMagic, AB_MAGIC_BCFILE)) {
        throw new @OsUntrusted IOException("Not a valid BCFile.");
      }
    }

    public static void write(@OsUntrusted DataOutput out) throws IOException {
      out.write(AB_MAGIC_BCFILE);
    }

    public static @OsUntrusted int size() {
      return AB_MAGIC_BCFILE.length;
    }
  }

  /**
   * Block region.
   */
  static final class BlockRegion implements @OsUntrusted Scalar {
    private final @OsUntrusted long offset;
    private final @OsUntrusted long compressedSize;
    private final @OsUntrusted long rawSize;

    public @OsUntrusted BlockRegion(@OsUntrusted DataInput in) throws IOException {
      offset = Utils.readVLong(in);
      compressedSize = Utils.readVLong(in);
      rawSize = Utils.readVLong(in);
    }

    public @OsUntrusted BlockRegion(@OsUntrusted long offset, @OsUntrusted long compressedSize, @OsUntrusted long rawSize) {
      this.offset = offset;
      this.compressedSize = compressedSize;
      this.rawSize = rawSize;
    }

    public void write(BCFile.@OsUntrusted BlockRegion this, @OsUntrusted DataOutput out) throws IOException {
      Utils.writeVLong(out, offset);
      Utils.writeVLong(out, compressedSize);
      Utils.writeVLong(out, rawSize);
    }

    public @OsUntrusted long getOffset(BCFile.@OsUntrusted BlockRegion this) {
      return offset;
    }

    public @OsUntrusted long getCompressedSize(BCFile.@OsUntrusted BlockRegion this) {
      return compressedSize;
    }

    public @OsUntrusted long getRawSize(BCFile.@OsUntrusted BlockRegion this) {
      return rawSize;
    }

    @Override
    public @OsUntrusted long magnitude(BCFile.@OsUntrusted BlockRegion this) {
      return offset;
    }
  }
}
