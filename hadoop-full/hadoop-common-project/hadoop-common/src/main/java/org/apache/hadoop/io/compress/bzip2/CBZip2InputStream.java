/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

/*
 * This package is based on the work done by Keiron Liddle, Aftex Software
 * <keiron@aftexsw.com> to whom the Ant project is very grateful for his
 * great code.
 */
package org.apache.hadoop.io.compress.bzip2;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.IOException;

import org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE;


/**
 * An input stream that decompresses from the BZip2 format (without the file
 * header chars) to be read as any other stream.
 *
 * <p>
 * The decompression requires large amounts of memory. Thus you should call the
 * {@link #close() close()} method as soon as possible, to force
 * <tt>CBZip2InputStream</tt> to release the allocated memory. See
 * {@link CBZip2OutputStream CBZip2OutputStream} for information about memory
 * usage.
 * </p>
 *
 * <p>
 * <tt>CBZip2InputStream</tt> reads bytes from the compressed source stream via
 * the single byte {@link java.io.InputStream#read() read()} method exclusively.
 * Thus you should consider to use a buffered source stream.
 * </p>
 *
 * <p>
 * This Ant code was enhanced so that it can de-compress blocks of bzip2 data.
 * Current position in the stream is an important statistic for Hadoop. For
 * example in LineRecordReader, we solely depend on the current position in the
 * stream to know about the progess. The notion of position becomes complicated
 * for compressed files. The Hadoop splitting is done in terms of compressed
 * file. But a compressed file deflates to a large amount of data. So we have
 * handled this problem in the following way.
 *
 * On object creation time, we find the next block start delimiter. Once such a
 * marker is found, the stream stops there (we discard any read compressed data
 * in this process) and the position is updated (i.e. the caller of this class
 * will find out the stream location). At this point we are ready for actual
 * reading (i.e. decompression) of data.
 *
 * The subsequent read calls give out data. The position is updated when the
 * caller of this class has read off the current block + 1 bytes. In between the
 * block reading, position is not updated. (We can only update the postion on
 * block boundaries).
 * </p>
 *
 * <p>
 * Instances of this class are not threadsafe.
 * </p>
 */
public class CBZip2InputStream extends @OsUntrusted InputStream implements @OsUntrusted BZip2Constants {


  public static final @OsUntrusted long BLOCK_DELIMITER = 0X314159265359L;// start of block
  public static final @OsUntrusted long EOS_DELIMITER = 0X177245385090L;// end of bzip2 stream
  private static final @OsUntrusted int DELIMITER_BIT_LENGTH = 48;
  @OsUntrusted
  READ_MODE readMode = READ_MODE.CONTINUOUS;
  // The variable records the current advertised position of the stream.
  private @OsUntrusted long reportedBytesReadFromCompressedStream = 0L;
  // The following variable keep record of compressed bytes read.
  private @OsUntrusted long bytesReadFromCompressedStream = 0L;
  private @OsUntrusted boolean lazyInitialization = false;
  private @OsUntrusted byte array @OsUntrusted [] = new @OsUntrusted byte @OsUntrusted [1];

  /**
  * Index of the last char in the block, so the block size == last + 1.
  */
  private @OsUntrusted int last;

  /**
  * Index in zptr[] of original string after sorting.
  */
  private @OsUntrusted int origPtr;

  /**
  * always: in the range 0 .. 9. The current block size is 100000 * this
  * number.
  */
  private @OsUntrusted int blockSize100k;

  private @OsUntrusted boolean blockRandomised = false;

  private @OsUntrusted long bsBuff;
  private @OsUntrusted long bsLive;
  private final @OsUntrusted CRC crc = new @OsUntrusted CRC();

  private @OsUntrusted int nInUse;

  private @OsUntrusted BufferedInputStream in;

  private @OsUntrusted int currentChar = -1;

  /**
   * A state machine to keep track of current state of the de-coder
   *
   */
  public enum STATE {

@OsUntrusted  EOF,  @OsUntrusted  START_BLOCK_STATE,  @OsUntrusted  RAND_PART_A_STATE,  @OsUntrusted  RAND_PART_B_STATE,  @OsUntrusted  RAND_PART_C_STATE,  @OsUntrusted  NO_RAND_PART_A_STATE,  @OsUntrusted  NO_RAND_PART_B_STATE,  @OsUntrusted  NO_RAND_PART_C_STATE,  @OsUntrusted  NO_PROCESS_STATE
  };

  private @OsUntrusted STATE currentState = STATE.START_BLOCK_STATE;

  private @OsUntrusted int storedBlockCRC, storedCombinedCRC;
  private @OsUntrusted int computedBlockCRC, computedCombinedCRC;

  private @OsUntrusted boolean skipResult = false;// used by skipToNextMarker
  private static @OsUntrusted boolean skipDecompression = false;

  // Variables used by setup* methods exclusively

  private @OsUntrusted int su_count;
  private @OsUntrusted int su_ch2;
  private @OsUntrusted int su_chPrev;
  private @OsUntrusted int su_i2;
  private @OsUntrusted int su_j2;
  private @OsUntrusted int su_rNToGo;
  private @OsUntrusted int su_rTPos;
  private @OsUntrusted int su_tPos;
  private @OsUntrusted char su_z;

  /**
  * All memory intensive stuff. This field is initialized by initBlock().
  */
  private CBZip2InputStream.@OsUntrusted Data data;

  /**
  * This method reports the processed bytes so far. Please note that this
  * statistic is only updated on block boundaries and only when the stream is
  * initiated in BYBLOCK mode.
  */
  public @OsUntrusted long getProcessedByteCount(@OsUntrusted CBZip2InputStream this) {
    return reportedBytesReadFromCompressedStream;
  }

  /**
   * This method keeps track of raw processed compressed
   * bytes.
   *
   * @param count count is the number of bytes to be
   *           added to raw processed bytes
   */

  protected void updateProcessedByteCount(@OsUntrusted CBZip2InputStream this, @OsUntrusted int count) {
    this.bytesReadFromCompressedStream += count;
  }

  /**
   * This method is called by the client of this
   * class in case there are any corrections in
   * the stream position.  One common example is
   * when client of this code removes starting BZ
   * characters from the compressed stream.
   *
   * @param count count bytes are added to the reported bytes
   *
   */
  public void updateReportedByteCount(@OsUntrusted CBZip2InputStream this, @OsUntrusted int count) {
    this.reportedBytesReadFromCompressedStream += count;
    this.updateProcessedByteCount(count);
  }

  /**
  * This method reads a Byte from the compressed stream. Whenever we need to
  * read from the underlying compressed stream, this method should be called
  * instead of directly calling the read method of the underlying compressed
  * stream. This method does important record keeping to have the statistic
  * that how many bytes have been read off the compressed stream.
  */
  private @OsUntrusted int readAByte(@OsUntrusted CBZip2InputStream this, @OsUntrusted InputStream inStream) throws IOException {
    @OsUntrusted
    int read = inStream.read();
    if (read >= 0) {
      this.updateProcessedByteCount(1);
    }
    return read;
  }

  /**
  * This method tries to find the marker (passed to it as the first parameter)
  * in the stream.  It can find bit patterns of length <= 63 bits.  Specifically
  * this method is used in CBZip2InputStream to find the end of block (EOB)
  * delimiter in the stream, starting from the current position of the stream.
  * If marker is found, the stream position will be right after marker at the
  * end of this call.
  *
  * @param marker  The bit pattern to be found in the stream
  * @param markerBitLength  No of bits in the marker
  *
  * @throws IOException
  * @throws IllegalArgumentException  if marketBitLength is greater than 63
  */
  public @OsUntrusted boolean skipToNextMarker(@OsUntrusted CBZip2InputStream this, @OsUntrusted long marker, @OsUntrusted int markerBitLength)
      throws IOException, IllegalArgumentException {
    try {
      if (markerBitLength > 63) {
        throw new @OsUntrusted IllegalArgumentException(
            "skipToNextMarker can not find patterns greater than 63 bits");
      }
      // pick next marketBitLength bits in the stream
      @OsUntrusted
      long bytes = 0;
      bytes = this.bsR(markerBitLength);
      if (bytes == -1) {
        return false;
      }
      while (true) {
        if (bytes == marker) {
          return true;

        } else {
          bytes = bytes << 1;
          bytes = bytes & ((1L << markerBitLength) - 1);
          @OsUntrusted
          int oneBit = (@OsUntrusted int) this.bsR(1);
          if (oneBit != -1) {
            bytes = bytes | oneBit;
          } else
            return false;
        }
      }
    } catch (@OsUntrusted IOException ex) {
      return false;
    }
  }

  protected void reportCRCError(@OsUntrusted CBZip2InputStream this) throws IOException {
    throw new @OsUntrusted IOException("crc error");
  }

  private void makeMaps(@OsUntrusted CBZip2InputStream this) {
    final @OsUntrusted boolean @OsUntrusted [] inUse = this.data.inUse;
    final @OsUntrusted byte @OsUntrusted [] seqToUnseq = this.data.seqToUnseq;

    @OsUntrusted
    int nInUseShadow = 0;

    for (@OsUntrusted int i = 0; i < 256; i++) {
      if (inUse[i])
        seqToUnseq[nInUseShadow++] = (@OsUntrusted byte) i;
    }

    this.nInUse = nInUseShadow;
  }

  /**
  * Constructs a new CBZip2InputStream which decompresses bytes read from the
  * specified stream.
  *
  * <p>
  * Although BZip2 headers are marked with the magic <tt>"Bz"</tt> this
  * constructor expects the next byte in the stream to be the first one after
  * the magic. Thus callers have to skip the first two bytes. Otherwise this
  * constructor will throw an exception.
  * </p>
  *
  * @throws IOException
  *             if the stream content is malformed or an I/O error occurs.
  * @throws NullPointerException
  *             if <tt>in == null</tt>
  */
  public @OsUntrusted CBZip2InputStream(final @OsUntrusted InputStream in, @OsUntrusted READ_MODE readMode)
      throws IOException {

    super();
    @OsUntrusted
    int blockSize = 0X39;// i.e 9
    this.blockSize100k = blockSize - '0';
    this.in = new @OsUntrusted BufferedInputStream(in, 1024 * 9);// >1 MB buffer
    this.readMode = readMode;
    if (readMode == READ_MODE.CONTINUOUS) {
      currentState = STATE.START_BLOCK_STATE;
      lazyInitialization = (in.available() == 0)?true:false;
      if(!lazyInitialization){
    init();
  }
    } else if (readMode == READ_MODE.BYBLOCK) {
      this.currentState = STATE.NO_PROCESS_STATE;
      skipResult = this.skipToNextMarker(CBZip2InputStream.BLOCK_DELIMITER,DELIMITER_BIT_LENGTH);
      this.reportedBytesReadFromCompressedStream = this.bytesReadFromCompressedStream;
      if(!skipDecompression){
        changeStateToProcessABlock();
      }
    }
  }

  /**
   * Returns the number of bytes between the current stream position
   * and the immediate next BZip2 block marker.
   *
   * @param in
   *             The InputStream
   *
   * @return long Number of bytes between current stream position and the
   * next BZip2 block start marker.
 * @throws IOException
   *
   */
  public static @OsUntrusted long numberOfBytesTillNextMarker(final @OsUntrusted InputStream in) throws IOException{
    CBZip2InputStream.skipDecompression = true;
    @OsUntrusted
    CBZip2InputStream anObject = null;

    anObject = new @OsUntrusted CBZip2InputStream(in, READ_MODE.BYBLOCK);

    return anObject.getProcessedByteCount();
  }

  public @OsUntrusted CBZip2InputStream(final @OsUntrusted InputStream in) throws IOException {
    this(in, READ_MODE.CONTINUOUS);
  }

  private void changeStateToProcessABlock(@OsUntrusted CBZip2InputStream this) throws IOException {
    if (skipResult == true) {
      initBlock();
      setupBlock();
    } else {
      this.currentState = STATE.EOF;
    }
  }


  @Override
  public @OsUntrusted int read(@OsUntrusted CBZip2InputStream this) throws IOException {

    if (this.in != null) {
      @OsUntrusted
      int result = this.read(array, 0, 1);
      @OsUntrusted
      int value = 0XFF & array[0];
      return (result > 0 ? value : result);

    } else {
      throw new @OsUntrusted IOException("stream closed");
    }
  }

  /**
   * In CONTINOUS reading mode, this read method starts from the
   * start of the compressed stream and end at the end of file by
   * emitting un-compressed data.  In this mode stream positioning
   * is not announced and should be ignored.
   *
   * In BYBLOCK reading mode, this read method informs about the end
   * of a BZip2 block by returning EOB.  At this event, the compressed
   * stream position is also announced.  This announcement tells that
   * how much of the compressed stream has been de-compressed and read
   * out of this class.  In between EOB events, the stream position is
   * not updated.
   *
   *
   * @throws IOException
   *             if the stream content is malformed or an I/O error occurs.
   *
   * @return int The return value greater than 0 are the bytes read.  A value
   * of -1 means end of stream while -2 represents end of block
   */


  @Override
  public @OsUntrusted int read(@OsUntrusted CBZip2InputStream this, final @OsUntrusted byte @OsUntrusted [] dest, final @OsUntrusted int offs, final @OsUntrusted int len)
      throws IOException {
    if (offs < 0) {
      throw new @OsUntrusted IndexOutOfBoundsException("offs(" + offs + ") < 0.");
    }
    if (len < 0) {
      throw new @OsUntrusted IndexOutOfBoundsException("len(" + len + ") < 0.");
    }
    if (offs + len > dest.length) {
      throw new @OsUntrusted IndexOutOfBoundsException("offs(" + offs + ") + len("
          + len + ") > dest.length(" + dest.length + ").");
    }
    if (this.in == null) {
      throw new @OsUntrusted IOException("stream closed");
    }

    if(lazyInitialization){
      this.init();
      this.lazyInitialization = false;
    }

    if(skipDecompression){
      changeStateToProcessABlock();
      CBZip2InputStream.skipDecompression = false;
    }

    final @OsUntrusted int hi = offs + len;
    @OsUntrusted
    int destOffs = offs;
    @OsUntrusted
    int b = 0;



    for (; ((destOffs < hi) && ((b = read0())) >= 0);) {
      dest[destOffs++] = (@OsUntrusted byte) b;

    }

    @OsUntrusted
    int result = destOffs - offs;
    if (result == 0) {
      //report 'end of block' or 'end of stream'
      result = b;

      skipResult = this.skipToNextMarker(CBZip2InputStream.BLOCK_DELIMITER, DELIMITER_BIT_LENGTH);
      //Exactly when we are about to start a new block, we advertise the stream position.
      this.reportedBytesReadFromCompressedStream = this.bytesReadFromCompressedStream;

      changeStateToProcessABlock();
    }
    return result;
  }

  private @OsUntrusted int read0(@OsUntrusted CBZip2InputStream this) throws IOException {
    final @OsUntrusted int retChar = this.currentChar;

    switch (this.currentState) {
    case EOF:
      return END_OF_STREAM;// return -1

    case NO_PROCESS_STATE:
      return END_OF_BLOCK;// return -2

    case START_BLOCK_STATE:
      throw new @OsUntrusted IllegalStateException();

    case RAND_PART_A_STATE:
      throw new @OsUntrusted IllegalStateException();

    case RAND_PART_B_STATE:
      setupRandPartB();
      break;

    case RAND_PART_C_STATE:
      setupRandPartC();
      break;

    case NO_RAND_PART_A_STATE:
      throw new @OsUntrusted IllegalStateException();

    case NO_RAND_PART_B_STATE:
      setupNoRandPartB();
      break;

    case NO_RAND_PART_C_STATE:
      setupNoRandPartC();
      break;

    default:
      throw new @OsUntrusted IllegalStateException();
    }

    return retChar;
  }

  private void init(@OsUntrusted CBZip2InputStream this) throws IOException {
    @OsUntrusted
    int magic2 = this.readAByte(in);
    if (magic2 != 'h') {
      throw new @OsUntrusted IOException("Stream is not BZip2 formatted: expected 'h'"
          + " as first byte but got '" + (@OsUntrusted char) magic2 + "'");
    }

    @OsUntrusted
    int blockSize = this.readAByte(in);
    if ((blockSize < '1') || (blockSize > '9')) {
      throw new @OsUntrusted IOException("Stream is not BZip2 formatted: illegal "
          + "blocksize " + (@OsUntrusted char) blockSize);
    }

    this.blockSize100k = blockSize - '0';

    initBlock();
    setupBlock();
  }

  private void initBlock(@OsUntrusted CBZip2InputStream this) throws IOException {
    if (this.readMode == READ_MODE.BYBLOCK) {
      // this.checkBlockIntegrity();
      this.storedBlockCRC = bsGetInt();
      this.blockRandomised = bsR(1) == 1;

      /**
      * Allocate data here instead in constructor, so we do not allocate
      * it if the input file is empty.
      */
      if (this.data == null) {
        this.data = new @OsUntrusted Data(this.blockSize100k);
      }

      // currBlockNo++;
      getAndMoveToFrontDecode();

      this.crc.initialiseCRC();
      this.currentState = STATE.START_BLOCK_STATE;
      return;
    }

    @OsUntrusted
    char magic0 = bsGetUByte();
    @OsUntrusted
    char magic1 = bsGetUByte();
    @OsUntrusted
    char magic2 = bsGetUByte();
    @OsUntrusted
    char magic3 = bsGetUByte();
    @OsUntrusted
    char magic4 = bsGetUByte();
    @OsUntrusted
    char magic5 = bsGetUByte();

    if (magic0 == 0x17 && magic1 == 0x72 && magic2 == 0x45
        && magic3 == 0x38 && magic4 == 0x50 && magic5 == 0x90) {
      complete(); // end of file
    } else if (magic0 != 0x31 || // '1'
        magic1 != 0x41 || // ')'
        magic2 != 0x59 || // 'Y'
        magic3 != 0x26 || // '&'
        magic4 != 0x53 || // 'S'
        magic5 != 0x59 // 'Y'
    ) {
      this.currentState = STATE.EOF;
      throw new @OsUntrusted IOException("bad block header");
    } else {
      this.storedBlockCRC = bsGetInt();
      this.blockRandomised = bsR(1) == 1;

      /**
      * Allocate data here instead in constructor, so we do not allocate
      * it if the input file is empty.
      */
      if (this.data == null) {
        this.data = new @OsUntrusted Data(this.blockSize100k);
      }

      // currBlockNo++;
      getAndMoveToFrontDecode();

      this.crc.initialiseCRC();
      this.currentState = STATE.START_BLOCK_STATE;
    }
  }

  private void endBlock(@OsUntrusted CBZip2InputStream this) throws IOException {
    this.computedBlockCRC = this.crc.getFinalCRC();

    // A bad CRC is considered a fatal error.
    if (this.storedBlockCRC != this.computedBlockCRC) {
      // make next blocks readable without error
      // (repair feature, not yet documented, not tested)
      this.computedCombinedCRC = (this.storedCombinedCRC << 1)
          | (this.storedCombinedCRC >>> 31);
      this.computedCombinedCRC ^= this.storedBlockCRC;

      reportCRCError();
    }

    this.computedCombinedCRC = (this.computedCombinedCRC << 1)
        | (this.computedCombinedCRC >>> 31);
    this.computedCombinedCRC ^= this.computedBlockCRC;
  }

  private void complete(@OsUntrusted CBZip2InputStream this) throws IOException {
    this.storedCombinedCRC = bsGetInt();
    this.currentState = STATE.EOF;
    this.data = null;

    if (this.storedCombinedCRC != this.computedCombinedCRC) {
      reportCRCError();
    }
  }

  @Override
  public void close(@OsUntrusted CBZip2InputStream this) throws IOException {
    @OsUntrusted
    InputStream inShadow = this.in;
    if (inShadow != null) {
      try {
        if (inShadow != System.in) {
          inShadow.close();
        }
      } finally {
        this.data = null;
        this.in = null;
      }
    }
  }

  private @OsUntrusted long bsR(@OsUntrusted CBZip2InputStream this, final @OsUntrusted long n) throws IOException {
    @OsUntrusted
    long bsLiveShadow = this.bsLive;
    @OsUntrusted
    long bsBuffShadow = this.bsBuff;

    if (bsLiveShadow < n) {
      final @OsUntrusted InputStream inShadow = this.in;
      do {
        @OsUntrusted
        int thech = readAByte(inShadow);

        if (thech < 0) {
          throw new @OsUntrusted IOException("unexpected end of stream");
        }

        bsBuffShadow = (bsBuffShadow << 8) | thech;
        bsLiveShadow += 8;
      } while (bsLiveShadow < n);

      this.bsBuff = bsBuffShadow;
    }

    this.bsLive = bsLiveShadow - n;
    return (bsBuffShadow >> (bsLiveShadow - n)) & ((1L << n) - 1);
  }

  private @OsUntrusted boolean bsGetBit(@OsUntrusted CBZip2InputStream this) throws IOException {
    @OsUntrusted
    long bsLiveShadow = this.bsLive;
    @OsUntrusted
    long bsBuffShadow = this.bsBuff;

    if (bsLiveShadow < 1) {
      @OsUntrusted
      int thech = this.readAByte(in);

      if (thech < 0) {
        throw new @OsUntrusted IOException("unexpected end of stream");
      }

      bsBuffShadow = (bsBuffShadow << 8) | thech;
      bsLiveShadow += 8;
      this.bsBuff = bsBuffShadow;
    }

    this.bsLive = bsLiveShadow - 1;
    return ((bsBuffShadow >> (bsLiveShadow - 1)) & 1) != 0;
  }

  private @OsUntrusted char bsGetUByte(@OsUntrusted CBZip2InputStream this) throws IOException {
    return (@OsUntrusted char) bsR(8);
  }

  private @OsUntrusted int bsGetInt(@OsUntrusted CBZip2InputStream this) throws IOException {
    return (@OsUntrusted int) ((((((bsR(8) << 8) | bsR(8)) << 8) | bsR(8)) << 8) | bsR(8));
  }

  /**
  * Called by createHuffmanDecodingTables() exclusively.
  */
  private static void hbCreateDecodeTables(final @OsUntrusted int @OsUntrusted [] limit,
      final @OsUntrusted int @OsUntrusted [] base, final @OsUntrusted int @OsUntrusted [] perm, final @OsUntrusted char @OsUntrusted [] length,
      final @OsUntrusted int minLen, final @OsUntrusted int maxLen, final @OsUntrusted int alphaSize) {
    for (@OsUntrusted int i = minLen, pp = 0; i <= maxLen; i++) {
      for (@OsUntrusted int j = 0; j < alphaSize; j++) {
        if (length[j] == i) {
          perm[pp++] = j;
        }
      }
    }

    for (@OsUntrusted int i = MAX_CODE_LEN; --i > 0;) {
      base[i] = 0;
      limit[i] = 0;
    }

    for (@OsUntrusted int i = 0; i < alphaSize; i++) {
      base[length[i] + 1]++;
    }

    for (@OsUntrusted int i = 1, b = base[0]; i < MAX_CODE_LEN; i++) {
      b += base[i];
      base[i] = b;
    }

    for (@OsUntrusted int i = minLen, vec = 0, b = base[i]; i <= maxLen; i++) {
      final @OsUntrusted int nb = base[i + 1];
      vec += nb - b;
      b = nb;
      limit[i] = vec - 1;
      vec <<= 1;
    }

    for (@OsUntrusted int i = minLen + 1; i <= maxLen; i++) {
      base[i] = ((limit[i - 1] + 1) << 1) - base[i];
    }
  }

  private void recvDecodingTables(@OsUntrusted CBZip2InputStream this) throws IOException {
    final @OsUntrusted Data dataShadow = this.data;
    final @OsUntrusted boolean @OsUntrusted [] inUse = dataShadow.inUse;
    final @OsUntrusted byte @OsUntrusted [] pos = dataShadow.recvDecodingTables_pos;
    final @OsUntrusted byte @OsUntrusted [] selector = dataShadow.selector;
    final @OsUntrusted byte @OsUntrusted [] selectorMtf = dataShadow.selectorMtf;

    @OsUntrusted
    int inUse16 = 0;

    /* Receive the mapping table */
    for (@OsUntrusted int i = 0; i < 16; i++) {
      if (bsGetBit()) {
        inUse16 |= 1 << i;
      }
    }

    for (@OsUntrusted int i = 256; --i >= 0;) {
      inUse[i] = false;
    }

    for (@OsUntrusted int i = 0; i < 16; i++) {
      if ((inUse16 & (1 << i)) != 0) {
        final @OsUntrusted int i16 = i << 4;
        for (@OsUntrusted int j = 0; j < 16; j++) {
          if (bsGetBit()) {
            inUse[i16 + j] = true;
          }
        }
      }
    }

    makeMaps();
    final @OsUntrusted int alphaSize = this.nInUse + 2;

    /* Now the selectors */
    final @OsUntrusted int nGroups = (@OsUntrusted int) bsR(3);
    final @OsUntrusted int nSelectors = (@OsUntrusted int) bsR(15);

    for (@OsUntrusted int i = 0; i < nSelectors; i++) {
      @OsUntrusted
      int j = 0;
      while (bsGetBit()) {
        j++;
      }
      selectorMtf[i] = (@OsUntrusted byte) j;
    }

    /* Undo the MTF values for the selectors. */
    for (@OsUntrusted int v = nGroups; --v >= 0;) {
      pos[v] = (@OsUntrusted byte) v;
    }

    for (@OsUntrusted int i = 0; i < nSelectors; i++) {
      @OsUntrusted
      int v = selectorMtf[i] & 0xff;
      final @OsUntrusted byte tmp = pos[v];
      while (v > 0) {
        // nearly all times v is zero, 4 in most other cases
        pos[v] = pos[v - 1];
        v--;
      }
      pos[0] = tmp;
      selector[i] = tmp;
    }

    final @OsUntrusted char @OsUntrusted [] @OsUntrusted [] len = dataShadow.temp_charArray2d;

    /* Now the coding tables */
    for (@OsUntrusted int t = 0; t < nGroups; t++) {
      @OsUntrusted
      int curr = (@OsUntrusted int) bsR(5);
      final @OsUntrusted char @OsUntrusted [] len_t = len[t];
      for (@OsUntrusted int i = 0; i < alphaSize; i++) {
        while (bsGetBit()) {
          curr += bsGetBit() ? -1 : 1;
        }
        len_t[i] = (@OsUntrusted char) curr;
      }
    }

    // finally create the Huffman tables
    createHuffmanDecodingTables(alphaSize, nGroups);
  }

  /**
  * Called by recvDecodingTables() exclusively.
  */
  private void createHuffmanDecodingTables(@OsUntrusted CBZip2InputStream this, final @OsUntrusted int alphaSize,
      final @OsUntrusted int nGroups) {
    final @OsUntrusted Data dataShadow = this.data;
    final @OsUntrusted char @OsUntrusted [] @OsUntrusted [] len = dataShadow.temp_charArray2d;
    final @OsUntrusted int @OsUntrusted [] minLens = dataShadow.minLens;
    final @OsUntrusted int @OsUntrusted [] @OsUntrusted [] limit = dataShadow.limit;
    final @OsUntrusted int @OsUntrusted [] @OsUntrusted [] base = dataShadow.base;
    final @OsUntrusted int @OsUntrusted [] @OsUntrusted [] perm = dataShadow.perm;

    for (@OsUntrusted int t = 0; t < nGroups; t++) {
      @OsUntrusted
      int minLen = 32;
      @OsUntrusted
      int maxLen = 0;
      final @OsUntrusted char @OsUntrusted [] len_t = len[t];
      for (@OsUntrusted int i = alphaSize; --i >= 0;) {
        final @OsUntrusted char lent = len_t[i];
        if (lent > maxLen) {
          maxLen = lent;
        }
        if (lent < minLen) {
          minLen = lent;
        }
      }
      hbCreateDecodeTables(limit[t], base[t], perm[t], len[t], minLen,
          maxLen, alphaSize);
      minLens[t] = minLen;
    }
  }

  private void getAndMoveToFrontDecode(@OsUntrusted CBZip2InputStream this) throws IOException {
    this.origPtr = (@OsUntrusted int) bsR(24);
    recvDecodingTables();

    final @OsUntrusted InputStream inShadow = this.in;
    final @OsUntrusted Data dataShadow = this.data;
    final @OsUntrusted byte @OsUntrusted [] ll8 = dataShadow.ll8;
    final @OsUntrusted int @OsUntrusted [] unzftab = dataShadow.unzftab;
    final @OsUntrusted byte @OsUntrusted [] selector = dataShadow.selector;
    final @OsUntrusted byte @OsUntrusted [] seqToUnseq = dataShadow.seqToUnseq;
    final @OsUntrusted char @OsUntrusted [] yy = dataShadow.getAndMoveToFrontDecode_yy;
    final @OsUntrusted int @OsUntrusted [] minLens = dataShadow.minLens;
    final @OsUntrusted int @OsUntrusted [] @OsUntrusted [] limit = dataShadow.limit;
    final @OsUntrusted int @OsUntrusted [] @OsUntrusted [] base = dataShadow.base;
    final @OsUntrusted int @OsUntrusted [] @OsUntrusted [] perm = dataShadow.perm;
    final @OsUntrusted int limitLast = this.blockSize100k * 100000;

    /*
    * Setting up the unzftab entries here is not strictly necessary, but it
    * does save having to do it later in a separate pass, and so saves a
    * block's worth of cache misses.
    */
    for (@OsUntrusted int i = 256; --i >= 0;) {
      yy[i] = (@OsUntrusted char) i;
      unzftab[i] = 0;
    }

    @OsUntrusted
    int groupNo = 0;
    @OsUntrusted
    int groupPos = G_SIZE - 1;
    final @OsUntrusted int eob = this.nInUse + 1;
    @OsUntrusted
    int nextSym = getAndMoveToFrontDecode0(0);
    @OsUntrusted
    int bsBuffShadow = (@OsUntrusted int) this.bsBuff;
    @OsUntrusted
    int bsLiveShadow = (@OsUntrusted int) this.bsLive;
    @OsUntrusted
    int lastShadow = -1;
    @OsUntrusted
    int zt = selector[groupNo] & 0xff;
    @OsUntrusted
    int @OsUntrusted [] base_zt = base[zt];
    @OsUntrusted
    int @OsUntrusted [] limit_zt = limit[zt];
    @OsUntrusted
    int @OsUntrusted [] perm_zt = perm[zt];
    @OsUntrusted
    int minLens_zt = minLens[zt];

    while (nextSym != eob) {
      if ((nextSym == RUNA) || (nextSym == RUNB)) {
        @OsUntrusted
        int s = -1;

        for (@OsUntrusted int n = 1; true; n <<= 1) {
          if (nextSym == RUNA) {
            s += n;
          } else if (nextSym == RUNB) {
            s += n << 1;
          } else {
            break;
          }

          if (groupPos == 0) {
            groupPos = G_SIZE - 1;
            zt = selector[++groupNo] & 0xff;
            base_zt = base[zt];
            limit_zt = limit[zt];
            perm_zt = perm[zt];
            minLens_zt = minLens[zt];
          } else {
            groupPos--;
          }

          @OsUntrusted
          int zn = minLens_zt;

          while (bsLiveShadow < zn) {
            final @OsUntrusted int thech = readAByte(inShadow);
            if (thech >= 0) {
              bsBuffShadow = (bsBuffShadow << 8) | thech;
              bsLiveShadow += 8;
              continue;
            } else {
              throw new @OsUntrusted IOException("unexpected end of stream");
            }
          }
          @OsUntrusted
          long zvec = (bsBuffShadow >> (bsLiveShadow - zn))
              & ((1 << zn) - 1);
          bsLiveShadow -= zn;

          while (zvec > limit_zt[zn]) {
            zn++;
            while (bsLiveShadow < 1) {
              final @OsUntrusted int thech = readAByte(inShadow);
              if (thech >= 0) {
                bsBuffShadow = (bsBuffShadow << 8) | thech;
                bsLiveShadow += 8;
                continue;
              } else {
                throw new @OsUntrusted IOException(
                    "unexpected end of stream");
              }
            }
            bsLiveShadow--;
            zvec = (zvec << 1)
                | ((bsBuffShadow >> bsLiveShadow) & 1);
          }
          nextSym = perm_zt[(@OsUntrusted int) (zvec - base_zt[zn])];
        }

        final @OsUntrusted byte ch = seqToUnseq[yy[0]];
        unzftab[ch & 0xff] += s + 1;

        while (s-- >= 0) {
          ll8[++lastShadow] = ch;
        }

        if (lastShadow >= limitLast) {
          throw new @OsUntrusted IOException("block overrun");
        }
      } else {
        if (++lastShadow >= limitLast) {
          throw new @OsUntrusted IOException("block overrun");
        }

        final @OsUntrusted char tmp = yy[nextSym - 1];
        unzftab[seqToUnseq[tmp] & 0xff]++;
        ll8[lastShadow] = seqToUnseq[tmp];

        /*
        * This loop is hammered during decompression, hence avoid
        * native method call overhead of System.arraycopy for very
        * small ranges to copy.
        */
        if (nextSym <= 16) {
          for (@OsUntrusted int j = nextSym - 1; j > 0;) {
            yy[j] = yy[--j];
          }
        } else {
          System.arraycopy(yy, 0, yy, 1, nextSym - 1);
        }

        yy[0] = tmp;

        if (groupPos == 0) {
          groupPos = G_SIZE - 1;
          zt = selector[++groupNo] & 0xff;
          base_zt = base[zt];
          limit_zt = limit[zt];
          perm_zt = perm[zt];
          minLens_zt = minLens[zt];
        } else {
          groupPos--;
        }

        @OsUntrusted
        int zn = minLens_zt;

        while (bsLiveShadow < zn) {
          final @OsUntrusted int thech = readAByte(inShadow);
          if (thech >= 0) {
            bsBuffShadow = (bsBuffShadow << 8) | thech;
            bsLiveShadow += 8;
            continue;
          } else {
            throw new @OsUntrusted IOException("unexpected end of stream");
          }
        }
        @OsUntrusted
        int zvec = (bsBuffShadow >> (bsLiveShadow - zn))
            & ((1 << zn) - 1);
        bsLiveShadow -= zn;

        while (zvec > limit_zt[zn]) {
          zn++;
          while (bsLiveShadow < 1) {
            final @OsUntrusted int thech = readAByte(inShadow);
            if (thech >= 0) {
              bsBuffShadow = (bsBuffShadow << 8) | thech;
              bsLiveShadow += 8;
              continue;
            } else {
              throw new @OsUntrusted IOException("unexpected end of stream");
            }
          }
          bsLiveShadow--;
          zvec = ((zvec << 1) | ((bsBuffShadow >> bsLiveShadow) & 1));
        }
        nextSym = perm_zt[zvec - base_zt[zn]];
      }
    }

    this.last = lastShadow;
    this.bsLive = bsLiveShadow;
    this.bsBuff = bsBuffShadow;
  }

  private @OsUntrusted int getAndMoveToFrontDecode0(@OsUntrusted CBZip2InputStream this, final @OsUntrusted int groupNo) throws IOException {
    final @OsUntrusted InputStream inShadow = this.in;
    final @OsUntrusted Data dataShadow = this.data;
    final @OsUntrusted int zt = dataShadow.selector[groupNo] & 0xff;
    final @OsUntrusted int @OsUntrusted [] limit_zt = dataShadow.limit[zt];
    @OsUntrusted
    int zn = dataShadow.minLens[zt];
    @OsUntrusted
    int zvec = (@OsUntrusted int) bsR(zn);
    @OsUntrusted
    int bsLiveShadow = (@OsUntrusted int) this.bsLive;
    @OsUntrusted
    int bsBuffShadow = (@OsUntrusted int) this.bsBuff;

    while (zvec > limit_zt[zn]) {
      zn++;
      while (bsLiveShadow < 1) {
        final @OsUntrusted int thech = readAByte(inShadow);

        if (thech >= 0) {
          bsBuffShadow = (bsBuffShadow << 8) | thech;
          bsLiveShadow += 8;
          continue;
        } else {
          throw new @OsUntrusted IOException("unexpected end of stream");
        }
      }
      bsLiveShadow--;
      zvec = (zvec << 1) | ((bsBuffShadow >> bsLiveShadow) & 1);
    }

    this.bsLive = bsLiveShadow;
    this.bsBuff = bsBuffShadow;

    return dataShadow.perm[zt][zvec - dataShadow.base[zt][zn]];
  }

  private void setupBlock(@OsUntrusted CBZip2InputStream this) throws IOException {
    if (this.data == null) {
      return;
    }

    final @OsUntrusted int @OsUntrusted [] cftab = this.data.cftab;
    final @OsUntrusted int @OsUntrusted [] tt = this.data.initTT(this.last + 1);
    final @OsUntrusted byte @OsUntrusted [] ll8 = this.data.ll8;
    cftab[0] = 0;
    System.arraycopy(this.data.unzftab, 0, cftab, 1, 256);

    for (@OsUntrusted int i = 1, c = cftab[0]; i <= 256; i++) {
      c += cftab[i];
      cftab[i] = c;
    }

    for (@OsUntrusted int i = 0, lastShadow = this.last; i <= lastShadow; i++) {
      tt[cftab[ll8[i] & 0xff]++] = i;
    }

    if ((this.origPtr < 0) || (this.origPtr >= tt.length)) {
      throw new @OsUntrusted IOException("stream corrupted");
    }

    this.su_tPos = tt[this.origPtr];
    this.su_count = 0;
    this.su_i2 = 0;
    this.su_ch2 = 256; /* not a char and not EOF */

    if (this.blockRandomised) {
      this.su_rNToGo = 0;
      this.su_rTPos = 0;
      setupRandPartA();
    } else {
      setupNoRandPartA();
    }
  }

  private void setupRandPartA(@OsUntrusted CBZip2InputStream this) throws IOException {
    if (this.su_i2 <= this.last) {
      this.su_chPrev = this.su_ch2;
      @OsUntrusted
      int su_ch2Shadow = this.data.ll8[this.su_tPos] & 0xff;
      this.su_tPos = this.data.tt[this.su_tPos];
      if (this.su_rNToGo == 0) {
        this.su_rNToGo = BZip2Constants.rNums[this.su_rTPos] - 1;
        if (++this.su_rTPos == 512) {
          this.su_rTPos = 0;
        }
      } else {
        this.su_rNToGo--;
      }
      this.su_ch2 = su_ch2Shadow ^= (this.su_rNToGo == 1) ? 1 : 0;
      this.su_i2++;
      this.currentChar = su_ch2Shadow;
      this.currentState = STATE.RAND_PART_B_STATE;
      this.crc.updateCRC(su_ch2Shadow);
    } else {
      endBlock();
      if (readMode == READ_MODE.CONTINUOUS) {
      initBlock();
      setupBlock();
      } else if (readMode == READ_MODE.BYBLOCK) {
        this.currentState = STATE.NO_PROCESS_STATE;
      }
    }
  }

  private void setupNoRandPartA(@OsUntrusted CBZip2InputStream this) throws IOException {
    if (this.su_i2 <= this.last) {
      this.su_chPrev = this.su_ch2;
      @OsUntrusted
      int su_ch2Shadow = this.data.ll8[this.su_tPos] & 0xff;
      this.su_ch2 = su_ch2Shadow;
      this.su_tPos = this.data.tt[this.su_tPos];
      this.su_i2++;
      this.currentChar = su_ch2Shadow;
      this.currentState = STATE.NO_RAND_PART_B_STATE;
      this.crc.updateCRC(su_ch2Shadow);
    } else {
      this.currentState = STATE.NO_RAND_PART_A_STATE;
      endBlock();
      if (readMode == READ_MODE.CONTINUOUS) {
      initBlock();
      setupBlock();
      } else if (readMode == READ_MODE.BYBLOCK) {
        this.currentState = STATE.NO_PROCESS_STATE;
      }
    }
  }

  private void setupRandPartB(@OsUntrusted CBZip2InputStream this) throws IOException {
    if (this.su_ch2 != this.su_chPrev) {
      this.currentState = STATE.RAND_PART_A_STATE;
      this.su_count = 1;
      setupRandPartA();
    } else if (++this.su_count >= 4) {
      this.su_z = (@OsUntrusted char) (this.data.ll8[this.su_tPos] & 0xff);
      this.su_tPos = this.data.tt[this.su_tPos];
      if (this.su_rNToGo == 0) {
        this.su_rNToGo = BZip2Constants.rNums[this.su_rTPos] - 1;
        if (++this.su_rTPos == 512) {
          this.su_rTPos = 0;
        }
      } else {
        this.su_rNToGo--;
      }
      this.su_j2 = 0;
      this.currentState = STATE.RAND_PART_C_STATE;
      if (this.su_rNToGo == 1) {
        this.su_z ^= 1;
      }
      setupRandPartC();
    } else {
      this.currentState = STATE.RAND_PART_A_STATE;
      setupRandPartA();
    }
  }

  private void setupRandPartC(@OsUntrusted CBZip2InputStream this) throws IOException {
    if (this.su_j2 < this.su_z) {
      this.currentChar = this.su_ch2;
      this.crc.updateCRC(this.su_ch2);
      this.su_j2++;
    } else {
      this.currentState = STATE.RAND_PART_A_STATE;
      this.su_i2++;
      this.su_count = 0;
      setupRandPartA();
    }
  }

  private void setupNoRandPartB(@OsUntrusted CBZip2InputStream this) throws IOException {
    if (this.su_ch2 != this.su_chPrev) {
      this.su_count = 1;
      setupNoRandPartA();
    } else if (++this.su_count >= 4) {
      this.su_z = (@OsUntrusted char) (this.data.ll8[this.su_tPos] & 0xff);
      this.su_tPos = this.data.tt[this.su_tPos];
      this.su_j2 = 0;
      setupNoRandPartC();
    } else {
      setupNoRandPartA();
    }
  }

  private void setupNoRandPartC(@OsUntrusted CBZip2InputStream this) throws IOException {
    if (this.su_j2 < this.su_z) {
      @OsUntrusted
      int su_ch2Shadow = this.su_ch2;
      this.currentChar = su_ch2Shadow;
      this.crc.updateCRC(su_ch2Shadow);
      this.su_j2++;
      this.currentState = STATE.NO_RAND_PART_C_STATE;
    } else {
      this.su_i2++;
      this.su_count = 0;
      setupNoRandPartA();
    }
  }

  private static final class Data extends @OsUntrusted Object {

    // (with blockSize 900k)
    final @OsUntrusted boolean @OsUntrusted [] inUse = new @OsUntrusted boolean @OsUntrusted [256]; // 256 byte

    final @OsUntrusted byte @OsUntrusted [] seqToUnseq = new @OsUntrusted byte @OsUntrusted [256]; // 256 byte
    final @OsUntrusted byte @OsUntrusted [] selector = new @OsUntrusted byte @OsUntrusted [MAX_SELECTORS]; // 18002 byte
    final @OsUntrusted byte @OsUntrusted [] selectorMtf = new @OsUntrusted byte @OsUntrusted [MAX_SELECTORS]; // 18002 byte

    /**
    * Freq table collected to save a pass over the data during
    * decompression.
    */
    final @OsUntrusted int @OsUntrusted [] unzftab = new @OsUntrusted int @OsUntrusted [256]; // 1024 byte

    final @OsUntrusted int @OsUntrusted [] @OsUntrusted [] limit = new int @OsUntrusted [N_GROUPS] @OsUntrusted [MAX_ALPHA_SIZE]; // 6192 byte
    final @OsUntrusted int @OsUntrusted [] @OsUntrusted [] base = new int @OsUntrusted [N_GROUPS] @OsUntrusted [MAX_ALPHA_SIZE]; // 6192 byte
    final @OsUntrusted int @OsUntrusted [] @OsUntrusted [] perm = new int @OsUntrusted [N_GROUPS] @OsUntrusted [MAX_ALPHA_SIZE]; // 6192 byte
    final @OsUntrusted int @OsUntrusted [] minLens = new @OsUntrusted int @OsUntrusted [N_GROUPS]; // 24 byte

    final @OsUntrusted int @OsUntrusted [] cftab = new @OsUntrusted int @OsUntrusted [257]; // 1028 byte
    final @OsUntrusted char @OsUntrusted [] getAndMoveToFrontDecode_yy = new @OsUntrusted char @OsUntrusted [256]; // 512 byte
    final @OsUntrusted char @OsUntrusted [] @OsUntrusted [] temp_charArray2d = new char @OsUntrusted [N_GROUPS] @OsUntrusted [MAX_ALPHA_SIZE]; // 3096
                                        // byte
    final @OsUntrusted byte @OsUntrusted [] recvDecodingTables_pos = new @OsUntrusted byte @OsUntrusted [N_GROUPS]; // 6 byte
    // ---------------
    // 60798 byte

    @OsUntrusted
    int @OsUntrusted [] tt; // 3600000 byte
    @OsUntrusted
    byte @OsUntrusted [] ll8; // 900000 byte

    // ---------------
    // 4560782 byte
    // ===============

    @OsUntrusted
    Data(@OsUntrusted int blockSize100k) {
      super();

      this.ll8 = new @OsUntrusted byte @OsUntrusted [blockSize100k * BZip2Constants.baseBlockSize];
    }

    /**
    * Initializes the {@link #tt} array.
    *
    * This method is called when the required length of the array is known.
    * I don't initialize it at construction time to avoid unneccessary
    * memory allocation when compressing small files.
    */
    final @OsUntrusted int @OsUntrusted [] initTT(CBZip2InputStream.@OsUntrusted Data this, @OsUntrusted int length) {
      @OsUntrusted
      int @OsUntrusted [] ttShadow = this.tt;

      // tt.length should always be >= length, but theoretically
      // it can happen, if the compressor mixed small and large
      // blocks. Normally only the last block will be smaller
      // than others.
      if ((ttShadow == null) || (ttShadow.length < length)) {
        this.tt = ttShadow = new @OsUntrusted int @OsUntrusted [length];
      }

      return ttShadow;
    }

  }
}
