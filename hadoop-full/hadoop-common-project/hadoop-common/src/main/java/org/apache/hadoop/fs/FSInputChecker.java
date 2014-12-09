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
package org.apache.hadoop.fs;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 * This is a generic input stream for verifying checksums for
 * data before it is read by a user.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
abstract public class FSInputChecker extends @OsUntrusted FSInputStream {
  public static final @OsUntrusted Log LOG 
  = LogFactory.getLog(FSInputChecker.class);
  
  /** The file name from which data is read from */
  protected @OsUntrusted Path file;
  private @OsUntrusted Checksum sum;
  private @OsUntrusted boolean verifyChecksum = true;
  private @OsUntrusted int maxChunkSize; // data bytes for checksum (eg 512)
  private @OsUntrusted byte @OsUntrusted [] buf; // buffer for non-chunk-aligned reading
  private @OsUntrusted byte @OsUntrusted [] checksum;
  private @OsUntrusted IntBuffer checksumInts; // wrapper on checksum buffer
  private @OsUntrusted int pos; // the position of the reader inside buf
  private @OsUntrusted int count; // the number of bytes currently in buf
  
  private @OsUntrusted int numOfRetries;
  
  // cached file position
  // this should always be a multiple of maxChunkSize
  private @OsUntrusted long chunkPos = 0;

  // Number of checksum chunks that can be read at once into a user
  // buffer. Chosen by benchmarks - higher values do not reduce
  // CPU usage. The size of the data reads made to the underlying stream
  // will be CHUNKS_PER_READ * maxChunkSize.
  private static final @OsUntrusted int CHUNKS_PER_READ = 32;
  protected static final @OsUntrusted int CHECKSUM_SIZE = 4; // 32-bit checksum

  /** Constructor
   * 
   * @param file The name of the file to be read
   * @param numOfRetries Number of read retries when ChecksumError occurs
   */
  protected @OsUntrusted FSInputChecker( @OsUntrusted Path file, @OsUntrusted int numOfRetries) {
    this.file = file;
    this.numOfRetries = numOfRetries;
  }
  
  /** Constructor
   * 
   * @param file The name of the file to be read
   * @param numOfRetries Number of read retries when ChecksumError occurs
   * @param sum the type of Checksum engine
   * @param chunkSize maximun chunk size
   * @param checksumSize the number byte of each checksum
   */
  protected @OsUntrusted FSInputChecker( @OsUntrusted Path file, @OsUntrusted int numOfRetries,
      @OsUntrusted
      boolean verifyChecksum, @OsUntrusted Checksum sum, @OsUntrusted int chunkSize, @OsUntrusted int checksumSize ) {
    this(file, numOfRetries);
    set(verifyChecksum, sum, chunkSize, checksumSize);
  }
  
  /**
   * Reads in checksum chunks into <code>buf</code> at <code>offset</code>
   * and checksum into <code>checksum</code>.
   * Since checksums can be disabled, there are two cases implementors need
   * to worry about:
   *
   *  (a) needChecksum() will return false:
   *     - len can be any positive value
   *     - checksum will be null
   *     Implementors should simply pass through to the underlying data stream.
   * or
   *  (b) needChecksum() will return true:
   *    - len >= maxChunkSize
   *    - checksum.length is a multiple of CHECKSUM_SIZE
   *    Implementors should read an integer number of data chunks into
   *    buf. The amount read should be bounded by len or by 
   *    checksum.length / CHECKSUM_SIZE * maxChunkSize. Note that len may
   *    be a value that is not a multiple of maxChunkSize, in which case
   *    the implementation may return less than len.
   *
   * The method is used for implementing read, therefore, it should be optimized
   * for sequential reading.
   *
   * @param pos chunkPos
   * @param buf desitination buffer
   * @param offset offset in buf at which to store data
   * @param len maximum number of bytes to read
   * @param checksum the data buffer into which to write checksums
   * @return number of bytes read
   */
  abstract protected @OsUntrusted int readChunk(@OsUntrusted FSInputChecker this, @OsUntrusted long pos, @OsUntrusted byte @OsUntrusted [] buf, @OsUntrusted int offset, @OsUntrusted int len,
      @OsUntrusted
      byte @OsUntrusted [] checksum) throws IOException;

  /** Return position of beginning of chunk containing pos. 
   *
   * @param pos a postion in the file
   * @return the starting position of the chunk which contains the byte
   */
  abstract protected @OsUntrusted long getChunkPosition(@OsUntrusted FSInputChecker this, @OsUntrusted long pos);

  /** Return true if there is a need for checksum verification */
  protected synchronized @OsUntrusted boolean needChecksum(@OsUntrusted FSInputChecker this) {
    return verifyChecksum && sum != null;
  }

  /**
   * Read one checksum-verified byte
   * 
   * @return     the next byte of data, or <code>-1</code> if the end of the
   *             stream is reached.
   * @exception  IOException  if an I/O error occurs.
   */

  @Override
  public synchronized @OsUntrusted int read(@OsUntrusted FSInputChecker this) throws IOException {
    if (pos >= count) {
      fill();
      if (pos >= count) {
        return -1;
      }
    }
    return buf[pos++] & 0xff;
  }
  
  /**
   * Read checksum verified bytes from this byte-input stream into 
   * the specified byte array, starting at the given offset.
   *
   * <p> This method implements the general contract of the corresponding
   * <code>{@link InputStream#read(byte[], int, int) read}</code> method of
   * the <code>{@link InputStream}</code> class.  As an additional
   * convenience, it attempts to read as many bytes as possible by repeatedly
   * invoking the <code>read</code> method of the underlying stream.  This
   * iterated <code>read</code> continues until one of the following
   * conditions becomes true: <ul>
   *
   *   <li> The specified number of bytes have been read,
   *
   *   <li> The <code>read</code> method of the underlying stream returns
   *   <code>-1</code>, indicating end-of-file.
   *
   * </ul> If the first <code>read</code> on the underlying stream returns
   * <code>-1</code> to indicate end-of-file then this method returns
   * <code>-1</code>.  Otherwise this method returns the number of bytes
   * actually read.
   *
   * @param      b     destination buffer.
   * @param      off   offset at which to start storing bytes.
   * @param      len   maximum number of bytes to read.
   * @return     the number of bytes read, or <code>-1</code> if the end of
   *             the stream has been reached.
   * @exception  IOException  if an I/O error occurs.
   *             ChecksumException if any checksum error occurs
   */
  @Override
  public synchronized @OsUntrusted int read(@OsUntrusted FSInputChecker this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) throws IOException {
    // parameter check
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new @OsUntrusted IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    @OsUntrusted
    int n = 0;
    for (;;) {
      @OsUntrusted
      int nread = read1(b, off + n, len - n);
      if (nread <= 0) 
        return (n == 0) ? nread : n;
      n += nread;
      if (n >= len)
        return n;
    }
  }
  
  /**
   * Fills the buffer with a chunk data. 
   * No mark is supported.
   * This method assumes that all data in the buffer has already been read in,
   * hence pos > count.
   */
  private void fill(@OsUntrusted FSInputChecker this  ) throws IOException {
    assert(pos>=count);
    // fill internal buffer
    count = readChecksumChunk(buf, 0, maxChunkSize);
    if (count < 0) count = 0;
  }
  
  /*
   * Read characters into a portion of an array, reading from the underlying
   * stream at most once if necessary.
   */
  private @OsUntrusted int read1(@OsUntrusted FSInputChecker this, @OsUntrusted byte b @OsUntrusted [], @OsUntrusted int off, @OsUntrusted int len)
  throws IOException {
    @OsUntrusted
    int avail = count-pos;
    if( avail <= 0 ) {
      if(len >= maxChunkSize) {
        // read a chunk to user buffer directly; avoid one copy
        @OsUntrusted
        int nread = readChecksumChunk(b, off, len);
        return nread;
      } else {
        // read a chunk into the local buffer
         fill();
        if( count <= 0 ) {
          return -1;
        } else {
          avail = count;
        }
      }
    }
    
    // copy content of the local buffer to the user buffer
    @OsUntrusted
    int cnt = (avail < len) ? avail : len;
    System.arraycopy(buf, pos, b, off, cnt);
    pos += cnt;
    return cnt;    
  }
  
  /* Read up one or more checksum chunk to array <i>b</i> at pos <i>off</i>
   * It requires at least one checksum chunk boundary
   * in between <cur_pos, cur_pos+len> 
   * and it stops reading at the last boundary or at the end of the stream;
   * Otherwise an IllegalArgumentException is thrown.
   * This makes sure that all data read are checksum verified.
   * 
   * @param b   the buffer into which the data is read.
   * @param off the start offset in array <code>b</code>
   *            at which the data is written.
   * @param len the maximum number of bytes to read.
   * @return    the total number of bytes read into the buffer, or
   *            <code>-1</code> if there is no more data because the end of
   *            the stream has been reached.
   * @throws IOException if an I/O error occurs.
   */ 
  private @OsUntrusted int readChecksumChunk(@OsUntrusted FSInputChecker this, @OsUntrusted byte b @OsUntrusted [], final @OsUntrusted int off, final @OsUntrusted int len)
  throws IOException {
    // invalidate buffer
    count = pos = 0;
          
    @OsUntrusted
    int read = 0;
    @OsUntrusted
    boolean retry = true;
    @OsUntrusted
    int retriesLeft = numOfRetries; 
    do {
      retriesLeft--;

      try {
        read = readChunk(chunkPos, b, off, len, checksum);
        if( read > 0) {
          if( needChecksum() ) {
            verifySums(b, off, read);
          }
          chunkPos += read;
        }
        retry = false;
      } catch (@OsUntrusted ChecksumException ce) {
         LOG.info("Found checksum error: b[" + off + ", " + (off+read) + "]="
              + StringUtils.byteToHexString(b, off, off + read), ce);
          if (retriesLeft == 0) {
            throw ce;
          }
          
          // try a new replica
          if (seekToNewSource(chunkPos)) {
            // Since at least one of the sources is different, 
            // the read might succeed, so we'll retry.
            seek(chunkPos);
          } else {
            // Neither the data stream nor the checksum stream are being read
            // from different sources, meaning we'll still get a checksum error 
            // if we try to do the read again.  We throw an exception instead.
            throw ce;
          }
        }
    } while (retry);
    return read;
  }

  private void verifySums(@OsUntrusted FSInputChecker this, final @OsUntrusted byte b @OsUntrusted [], final @OsUntrusted int off, @OsUntrusted int read)
    throws ChecksumException
  {
    @OsUntrusted
    int leftToVerify = read;
    @OsUntrusted
    int verifyOff = 0;
    checksumInts.rewind();
    checksumInts.limit((read - 1)/maxChunkSize + 1);

    while (leftToVerify > 0) {
      sum.update(b, off + verifyOff, Math.min(leftToVerify, maxChunkSize));
      @OsUntrusted
      int expected = checksumInts.get();
      @OsUntrusted
      int calculated = (@OsUntrusted int)sum.getValue();
      sum.reset();

      if (expected != calculated) {
        @OsUntrusted
        long errPos = chunkPos + verifyOff;
        throw new @OsUntrusted ChecksumException(
         "Checksum error: "+file+" at "+ errPos +
          " exp: " + expected + " got: " + calculated, errPos );
      }
      leftToVerify -= maxChunkSize;
      verifyOff += maxChunkSize;
    }
  }

  /**
   * Convert a checksum byte array to a long
   * This is deprecated since 0.22 since it is no longer in use
   * by this class.
   */
  @Deprecated
  static public @OsUntrusted long checksum2long(@OsUntrusted byte @OsUntrusted [] checksum) {
    @OsUntrusted
    long crc = 0L;
    for(@OsUntrusted int i=0; i<checksum.length; i++) {
      crc |= (0xffL&(@OsUntrusted long)checksum[i])<<((checksum.length-i-1)*8);
    }
    return crc;
  }

  @Override
  public synchronized @OsUntrusted long getPos(@OsUntrusted FSInputChecker this) throws IOException {
    return chunkPos-Math.max(0L, count - pos);
  }

  @Override
  public synchronized @OsUntrusted int available(@OsUntrusted FSInputChecker this) throws IOException {
    return Math.max(0, count - pos);
  }
  
  /**
   * Skips over and discards <code>n</code> bytes of data from the
   * input stream.
   *
   * <p>This method may skip more bytes than are remaining in the backing
   * file. This produces no exception and the number of bytes skipped
   * may include some number of bytes that were beyond the EOF of the
   * backing file. Attempting to read from the stream after skipping past
   * the end will result in -1 indicating the end of the file.
   *
   *<p>If <code>n</code> is negative, no bytes are skipped.
   *
   * @param      n   the number of bytes to be skipped.
   * @return     the actual number of bytes skipped.
   * @exception  IOException  if an I/O error occurs.
   *             ChecksumException if the chunk to skip to is corrupted
   */
  @Override
  public synchronized @OsUntrusted long skip(@OsUntrusted FSInputChecker this, @OsUntrusted long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    seek(getPos()+n);
    return n;
  }

  /**
   * Seek to the given position in the stream.
   * The next read() will be from that position.
   * 
   * <p>This method may seek past the end of the file.
   * This produces no exception and an attempt to read from
   * the stream will result in -1 indicating the end of the file.
   *
   * @param      pos   the postion to seek to.
   * @exception  IOException  if an I/O error occurs.
   *             ChecksumException if the chunk to seek to is corrupted
   */

  @Override
  public synchronized void seek(@OsUntrusted FSInputChecker this, @OsUntrusted long pos) throws IOException {
    if( pos<0 ) {
      return;
    }
    // optimize: check if the pos is in the buffer
    @OsUntrusted
    long start = chunkPos - this.count;
    if( pos>=start && pos<chunkPos) {
      this.pos = (@OsUntrusted int)(pos-start);
      return;
    }
    
    // reset the current state
    resetState();
    
    // seek to a checksum boundary
    chunkPos = getChunkPosition(pos);
    
    // scan to the desired position
    @OsUntrusted
    int delta = (@OsUntrusted int)(pos - chunkPos);
    if( delta > 0) {
      readFully(this, new @OsUntrusted byte @OsUntrusted [delta], 0, delta);
    }
  }

  /**
   * A utility function that tries to read up to <code>len</code> bytes from
   * <code>stm</code>
   * 
   * @param stm    an input stream
   * @param buf    destiniation buffer
   * @param offset offset at which to store data
   * @param len    number of bytes to read
   * @return actual number of bytes read
   * @throws IOException if there is any IO error
   */
  protected static @OsUntrusted int readFully(@OsUntrusted InputStream stm, 
      @OsUntrusted
      byte @OsUntrusted [] buf, @OsUntrusted int offset, @OsUntrusted int len) throws IOException {
    @OsUntrusted
    int n = 0;
    for (;;) {
      @OsUntrusted
      int nread = stm.read(buf, offset + n, len - n);
      if (nread <= 0) 
        return (n == 0) ? nread : n;
      n += nread;
      if (n >= len)
        return n;
    }
  }
  
  /**
   * Set the checksum related parameters
   * @param verifyChecksum whether to verify checksum
   * @param sum which type of checksum to use
   * @param maxChunkSize maximun chunk size
   * @param checksumSize checksum size
   */
  final protected synchronized void set(@OsUntrusted FSInputChecker this, @OsUntrusted boolean verifyChecksum,
      @OsUntrusted
      Checksum sum, @OsUntrusted int maxChunkSize, @OsUntrusted int checksumSize) {

    // The code makes assumptions that checksums are always 32-bit.
    assert !verifyChecksum || sum == null || checksumSize == CHECKSUM_SIZE;

    this.maxChunkSize = maxChunkSize;
    this.verifyChecksum = verifyChecksum;
    this.sum = sum;
    this.buf = new @OsUntrusted byte @OsUntrusted [maxChunkSize];
    // The size of the checksum array here determines how much we can
    // read in a single call to readChunk
    this.checksum = new @OsUntrusted byte @OsUntrusted [CHUNKS_PER_READ * checksumSize];
    this.checksumInts = ByteBuffer.wrap(checksum).asIntBuffer();
    this.count = 0;
    this.pos = 0;
  }

  @Override
  final public @OsUntrusted boolean markSupported(@OsUntrusted FSInputChecker this) {
    return false;
  }
  
  @Override
  final public void mark(@OsUntrusted FSInputChecker this, @OsUntrusted int readlimit) {
  }
  
  @Override
  final public void reset(@OsUntrusted FSInputChecker this) throws IOException {
    throw new @OsUntrusted IOException("mark/reset not supported");
  }
  

  /* reset this FSInputChecker's state */
  private void resetState(@OsUntrusted FSInputChecker this) {
    // invalidate buffer
    count = 0;
    pos = 0;
    // reset Checksum
    if (sum != null) {
      sum.reset();
    }
  }
}
