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
import java.io.*;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.PureJavaCrc32;

/**
 * Abstract Checksumed Fs.
 * It provide a basic implementation of a Checksumed Fs,
 * which creates a checksum file for each raw file.
 * It generates & verifies checksums at the client side.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public abstract class ChecksumFs extends @OsUntrusted FilterFs {
  private static final @OsUntrusted byte @OsUntrusted [] CHECKSUM_VERSION = new @OsUntrusted byte @OsUntrusted [] {'c', 'r', 'c', 0};
  private @OsUntrusted int defaultBytesPerChecksum = 512;
  private @OsUntrusted boolean verifyChecksum = true;

  public static @OsUntrusted double getApproxChkSumLength(@OsUntrusted long size) {
    return ChecksumFSOutputSummer.CHKSUM_AS_FRACTION * size;
  }
  
  public @OsUntrusted ChecksumFs(@OsUntrusted AbstractFileSystem theFs)
    throws IOException, URISyntaxException {
    super(theFs);
    defaultBytesPerChecksum = 
      getMyFs().getServerDefaults().getBytesPerChecksum();
  }
  
  /**
   * Set whether to verify checksum.
   */
  @Override
  public void setVerifyChecksum(@OsUntrusted ChecksumFs this, @OsUntrusted boolean inVerifyChecksum) {
    this.verifyChecksum = inVerifyChecksum;
  }

  /** get the raw file system. */
  public @OsUntrusted AbstractFileSystem getRawFs(@OsUntrusted ChecksumFs this) {
    return getMyFs();
  }

  /** Return the name of the checksum file associated with a file.*/
  public @OsUntrusted Path getChecksumFile(@OsUntrusted ChecksumFs this, @OsUntrusted Path file) {
    return new @OsUntrusted Path(file.getParent(), "." + file.getName() + ".crc");
  }

  /** Return true iff file is a checksum file name.*/
  public static @OsUntrusted boolean isChecksumFile(@OsUntrusted Path file) {
    @OsUntrusted
    String name = file.getName();
    return name.startsWith(".") && name.endsWith(".crc");
  }

  /** Return the length of the checksum file given the size of the 
   * actual file.
   **/
  public @OsUntrusted long getChecksumFileLength(@OsUntrusted ChecksumFs this, @OsUntrusted Path file, @OsUntrusted long fileSize) {
    return getChecksumLength(fileSize, getBytesPerSum());
  }

  /** Return the bytes Per Checksum. */
  public @OsUntrusted int getBytesPerSum(@OsUntrusted ChecksumFs this) {
    return defaultBytesPerChecksum;
  }

  private @OsUntrusted int getSumBufferSize(@OsUntrusted ChecksumFs this, @OsUntrusted int bytesPerSum, @OsUntrusted int bufferSize)
    throws IOException {
    @OsUntrusted
    int defaultBufferSize =  getMyFs().getServerDefaults().getFileBufferSize();
    @OsUntrusted
    int proportionalBufferSize = bufferSize / bytesPerSum;
    return Math.max(bytesPerSum,
                    Math.max(proportionalBufferSize, defaultBufferSize));
  }

  /*******************************************************
   * For open()'s FSInputStream
   * It verifies that data matches checksums.
   *******************************************************/
  private static class ChecksumFSInputChecker extends @OsUntrusted FSInputChecker {
    public static final @OsUntrusted Log LOG 
      = LogFactory.getLog(FSInputChecker.class);
    private static final @OsUntrusted int HEADER_LENGTH = 8;
    
    private @OsUntrusted ChecksumFs fs;
    private @OsUntrusted FSDataInputStream datas;
    private @OsUntrusted FSDataInputStream sums;
    private @OsUntrusted int bytesPerSum = 1;
    private @OsUntrusted long fileLen = -1L;
    
    public @OsUntrusted ChecksumFSInputChecker(@OsUntrusted ChecksumFs fs, @OsUntrusted Path file)
      throws IOException, UnresolvedLinkException {
      this(fs, file, fs.getServerDefaults().getFileBufferSize());
    }
    
    public @OsUntrusted ChecksumFSInputChecker(@OsUntrusted ChecksumFs fs, @OsUntrusted Path file, @OsUntrusted int bufferSize)
      throws IOException, UnresolvedLinkException {
      super(file, fs.getFileStatus(file).getReplication());
      this.datas = fs.getRawFs().open(file, bufferSize);
      this.fs = fs;
      @OsUntrusted
      Path sumFile = fs.getChecksumFile(file);
      try {
        @OsUntrusted
        int sumBufferSize = fs.getSumBufferSize(fs.getBytesPerSum(),
                                                bufferSize);
        sums = fs.getRawFs().open(sumFile, sumBufferSize);

        @OsUntrusted
        byte @OsUntrusted [] version = new @OsUntrusted byte @OsUntrusted [CHECKSUM_VERSION.length];
        sums.readFully(version);
        if (!Arrays.equals(version, CHECKSUM_VERSION)) {
          throw new @OsUntrusted IOException("Not a checksum file: "+sumFile);
        }
        this.bytesPerSum = sums.readInt();
        set(fs.verifyChecksum, new @OsUntrusted PureJavaCrc32(), bytesPerSum, 4);
      } catch (@OsUntrusted FileNotFoundException e) {         // quietly ignore
        set(fs.verifyChecksum, null, 1, 0);
      } catch (@OsUntrusted IOException e) {                   // loudly ignore
        LOG.warn("Problem opening checksum file: "+ file + 
                 ".  Ignoring exception: " , e); 
        set(fs.verifyChecksum, null, 1, 0);
      }
    }
    
    private @OsUntrusted long getChecksumFilePos(ChecksumFs.@OsUntrusted ChecksumFSInputChecker this, @OsUntrusted long dataPos) {
      return HEADER_LENGTH + 4*(dataPos/bytesPerSum);
    }
    
    @Override
    protected @OsUntrusted long getChunkPosition(ChecksumFs.@OsUntrusted ChecksumFSInputChecker this, @OsUntrusted long dataPos) {
      return dataPos/bytesPerSum*bytesPerSum;
    }
    
    @Override
    public @OsUntrusted int available(ChecksumFs.@OsUntrusted ChecksumFSInputChecker this) throws IOException {
      return datas.available() + super.available();
    }
    
    @Override
    public @OsUntrusted int read(ChecksumFs.@OsUntrusted ChecksumFSInputChecker this, @OsUntrusted long position, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len)
      throws IOException, UnresolvedLinkException {
      // parameter check
      if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
        throw new @OsUntrusted IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }
      if (position<0) {
        throw new @OsUntrusted IllegalArgumentException(
            "Parameter position can not to be negative");
      }

      @OsUntrusted
      ChecksumFSInputChecker checker = new @OsUntrusted ChecksumFSInputChecker(fs, file);
      checker.seek(position);
      @OsUntrusted
      int nread = checker.read(b, off, len);
      checker.close();
      return nread;
    }
    
    @Override
    public void close(ChecksumFs.@OsUntrusted ChecksumFSInputChecker this) throws IOException {
      datas.close();
      if (sums != null) {
        sums.close();
      }
      set(fs.verifyChecksum, null, 1, 0);
    }
    
    @Override
    public @OsUntrusted boolean seekToNewSource(ChecksumFs.@OsUntrusted ChecksumFSInputChecker this, @OsUntrusted long targetPos) throws IOException {
      final @OsUntrusted long sumsPos = getChecksumFilePos(targetPos);
      fs.reportChecksumFailure(file, datas, targetPos, sums, sumsPos);
      final @OsUntrusted boolean newDataSource = datas.seekToNewSource(targetPos);
      return sums.seekToNewSource(sumsPos) || newDataSource;
    }

    @Override
    protected @OsUntrusted int readChunk(ChecksumFs.@OsUntrusted ChecksumFSInputChecker this, @OsUntrusted long pos, @OsUntrusted byte @OsUntrusted [] buf, @OsUntrusted int offset, @OsUntrusted int len,
        @OsUntrusted
        byte @OsUntrusted [] checksum) throws IOException {
      @OsUntrusted
      boolean eof = false;
      if (needChecksum()) {
        assert checksum != null; // we have a checksum buffer
        assert checksum.length % CHECKSUM_SIZE == 0; // it is sane length
        assert len >= bytesPerSum; // we must read at least one chunk

        final @OsUntrusted int checksumsToRead = Math.min(
          len/bytesPerSum, // number of checksums based on len to read
          checksum.length / CHECKSUM_SIZE); // size of checksum buffer
        @OsUntrusted
        long checksumPos = getChecksumFilePos(pos); 
        if(checksumPos != sums.getPos()) {
          sums.seek(checksumPos);
        }

        @OsUntrusted
        int sumLenRead = sums.read(checksum, 0, CHECKSUM_SIZE * checksumsToRead);
        if (sumLenRead >= 0 && sumLenRead % CHECKSUM_SIZE != 0) {
          throw new @OsUntrusted EOFException("Checksum file not a length multiple of checksum size " +
                                 "in " + file + " at " + pos + " checksumpos: " + checksumPos +
                                 " sumLenread: " + sumLenRead );
        }
        if (sumLenRead <= 0) { // we're at the end of the file
          eof = true;
        } else {
          // Adjust amount of data to read based on how many checksum chunks we read
          len = Math.min(len, bytesPerSum * (sumLenRead / CHECKSUM_SIZE));
        }
      }
      if (pos != datas.getPos()) {
        datas.seek(pos);
      }
      @OsUntrusted
      int nread = readFully(datas, buf, offset, len);
      if (eof && nread > 0) {
        throw new @OsUntrusted ChecksumException("Checksum error: "+file+" at "+pos, pos);
      }
      return nread;
    }
    
    /* Return the file length */
    private @OsUntrusted long getFileLength(ChecksumFs.@OsUntrusted ChecksumFSInputChecker this) throws IOException, UnresolvedLinkException {
      if (fileLen==-1L) {
        fileLen = fs.getFileStatus(file).getLen();
      }
      return fileLen;
    }
    
    /**
     * Skips over and discards <code>n</code> bytes of data from the
     * input stream.
     *
     * The <code>skip</code> method skips over some smaller number of bytes
     * when reaching end of file before <code>n</code> bytes have been skipped.
     * The actual number of bytes skipped is returned.  If <code>n</code> is
     * negative, no bytes are skipped.
     *
     * @param      n   the number of bytes to be skipped.
     * @return     the actual number of bytes skipped.
     * @exception  IOException  if an I/O error occurs.
     *             ChecksumException if the chunk to skip to is corrupted
     */
    @Override
    public synchronized @OsUntrusted long skip(ChecksumFs.@OsUntrusted ChecksumFSInputChecker this, @OsUntrusted long n) throws IOException { 
      final @OsUntrusted long curPos = getPos();
      final @OsUntrusted long fileLength = getFileLength();
      if (n+curPos > fileLength) {
        n = fileLength - curPos;
      }
      return super.skip(n);
    }
    
    /**
     * Seek to the given position in the stream.
     * The next read() will be from that position.
     * 
     * <p>This method does not allow seek past the end of the file.
     * This produces IOException.
     *
     * @param      pos   the postion to seek to.
     * @exception  IOException  if an I/O error occurs or seeks after EOF
     *             ChecksumException if the chunk to seek to is corrupted
     */

    @Override
    public synchronized void seek(ChecksumFs.@OsUntrusted ChecksumFSInputChecker this, @OsUntrusted long pos) throws IOException { 
      if (pos>getFileLength()) {
        throw new @OsUntrusted IOException("Cannot seek after EOF");
      }
      super.seek(pos);
    }

  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Override
  public @OsUntrusted FSDataInputStream open(@OsUntrusted ChecksumFs this, @OsUntrusted Path f, @OsUntrusted int bufferSize) 
    throws IOException, UnresolvedLinkException {
    return new @OsUntrusted FSDataInputStream(
        new @OsUntrusted ChecksumFSInputChecker(this, f, bufferSize));
  }

  /**
   * Calculated the length of the checksum file in bytes.
   * @param size the length of the data file in bytes
   * @param bytesPerSum the number of bytes in a checksum block
   * @return the number of bytes in the checksum file
   */
  public static @OsUntrusted long getChecksumLength(@OsUntrusted long size, @OsUntrusted int bytesPerSum) {
    //the checksum length is equal to size passed divided by bytesPerSum +
    //bytes written in the beginning of the checksum file.  
    return ((size + bytesPerSum - 1) / bytesPerSum) * 4 +
             CHECKSUM_VERSION.length + 4;  
  }

  /** This class provides an output stream for a checksummed file.
   * It generates checksums for data. */
  private static class ChecksumFSOutputSummer extends @OsUntrusted FSOutputSummer {
    private @OsUntrusted FSDataOutputStream datas;    
    private @OsUntrusted FSDataOutputStream sums;
    private static final @OsUntrusted float CHKSUM_AS_FRACTION = 0.01f;
    private @OsUntrusted boolean isClosed = false;
    
    
    public @OsUntrusted ChecksumFSOutputSummer(final @OsUntrusted ChecksumFs fs, final @OsUntrusted Path file, 
      final @OsUntrusted EnumSet<@OsUntrusted CreateFlag> createFlag,
      final @OsUntrusted FsPermission absolutePermission, final @OsUntrusted int bufferSize,
      final @OsUntrusted short replication, final @OsUntrusted long blockSize, 
      final @OsUntrusted Progressable progress, final @OsUntrusted ChecksumOpt checksumOpt,
      final @OsUntrusted boolean createParent) throws IOException {
      super(new @OsUntrusted PureJavaCrc32(), fs.getBytesPerSum(), 4);

      // checksumOpt is passed down to the raw fs. Unless it implements
      // checksum impelemts internally, checksumOpt will be ignored.
      // If the raw fs does checksum internally, we will end up with
      // two layers of checksumming. i.e. checksumming checksum file.
      this.datas = fs.getRawFs().createInternal(file, createFlag,
          absolutePermission, bufferSize, replication, blockSize, progress,
           checksumOpt,  createParent);
      
      // Now create the chekcsumfile; adjust the buffsize
      @OsUntrusted
      int bytesPerSum = fs.getBytesPerSum();
      @OsUntrusted
      int sumBufferSize = fs.getSumBufferSize(bytesPerSum, bufferSize);
      this.sums = fs.getRawFs().createInternal(fs.getChecksumFile(file),
          EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
          absolutePermission, sumBufferSize, replication, blockSize, progress,
          checksumOpt, createParent);
      sums.write(CHECKSUM_VERSION, 0, CHECKSUM_VERSION.length);
      sums.writeInt(bytesPerSum);
    }
    
    @Override
    public void close(ChecksumFs.@OsUntrusted ChecksumFSOutputSummer this) throws IOException {
      try {
        flushBuffer();
        sums.close();
        datas.close();
      } finally {
        isClosed = true;
      }
    }
    
    @Override
    protected void writeChunk(ChecksumFs.@OsUntrusted ChecksumFSOutputSummer this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int offset, @OsUntrusted int len, @OsUntrusted byte @OsUntrusted [] checksum)
      throws IOException {
      datas.write(b, offset, len);
      sums.write(checksum);
    }

    @Override
    protected void checkClosed(ChecksumFs.@OsUntrusted ChecksumFSOutputSummer this) throws IOException {
      if (isClosed) {
        throw new @OsUntrusted ClosedChannelException();
      }
    }
  }

  @Override
  public @OsUntrusted FSDataOutputStream createInternal(@OsUntrusted ChecksumFs this, @OsUntrusted Path f,
      @OsUntrusted
      EnumSet<@OsUntrusted CreateFlag> createFlag, @OsUntrusted FsPermission absolutePermission,
      @OsUntrusted
      int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize, @OsUntrusted Progressable progress,
      @OsUntrusted
      ChecksumOpt checksumOpt, @OsUntrusted boolean createParent) throws IOException {
    final @OsUntrusted FSDataOutputStream out = new @OsUntrusted FSDataOutputStream(
        new @OsUntrusted ChecksumFSOutputSummer(this, f, createFlag, absolutePermission,
            bufferSize, replication, blockSize, progress,
            checksumOpt,  createParent), null);
    return out;
  }

  /** Check if exists.
   * @param f source file
   */
  private @OsUntrusted boolean exists(@OsUntrusted ChecksumFs this, @OsUntrusted Path f) 
    throws IOException, UnresolvedLinkException {
    try {
      return getMyFs().getFileStatus(f) != null;
    } catch (@OsUntrusted FileNotFoundException e) {
      return false;
    }
  }
  
  /** True iff the named path is a directory.
   * Note: Avoid using this method. Instead reuse the FileStatus 
   * returned by getFileStatus() or listStatus() methods.
   */
  private @OsUntrusted boolean isDirectory(@OsUntrusted ChecksumFs this, @OsUntrusted Path f) 
    throws IOException, UnresolvedLinkException {
    try {
      return getMyFs().getFileStatus(f).isDirectory();
    } catch (@OsUntrusted FileNotFoundException e) {
      return false;               // f does not exist
    }
  }
  /**
   * Set replication for an existing file.
   * Implement the abstract <tt>setReplication</tt> of <tt>FileSystem</tt>
   * @param src file name
   * @param replication new replication
   * @throws IOException
   * @return true if successful;
   *         false if file does not exist or is a directory
   */
  @Override
  public @OsUntrusted boolean setReplication(@OsUntrusted ChecksumFs this, @OsUntrusted Path src, @OsUntrusted short replication)
    throws IOException, UnresolvedLinkException {
    @OsUntrusted
    boolean value = getMyFs().setReplication(src, replication);
    if (!value) {
      return false;
    }
    @OsUntrusted
    Path checkFile = getChecksumFile(src);
    if (exists(checkFile)) {
      getMyFs().setReplication(checkFile, replication);
    }
    return true;
  }

  /**
   * Rename files/dirs.
   */
  @Override
  public void renameInternal(@OsUntrusted ChecksumFs this, @OsUntrusted Path src, @OsUntrusted Path dst) 
    throws IOException, UnresolvedLinkException {
    if (isDirectory(src)) {
      getMyFs().rename(src, dst);
    } else {
      getMyFs().rename(src, dst);

      @OsUntrusted
      Path checkFile = getChecksumFile(src);
      if (exists(checkFile)) { //try to rename checksum
        if (isDirectory(dst)) {
          getMyFs().rename(checkFile, dst);
        } else {
          getMyFs().rename(checkFile, getChecksumFile(dst));
        }
      }
    }
  }

  /**
   * Implement the delete(Path, boolean) in checksum
   * file system.
   */
  @Override
  public @OsUntrusted boolean delete(@OsUntrusted ChecksumFs this, @OsUntrusted Path f, @OsUntrusted boolean recursive) 
    throws IOException, UnresolvedLinkException {
    @OsUntrusted
    FileStatus fstatus = null;
    try {
      fstatus = getMyFs().getFileStatus(f);
    } catch(@OsUntrusted FileNotFoundException e) {
      return false;
    }
    if (fstatus.isDirectory()) {
      //this works since the crcs are in the same
      //directories and the files. so we just delete
      //everything in the underlying filesystem
      return getMyFs().delete(f, recursive);
    } else {
      @OsUntrusted
      Path checkFile = getChecksumFile(f);
      if (exists(checkFile)) {
        getMyFs().delete(checkFile, true);
      }
      return getMyFs().delete(f, true);
    }
  }

  /**
   * Report a checksum error to the file system.
   * @param f the file name containing the error
   * @param in the stream open on the file
   * @param inPos the position of the beginning of the bad data in the file
   * @param sums the stream open on the checksum file
   * @param sumsPos the position of the beginning of the bad data in the
   *         checksum file
   * @return if retry is neccessary
   */
  public @OsUntrusted boolean reportChecksumFailure(@OsUntrusted ChecksumFs this, @OsUntrusted Path f, @OsUntrusted FSDataInputStream in,
    @OsUntrusted
    long inPos, @OsUntrusted FSDataInputStream sums, @OsUntrusted long sumsPos) {
    return false;
  }

  @Override
  public @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted ChecksumFs this, @OsUntrusted Path f) throws IOException,
      UnresolvedLinkException {
    @OsUntrusted
    ArrayList<@OsUntrusted FileStatus> results = new @OsUntrusted ArrayList<@OsUntrusted FileStatus>();
    @OsUntrusted
    FileStatus @OsUntrusted [] listing = getMyFs().listStatus(f);
    if (listing != null) {
      for (@OsUntrusted int i = 0; i < listing.length; i++) {
        if (!isChecksumFile(listing[i].getPath())) {
          results.add(listing[i]);
        }
      }
    }
    return results.toArray(new @OsUntrusted FileStatus @OsUntrusted [results.size()]);
  }
}
