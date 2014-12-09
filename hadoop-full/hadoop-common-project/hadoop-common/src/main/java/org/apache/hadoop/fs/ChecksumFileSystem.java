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
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.PureJavaCrc32;

/****************************************************************
 * Abstract Checksumed FileSystem.
 * It provide a basic implementation of a Checksumed FileSystem,
 * which creates a checksum file for each raw file.
 * It generates & verifies checksums at the client side.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class ChecksumFileSystem extends @OsUntrusted FilterFileSystem {
  private static final @OsUntrusted byte @OsUntrusted [] CHECKSUM_VERSION = new @OsUntrusted byte @OsUntrusted [] {'c', 'r', 'c', 0};
  private @OsUntrusted int bytesPerChecksum = 512;
  private @OsUntrusted boolean verifyChecksum = true;
  private @OsUntrusted boolean writeChecksum = true;

  public static @OsUntrusted double getApproxChkSumLength(@OsUntrusted long size) {
    return ChecksumFSOutputSummer.CHKSUM_AS_FRACTION * size;
  }
  
  public @OsUntrusted ChecksumFileSystem(@OsUntrusted FileSystem fs) {
    super(fs);
  }

  @Override
  public void setConf(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      bytesPerChecksum = conf.getInt(LocalFileSystemConfigKeys.LOCAL_FS_BYTES_PER_CHECKSUM_KEY,
		                     LocalFileSystemConfigKeys.LOCAL_FS_BYTES_PER_CHECKSUM_DEFAULT);
    }
  }
  
  /**
   * Set whether to verify checksum.
   */
  @Override
  public void setVerifyChecksum(@OsUntrusted ChecksumFileSystem this, @OsUntrusted boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
  }

  @Override
  public void setWriteChecksum(@OsUntrusted ChecksumFileSystem this, @OsUntrusted boolean writeChecksum) {
    this.writeChecksum = writeChecksum;
  }
  
  /** get the raw file system */
  @Override
  public @OsUntrusted FileSystem getRawFileSystem(@OsUntrusted ChecksumFileSystem this) {
    return fs;
  }

  /** Return the name of the checksum file associated with a file.*/
  public @OsUntrusted Path getChecksumFile(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path file) {
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
  public @OsUntrusted long getChecksumFileLength(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path file, @OsUntrusted long fileSize) {
    return getChecksumLength(fileSize, getBytesPerSum());
  }

  /** Return the bytes Per Checksum */
  public @OsUntrusted int getBytesPerSum(@OsUntrusted ChecksumFileSystem this) {
    return bytesPerChecksum;
  }

  private @OsUntrusted int getSumBufferSize(@OsUntrusted ChecksumFileSystem this, @OsUntrusted int bytesPerSum, @OsUntrusted int bufferSize) {
    @OsUntrusted
    int defaultBufferSize = getConf().getInt(
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY,
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT);
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
    private @OsUntrusted ChecksumFileSystem fs;
    private @OsUntrusted FSDataInputStream datas;
    private @OsUntrusted FSDataInputStream sums;
    
    private static final @OsUntrusted int HEADER_LENGTH = 8;
    
    private @OsUntrusted int bytesPerSum = 1;
    
    public @OsUntrusted ChecksumFSInputChecker(@OsUntrusted ChecksumFileSystem fs, @OsUntrusted Path file)
      throws IOException {
      this(fs, file, fs.getConf().getInt(
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY, 
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT));
    }
    
    public @OsUntrusted ChecksumFSInputChecker(@OsUntrusted ChecksumFileSystem fs, @OsUntrusted Path file, @OsUntrusted int bufferSize)
      throws IOException {
      super( file, fs.getFileStatus(file).getReplication() );
      this.datas = fs.getRawFileSystem().open(file, bufferSize);
      this.fs = fs;
      @OsUntrusted
      Path sumFile = fs.getChecksumFile(file);
      try {
        @OsUntrusted
        int sumBufferSize = fs.getSumBufferSize(fs.getBytesPerSum(), bufferSize);
        sums = fs.getRawFileSystem().open(sumFile, sumBufferSize);

        @OsUntrusted
        byte @OsUntrusted [] version = new @OsUntrusted byte @OsUntrusted [CHECKSUM_VERSION.length];
        sums.readFully(version);
        if (!Arrays.equals(version, CHECKSUM_VERSION))
          throw new @OsUntrusted IOException("Not a checksum file: "+sumFile);
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
    
    private @OsUntrusted long getChecksumFilePos( ChecksumFileSystem.@OsUntrusted ChecksumFSInputChecker this, @OsUntrusted long dataPos ) {
      return HEADER_LENGTH + 4*(dataPos/bytesPerSum);
    }
    
    @Override
    protected @OsUntrusted long getChunkPosition( ChecksumFileSystem.@OsUntrusted ChecksumFSInputChecker this, @OsUntrusted long dataPos ) {
      return dataPos/bytesPerSum*bytesPerSum;
    }
    
    @Override
    public @OsUntrusted int available(ChecksumFileSystem.@OsUntrusted ChecksumFSInputChecker this) throws IOException {
      return datas.available() + super.available();
    }
    
    @Override
    public @OsUntrusted int read(ChecksumFileSystem.@OsUntrusted ChecksumFSInputChecker this, @OsUntrusted long position, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len)
      throws IOException {
      // parameter check
      if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
        throw new @OsUntrusted IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }
      if( position<0 ) {
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
    public void close(ChecksumFileSystem.@OsUntrusted ChecksumFSInputChecker this) throws IOException {
      datas.close();
      if( sums != null ) {
        sums.close();
      }
      set(fs.verifyChecksum, null, 1, 0);
    }
    

    @Override
    public @OsUntrusted boolean seekToNewSource(ChecksumFileSystem.@OsUntrusted ChecksumFSInputChecker this, @OsUntrusted long targetPos) throws IOException {
      @OsUntrusted
      long sumsPos = getChecksumFilePos(targetPos);
      fs.reportChecksumFailure(file, datas, targetPos, sums, sumsPos);
      @OsUntrusted
      boolean newDataSource = datas.seekToNewSource(targetPos);
      return sums.seekToNewSource(sumsPos) || newDataSource;
    }

    @Override
    protected @OsUntrusted int readChunk(ChecksumFileSystem.@OsUntrusted ChecksumFSInputChecker this, @OsUntrusted long pos, @OsUntrusted byte @OsUntrusted [] buf, @OsUntrusted int offset, @OsUntrusted int len,
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
          throw new @OsUntrusted ChecksumException(
            "Checksum file not a length multiple of checksum size "/* +
            //AVOID CHECKER FRAMEWORK EXCEPTION
            "in " + file + " at " + pos + " checksumpos: " + checksumPos +
            " sumLenread: " + sumLenRead*/,
            pos);
        }
        if (sumLenRead <= 0) { // we're at the end of the file
          eof = true;
        } else {
          // Adjust amount of data to read based on how many checksum chunks we read
          len = Math.min(len, bytesPerSum * (sumLenRead / CHECKSUM_SIZE));
        }
      }
      if(pos != datas.getPos()) {
        datas.seek(pos);
      }
      @OsUntrusted
      int nread = readFully(datas, buf, offset, len);
      if (eof && nread > 0) {
        //ostrusted Avoid Checker Framework exception
        throw new @OsUntrusted ChecksumException("Checksum error:"/*"Checksum error: "+file+" at "+pos*/, pos);
      }
      return nread;
    }
  }
  
  private static class FSDataBoundedInputStream extends @OsUntrusted FSDataInputStream {
    private @OsUntrusted FileSystem fs;
    private @OsUntrusted Path file;
    private @OsUntrusted long fileLen = -1L;

    @OsUntrusted
    FSDataBoundedInputStream(@OsUntrusted FileSystem fs, @OsUntrusted Path file, @OsUntrusted InputStream in)
        throws IOException {
      super(in);
      this.fs = fs;
      this.file = file;
    }
    
    @Override
    public @OsUntrusted boolean markSupported(ChecksumFileSystem.@OsUntrusted FSDataBoundedInputStream this) {
      return false;
    }
    
    /* Return the file length */
    private @OsUntrusted long getFileLength(ChecksumFileSystem.@OsUntrusted FSDataBoundedInputStream this) throws IOException {
      if( fileLen==-1L ) {
        fileLen = fs.getContentSummary(file).getLength();
      }
      return fileLen;
    }
    
    /**
     * Skips over and discards <code>n</code> bytes of data from the
     * input stream.
     *
     *The <code>skip</code> method skips over some smaller number of bytes
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
    public synchronized @OsUntrusted long skip(ChecksumFileSystem.@OsUntrusted FSDataBoundedInputStream this, @OsUntrusted long n) throws IOException {
      @OsUntrusted
      long curPos = getPos();
      @OsUntrusted
      long fileLength = getFileLength();
      if( n+curPos > fileLength ) {
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
    public synchronized void seek(ChecksumFileSystem.@OsUntrusted FSDataBoundedInputStream this, @OsUntrusted long pos) throws IOException {
      if(pos>getFileLength()) {
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
  public @OsUntrusted FSDataInputStream open(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path f, @OsUntrusted int bufferSize) throws IOException {
    @OsUntrusted
    FileSystem fs;
    @OsUntrusted
    InputStream in;
    if (verifyChecksum) {
      fs = this;
      in = new @OsUntrusted ChecksumFSInputChecker(this, f, bufferSize);
    } else {
      fs = getRawFileSystem();
      in = fs.open(f, bufferSize);
    }
    return new @OsUntrusted FSDataBoundedInputStream(fs, f, in);
  }

  @Override
  public @OsUntrusted FSDataOutputStream append(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path f, @OsUntrusted int bufferSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    throw new @OsUntrusted IOException("Not supported");
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
    
    public @OsUntrusted ChecksumFSOutputSummer(@OsUntrusted ChecksumFileSystem fs, 
                          @OsUntrusted
                          Path file, 
                          @OsUntrusted
                          boolean overwrite,
                          @OsUntrusted
                          int bufferSize,
                          @OsUntrusted
                          short replication,
                          @OsUntrusted
                          long blockSize,
                          @OsUntrusted
                          Progressable progress)
      throws IOException {
      super(new @OsUntrusted PureJavaCrc32(), fs.getBytesPerSum(), 4);
      @OsUntrusted
      int bytesPerSum = fs.getBytesPerSum();
      this.datas = fs.getRawFileSystem().create(file, overwrite, bufferSize, 
                                         replication, blockSize, progress);
      @OsUntrusted
      int sumBufferSize = fs.getSumBufferSize(bytesPerSum, bufferSize);
      this.sums = fs.getRawFileSystem().create(fs.getChecksumFile(file), true, 
                                               sumBufferSize, replication,
                                               blockSize);
      sums.write(CHECKSUM_VERSION, 0, CHECKSUM_VERSION.length);
      sums.writeInt(bytesPerSum);
    }
    
    @Override
    public void close(ChecksumFileSystem.@OsUntrusted ChecksumFSOutputSummer this) throws IOException {
      try {
        flushBuffer();
        sums.close();
        datas.close();
      } finally {
        isClosed = true;
      }
    }
    
    @Override
    protected void writeChunk(ChecksumFileSystem.@OsUntrusted ChecksumFSOutputSummer this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int offset, @OsUntrusted int len, @OsUntrusted byte @OsUntrusted [] checksum)
    throws IOException {
      datas.write(b, offset, len);
      sums.write(checksum);
    }

    @Override
    protected void checkClosed(ChecksumFileSystem.@OsUntrusted ChecksumFSOutputSummer this) throws IOException {
      if (isClosed) {
        throw new @OsUntrusted ClosedChannelException();
      }
    }
  }

  @Override
  public @OsUntrusted FSDataOutputStream create(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission,
      @OsUntrusted
      boolean overwrite, @OsUntrusted int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    return create(f, permission, overwrite, true, bufferSize,
        replication, blockSize, progress);
  }

  private @OsUntrusted FSDataOutputStream create(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission,
      @OsUntrusted
      boolean overwrite, @OsUntrusted boolean createParent, @OsUntrusted int bufferSize,
      @OsUntrusted
      short replication, @OsUntrusted long blockSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    @OsUntrusted
    Path parent = f.getParent();
    if (parent != null) {
      if (!createParent && !exists(parent)) {
        throw new @OsUntrusted FileNotFoundException("Parent directory doesn't exist: "
            + parent);
      } else if (!mkdirs(parent)) {
        throw new @OsUntrusted IOException("Mkdirs failed to create " + parent);
      }
    }
    final @OsUntrusted FSDataOutputStream out;
    if (writeChecksum) {
      out = new @OsUntrusted FSDataOutputStream(
          new @OsUntrusted ChecksumFSOutputSummer(this, f, overwrite, bufferSize, replication,
              blockSize, progress), null);
    } else {
      out = fs.create(f, permission, overwrite, bufferSize, replication,
          blockSize, progress);
      // remove the checksum file since we aren't writing one
      @OsUntrusted
      Path checkFile = getChecksumFile(f);
      if (fs.exists(checkFile)) {
        fs.delete(checkFile, true);
      }
    }
    if (permission != null) {
      setPermission(f, permission);
    }
    return out;
  }

  @Override
  public @OsUntrusted FSDataOutputStream createNonRecursive(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission,
      @OsUntrusted
      boolean overwrite, @OsUntrusted int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    return create(f, permission, overwrite, false, bufferSize, replication,
        blockSize, progress);
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
  public @OsUntrusted boolean setReplication(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path src, @OsUntrusted short replication) throws IOException {
    @OsUntrusted
    boolean value = fs.setReplication(src, replication);
    if (!value)
      return false;

    @OsUntrusted
    Path checkFile = getChecksumFile(src);
    if (exists(checkFile))
      fs.setReplication(checkFile, replication);

    return true;
  }

  /**
   * Rename files/dirs
   */
  @Override
  public @OsUntrusted boolean rename(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path src, @OsUntrusted Path dst) throws IOException {
    if (fs.isDirectory(src)) {
      return fs.rename(src, dst);
    } else {
      if (fs.isDirectory(dst)) {
        dst = new @OsUntrusted Path(dst, src.getName());
      }

      @OsUntrusted
      boolean value = fs.rename(src, dst);
      if (!value)
        return false;

      @OsUntrusted
      Path srcCheckFile = getChecksumFile(src);
      @OsUntrusted
      Path dstCheckFile = getChecksumFile(dst);
      if (fs.exists(srcCheckFile)) { //try to rename checksum
        value = fs.rename(srcCheckFile, dstCheckFile);
      } else if (fs.exists(dstCheckFile)) {
        // no src checksum, so remove dst checksum
        value = fs.delete(dstCheckFile, true); 
      }

      return value;
    }
  }

  /**
   * Implement the delete(Path, boolean) in checksum
   * file system.
   */
  @Override
  public @OsUntrusted boolean delete(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path f, @OsUntrusted boolean recursive) throws IOException{
    @OsUntrusted
    FileStatus fstatus = null;
    try {
      fstatus = fs.getFileStatus(f);
    } catch(@OsUntrusted FileNotFoundException e) {
      return false;
    }
    if (fstatus.isDirectory()) {
      //this works since the crcs are in the same
      //directories and the files. so we just delete
      //everything in the underlying filesystem
      return fs.delete(f, recursive);
    } else {
      @OsUntrusted
      Path checkFile = getChecksumFile(f);
      if (fs.exists(checkFile)) {
        fs.delete(checkFile, true);
      }
      return fs.delete(f, true);
    }
  }
    
  final private static @OsUntrusted PathFilter DEFAULT_FILTER = new @OsUntrusted PathFilter() {
    @Override
    public @OsUntrusted boolean accept(@OsUntrusted Path file) {
      return !isChecksumFile(file);
    }
  };

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * 
   * @param f
   *          given path
   * @return the statuses of the files/directories in the given patch
   * @throws IOException
   */
  @Override
  public @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path f) throws IOException {
    return fs.listStatus(f, DEFAULT_FILTER);
  }
  
  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * 
   * @param f
   *          given path
   * @return the statuses of the files/directories in the given patch
   * @throws IOException
   */
  @Override
  public @OsUntrusted RemoteIterator<@OsUntrusted LocatedFileStatus> listLocatedStatus(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path f)
  throws IOException {
    return fs.listLocatedStatus(f, DEFAULT_FILTER);
  }
  
  @Override
  public @OsUntrusted boolean mkdirs(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path f) throws IOException {
    return fs.mkdirs(f);
  }

  @Override
  public void copyFromLocalFile(@OsUntrusted ChecksumFileSystem this, @OsUntrusted boolean delSrc, @OsUntrusted Path src, @OsUntrusted Path dst)
    throws IOException {
    @OsUntrusted
    Configuration conf = getConf();
    FileUtil.copy(getLocal(conf), src, this, dst, delSrc, conf);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   */
  @Override
  public void copyToLocalFile(@OsUntrusted ChecksumFileSystem this, @OsUntrusted boolean delSrc, @OsUntrusted Path src, @OsUntrusted Path dst)
    throws IOException {
    @OsUntrusted
    Configuration conf = getConf();
    FileUtil.copy(this, src, getLocal(conf), dst, delSrc, conf);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * If src and dst are directories, the copyCrc parameter
   * determines whether to copy CRC files.
   */
  public void copyToLocalFile(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path src, @OsUntrusted Path dst, @OsUntrusted boolean copyCrc)
    throws IOException {
    if (!fs.isDirectory(src)) { // source is a file
      fs.copyToLocalFile(src, dst);
      @OsUntrusted
      FileSystem localFs = getLocal(getConf()).getRawFileSystem();
      if (localFs.isDirectory(dst)) {
        dst = new @OsUntrusted Path(dst, src.getName());
      }
      dst = getChecksumFile(dst);
      if (localFs.exists(dst)) { //remove old local checksum file
        localFs.delete(dst, true);
      }
      @OsUntrusted
      Path checksumFile = getChecksumFile(src);
      if (copyCrc && fs.exists(checksumFile)) { //copy checksum file
        fs.copyToLocalFile(checksumFile, dst);
      }
    } else {
      @OsUntrusted
      FileStatus @OsUntrusted [] srcs = listStatus(src);
      for (@OsUntrusted FileStatus srcFile : srcs) {
        copyToLocalFile(srcFile.getPath(), 
                        new @OsUntrusted Path(dst, srcFile.getPath().getName()), copyCrc);
      }
    }
  }

  @Override
  public @OsUntrusted Path startLocalOutput(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path fsOutputFile, @OsUntrusted Path tmpLocalFile)
    throws IOException {
    return tmpLocalFile;
  }

  @Override
  public void completeLocalOutput(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path fsOutputFile, @OsUntrusted Path tmpLocalFile)
    throws IOException {
    moveFromLocalFile(tmpLocalFile, fsOutputFile);
  }

  /**
   * Report a checksum error to the file system.
   * @param f the file name containing the error
   * @param in the stream open on the file
   * @param inPos the position of the beginning of the bad data in the file
   * @param sums the stream open on the checksum file
   * @param sumsPos the position of the beginning of the bad data in the checksum file
   * @return if retry is neccessary
   */
  public @OsUntrusted boolean reportChecksumFailure(@OsUntrusted ChecksumFileSystem this, @OsUntrusted Path f, @OsUntrusted FSDataInputStream in,
                                       @OsUntrusted
                                       long inPos, @OsUntrusted FSDataInputStream sums, @OsUntrusted long sumsPos) {
    return false;
  }
}
