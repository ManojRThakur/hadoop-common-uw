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

package org.apache.hadoop.util;

import ostrusted.quals.OsUntrusted;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;

/**
 * This class provides inteface and utilities for processing checksums for
 * DFS data transfers.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DataChecksum implements @OsUntrusted Checksum {
  
  // Misc constants
  public static final @OsUntrusted int HEADER_LEN = 5; /// 1 byte type and 4 byte len
  
  // checksum types
  public static final @OsUntrusted int CHECKSUM_NULL    = 0;
  public static final @OsUntrusted int CHECKSUM_CRC32   = 1;
  public static final @OsUntrusted int CHECKSUM_CRC32C  = 2;
  public static final @OsUntrusted int CHECKSUM_DEFAULT = 3; 
  public static final @OsUntrusted int CHECKSUM_MIXED   = 4;
 
  /** The checksum types */
  public static enum Type {

@OsUntrusted  NULL  (CHECKSUM_NULL, 0),

@OsUntrusted  CRC32 (CHECKSUM_CRC32, 4),

@OsUntrusted  CRC32C(CHECKSUM_CRC32C, 4),

@OsUntrusted  DEFAULT(CHECKSUM_DEFAULT, 0), // This cannot be used to create DataChecksum

@OsUntrusted  MIXED (CHECKSUM_MIXED, 0); // This cannot be used to create DataChecksum

    public final @OsUntrusted int id;
    public final @OsUntrusted int size;
    
    private @OsUntrusted Type(@OsUntrusted int id, @OsUntrusted int size) {
      this.id = id;
      this.size = size;
    }

    /** @return the type corresponding to the id. */
    public static @OsUntrusted Type valueOf(@OsUntrusted int id) {
      if (id < 0 || id >= values().length) {
        throw new @OsUntrusted IllegalArgumentException("id=" + id
            + " out of range [0, " + values().length + ")");
      }
      return values()[id];
    }
  }


  public static @OsUntrusted DataChecksum newDataChecksum(@OsUntrusted Type type, @OsUntrusted int bytesPerChecksum ) {
    if ( bytesPerChecksum <= 0 ) {
      return null;
    }
    
    switch ( type ) {
    case NULL :
      return new @OsUntrusted DataChecksum(type, new @OsUntrusted ChecksumNull(), bytesPerChecksum );
    case CRC32 :
      return new @OsUntrusted DataChecksum(type, new @OsUntrusted PureJavaCrc32(), bytesPerChecksum );
    case CRC32C:
      return new @OsUntrusted DataChecksum(type, new @OsUntrusted PureJavaCrc32C(), bytesPerChecksum);
    default:
      return null;  
    }
  }
  
  /**
   * Creates a DataChecksum from HEADER_LEN bytes from arr[offset].
   * @return DataChecksum of the type in the array or null in case of an error.
   */
  public static @OsUntrusted DataChecksum newDataChecksum( @OsUntrusted byte bytes @OsUntrusted [], @OsUntrusted int offset ) {
    if ( offset < 0 || bytes.length < offset + HEADER_LEN ) {
      return null;
    }
    
    // like readInt():
    @OsUntrusted
    int bytesPerChecksum = ( (bytes[offset+1] & 0xff) << 24 ) | 
                           ( (bytes[offset+2] & 0xff) << 16 ) |
                           ( (bytes[offset+3] & 0xff) << 8 )  |
                           ( (bytes[offset+4] & 0xff) );
    return newDataChecksum( Type.valueOf(bytes[offset]), bytesPerChecksum );
  }
  
  /**
   * This constructucts a DataChecksum by reading HEADER_LEN bytes from
   * input stream <i>in</i>
   */
  public static @OsUntrusted DataChecksum newDataChecksum( @OsUntrusted DataInputStream in )
                                 throws IOException {
    @OsUntrusted
    int type = in.readByte();
    @OsUntrusted
    int bpc = in.readInt();
    @OsUntrusted
    DataChecksum summer = newDataChecksum(Type.valueOf(type), bpc );
    if ( summer == null ) {
      throw new @OsUntrusted IOException( "Could not create DataChecksum of type " +
                             type + " with bytesPerChecksum " + bpc );
    }
    return summer;
  }
  
  /**
   * Writes the checksum header to the output stream <i>out</i>.
   */
  public void writeHeader( @OsUntrusted DataChecksum this, @OsUntrusted DataOutputStream out ) 
                           throws IOException { 
    out.writeByte( type.id );
    out.writeInt( bytesPerChecksum );
  }

  public @OsUntrusted byte @OsUntrusted [] getHeader(@OsUntrusted DataChecksum this) {
    @OsUntrusted
    byte @OsUntrusted [] header = new @OsUntrusted byte @OsUntrusted [DataChecksum.HEADER_LEN];
    header[0] = (@OsUntrusted byte) (type.id & 0xff);
    // Writing in buffer just like DataOutput.WriteInt()
    header[1+0] = (@OsUntrusted byte) ((bytesPerChecksum >>> 24) & 0xff);
    header[1+1] = (@OsUntrusted byte) ((bytesPerChecksum >>> 16) & 0xff);
    header[1+2] = (@OsUntrusted byte) ((bytesPerChecksum >>> 8) & 0xff);
    header[1+3] = (@OsUntrusted byte) (bytesPerChecksum & 0xff);
    return header;
  }
  
  /**
   * Writes the current checksum to the stream.
   * If <i>reset</i> is true, then resets the checksum.
   * @return number of bytes written. Will be equal to getChecksumSize();
   */
   public @OsUntrusted int writeValue( @OsUntrusted DataChecksum this, @OsUntrusted DataOutputStream out, @OsUntrusted boolean reset )
                          throws IOException {
     if ( type.size <= 0 ) {
       return 0;
     }

     if ( type.size == 4 ) {
       out.writeInt( (@OsUntrusted int) summer.getValue() );
     } else {
       throw new @OsUntrusted IOException( "Unknown Checksum " + type );
     }
     
     if ( reset ) {
       reset();
     }
     
     return type.size;
   }
   
   /**
    * Writes the current checksum to a buffer.
    * If <i>reset</i> is true, then resets the checksum.
    * @return number of bytes written. Will be equal to getChecksumSize();
    */
    public @OsUntrusted int writeValue( @OsUntrusted DataChecksum this, @OsUntrusted byte @OsUntrusted [] buf, @OsUntrusted int offset, @OsUntrusted boolean reset )
                           throws IOException {
      if ( type.size <= 0 ) {
        return 0;
      }

      if ( type.size == 4 ) {
        @OsUntrusted
        int checksum = (@OsUntrusted int) summer.getValue();
        buf[offset+0] = (@OsUntrusted byte) ((checksum >>> 24) & 0xff);
        buf[offset+1] = (@OsUntrusted byte) ((checksum >>> 16) & 0xff);
        buf[offset+2] = (@OsUntrusted byte) ((checksum >>> 8) & 0xff);
        buf[offset+3] = (@OsUntrusted byte) (checksum & 0xff);
      } else {
        throw new @OsUntrusted IOException( "Unknown Checksum " + type );
      }
      
      if ( reset ) {
        reset();
      }
      
      return type.size;
    }
   
   /**
    * Compares the checksum located at buf[offset] with the current checksum.
    * @return true if the checksum matches and false otherwise.
    */
   public @OsUntrusted boolean compare( @OsUntrusted DataChecksum this, @OsUntrusted byte buf @OsUntrusted [], @OsUntrusted int offset ) {
     if ( type.size == 4 ) {
       @OsUntrusted
       int checksum = ( (buf[offset+0] & 0xff) << 24 ) | 
                      ( (buf[offset+1] & 0xff) << 16 ) |
                      ( (buf[offset+2] & 0xff) << 8 )  |
                      ( (buf[offset+3] & 0xff) );
       return checksum == (@OsUntrusted int) summer.getValue();
     }
     return type.size == 0;
   }
   
  private final @OsUntrusted Type type;
  private final @OsUntrusted Checksum summer;
  private final @OsUntrusted int bytesPerChecksum;
  private @OsUntrusted int inSum = 0;
  
  private @OsUntrusted DataChecksum( @OsUntrusted Type type, @OsUntrusted Checksum checksum, @OsUntrusted int chunkSize ) {
    this.type = type;
    summer = checksum;
    bytesPerChecksum = chunkSize;
  }
  
  // Accessors
  public @OsUntrusted Type getChecksumType(@OsUntrusted DataChecksum this) {
    return type;
  }
  public @OsUntrusted int getChecksumSize(@OsUntrusted DataChecksum this) {
    return type.size;
  }
  public @OsUntrusted int getBytesPerChecksum(@OsUntrusted DataChecksum this) {
    return bytesPerChecksum;
  }
  public @OsUntrusted int getNumBytesInSum(@OsUntrusted DataChecksum this) {
    return inSum;
  }
  
  public static final @OsUntrusted int SIZE_OF_INTEGER = Integer.SIZE / Byte.SIZE;
  static public @OsUntrusted int getChecksumHeaderSize() {
    return 1 + SIZE_OF_INTEGER; // type byte, bytesPerChecksum int
  }
  //Checksum Interface. Just a wrapper around member summer.
  @Override
  public @OsUntrusted long getValue(@OsUntrusted DataChecksum this) {
    return summer.getValue();
  }
  @Override
  public void reset(@OsUntrusted DataChecksum this) {
    summer.reset();
    inSum = 0;
  }
  @Override
  public void update( @OsUntrusted DataChecksum this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len ) {
    if ( len > 0 ) {
      summer.update( b, off, len );
      inSum += len;
    }
  }
  @Override
  public void update( @OsUntrusted DataChecksum this, @OsUntrusted int b ) {
    summer.update( b );
    inSum += 1;
  }
  
  /**
   * Verify that the given checksums match the given data.
   * 
   * The 'mark' of the ByteBuffer parameters may be modified by this function,.
   * but the position is maintained.
   *  
   * @param data the DirectByteBuffer pointing to the data to verify.
   * @param checksums the DirectByteBuffer pointing to a series of stored
   *                  checksums
   * @param fileName the name of the file being read, for error-reporting
   * @param basePos the file position to which the start of 'data' corresponds
   * @throws ChecksumException if the checksums do not match
   */
  public void verifyChunkedSums(@OsUntrusted DataChecksum this, @OsUntrusted ByteBuffer data, @OsUntrusted ByteBuffer checksums,
      @OsUntrusted
      String fileName, @OsUntrusted long basePos)
  throws ChecksumException {
    if (type.size == 0) return;
    
    if (data.hasArray() && checksums.hasArray()) {
      verifyChunkedSums(
          data.array(), data.arrayOffset() + data.position(), data.remaining(),
          checksums.array(), checksums.arrayOffset() + checksums.position(),
          fileName, basePos);
      return;
    }
    if (NativeCrc32.isAvailable()) {
      NativeCrc32.verifyChunkedSums(bytesPerChecksum, type.id, checksums, data,
          fileName, basePos);
      return;
    }
    
    @OsUntrusted
    int startDataPos = data.position();
    data.mark();
    checksums.mark();
    try {
      @OsUntrusted
      byte @OsUntrusted [] buf = new @OsUntrusted byte @OsUntrusted [bytesPerChecksum];
      @OsUntrusted
      byte @OsUntrusted [] sum = new @OsUntrusted byte @OsUntrusted [type.size];
      while (data.remaining() > 0) {
        @OsUntrusted
        int n = Math.min(data.remaining(), bytesPerChecksum);
        checksums.get(sum);
        data.get(buf, 0, n);
        summer.reset();
        summer.update(buf, 0, n);
        @OsUntrusted
        int calculated = (@OsUntrusted int)summer.getValue();
        @OsUntrusted
        int stored = (sum[0] << 24 & 0xff000000) |
          (sum[1] << 16 & 0xff0000) |
          (sum[2] << 8 & 0xff00) |
          sum[3] & 0xff;
        if (calculated != stored) {
          @OsUntrusted
          long errPos = basePos + data.position() - startDataPos - n;
          throw new @OsUntrusted ChecksumException(
              "Checksum error: "+ fileName + " at "+ errPos +
              " exp: " + stored + " got: " + calculated, errPos);
        }
      }
    } finally {
      data.reset();
      checksums.reset();
    }
  }
  
  /**
   * Implementation of chunked verification specifically on byte arrays. This
   * is to avoid the copy when dealing with ByteBuffers that have array backing.
   */
  private void verifyChunkedSums(
      @OsUntrusted DataChecksum this, @OsUntrusted
      byte @OsUntrusted [] data, @OsUntrusted int dataOff, @OsUntrusted int dataLen,
      @OsUntrusted
      byte @OsUntrusted [] checksums, @OsUntrusted int checksumsOff, @OsUntrusted String fileName,
      @OsUntrusted
      long basePos) throws ChecksumException {
    
    @OsUntrusted
    int remaining = dataLen;
    @OsUntrusted
    int dataPos = 0;
    while (remaining > 0) {
      @OsUntrusted
      int n = Math.min(remaining, bytesPerChecksum);
      
      summer.reset();
      summer.update(data, dataOff + dataPos, n);
      dataPos += n;
      remaining -= n;
      
      @OsUntrusted
      int calculated = (@OsUntrusted int)summer.getValue();
      @OsUntrusted
      int stored = (checksums[checksumsOff] << 24 & 0xff000000) |
        (checksums[checksumsOff + 1] << 16 & 0xff0000) |
        (checksums[checksumsOff + 2] << 8 & 0xff00) |
        checksums[checksumsOff + 3] & 0xff;
      checksumsOff += 4;
      if (calculated != stored) {
        @OsUntrusted
        long errPos = basePos + dataPos - n;
        throw new @OsUntrusted ChecksumException(
            "Checksum error: "+ fileName + " at "+ errPos +
            " exp: " + stored + " got: " + calculated, errPos);
      }
    }
  }

  /**
   * Calculate checksums for the given data.
   * 
   * The 'mark' of the ByteBuffer parameters may be modified by this function,
   * but the position is maintained.
   * 
   * @param data the DirectByteBuffer pointing to the data to checksum.
   * @param checksums the DirectByteBuffer into which checksums will be
   *                  stored. Enough space must be available in this
   *                  buffer to put the checksums.
   */
  public void calculateChunkedSums(@OsUntrusted DataChecksum this, @OsUntrusted ByteBuffer data, @OsUntrusted ByteBuffer checksums) {
    if (type.size == 0) return;
    
    if (data.hasArray() && checksums.hasArray()) {
      calculateChunkedSums(data.array(), data.arrayOffset() + data.position(), data.remaining(),
          checksums.array(), checksums.arrayOffset() + checksums.position());
      return;
    }
    
    data.mark();
    checksums.mark();
    try {
      @OsUntrusted
      byte @OsUntrusted [] buf = new @OsUntrusted byte @OsUntrusted [bytesPerChecksum];
      while (data.remaining() > 0) {
        @OsUntrusted
        int n = Math.min(data.remaining(), bytesPerChecksum);
        data.get(buf, 0, n);
        summer.reset();
        summer.update(buf, 0, n);
        checksums.putInt((@OsUntrusted int)summer.getValue());
      }
    } finally {
      data.reset();
      checksums.reset();
    }
  }

  /**
   * Implementation of chunked calculation specifically on byte arrays. This
   * is to avoid the copy when dealing with ByteBuffers that have array backing.
   */
  private void calculateChunkedSums(
      @OsUntrusted DataChecksum this, @OsUntrusted
      byte @OsUntrusted [] data, @OsUntrusted int dataOffset, @OsUntrusted int dataLength,
      @OsUntrusted
      byte @OsUntrusted [] sums, @OsUntrusted int sumsOffset) {

    @OsUntrusted
    int remaining = dataLength;
    while (remaining > 0) {
      @OsUntrusted
      int n = Math.min(remaining, bytesPerChecksum);
      summer.reset();
      summer.update(data, dataOffset, n);
      dataOffset += n;
      remaining -= n;
      @OsUntrusted
      long calculated = summer.getValue();
      sums[sumsOffset++] = (@OsUntrusted byte) (calculated >> 24);
      sums[sumsOffset++] = (@OsUntrusted byte) (calculated >> 16);
      sums[sumsOffset++] = (@OsUntrusted byte) (calculated >> 8);
      sums[sumsOffset++] = (@OsUntrusted byte) (calculated);
    }
  }

  @Override
  public @OsUntrusted boolean equals(@OsUntrusted DataChecksum this, @OsUntrusted Object other) {
    if (!(other instanceof @OsUntrusted DataChecksum)) {
      return false;
    }
    @OsUntrusted
    DataChecksum o = (@OsUntrusted DataChecksum)other;
    return o.bytesPerChecksum == this.bytesPerChecksum &&
      o.type == this.type;
  }
  
  @Override
  public @OsUntrusted int hashCode(@OsUntrusted DataChecksum this) {
    return (this.type.id + 31) * this.bytesPerChecksum;
  }
  
  @Override
  public @OsUntrusted String toString(@OsUntrusted DataChecksum this) {
    return "DataChecksum(type=" + type +
      ", chunkSize=" + bytesPerChecksum + ")";
  }
  
  /**
   * This just provides a dummy implimentation for Checksum class
   * This is used when there is no checksum available or required for 
   * data
   */
  static class ChecksumNull implements @OsUntrusted Checksum {
    
    public @OsUntrusted ChecksumNull() {}
    
    //Dummy interface
    @Override
    public @OsUntrusted long getValue(DataChecksum.@OsUntrusted ChecksumNull this) { return 0; }
    @Override
    public void reset(DataChecksum.@OsUntrusted ChecksumNull this) {}
    @Override
    public void update(DataChecksum.@OsUntrusted ChecksumNull this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {}
    @Override
    public void update(DataChecksum.@OsUntrusted ChecksumNull this, @OsUntrusted int b) {}
  };
}
