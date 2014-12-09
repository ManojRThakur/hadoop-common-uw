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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.DataChecksum;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.znerd.xmlenc.XMLOutputter;

import org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;

/** MD5 of MD5 of CRC32. */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
public class MD5MD5CRC32FileChecksum extends @OsUntrusted FileChecksum {
  public static final @OsUntrusted int LENGTH = MD5Hash.MD5_LEN
      + (Integer.SIZE + Long.SIZE)/Byte.SIZE;

  private @OsUntrusted int bytesPerCRC;
  private @OsUntrusted long crcPerBlock;
  private @OsUntrusted MD5Hash md5;

  /** Same as this(0, 0, null) */
  public @OsUntrusted MD5MD5CRC32FileChecksum() {
    this(0, 0, null);
  }

  /** Create a MD5FileChecksum */
  public @OsUntrusted MD5MD5CRC32FileChecksum(@OsUntrusted int bytesPerCRC, @OsUntrusted long crcPerBlock, @OsUntrusted MD5Hash md5) {
    this.bytesPerCRC = bytesPerCRC;
    this.crcPerBlock = crcPerBlock;
    this.md5 = md5;
  }
  
  @Override
  public @OsUntrusted String getAlgorithmName(@OsUntrusted MD5MD5CRC32FileChecksum this) {
    return "MD5-of-" + crcPerBlock + "MD5-of-" + bytesPerCRC +
        getCrcType().name();
  }

  public static DataChecksum.@OsUntrusted Type getCrcTypeFromAlgorithmName(@OsUntrusted String algorithm)
      throws IOException {
    if (algorithm.endsWith(DataChecksum.Type.CRC32.name())) {
      return DataChecksum.Type.CRC32;
    } else if (algorithm.endsWith(DataChecksum.Type.CRC32C.name())) {
      return DataChecksum.Type.CRC32C;
    }

    throw new @OsUntrusted IOException("Unknown checksum type in " + algorithm);
  }
 
  @Override
  public @OsUntrusted int getLength(@OsUntrusted MD5MD5CRC32FileChecksum this) {return LENGTH;}
 
  @Override
  public @OsUntrusted byte @OsUntrusted [] getBytes(@OsUntrusted MD5MD5CRC32FileChecksum this) {
    return WritableUtils.toByteArray(this);
  }

  /** returns the CRC type */
  public DataChecksum.@OsUntrusted Type getCrcType(@OsUntrusted MD5MD5CRC32FileChecksum this) {
    // default to the one that is understood by all releases.
    return DataChecksum.Type.CRC32;
  }

  public @OsUntrusted ChecksumOpt getChecksumOpt(@OsUntrusted MD5MD5CRC32FileChecksum this) {
    return new @OsUntrusted ChecksumOpt(getCrcType(), bytesPerCRC);
  }

  @Override
  public void readFields(@OsUntrusted MD5MD5CRC32FileChecksum this, @OsUntrusted DataInput in) throws IOException {
    bytesPerCRC = in.readInt();
    crcPerBlock = in.readLong();
    md5 = MD5Hash.read(in);
  }
 
  @Override
  public void write(@OsUntrusted MD5MD5CRC32FileChecksum this, @OsUntrusted DataOutput out) throws IOException {
    out.writeInt(bytesPerCRC);
    out.writeLong(crcPerBlock);
    md5.write(out);    
  }

  /** Write that object to xml output. */
  public static void write(@OsUntrusted XMLOutputter xml, @OsUntrusted MD5MD5CRC32FileChecksum that
      ) throws IOException {
    xml.startTag(MD5MD5CRC32FileChecksum.class.getName());
    if (that != null) {
      xml.attribute("bytesPerCRC", "" + that.bytesPerCRC);
      xml.attribute("crcPerBlock", "" + that.crcPerBlock);
      xml.attribute("crcType", ""+ that.getCrcType().name());
      xml.attribute("md5", "" + that.md5);
    }
    xml.endTag();
  }

  /** Return the object represented in the attributes. */
  public static @OsUntrusted MD5MD5CRC32FileChecksum valueOf(@OsUntrusted Attributes attrs
      ) throws SAXException {
    final @OsUntrusted String bytesPerCRC = attrs.getValue("bytesPerCRC");
    final @OsUntrusted String crcPerBlock = attrs.getValue("crcPerBlock");
    final @OsUntrusted String md5 = attrs.getValue("md5");
    @OsUntrusted
    String crcType = attrs.getValue("crcType");
    DataChecksum.@OsUntrusted Type finalCrcType;
    if (bytesPerCRC == null || crcPerBlock == null || md5 == null) {
      return null;
    }

    try {
      // old versions don't support crcType.
      if (crcType == null || crcType.equals("")) {
        finalCrcType = DataChecksum.Type.CRC32;
      } else {
        finalCrcType = DataChecksum.Type.valueOf(crcType);
      }

      switch (finalCrcType) {
        case CRC32:
          return new @OsUntrusted MD5MD5CRC32GzipFileChecksum(
              Integer.valueOf(bytesPerCRC),
              Integer.valueOf(crcPerBlock),
              new @OsUntrusted MD5Hash(md5));
        case CRC32C:
          return new @OsUntrusted MD5MD5CRC32CastagnoliFileChecksum(
              Integer.valueOf(bytesPerCRC),
              Integer.valueOf(crcPerBlock),
              new @OsUntrusted MD5Hash(md5));
        default:
          // we should never get here since finalCrcType will
          // hold a valid type or we should have got an exception.
          return null;
      }
    } catch (@OsUntrusted Exception e) {
      throw new @OsUntrusted SAXException("Invalid attributes: bytesPerCRC=" + bytesPerCRC
          + ", crcPerBlock=" + crcPerBlock + ", crcType=" + crcType 
          + ", md5=" + md5, e);
    }
  }
 
  @Override
  public @OsUntrusted String toString(@OsUntrusted MD5MD5CRC32FileChecksum this) {
    return getAlgorithmName() + ":" + md5;
  }
}
