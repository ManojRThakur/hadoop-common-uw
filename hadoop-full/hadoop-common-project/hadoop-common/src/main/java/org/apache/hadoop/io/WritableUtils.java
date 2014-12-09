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
import java.io.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@InterfaceAudience.Public
@InterfaceStability.Stable
public final class WritableUtils  {

  public static @OsUntrusted byte @OsUntrusted [] readCompressedByteArray(@OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    int length = in.readInt();
    if (length == -1) return null;
    @OsUntrusted
    byte @OsUntrusted [] buffer = new @OsUntrusted byte @OsUntrusted [length];
    in.readFully(buffer);      // could/should use readFully(buffer,0,length)?
    @OsUntrusted
    GZIPInputStream gzi = new @OsUntrusted GZIPInputStream(new @OsUntrusted ByteArrayInputStream(buffer, 0, buffer.length));
    @OsUntrusted
    byte @OsUntrusted [] outbuf = new @OsUntrusted byte @OsUntrusted [length];
    @OsUntrusted
    ByteArrayOutputStream bos =  new @OsUntrusted ByteArrayOutputStream();
    @OsUntrusted
    int len;
    while((len=gzi.read(outbuf, 0, outbuf.length)) != -1){
      bos.write(outbuf, 0, len);
    }
    @OsUntrusted
    byte @OsUntrusted [] decompressed =  bos.toByteArray();
    bos.close();
    gzi.close();
    return decompressed;
  }

  public static void skipCompressedByteArray(@OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    int length = in.readInt();
    if (length != -1) {
      skipFully(in, length);
    }
  }

  public static @OsUntrusted int  writeCompressedByteArray(@OsUntrusted DataOutput out, 
                                              @OsUntrusted
                                              byte @OsUntrusted [] bytes) throws IOException {
    if (bytes != null) {
      @OsUntrusted
      ByteArrayOutputStream bos =  new @OsUntrusted ByteArrayOutputStream();
      @OsUntrusted
      GZIPOutputStream gzout = new @OsUntrusted GZIPOutputStream(bos);
      try {
        gzout.write(bytes, 0, bytes.length);
        gzout.close();
        gzout = null;
      } finally {
        IOUtils.closeStream(gzout);
      }
      @OsUntrusted
      byte @OsUntrusted [] buffer = bos.toByteArray();
      @OsUntrusted
      int len = buffer.length;
      out.writeInt(len);
      out.write(buffer, 0, len);
      /* debug only! Once we have confidence, can lose this. */
      return ((bytes.length != 0) ? (100*buffer.length)/bytes.length : 0);
    } else {
      out.writeInt(-1);
      return -1;
    }
  }


  /* Ugly utility, maybe someone else can do this better  */
  public static @OsUntrusted String readCompressedString(@OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    byte @OsUntrusted [] bytes = readCompressedByteArray(in);
    if (bytes == null) return null;
    return new @OsUntrusted String(bytes, "UTF-8");
  }


  public static @OsUntrusted int  writeCompressedString(@OsUntrusted DataOutput out, @OsUntrusted String s) throws IOException {
    return writeCompressedByteArray(out, (s != null) ? s.getBytes("UTF-8") : null);
  }

  /*
   *
   * Write a String as a Network Int n, followed by n Bytes
   * Alternative to 16 bit read/writeUTF.
   * Encoding standard is... ?
   * 
   */
  public static void writeString(@OsUntrusted DataOutput out, @OsUntrusted String s) throws IOException {
    if (s != null) {
      @OsUntrusted
      byte @OsUntrusted [] buffer = s.getBytes("UTF-8");
      @OsUntrusted
      int len = buffer.length;
      out.writeInt(len);
      out.write(buffer, 0, len);
    } else {
      out.writeInt(-1);
    }
  }

  /*
   * Read a String as a Network Int n, followed by n Bytes
   * Alternative to 16 bit read/writeUTF.
   * Encoding standard is... ?
   *
   */
  public static @OsUntrusted String readString(@OsUntrusted DataInput in) throws IOException{
    @OsUntrusted
    int length = in.readInt();
    if (length == -1) return null;
    @OsUntrusted
    byte @OsUntrusted [] buffer = new @OsUntrusted byte @OsUntrusted [length];
    in.readFully(buffer);      // could/should use readFully(buffer,0,length)?
    return new @OsUntrusted String(buffer,"UTF-8");  
  }


  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
   * Could be generalised using introspection.
   *
   */
  public static void writeStringArray(@OsUntrusted DataOutput out, @OsUntrusted String @OsUntrusted [] s) throws IOException{
    out.writeInt(s.length);
    for(@OsUntrusted int i = 0; i < s.length; i++) {
      writeString(out, s[i]);
    }
  }

  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array of
   * compressed Strings. Handles also null arrays and null values.
   * Could be generalised using introspection.
   *
   */
  public static void writeCompressedStringArray(@OsUntrusted DataOutput out, @OsUntrusted String @OsUntrusted [] s) throws IOException{
    if (s == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(s.length);
    for(@OsUntrusted int i = 0; i < s.length; i++) {
      writeCompressedString(out, s[i]);
    }
  }

  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
   * Could be generalised using introspection. Actually this bit couldn't...
   *
   */
  public static @OsUntrusted String @OsUntrusted [] readStringArray(@OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    int len = in.readInt();
    if (len == -1) return null;
    @OsUntrusted
    String @OsUntrusted [] s = new @OsUntrusted String @OsUntrusted [len];
    for(@OsUntrusted int i = 0; i < len; i++) {
      s[i] = readString(in);
    }
    return s;
  }


  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
   * Could be generalised using introspection. Handles null arrays and null values.
   *
   */
  public static  @OsUntrusted String @OsUntrusted [] readCompressedStringArray(@OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    int len = in.readInt();
    if (len == -1) return null;
    @OsUntrusted
    String @OsUntrusted [] s = new @OsUntrusted String @OsUntrusted [len];
    for(@OsUntrusted int i = 0; i < len; i++) {
      s[i] = readCompressedString(in);
    }
    return s;
  }


  /*
   *
   * Test Utility Method Display Byte Array. 
   *
   */
  public static void displayByteArray(@OsUntrusted byte @OsUntrusted [] record){
    @OsUntrusted
    int i;
    for(i=0;i < record.length -1; i++){
      if (i % 16 == 0) { System.out.println(); }
      System.out.print(Integer.toHexString(record[i]  >> 4 & 0x0F));
      System.out.print(Integer.toHexString(record[i] & 0x0F));
      System.out.print(",");
    }
    System.out.print(Integer.toHexString(record[i]  >> 4 & 0x0F));
    System.out.print(Integer.toHexString(record[i] & 0x0F));
    System.out.println();
  }

  /**
   * Make a copy of a writable object using serialization to a buffer.
   * @param orig The object to copy
   * @return The copied object
   */
  public static <@OsUntrusted T extends @OsUntrusted Writable> @OsUntrusted T clone(@OsUntrusted T orig, @OsUntrusted Configuration conf) {
    try {
      @SuppressWarnings("unchecked") // Unchecked cast from Class to Class<T>
      @OsUntrusted
      T newInst = ReflectionUtils.newInstance((@OsUntrusted Class<@OsUntrusted T>) orig.getClass(), conf);
      ReflectionUtils.copy(conf, orig, newInst);
      return newInst;
    } catch (@OsUntrusted IOException e) {
      throw new @OsUntrusted RuntimeException("Error writing/reading clone buffer", e);
    }
  }

  /**
   * Make a copy of the writable object using serialiation to a buffer
   * @param dst the object to copy from
   * @param src the object to copy into, which is destroyed
   * @throws IOException
   * @deprecated use ReflectionUtils.cloneInto instead.
   */
  @Deprecated
  public static void cloneInto(@OsUntrusted Writable dst, @OsUntrusted Writable src) throws IOException {
    ReflectionUtils.cloneWritableInto(dst, src);
  }

  /**
   * Serializes an integer to a binary stream with zero-compressed encoding.
   * For -120 <= i <= 127, only one byte is used with the actual value.
   * For other values of i, the first byte value indicates whether the
   * integer is positive or negative, and the number of bytes that follow.
   * If the first byte value v is between -121 and -124, the following integer
   * is positive, with number of bytes that follow are -(v+120).
   * If the first byte value v is between -125 and -128, the following integer
   * is negative, with number of bytes that follow are -(v+124). Bytes are
   * stored in the high-non-zero-byte-first order.
   *
   * @param stream Binary output stream
   * @param i Integer to be serialized
   * @throws java.io.IOException 
   */
  public static void writeVInt(@OsUntrusted DataOutput stream, @OsUntrusted int i) throws IOException {
    writeVLong(stream, i);
  }
  
  /**
   * Serializes a long to a binary stream with zero-compressed encoding.
   * For -112 <= i <= 127, only one byte is used with the actual value.
   * For other values of i, the first byte value indicates whether the
   * long is positive or negative, and the number of bytes that follow.
   * If the first byte value v is between -113 and -120, the following long
   * is positive, with number of bytes that follow are -(v+112).
   * If the first byte value v is between -121 and -128, the following long
   * is negative, with number of bytes that follow are -(v+120). Bytes are
   * stored in the high-non-zero-byte-first order.
   * 
   * @param stream Binary output stream
   * @param i Long to be serialized
   * @throws java.io.IOException 
   */
  public static void writeVLong(@OsUntrusted DataOutput stream, @OsUntrusted long i) throws IOException {
    if (i >= -112 && i <= 127) {
      stream.writeByte((@OsUntrusted byte)i);
      return;
    }
      
    @OsUntrusted
    int len = -112;
    if (i < 0) {
      i ^= -1L; // take one's complement'
      len = -120;
    }
      
    @OsUntrusted
    long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }
      
    stream.writeByte((@OsUntrusted byte)len);
      
    len = (len < -120) ? -(len + 120) : -(len + 112);
      
    for (@OsUntrusted int idx = len; idx != 0; idx--) {
      @OsUntrusted
      int shiftbits = (idx - 1) * 8;
      @OsUntrusted
      long mask = 0xFFL << shiftbits;
      stream.writeByte((@OsUntrusted byte)((i & mask) >> shiftbits));
    }
  }
  

  /**
   * Reads a zero-compressed encoded long from input stream and returns it.
   * @param stream Binary input stream
   * @throws java.io.IOException 
   * @return deserialized long from stream.
   */
  public static @OsUntrusted long readVLong(@OsUntrusted DataInput stream) throws IOException {
    @OsUntrusted
    byte firstByte = stream.readByte();
    @OsUntrusted
    int len = decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    @OsUntrusted
    long i = 0;
    for (@OsUntrusted int idx = 0; idx < len-1; idx++) {
      @OsUntrusted
      byte b = stream.readByte();
      i = i << 8;
      i = i | (b & 0xFF);
    }
    @SuppressWarnings("ostrusted") // TypeChecking error none of the results need to be trusted
    long result = (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
    return result;
  }

  /**
   * Reads a zero-compressed encoded integer from input stream and returns it.
   * @param stream Binary input stream
   * @throws java.io.IOException 
   * @return deserialized integer from stream.
   */
  public static @OsUntrusted int readVInt(@OsUntrusted DataInput stream) throws IOException {
    @OsUntrusted
    long n = readVLong(stream);
    if ((n > Integer.MAX_VALUE) || (n < Integer.MIN_VALUE)) {
      throw new @OsUntrusted IOException("value too long to fit in integer");
    }
    return (@OsUntrusted int)n;
  }

  /**
   * Reads an integer from the input stream and returns it.
   *
   * This function validates that the integer is between [lower, upper],
   * inclusive.
   *
   * @param stream Binary input stream
   * @throws java.io.IOException
   * @return deserialized integer from stream
   */
  public static @OsUntrusted int readVIntInRange(@OsUntrusted DataInput stream, @OsUntrusted int lower, @OsUntrusted int upper)
      throws IOException {
    @OsUntrusted
    long n = readVLong(stream);
    if (n < lower) {
      if (lower == 0) {
        throw new @OsUntrusted IOException("expected non-negative integer, got " + n);
      } else {
        throw new @OsUntrusted IOException("expected integer greater than or equal to " +
            lower + ", got " + n);
      }
    }
    if (n > upper) {
      throw new @OsUntrusted IOException("expected integer less or equal to " + upper +
          ", got " + n);
    }
    return (@OsUntrusted int)n;
  }

  /**
   * Given the first byte of a vint/vlong, determine the sign
   * @param value the first byte
   * @return is the value negative
   */
  public static @OsUntrusted boolean isNegativeVInt(@OsUntrusted byte value) {
    return value < -120 || (value >= -112 && value < 0);
  }

  /**
   * Parse the first byte of a vint/vlong to determine the number of bytes
   * @param value the first byte of the vint/vlong
   * @return the total number of bytes (1 to 9)
   */
  public static @OsUntrusted int decodeVIntSize(@OsUntrusted byte value) {
    if (value >= -112) {
      return 1;
    } else if (value < -120) {
      return -119 - value;
    }
    return -111 - value;
  }

  /**
   * Get the encoded length if an integer is stored in a variable-length format
   * @return the encoded length 
   */
  public static @OsUntrusted int getVIntSize(@OsUntrusted long i) {
    if (i >= -112 && i <= 127) {
      return 1;
    }
      
    if (i < 0) {
      i ^= -1L; // take one's complement'
    }
    // find the number of bytes with non-leading zeros
    @OsUntrusted
    int dataBits = Long.SIZE - Long.numberOfLeadingZeros(i);
    // find the number of data bytes + length byte
    return (dataBits + 7) / 8 + 1;
  }
  /**
   * Read an Enum value from DataInput, Enums are read and written 
   * using String values. 
   * @param <T> Enum type
   * @param in DataInput to read from 
   * @param enumType Class type of Enum
   * @return Enum represented by String read from DataInput
   * @throws IOException
   */
  public static <@OsUntrusted T extends @OsUntrusted Enum<@OsUntrusted T>> @OsUntrusted T readEnum(@OsUntrusted DataInput in, @OsUntrusted Class<@OsUntrusted T> enumType)
    throws IOException{
    return T.valueOf(enumType, Text.readString(in));
  }
  /**
   * writes String value of enum to DataOutput. 
   * @param out Dataoutput stream
   * @param enumVal enum value
   * @throws IOException
   */
  public static void writeEnum(@OsUntrusted DataOutput out,  @OsUntrusted Enum<@OsUntrusted ? extends java.lang.@OsUntrusted Object> enumVal) 
    throws IOException{
    Text.writeString(out, enumVal.name()); 
  }
  /**
   * Skip <i>len</i> number of bytes in input stream<i>in</i>
   * @param in input stream
   * @param len number of bytes to skip
   * @throws IOException when skipped less number of bytes
   */
  public static void skipFully(@OsUntrusted DataInput in, @OsUntrusted int len) throws IOException {
    @OsUntrusted
    int total = 0;
    @OsUntrusted
    int cur = 0;

    while ((total<len) && ((cur = in.skipBytes(len-total)) > 0)) {
        total += cur;
    }

    if (total<len) {
      throw new @OsUntrusted IOException("Not able to skip " + len + " bytes, possibly " +
                            "due to end of input.");
    }
  }

  /** Convert writables to a byte array */
  public static @OsUntrusted byte @OsUntrusted [] toByteArray(@OsUntrusted Writable @OsUntrusted ... writables) {
    final @OsUntrusted DataOutputBuffer out = new @OsUntrusted DataOutputBuffer();
    try {
      for(@OsUntrusted Writable w : writables) {
        w.write(out);
      }
      out.close();
    } catch (@OsUntrusted IOException e) {
      throw new @OsUntrusted RuntimeException("Fail to convert writables to a byte array",e);
    }
    return out.getData();
  }

  /**
   * Read a string, but check it for sanity. The format consists of a vint
   * followed by the given number of bytes.
   * @param in the stream to read from
   * @param maxLength the largest acceptable length of the encoded string
   * @return the bytes as a string
   * @throws IOException if reading from the DataInput fails
   * @throws IllegalArgumentException if the encoded byte size for string 
             is negative or larger than maxSize. Only the vint is read.
   */
  public static @OsUntrusted String readStringSafely(@OsUntrusted DataInput in,
                                        @OsUntrusted
                                        int maxLength
                                        ) throws IOException, 
                                                 IllegalArgumentException {
    @OsUntrusted
    int length = readVInt(in);
    if (length < 0 || length > maxLength) {
      throw new @OsUntrusted IllegalArgumentException("Encoded byte size for String was " + length + 
                                         ", which is outside of 0.." +
                                         maxLength + " range.");
    }
    @OsUntrusted
    byte @OsUntrusted [] bytes = new @OsUntrusted byte @OsUntrusted [length];
    in.readFully(bytes, 0, length);
    return Text.decode(bytes);
  }
}
