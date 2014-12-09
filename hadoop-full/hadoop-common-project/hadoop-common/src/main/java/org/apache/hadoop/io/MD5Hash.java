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
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.util.Arrays;
import java.security.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** A Writable for MD5 hash values.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MD5Hash implements @OsUntrusted WritableComparable<@OsUntrusted MD5Hash> {
  public static final @OsUntrusted int MD5_LEN = 16;

  private static @OsUntrusted ThreadLocal<@OsUntrusted MessageDigest> DIGESTER_FACTORY = new @OsUntrusted ThreadLocal<@OsUntrusted MessageDigest>() {
    @Override
    protected @OsUntrusted MessageDigest initialValue() {
      try {
        return MessageDigest.getInstance("MD5");
      } catch (@OsUntrusted NoSuchAlgorithmException e) {
        throw new @OsUntrusted RuntimeException(e);
      }
    }
  };

  private @OsUntrusted byte @OsUntrusted [] digest;

  /** Constructs an MD5Hash. */
  public @OsUntrusted MD5Hash() {
    this.digest = new @OsUntrusted byte @OsUntrusted [MD5_LEN];
  }

  /** Constructs an MD5Hash from a hex string. */
  public @OsUntrusted MD5Hash(@OsUntrusted String hex) {
    setDigest(hex);
  }
  
  /** Constructs an MD5Hash with a specified value. */
  public @OsUntrusted MD5Hash(@OsUntrusted byte @OsUntrusted [] digest) {
    if (digest.length != MD5_LEN)
      throw new @OsUntrusted IllegalArgumentException("Wrong length: " + digest.length);
    this.digest = digest;
  }
  
  // javadoc from Writable
  @Override
  public void readFields(@OsUntrusted MD5Hash this, @OsUntrusted DataInput in) throws IOException {
    in.readFully(digest);
  }

  /** Constructs, reads and returns an instance. */
  public static @OsUntrusted MD5Hash read(@OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    MD5Hash result = new @OsUntrusted MD5Hash();
    result.readFields(in);
    return result;
  }

  // javadoc from Writable
  @Override
  public void write(@OsUntrusted MD5Hash this, @OsUntrusted DataOutput out) throws IOException {
    out.write(digest);
  }

  /** Copy the contents of another instance into this instance. */
  public void set(@OsUntrusted MD5Hash this, @OsUntrusted MD5Hash that) {
    System.arraycopy(that.digest, 0, this.digest, 0, MD5_LEN);
  }

  /** Returns the digest bytes. */
  public @OsUntrusted byte @OsUntrusted [] getDigest(@OsUntrusted MD5Hash this) { return digest; }

  /** Construct a hash value for a byte array. */
  public static @OsUntrusted MD5Hash digest(@OsUntrusted byte @OsUntrusted [] data) {
    return digest(data, 0, data.length);
  }

  /**
   * Create a thread local MD5 digester
   */
  public static @OsUntrusted MessageDigest getDigester() {
    @OsUntrusted
    MessageDigest digester = DIGESTER_FACTORY.get();
    digester.reset();
    return digester;
  }

  /** Construct a hash value for the content from the InputStream. */
  public static @OsUntrusted MD5Hash digest(@OsUntrusted InputStream in) throws IOException {
    final @OsUntrusted byte @OsUntrusted [] buffer = new @OsUntrusted byte @OsUntrusted [4*1024]; 

    final @OsUntrusted MessageDigest digester = getDigester();
    for(@OsUntrusted int n; (n = in.read(buffer)) != -1; ) {
      digester.update(buffer, 0, n);
    }

    return new @OsUntrusted MD5Hash(digester.digest());
  }

  /** Construct a hash value for a byte array. */
  public static @OsUntrusted MD5Hash digest(@OsUntrusted byte @OsUntrusted [] data, @OsUntrusted int start, @OsUntrusted int len) {
    @OsUntrusted
    byte @OsUntrusted [] digest;
    @OsUntrusted
    MessageDigest digester = getDigester();
    digester.update(data, start, len);
    digest = digester.digest();
    return new @OsUntrusted MD5Hash(digest);
  }

  /** Construct a hash value for a String. */
  public static @OsUntrusted MD5Hash digest(@OsUntrusted String string) {
    return digest(UTF8.getBytes(string));
  }

  /** Construct a hash value for a String. */
  public static @OsUntrusted MD5Hash digest(@OsUntrusted UTF8 utf8) {
    return digest(utf8.getBytes(), 0, utf8.getLength());
  }

  /** Construct a half-sized version of this MD5.  Fits in a long **/
  public @OsUntrusted long halfDigest(@OsUntrusted MD5Hash this) {
    @OsUntrusted
    long value = 0;
    for (@OsUntrusted int i = 0; i < 8; i++)
      value |= ((digest[i] & 0xffL) << (8*(7-i)));
    return value;
  }

  /**
   * Return a 32-bit digest of the MD5.
   * @return the first 4 bytes of the md5
   */
  public @OsUntrusted int quarterDigest(@OsUntrusted MD5Hash this) {
    @OsUntrusted
    int value = 0;
    for (@OsUntrusted int i = 0; i < 4; i++)
      value |= ((digest[i] & 0xff) << (8*(3-i)));
    return value;    
  }

  /** Returns true iff <code>o</code> is an MD5Hash whose digest contains the
   * same values.  */
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted MD5Hash this, @OsUntrusted Object o) {
    if (!(o instanceof @OsUntrusted MD5Hash))
      return false;
    @OsUntrusted
    MD5Hash other = (@OsUntrusted MD5Hash)o;
    return Arrays.equals(this.digest, other.digest);
  }

  /** Returns a hash code value for this object.
   * Only uses the first 4 bytes, since md5s are evenly distributed.
   */
  @Override
  public @OsUntrusted int hashCode(@OsUntrusted MD5Hash this) {
    return quarterDigest();
  }


  /** Compares this object with the specified object for order.*/
  @Override
  public @OsUntrusted int compareTo(@OsUntrusted MD5Hash this, @OsUntrusted MD5Hash that) {
    return WritableComparator.compareBytes(this.digest, 0, MD5_LEN,
                                           that.digest, 0, MD5_LEN);
  }

  /** A WritableComparator optimized for MD5Hash keys. */
  public static class Comparator extends @OsUntrusted WritableComparator {
    public @OsUntrusted Comparator() {
      super(MD5Hash.class);
    }

    @Override
    public @OsUntrusted int compare(MD5Hash.@OsUntrusted Comparator this, @OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1,
                       @OsUntrusted
                       byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2) {
      return compareBytes(b1, s1, MD5_LEN, b2, s2, MD5_LEN);
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(MD5Hash.class, new @OsUntrusted Comparator());
  }

  private static final @OsUntrusted char @OsUntrusted [] HEX_DIGITS =
  new char @OsUntrusted [] {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};

  /** Returns a string representation of this object. */
  @Override
  public @OsUntrusted String toString(@OsUntrusted MD5Hash this) {
    @OsUntrusted
    StringBuilder buf = new @OsUntrusted StringBuilder(MD5_LEN*2);
    for (@OsUntrusted int i = 0; i < MD5_LEN; i++) {
      @OsUntrusted
      int b = digest[i];
      buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
      buf.append(HEX_DIGITS[b & 0xf]);
    }
    return buf.toString();
  }

  /** Sets the digest value from a hex string. */
  public void setDigest(@OsUntrusted MD5Hash this, @OsUntrusted String hex) {
    if (hex.length() != MD5_LEN*2)
      throw new @OsUntrusted IllegalArgumentException("Wrong length: " + hex.length());
    @OsUntrusted
    byte @OsUntrusted [] digest = new @OsUntrusted byte @OsUntrusted [MD5_LEN];
    for (@OsUntrusted int i = 0; i < MD5_LEN; i++) {
      @OsUntrusted
      int j = i << 1;
      digest[i] = (@OsUntrusted byte)(charToNibble(hex.charAt(j)) << 4 |
                         charToNibble(hex.charAt(j+1)));
    }
    this.digest = digest;
  }

  private static final @OsUntrusted int charToNibble(@OsUntrusted char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    } else if (c >= 'a' && c <= 'f') {
      return 0xa + (c - 'a');
    } else if (c >= 'A' && c <= 'F') {
      return 0xA + (c - 'A');
    } else {
      throw new @OsUntrusted RuntimeException("Not a hex character: " + c);
    }
  }


}
