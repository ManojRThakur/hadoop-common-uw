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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class UTF8ByteArrayUtils {
  /**
   * Find the first occurrence of the given byte b in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @param start starting offset
   * @param end ending position
   * @param b the byte to find
   * @return position that first byte occures otherwise -1
   */
  public static @OsUntrusted int findByte(@OsUntrusted byte @OsUntrusted [] utf, @OsUntrusted int start, @OsUntrusted int end, @OsUntrusted byte b) {
    for(@OsUntrusted int i=start; i<end; i++) {
      if (utf[i]==b) {
        return i;
      }
    }
    return -1;      
  }

  /**
   * Find the first occurrence of the given bytes b in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @param start starting offset
   * @param end ending position
   * @param b the bytes to find
   * @return position that first byte occures otherwise -1
   */
  public static @OsUntrusted int findBytes(@OsUntrusted byte @OsUntrusted [] utf, @OsUntrusted int start, @OsUntrusted int end, @OsUntrusted byte @OsUntrusted [] b) {
    @OsUntrusted
    int matchEnd = end - b.length;
    for(@OsUntrusted int i=start; i<=matchEnd; i++) {
      @OsUntrusted
      boolean matched = true;
      for(@OsUntrusted int j=0; j<b.length; j++) {
        if (utf[i+j] != b[j]) {
          matched = false;
          break;
        }
      }
      if (matched) {
        return i;
      }
    }
    return -1;      
  }
    
  /**
   * Find the nth occurrence of the given byte b in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @param start starting offset
   * @param length the length of byte array
   * @param b the byte to find
   * @param n the desired occurrence of the given byte
   * @return position that nth occurrence of the given byte if exists; otherwise -1
   */
  public static @OsUntrusted int findNthByte(@OsUntrusted byte @OsUntrusted [] utf, @OsUntrusted int start, @OsUntrusted int length, @OsUntrusted byte b, @OsUntrusted int n) {
    @OsUntrusted
    int pos = -1;
    @OsUntrusted
    int nextStart = start;
    for (@OsUntrusted int i = 0; i < n; i++) {
      pos = findByte(utf, nextStart, length, b);
      if (pos < 0) {
        return pos;
      }
      nextStart = pos + 1;
    }
    return pos;      
  }
  
  /**
   * Find the nth occurrence of the given byte b in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @param b the byte to find
   * @param n the desired occurrence of the given byte
   * @return position that nth occurrence of the given byte if exists; otherwise -1
   */
  public static @OsUntrusted int findNthByte(@OsUntrusted byte @OsUntrusted [] utf, @OsUntrusted byte b, @OsUntrusted int n) {
    return findNthByte(utf, 0, utf.length, b, n);      
  }

}

