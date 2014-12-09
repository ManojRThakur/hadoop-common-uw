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
package org.apache.hadoop.ipc;

import ostrusted.quals.OsUntrusted;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.hadoop.classification.InterfaceAudience;

import com.google.common.base.Preconditions;

/**
 * A class defining a set of static helper methods to provide conversion between
 * bytes and string for UUID-based client Id.
 */
@InterfaceAudience.Private
public class ClientId {
  
  /** The byte array of a UUID should be 16 */ 
  public static final @OsUntrusted int BYTE_LENGTH = 16;
  private static final @OsUntrusted int shiftWidth = 8;
  
  /**
   * Return clientId as byte[]
   */
  public static @OsUntrusted byte @OsUntrusted [] getClientId() {
    @OsUntrusted
    UUID uuid = UUID.randomUUID();
    @OsUntrusted
    ByteBuffer buf = ByteBuffer.wrap(new @OsUntrusted byte @OsUntrusted [BYTE_LENGTH]);
    buf.putLong(uuid.getMostSignificantBits());
    buf.putLong(uuid.getLeastSignificantBits());
    return buf.array();
  }
  
  /** Convert a clientId byte[] to string */
  public static @OsUntrusted String toString(@OsUntrusted byte @OsUntrusted [] clientId) {
    // clientId can be null or an empty array
    if (clientId == null || clientId.length == 0) {
      return "";
    }
    // otherwise should be 16 bytes
    Preconditions.checkArgument(clientId.length == BYTE_LENGTH);
    @OsUntrusted
    long msb = getMsb(clientId);
    @OsUntrusted
    long lsb = getLsb(clientId);
    return (new @OsUntrusted UUID(msb, lsb)).toString();
  }
  
  public static @OsUntrusted long getMsb(@OsUntrusted byte @OsUntrusted [] clientId) {
    @OsUntrusted
    long msb = 0;
    for (@OsUntrusted int i = 0; i < BYTE_LENGTH/2; i++) {
      msb = (msb << shiftWidth) | (clientId[i] & 0xff);
    }
    return msb;
  }
  
  public static @OsUntrusted long getLsb(@OsUntrusted byte @OsUntrusted [] clientId) {
    @OsUntrusted
    long lsb = 0;
    for (@OsUntrusted int i = BYTE_LENGTH/2; i < BYTE_LENGTH; i++) {
      lsb = (lsb << shiftWidth) | (clientId[i] & 0xff);
    }
    return lsb;
  }
  
  /** Convert from clientId string byte[] representation of clientId */
  public static @OsUntrusted byte @OsUntrusted [] toBytes(@OsUntrusted String id) {
    if (id == null || "".equals(id)) {
      return new @OsUntrusted byte @OsUntrusted [0];
    }
    @OsUntrusted
    UUID uuid = UUID.fromString(id);
    @OsUntrusted
    ByteBuffer buf = ByteBuffer.wrap(new @OsUntrusted byte @OsUntrusted [BYTE_LENGTH]);
    buf.putLong(uuid.getMostSignificantBits());
    buf.putLong(uuid.getLeastSignificantBits());
    return buf.array();
  }

}
