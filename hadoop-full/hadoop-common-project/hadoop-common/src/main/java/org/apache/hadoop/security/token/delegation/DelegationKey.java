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

package org.apache.hadoop.security.token.delegation;

import ostrusted.quals.OsUntrusted;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import javax.crypto.SecretKey;

import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Key used for generating and verifying delegation tokens
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DelegationKey implements @OsUntrusted Writable {
  private @OsUntrusted int keyId;
  private @OsUntrusted long expiryDate;
  @Nullable
  private @OsUntrusted byte @OsUntrusted [] keyBytes = null;
  private static final @OsUntrusted int MAX_KEY_LEN = 1024 * 1024;

  /** Default constructore required for Writable */
  public @OsUntrusted DelegationKey() {
    this(0, 0L, (@OsUntrusted SecretKey)null);
  }

  public @OsUntrusted DelegationKey(@OsUntrusted int keyId, @OsUntrusted long expiryDate, @OsUntrusted SecretKey key) {
    this(keyId, expiryDate, key != null ? key.getEncoded() : null);
  }
  
  public @OsUntrusted DelegationKey(@OsUntrusted int keyId, @OsUntrusted long expiryDate, @OsUntrusted byte @OsUntrusted [] encodedKey) {
    this.keyId = keyId;
    this.expiryDate = expiryDate;
    if (encodedKey != null) {
      if (encodedKey.length > MAX_KEY_LEN) {
        throw new @OsUntrusted RuntimeException("can't create " + encodedKey.length +
            " byte long DelegationKey.");
      }
      this.keyBytes = encodedKey;
    }
  }

  public @OsUntrusted int getKeyId(@OsUntrusted DelegationKey this) {
    return keyId;
  }

  public @OsUntrusted long getExpiryDate(@OsUntrusted DelegationKey this) {
    return expiryDate;
  }

  public @OsUntrusted SecretKey getKey(@OsUntrusted DelegationKey this) {
    if (keyBytes == null || keyBytes.length == 0) {
      return null;
    } else {
      @OsUntrusted
      SecretKey key = AbstractDelegationTokenSecretManager.createSecretKey(keyBytes);
      return key;
    }
  }
  
  public @OsUntrusted byte @OsUntrusted [] getEncodedKey(@OsUntrusted DelegationKey this) {
    return keyBytes;
  }

  public void setExpiryDate(@OsUntrusted DelegationKey this, @OsUntrusted long expiryDate) {
    this.expiryDate = expiryDate;
  }

  /**
   */
  @Override
  public void write(@OsUntrusted DelegationKey this, @OsUntrusted DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, keyId);
    WritableUtils.writeVLong(out, expiryDate);
    if (keyBytes == null) {
      WritableUtils.writeVInt(out, -1);
    } else {
      WritableUtils.writeVInt(out, keyBytes.length);
      out.write(keyBytes);
    }
  }

  /**
   */
  @Override
  public void readFields(@OsUntrusted DelegationKey this, @OsUntrusted DataInput in) throws IOException {
    keyId = WritableUtils.readVInt(in);
    expiryDate = WritableUtils.readVLong(in);
    @OsUntrusted
    int len = WritableUtils.readVIntInRange(in, -1, MAX_KEY_LEN);
    if (len == -1) {
      keyBytes = null;
    } else {
      keyBytes = new @OsUntrusted byte @OsUntrusted [len];
      in.readFully(keyBytes);
    }
  }

  @Override
  public @OsUntrusted int hashCode(@OsUntrusted DelegationKey this) {
    final @OsUntrusted int prime = 31;
    @OsUntrusted
    int result = 1;
    result = prime * result + (@OsUntrusted int) (expiryDate ^ (expiryDate >>> 32));
    result = prime * result + Arrays.hashCode(keyBytes);
    result = prime * result + keyId;
    return result;
  }

  @Override
  public @OsUntrusted boolean equals(@OsUntrusted DelegationKey this, @OsUntrusted Object right) {
    if (this == right) {
      return true;
    } else if (right == null || getClass() != right.getClass()) {
      return false;
    } else {
      @OsUntrusted
      DelegationKey r = (@OsUntrusted DelegationKey) right;
      return keyId == r.keyId &&
             expiryDate == r.expiryDate &&
             Arrays.equals(keyBytes, r.keyBytes);
    }
  }

}
