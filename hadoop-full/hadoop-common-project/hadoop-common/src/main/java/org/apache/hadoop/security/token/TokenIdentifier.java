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

package org.apache.hadoop.security.token;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * An identifier that identifies a token, may contain public information 
 * about a token, including its kind (or type).
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public abstract class TokenIdentifier implements @OsUntrusted Writable {

  private @OsUntrusted String trackingId = null;

  /**
   * Get the token kind
   * @return the kind of the token
   */
  public abstract @OsUntrusted Text getKind(@OsUntrusted TokenIdentifier this);

  /**
   * Get the Ugi with the username encoded in the token identifier
   * 
   * @return the username. null is returned if username in the identifier is
   *         empty or null.
   */
  public abstract @OsUntrusted UserGroupInformation getUser(@OsUntrusted TokenIdentifier this);

  /**
   * Get the bytes for the token identifier
   * @return the bytes of the identifier
   */
  public @OsUntrusted byte @OsUntrusted [] getBytes(@OsUntrusted TokenIdentifier this) {
    @OsUntrusted
    DataOutputBuffer buf = new @OsUntrusted DataOutputBuffer(4096);
    try {
      this.write(buf);
    } catch (@OsUntrusted IOException ie) {
      throw new @OsUntrusted RuntimeException("i/o error in getBytes", ie);
    }
    return Arrays.copyOf(buf.getData(), buf.getLength());
  }

  /**
   * Returns a tracking identifier that can be used to associate usages of a
   * token across multiple client sessions.
   *
   * Currently, this function just returns an MD5 of {{@link #getBytes()}.
   *
   * @return tracking identifier
   */
  public @OsUntrusted String getTrackingId(@OsUntrusted TokenIdentifier this) {
    if (trackingId == null) {
      trackingId = DigestUtils.md5Hex(getBytes());
    }
    return trackingId;
  }

  //Need to widen the readFields, this would be better taken care of through qualifier polymorphism
  public abstract void readFields(java.io. @OsUntrusted DataInput in) throws IOException;
}
