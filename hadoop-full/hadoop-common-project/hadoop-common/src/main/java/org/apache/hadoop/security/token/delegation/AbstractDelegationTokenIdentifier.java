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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.TokenIdentifier;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public abstract class AbstractDelegationTokenIdentifier 
extends @OsUntrusted TokenIdentifier {
  private static final @OsUntrusted byte VERSION = 0;

  private @OsUntrusted Text owner;
  private @OsUntrusted Text renewer;
  private @OsUntrusted Text realUser;
  private @OsUntrusted long issueDate;
  private @OsUntrusted long maxDate;
  private @OsUntrusted int sequenceNumber;
  private @OsUntrusted int masterKeyId = 0;
  
  public @OsUntrusted AbstractDelegationTokenIdentifier() {
    this(new @OsUntrusted Text(), new @OsUntrusted Text(), new @OsUntrusted Text());
  }
  
  public @OsUntrusted AbstractDelegationTokenIdentifier(@OsUntrusted Text owner, @OsUntrusted Text renewer, @OsUntrusted Text realUser) {
    if (owner == null) {
      this.owner = new @OsUntrusted Text();
    } else {
      this.owner = owner;
    }
    if (renewer == null) {
      this.renewer = new @OsUntrusted Text();
    } else {
      @OsUntrusted
      HadoopKerberosName renewerKrbName = new @OsUntrusted HadoopKerberosName(renewer.toString());
      try {
        this.renewer = new @OsUntrusted Text(renewerKrbName.getShortName());
      } catch (@OsUntrusted IOException e) {
        throw new @OsUntrusted RuntimeException(e);
      }
    }
    if (realUser == null) {
      this.realUser = new @OsUntrusted Text();
    } else {
      this.realUser = realUser;
    }
    issueDate = 0;
    maxDate = 0;
  }

  @Override
  public abstract @OsUntrusted Text getKind(@OsUntrusted AbstractDelegationTokenIdentifier this);
  
  /**
   * Get the username encoded in the token identifier
   * 
   * @return the username or owner
   */
  @Override
  public @OsUntrusted UserGroupInformation getUser(@OsUntrusted AbstractDelegationTokenIdentifier this) {
    if ( (owner == null) || (owner.toString().isEmpty())) {
      return null;
    }
    final @OsUntrusted UserGroupInformation realUgi;
    final @OsUntrusted UserGroupInformation ugi;
    if ((realUser == null) || (realUser.toString().isEmpty())
        || realUser.equals(owner)) {
      ugi = realUgi = UserGroupInformation.createRemoteUser(owner.toString());
    } else {
      realUgi = UserGroupInformation.createRemoteUser(realUser.toString());
      ugi = UserGroupInformation.createProxyUser(owner.toString(), realUgi);
    }
    realUgi.setAuthenticationMethod(AuthenticationMethod.TOKEN);
    return ugi;
  }

  public @OsUntrusted Text getOwner(@OsUntrusted AbstractDelegationTokenIdentifier this) {
    return owner;
  }

  public @OsUntrusted Text getRenewer(@OsUntrusted AbstractDelegationTokenIdentifier this) {
    return renewer;
  }
  
  public @OsUntrusted Text getRealUser(@OsUntrusted AbstractDelegationTokenIdentifier this) {
    return realUser;
  }
  
  public void setIssueDate(@OsUntrusted AbstractDelegationTokenIdentifier this, @OsUntrusted long issueDate) {
    this.issueDate = issueDate;
  }
  
  public @OsUntrusted long getIssueDate(@OsUntrusted AbstractDelegationTokenIdentifier this) {
    return issueDate;
  }
  
  public void setMaxDate(@OsUntrusted AbstractDelegationTokenIdentifier this, @OsUntrusted long maxDate) {
    this.maxDate = maxDate;
  }
  
  public @OsUntrusted long getMaxDate(@OsUntrusted AbstractDelegationTokenIdentifier this) {
    return maxDate;
  }

  public void setSequenceNumber(@OsUntrusted AbstractDelegationTokenIdentifier this, @OsUntrusted int seqNum) {
    this.sequenceNumber = seqNum;
  }
  
  public @OsUntrusted int getSequenceNumber(@OsUntrusted AbstractDelegationTokenIdentifier this) {
    return sequenceNumber;
  }

  public void setMasterKeyId(@OsUntrusted AbstractDelegationTokenIdentifier this, @OsUntrusted int newId) {
    masterKeyId = newId;
  }

  public @OsUntrusted int getMasterKeyId(@OsUntrusted AbstractDelegationTokenIdentifier this) {
    return masterKeyId;
  }

  static @OsUntrusted boolean isEqual(@OsUntrusted Object a, @OsUntrusted Object b) {
    return a == null ? b == null : a.equals(b);
  }
  
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted AbstractDelegationTokenIdentifier this, @OsUntrusted Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof @OsUntrusted AbstractDelegationTokenIdentifier) {
      @OsUntrusted
      AbstractDelegationTokenIdentifier that = (@OsUntrusted AbstractDelegationTokenIdentifier) obj;
      return this.sequenceNumber == that.sequenceNumber 
          && this.issueDate == that.issueDate 
          && this.maxDate == that.maxDate
          && this.masterKeyId == that.masterKeyId
          && isEqual(this.owner, that.owner) 
          && isEqual(this.renewer, that.renewer)
          && isEqual(this.realUser, that.realUser);
    }
    return false;
  }

  @Override
  public @OsUntrusted int hashCode(@OsUntrusted AbstractDelegationTokenIdentifier this) {
    return this.sequenceNumber;
  }
  
  @Override
  public void readFields(@OsUntrusted AbstractDelegationTokenIdentifier this, @OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    byte version = in.readByte();
    if (version != VERSION) {
	throw new @OsUntrusted IOException("Unknown version of delegation token " + 
                              version);
    }
    owner.readFields(in, Text.DEFAULT_MAX_LEN);
    renewer.readFields(in, Text.DEFAULT_MAX_LEN);
    realUser.readFields(in, Text.DEFAULT_MAX_LEN);
    issueDate = WritableUtils.readVLong(in);
    maxDate = WritableUtils.readVLong(in);
    sequenceNumber = WritableUtils.readVInt(in);
    masterKeyId = WritableUtils.readVInt(in);
  }

  @VisibleForTesting
  void writeImpl(@OsUntrusted AbstractDelegationTokenIdentifier this, @OsUntrusted DataOutput out) throws IOException {
    out.writeByte(VERSION);
    owner.write(out);
    renewer.write(out);
    realUser.write(out);
    WritableUtils.writeVLong(out, issueDate);
    WritableUtils.writeVLong(out, maxDate);
    WritableUtils.writeVInt(out, sequenceNumber);
    WritableUtils.writeVInt(out, masterKeyId);
  }
  
  @Override
  public void write(@OsUntrusted AbstractDelegationTokenIdentifier this, @OsUntrusted DataOutput out) throws IOException {
    if (owner.getLength() > Text.DEFAULT_MAX_LEN) {
      throw new @OsUntrusted IOException("owner is too long to be serialized!");
    }
    if (renewer.getLength() > Text.DEFAULT_MAX_LEN) {
      throw new @OsUntrusted IOException("renewer is too long to be serialized!");
    }
    if (realUser.getLength() > Text.DEFAULT_MAX_LEN) {
      throw new @OsUntrusted IOException("realuser is too long to be serialized!");
    }
    writeImpl(out);
  }
  
  @Override
  public @OsUntrusted String toString(@OsUntrusted AbstractDelegationTokenIdentifier this) {
    @OsUntrusted
    StringBuilder buffer = new @OsUntrusted StringBuilder();
    buffer
        .append("owner=" + owner + ", renewer=" + renewer + ", realUser="
            + realUser + ", issueDate=" + issueDate + ", maxDate=" + maxDate
            + ", sequenceNumber=" + sequenceNumber + ", masterKeyId="
            + masterKeyId);
    return buffer.toString();
  }
}
