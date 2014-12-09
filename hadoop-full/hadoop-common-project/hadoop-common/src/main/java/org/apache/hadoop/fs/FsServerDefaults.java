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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.DataChecksum;

/****************************************************
 * Provides server default configuration values to clients.
 * 
 ****************************************************/
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FsServerDefaults implements @OsUntrusted Writable {

  static { // register a ctor
    WritableFactories.setFactory(FsServerDefaults.class, new @OsUntrusted WritableFactory() {
      @Override
      public @OsUntrusted Writable newInstance() {
        return new @OsUntrusted FsServerDefaults();
      }
    });
  }

  private @OsUntrusted long blockSize;
  private @OsUntrusted int bytesPerChecksum;
  private @OsUntrusted int writePacketSize;
  private @OsUntrusted short replication;
  private @OsUntrusted int fileBufferSize;
  private @OsUntrusted boolean encryptDataTransfer;
  private @OsUntrusted long trashInterval;
  private DataChecksum.@OsUntrusted Type checksumType;

  public @OsUntrusted FsServerDefaults() {
  }

  public @OsUntrusted FsServerDefaults(@OsUntrusted long blockSize, @OsUntrusted int bytesPerChecksum,
      @OsUntrusted
      int writePacketSize, @OsUntrusted short replication, @OsUntrusted int fileBufferSize,
      @OsUntrusted
      boolean encryptDataTransfer, @OsUntrusted long trashInterval,
      DataChecksum.@OsUntrusted Type checksumType) {
    this.blockSize = blockSize;
    this.bytesPerChecksum = bytesPerChecksum;
    this.writePacketSize = writePacketSize;
    this.replication = replication;
    this.fileBufferSize = fileBufferSize;
    this.encryptDataTransfer = encryptDataTransfer;
    this.trashInterval = trashInterval;
    this.checksumType = checksumType;
  }

  public @OsUntrusted long getBlockSize(@OsUntrusted FsServerDefaults this) {
    return blockSize;
  }

  public @OsUntrusted int getBytesPerChecksum(@OsUntrusted FsServerDefaults this) {
    return bytesPerChecksum;
  }

  public @OsUntrusted int getWritePacketSize(@OsUntrusted FsServerDefaults this) {
    return writePacketSize;
  }

  public @OsUntrusted short getReplication(@OsUntrusted FsServerDefaults this) {
    return replication;
  }

  public @OsUntrusted int getFileBufferSize(@OsUntrusted FsServerDefaults this) {
    return fileBufferSize;
  }
  
  public @OsUntrusted boolean getEncryptDataTransfer(@OsUntrusted FsServerDefaults this) {
    return encryptDataTransfer;
  }

  public @OsUntrusted long getTrashInterval(@OsUntrusted FsServerDefaults this) {
    return trashInterval;
  }

  public DataChecksum.@OsUntrusted Type getChecksumType(@OsUntrusted FsServerDefaults this) {
    return checksumType;
  }

  // /////////////////////////////////////////
  // Writable
  // /////////////////////////////////////////
  @Override
  @InterfaceAudience.Private
  public void write(@OsUntrusted FsServerDefaults this, @OsUntrusted DataOutput out) throws IOException {
    out.writeLong(blockSize);
    out.writeInt(bytesPerChecksum);
    out.writeInt(writePacketSize);
    out.writeShort(replication);
    out.writeInt(fileBufferSize);
    WritableUtils.writeEnum(out, checksumType);
  }

  @Override
  @InterfaceAudience.Private
  public void readFields(@OsUntrusted FsServerDefaults this, @OsUntrusted DataInput in) throws IOException {
    blockSize = in.readLong();
    bytesPerChecksum = in.readInt();
    writePacketSize = in.readInt();
    replication = in.readShort();
    fileBufferSize = in.readInt();
    checksumType = WritableUtils.readEnum(in, DataChecksum.Type.class);
  }
}
