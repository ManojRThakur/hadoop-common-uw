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

package org.apache.hadoop.fs.local;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.util.DataChecksum;

/** 
 * This class contains constants for configuration keys used
 * in the local file system, raw local fs and checksum fs.
 *
 * Note that the settings for unimplemented features are ignored. 
 * E.g. checksum related settings are just place holders. Even when
 * wrapped with {@link ChecksumFileSystem}, these settings are not
 * used.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class LocalConfigKeys extends @OsUntrusted CommonConfigurationKeys {
  public static final @OsUntrusted String BLOCK_SIZE_KEY = "file.blocksize";
  public static final @OsUntrusted long BLOCK_SIZE_DEFAULT = 64*1024*1024;
  public static final @OsUntrusted String  REPLICATION_KEY = "file.replication";
  public static final @OsUntrusted short REPLICATION_DEFAULT = 1;
  public static final @OsUntrusted String STREAM_BUFFER_SIZE_KEY = "file.stream-buffer-size";
  public static final @OsUntrusted int STREAM_BUFFER_SIZE_DEFAULT = 4096;
  public static final @OsUntrusted String BYTES_PER_CHECKSUM_KEY = "file.bytes-per-checksum";
  public static final @OsUntrusted int BYTES_PER_CHECKSUM_DEFAULT = 512;
  public static final @OsUntrusted String CLIENT_WRITE_PACKET_SIZE_KEY =
                                                "file.client-write-packet-size";
  public static final @OsUntrusted int CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64*1024;
  public static final @OsUntrusted boolean ENCRYPT_DATA_TRANSFER_DEFAULT = false;
  public static final @OsUntrusted long FS_TRASH_INTERVAL_DEFAULT = 0;
  public static final DataChecksum.@OsUntrusted Type CHECKSUM_TYPE_DEFAULT =
      DataChecksum.Type.CRC32;
  public static @OsUntrusted FsServerDefaults getServerDefaults() throws IOException {
    return new @OsUntrusted FsServerDefaults(
        BLOCK_SIZE_DEFAULT,
        BYTES_PER_CHECKSUM_DEFAULT,
        CLIENT_WRITE_PACKET_SIZE_DEFAULT,
        REPLICATION_DEFAULT,
        STREAM_BUFFER_SIZE_DEFAULT,
        ENCRYPT_DATA_TRANSFER_DEFAULT,
        FS_TRASH_INTERVAL_DEFAULT,
        CHECKSUM_TYPE_DEFAULT);
  }
}
  
