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

package org.apache.hadoop.fs.s3;

import ostrusted.quals.OsUntrusted;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IOUtils;

/**
 * Holds file metadata including type (regular file, or directory),
 * and the list of blocks that are pointers to the data.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class INode {
	
  enum FileType {

@OsUntrusted  DIRECTORY,  @OsUntrusted  FILE
  }
  
  public static final @OsUntrusted FileType @OsUntrusted [] FILE_TYPES = new FileType @OsUntrusted [] {
    FileType.DIRECTORY,
    FileType.FILE
  };

  public static final @OsUntrusted INode DIRECTORY_INODE = new @OsUntrusted INode(FileType.DIRECTORY, null);
  
  private @OsUntrusted FileType fileType;
  private @OsUntrusted Block @OsUntrusted [] blocks;

  public @OsUntrusted INode(@OsUntrusted FileType fileType, @OsUntrusted Block @OsUntrusted [] blocks) {
    this.fileType = fileType;
    if (isDirectory() && blocks != null) {
      throw new @OsUntrusted IllegalArgumentException("A directory cannot contain blocks.");
    }
    this.blocks = blocks;
  }

  public @OsUntrusted Block @OsUntrusted [] getBlocks(@OsUntrusted INode this) {
    return blocks;
  }
  
  public @OsUntrusted FileType getFileType(@OsUntrusted INode this) {
    return fileType;
  }

  public @OsUntrusted boolean isDirectory(@OsUntrusted INode this) {
    return fileType == FileType.DIRECTORY;
  }  

  public @OsUntrusted boolean isFile(@OsUntrusted INode this) {
    return fileType == FileType.FILE;
  }
  
  public @OsUntrusted long getSerializedLength(@OsUntrusted INode this) {
    return 1L + (blocks == null ? 0 : 4 + blocks.length * 16);
  }
  

  public @OsUntrusted InputStream serialize(@OsUntrusted INode this) throws IOException {
    @OsUntrusted
    ByteArrayOutputStream bytes = new @OsUntrusted ByteArrayOutputStream();
    @OsUntrusted
    DataOutputStream out = new @OsUntrusted DataOutputStream(bytes);
    try {
      out.writeByte(fileType.ordinal());
      if (isFile()) {
        out.writeInt(blocks.length);
        for (@OsUntrusted int i = 0; i < blocks.length; i++) {
          out.writeLong(blocks[i].getId());
          out.writeLong(blocks[i].getLength());
        }
      }
      out.close();
      out = null;
    } finally {
      IOUtils.closeStream(out);
    }
    return new @OsUntrusted ByteArrayInputStream(bytes.toByteArray());
  }
  
  public static @OsUntrusted INode deserialize(@OsUntrusted InputStream in) throws IOException {
    if (in == null) {
      return null;
    }
    @OsUntrusted
    DataInputStream dataIn = new @OsUntrusted DataInputStream(in);
    @OsUntrusted
    FileType fileType = INode.FILE_TYPES[dataIn.readByte()];
    switch (fileType) {
    case DIRECTORY:
      in.close();
      return INode.DIRECTORY_INODE;
    case FILE:
      @OsUntrusted
      int numBlocks = dataIn.readInt();
      @OsUntrusted
      Block @OsUntrusted [] blocks = new @OsUntrusted Block @OsUntrusted [numBlocks];
      for (@OsUntrusted int i = 0; i < numBlocks; i++) {
        @OsUntrusted
        long id = dataIn.readLong();
        @OsUntrusted
        long length = dataIn.readLong();
        blocks[i] = new @OsUntrusted Block(id, length);
      }
      in.close();
      return new @OsUntrusted INode(fileType, blocks);
    default:
      throw new @OsUntrusted IllegalArgumentException("Cannot deserialize inode.");
    }    
  }  
  
}
