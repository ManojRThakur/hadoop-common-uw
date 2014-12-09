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
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class S3InputStream extends @OsUntrusted FSInputStream {

  private @OsUntrusted FileSystemStore store;

  private @OsUntrusted Block @OsUntrusted [] blocks;

  private @OsUntrusted boolean closed;

  private @OsUntrusted long fileLength;

  private @OsUntrusted long pos = 0;

  private @OsUntrusted File blockFile;
  
  private @OsUntrusted DataInputStream blockStream;

  private @OsUntrusted long blockEnd = -1;
  
  private FileSystem.@OsUntrusted Statistics stats;
  
  private static final @OsUntrusted Log LOG = 
    LogFactory.getLog(S3InputStream.class.getName());


  @Deprecated
  public @OsUntrusted S3InputStream(@OsUntrusted Configuration conf, @OsUntrusted FileSystemStore store,
                       @OsUntrusted
                       INode inode) {
    this(conf, store, inode, null);
  }

  public @OsUntrusted S3InputStream(@OsUntrusted Configuration conf, @OsUntrusted FileSystemStore store,
                       @OsUntrusted
                       INode inode, FileSystem.@OsUntrusted Statistics stats) {
    
    this.store = store;
    this.stats = stats;
    this.blocks = inode.getBlocks();
    for (@OsUntrusted Block block : blocks) {
      this.fileLength += block.getLength();
    }
  }

  @Override
  public synchronized @OsUntrusted long getPos(@OsUntrusted S3InputStream this) throws IOException {
    return pos;
  }

  @Override
  public synchronized @OsUntrusted int available(@OsUntrusted S3InputStream this) throws IOException {
    return (@OsUntrusted int) (fileLength - pos);
  }

  @Override
  public synchronized void seek(@OsUntrusted S3InputStream this, @OsUntrusted long targetPos) throws IOException {
    if (targetPos > fileLength) {
      throw new @OsUntrusted IOException("Cannot seek after EOF");
    }
    pos = targetPos;
    blockEnd = -1;
  }

  @Override
  public synchronized @OsUntrusted boolean seekToNewSource(@OsUntrusted S3InputStream this, @OsUntrusted long targetPos) throws IOException {
    return false;
  }

  @Override
  public synchronized @OsUntrusted int read(@OsUntrusted S3InputStream this) throws IOException {
    if (closed) {
      throw new @OsUntrusted IOException("Stream closed");
    }
    @OsUntrusted
    int result = -1;
    if (pos < fileLength) {
      if (pos > blockEnd) {
        blockSeekTo(pos);
      }
      result = blockStream.read();
      if (result >= 0) {
        pos++;
      }
    }
    if (stats != null && result >= 0) {
      stats.incrementBytesRead(1);
    }
    return result;
  }

  @Override
  public synchronized @OsUntrusted int read(@OsUntrusted S3InputStream this, @OsUntrusted byte buf @OsUntrusted [], @OsUntrusted int off, @OsUntrusted int len) throws IOException {
    if (closed) {
      throw new @OsUntrusted IOException("Stream closed");
    }
    if (pos < fileLength) {
      if (pos > blockEnd) {
        blockSeekTo(pos);
      }
      @OsUntrusted
      int realLen = (@OsUntrusted int) Math.min((long) len, (blockEnd - pos + 1L));
      @OsUntrusted
      int result = blockStream.read(buf, off, realLen);
      if (result >= 0) {
        pos += result;
      }
      if (stats != null && result > 0) {
        stats.incrementBytesRead(result);
      }
      return result;
    }
    return -1;
  }

  private synchronized void blockSeekTo(@OsUntrusted S3InputStream this, @OsUntrusted long target) throws IOException {
    //
    // Compute desired block
    //
    @OsUntrusted
    int targetBlock = -1;
    @OsUntrusted
    long targetBlockStart = 0;
    @OsUntrusted
    long targetBlockEnd = 0;
    for (@OsUntrusted int i = 0; i < blocks.length; i++) {
      @OsUntrusted
      long blockLength = blocks[i].getLength();
      targetBlockEnd = targetBlockStart + blockLength - 1;

      if (target >= targetBlockStart && target <= targetBlockEnd) {
        targetBlock = i;
        break;
      } else {
        targetBlockStart = targetBlockEnd + 1;
      }
    }
    if (targetBlock < 0) {
      throw new @OsUntrusted IOException(
                            "Impossible situation: could not find target position " + target);
    }
    @OsUntrusted
    long offsetIntoBlock = target - targetBlockStart;

    // read block blocks[targetBlock] from position offsetIntoBlock

    this.blockFile = store.retrieveBlock(blocks[targetBlock], offsetIntoBlock);

    this.pos = target;
    this.blockEnd = targetBlockEnd;
    this.blockStream = new @OsUntrusted DataInputStream(new @OsUntrusted FileInputStream(blockFile));

  }

  @Override
  public void close(@OsUntrusted S3InputStream this) throws IOException {
    if (closed) {
      return;
    }
    if (blockStream != null) {
      blockStream.close();
      blockStream = null;
    }
    if (blockFile != null) {
      @OsUntrusted
      boolean b = blockFile.delete();
      if (!b) {
        LOG.warn("Ignoring failed delete");
      }
    }
    super.close();
    closed = true;
  }

  /**
   * We don't support marks.
   */
  @Override
  public @OsUntrusted boolean markSupported(@OsUntrusted S3InputStream this) {
    return false;
  }

  @Override
  public void mark(@OsUntrusted S3InputStream this, @OsUntrusted int readLimit) {
    // Do nothing
  }

  @Override
  public void reset(@OsUntrusted S3InputStream this) throws IOException {
    throw new @OsUntrusted IOException("Mark not supported");
  }

}
