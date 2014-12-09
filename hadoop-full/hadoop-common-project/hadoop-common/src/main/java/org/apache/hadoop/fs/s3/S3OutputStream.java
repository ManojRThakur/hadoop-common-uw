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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.INode.FileType;
import org.apache.hadoop.util.Progressable;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class S3OutputStream extends @OsUntrusted OutputStream {

  private @OsUntrusted Configuration conf;
  
  private @OsUntrusted int bufferSize;

  private @OsUntrusted FileSystemStore store;

  private @OsUntrusted Path path;

  private @OsUntrusted long blockSize;

  private @OsUntrusted File backupFile;

  private @OsUntrusted OutputStream backupStream;

  private @OsUntrusted Random r = new @OsUntrusted Random();

  private @OsUntrusted boolean closed;

  private @OsUntrusted int pos = 0;

  private @OsUntrusted long filePos = 0;

  private @OsUntrusted int bytesWrittenToBlock = 0;

  private @OsUntrusted byte @OsUntrusted [] outBuf;

  private @OsUntrusted List<@OsUntrusted Block> blocks = new @OsUntrusted ArrayList<@OsUntrusted Block>();

  private @OsUntrusted Block nextBlock;
  
  private static final @OsUntrusted Log LOG = 
    LogFactory.getLog(S3OutputStream.class.getName());


  public @OsUntrusted S3OutputStream(@OsUntrusted Configuration conf, @OsUntrusted FileSystemStore store,
                        @OsUntrusted
                        Path path, @OsUntrusted long blockSize, @OsUntrusted Progressable progress,
                        @OsUntrusted
                        int buffersize) throws IOException {
    
    this.conf = conf;
    this.store = store;
    this.path = path;
    this.blockSize = blockSize;
    this.backupFile = newBackupFile();
    this.backupStream = new @OsUntrusted FileOutputStream(backupFile);
    this.bufferSize = buffersize;
    this.outBuf = new @OsUntrusted byte @OsUntrusted [bufferSize];

  }

  private @OsUntrusted File newBackupFile(@OsUntrusted S3OutputStream this) throws IOException {
    @OsUntrusted
    File dir = new @OsUntrusted File(conf.get("fs.s3.buffer.dir"));
    if (!dir.exists() && !dir.mkdirs()) {
      throw new @OsUntrusted IOException("Cannot create S3 buffer directory: " + dir);
    }
    @OsUntrusted
    File result = File.createTempFile("output-", ".tmp", dir);
    result.deleteOnExit();
    return result;
  }

  public @OsUntrusted long getPos(@OsUntrusted S3OutputStream this) throws IOException {
    return filePos;
  }

  @Override
  public synchronized void write(@OsUntrusted S3OutputStream this, @OsUntrusted int b) throws IOException {
    if (closed) {
      throw new @OsUntrusted IOException("Stream closed");
    }

    if ((bytesWrittenToBlock + pos == blockSize) || (pos >= bufferSize)) {
      flush();
    }
    outBuf[pos++] = (@OsUntrusted byte) b;
    filePos++;
  }

  @Override
  public synchronized void write(@OsUntrusted S3OutputStream this, @OsUntrusted byte b @OsUntrusted [], @OsUntrusted int off, @OsUntrusted int len) throws IOException {
    if (closed) {
      throw new @OsUntrusted IOException("Stream closed");
    }
    while (len > 0) {
      @OsUntrusted
      int remaining = bufferSize - pos;
      @OsUntrusted
      int toWrite = Math.min(remaining, len);
      System.arraycopy(b, off, outBuf, pos, toWrite);
      pos += toWrite;
      off += toWrite;
      len -= toWrite;
      filePos += toWrite;

      if ((bytesWrittenToBlock + pos >= blockSize) || (pos == bufferSize)) {
        flush();
      }
    }
  }

  @Override
  public synchronized void flush(@OsUntrusted S3OutputStream this) throws IOException {
    if (closed) {
      throw new @OsUntrusted IOException("Stream closed");
    }

    if (bytesWrittenToBlock + pos >= blockSize) {
      flushData((@OsUntrusted int) blockSize - bytesWrittenToBlock);
    }
    if (bytesWrittenToBlock == blockSize) {
      endBlock();
    }
    flushData(pos);
  }

  private synchronized void flushData(@OsUntrusted S3OutputStream this, @OsUntrusted int maxPos) throws IOException {
    @OsUntrusted
    int workingPos = Math.min(pos, maxPos);

    if (workingPos > 0) {
      //
      // To the local block backup, write just the bytes
      //
      backupStream.write(outBuf, 0, workingPos);

      //
      // Track position
      //
      bytesWrittenToBlock += workingPos;
      System.arraycopy(outBuf, workingPos, outBuf, 0, pos - workingPos);
      pos -= workingPos;
    }
  }

  private synchronized void endBlock(@OsUntrusted S3OutputStream this) throws IOException {
    //
    // Done with local copy
    //
    backupStream.close();

    //
    // Send it to S3
    //
    // TODO: Use passed in Progressable to report progress.
    nextBlockOutputStream();
    store.storeBlock(nextBlock, backupFile);
    internalClose();

    //
    // Delete local backup, start new one
    //
    @OsUntrusted
    boolean b = backupFile.delete();
    if (!b) {
      LOG.warn("Ignoring failed delete");
    }
    backupFile = newBackupFile();
    backupStream = new @OsUntrusted FileOutputStream(backupFile);
    bytesWrittenToBlock = 0;
  }

  private synchronized void nextBlockOutputStream(@OsUntrusted S3OutputStream this) throws IOException {
    @OsUntrusted
    long blockId = r.nextLong();
    while (store.blockExists(blockId)) {
      blockId = r.nextLong();
    }
    nextBlock = new @OsUntrusted Block(blockId, bytesWrittenToBlock);
    blocks.add(nextBlock);
    bytesWrittenToBlock = 0;
  }

  private synchronized void internalClose(@OsUntrusted S3OutputStream this) throws IOException {
    @OsUntrusted
    INode inode = new @OsUntrusted INode(FileType.FILE, blocks.toArray(new @OsUntrusted Block @OsUntrusted [blocks
                                                                    .size()]));
    store.storeINode(path, inode);
  }

  @Override
  public synchronized void close(@OsUntrusted S3OutputStream this) throws IOException {
    if (closed) {
      return;
    }

    flush();
    if (filePos == 0 || bytesWrittenToBlock != 0) {
      endBlock();
    }

    backupStream.close();
    @OsUntrusted
    boolean b = backupFile.delete();
    if (!b) {
      LOG.warn("Ignoring failed delete");
    }

    super.close();

    closed = true;
  }

}
