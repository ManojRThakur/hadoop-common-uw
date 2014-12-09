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
package org.apache.hadoop.fs.ftp;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FTPInputStream extends @OsUntrusted FSInputStream {

  @OsUntrusted
  InputStream wrappedStream;
  @OsUntrusted
  FTPClient client;
  FileSystem.@OsUntrusted Statistics stats;
  @OsUntrusted
  boolean closed;
  @OsUntrusted
  long pos;

  public @OsUntrusted FTPInputStream(@OsUntrusted InputStream stream, @OsUntrusted FTPClient client,
      FileSystem.@OsUntrusted Statistics stats) {
    if (stream == null) {
      throw new @OsUntrusted IllegalArgumentException("Null InputStream");
    }
    if (client == null || !client.isConnected()) {
      throw new @OsUntrusted IllegalArgumentException("FTP client null or not connected");
    }
    this.wrappedStream = stream;
    this.client = client;
    this.stats = stats;
    this.pos = 0;
    this.closed = false;
  }

  @Override
  public @OsUntrusted long getPos(@OsUntrusted FTPInputStream this) throws IOException {
    return pos;
  }

  // We don't support seek.
  @Override
  public void seek(@OsUntrusted FTPInputStream this, @OsUntrusted long pos) throws IOException {
    throw new @OsUntrusted IOException("Seek not supported");
  }

  @Override
  public @OsUntrusted boolean seekToNewSource(@OsUntrusted FTPInputStream this, @OsUntrusted long targetPos) throws IOException {
    throw new @OsUntrusted IOException("Seek not supported");
  }

  @Override
  public synchronized @OsUntrusted int read(@OsUntrusted FTPInputStream this) throws IOException {
    if (closed) {
      throw new @OsUntrusted IOException("Stream closed");
    }

    @OsUntrusted
    int byteRead = wrappedStream.read();
    if (byteRead >= 0) {
      pos++;
    }
    if (stats != null && byteRead >= 0) {
      stats.incrementBytesRead(1);
    }
    return byteRead;
  }

  @Override
  public synchronized @OsUntrusted int read(@OsUntrusted FTPInputStream this, @OsUntrusted byte buf @OsUntrusted [], @OsUntrusted int off, @OsUntrusted int len) throws IOException {
    if (closed) {
      throw new @OsUntrusted IOException("Stream closed");
    }

    @OsUntrusted
    int result = wrappedStream.read(buf, off, len);
    if (result > 0) {
      pos += result;
    }
    if (stats != null && result > 0) {
      stats.incrementBytesRead(result);
    }

    return result;
  }

  @Override
  public synchronized void close(@OsUntrusted FTPInputStream this) throws IOException {
    if (closed) {
      throw new @OsUntrusted IOException("Stream closed");
    }
    super.close();
    closed = true;
    if (!client.isConnected()) {
      throw new @OsUntrusted FTPException("Client not connected");
    }

    @OsUntrusted
    boolean cmdCompleted = client.completePendingCommand();
    client.logout();
    client.disconnect();
    if (!cmdCompleted) {
      throw new @OsUntrusted FTPException("Could not complete transfer, Reply Code - "
          + client.getReplyCode());
    }
  }

  // Not supported.

  @Override
  public @OsUntrusted boolean markSupported(@OsUntrusted FTPInputStream this) {
    return false;
  }

  @Override
  public void mark(@OsUntrusted FTPInputStream this, @OsUntrusted int readLimit) {
    // Do nothing
  }

  @Override
  public void reset(@OsUntrusted FTPInputStream this) throws IOException {
    throw new @OsUntrusted IOException("Mark not supported");
  }
}
