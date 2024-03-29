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
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** Utility that wraps a {@link OutputStream} in a {@link DataOutputStream}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FSDataOutputStream extends @OsUntrusted DataOutputStream
    implements @OsUntrusted Syncable, @OsUntrusted CanSetDropBehind {
  private final @OsUntrusted OutputStream wrappedStream;

  private static class PositionCache extends @OsUntrusted FilterOutputStream {
    private final FileSystem.@OsUntrusted Statistics statistics;
    private @OsUntrusted long position;

    @OsUntrusted
    PositionCache(@OsUntrusted OutputStream out, FileSystem.@OsUntrusted Statistics stats, @OsUntrusted long pos) {
      super(out);
      statistics = stats;
      position = pos;
    }

    @Override
    public void write(FSDataOutputStream.@OsUntrusted PositionCache this, @OsUntrusted int b) throws IOException {
      out.write(b);
      position++;
      if (statistics != null) {
        statistics.incrementBytesWritten(1);
      }
    }
    
    @Override
    public void write(FSDataOutputStream.@OsUntrusted PositionCache this, @OsUntrusted byte b @OsUntrusted [], @OsUntrusted int off, @OsUntrusted int len) throws IOException {
      out.write(b, off, len);
      position += len;                            // update position
      if (statistics != null) {
        statistics.incrementBytesWritten(len);
      }
    }
      
    @OsUntrusted
    long getPos(FSDataOutputStream.@OsUntrusted PositionCache this) {
      return position;                            // return cached position
    }

    @Override
    public void close(FSDataOutputStream.@OsUntrusted PositionCache this) throws IOException {
      out.close();
    }
  }

  public @OsUntrusted FSDataOutputStream(@OsUntrusted OutputStream out, FileSystem.@OsUntrusted Statistics stats) {
    this(out, stats, 0);
  }

  public @OsUntrusted FSDataOutputStream(@OsUntrusted OutputStream out, FileSystem.@OsUntrusted Statistics stats,
                            @OsUntrusted
                            long startPosition) {
    super(new @OsUntrusted PositionCache(out, stats, startPosition));
    wrappedStream = out;
  }
  
  /**
   * Get the current position in the output stream.
   *
   * @return the current position in the output stream
   */
  public @OsUntrusted long getPos(@OsUntrusted FSDataOutputStream this) {
    return ((@OsUntrusted PositionCache)out).getPos();
  }

  /**
   * Close the underlying output stream.
   */
  @Override
  public void close(@OsUntrusted FSDataOutputStream this) throws IOException {
    out.close(); // This invokes PositionCache.close()
  }

  /**
   * Get a reference to the wrapped output stream. Used by unit tests.
   *
   * @return the underlying output stream
   */
  @InterfaceAudience.LimitedPrivate({"HDFS"})
  public @OsUntrusted OutputStream getWrappedStream(@OsUntrusted FSDataOutputStream this) {
    return wrappedStream;
  }

  @Override  // Syncable
  public void hflush(@OsUntrusted FSDataOutputStream this) throws IOException {
    if (wrappedStream instanceof @OsUntrusted Syncable) {
      ((@OsUntrusted Syncable)wrappedStream).hflush();
    } else {
      wrappedStream.flush();
    }
  }
  
  @Override  // Syncable
  public void hsync(@OsUntrusted FSDataOutputStream this) throws IOException {
    if (wrappedStream instanceof @OsUntrusted Syncable) {
      ((@OsUntrusted Syncable)wrappedStream).hsync();
    } else {
      wrappedStream.flush();
    }
  }

  @Override
  public void setDropBehind(@OsUntrusted FSDataOutputStream this, @OsUntrusted Boolean dropBehind) throws IOException {
    try {
      ((@OsUntrusted CanSetDropBehind)wrappedStream).setDropBehind(dropBehind);
    } catch (@OsUntrusted ClassCastException e) {
      throw new @OsUntrusted UnsupportedOperationException("the wrapped stream does " +
          "not support setting the drop-behind caching setting.");
    }
  }
}
