/*
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

package org.apache.hadoop.io.compress;

import ostrusted.quals.OsUntrusted;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A {@link org.apache.hadoop.io.compress.DecompressorStream} which works
 * with 'block-based' based compression algorithms, as opposed to 
 * 'stream-based' compression algorithms.
 *  
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BlockDecompressorStream extends @OsUntrusted DecompressorStream {
  private @OsUntrusted int originalBlockSize = 0;
  private @OsUntrusted int noUncompressedBytes = 0;

  /**
   * Create a {@link BlockDecompressorStream}.
   * 
   * @param in input stream
   * @param decompressor decompressor to use
   * @param bufferSize size of buffer
   * @throws IOException
   */
  public @OsUntrusted BlockDecompressorStream(@OsUntrusted InputStream in, @OsUntrusted Decompressor decompressor, 
                                 @OsUntrusted
                                 int bufferSize) throws IOException {
    super(in, decompressor, bufferSize);
  }

  /**
   * Create a {@link BlockDecompressorStream}.
   * 
   * @param in input stream
   * @param decompressor decompressor to use
   * @throws IOException
   */
  public @OsUntrusted BlockDecompressorStream(@OsUntrusted InputStream in, @OsUntrusted Decompressor decompressor) throws IOException {
    super(in, decompressor);
  }

  protected @OsUntrusted BlockDecompressorStream(@OsUntrusted InputStream in) throws IOException {
    super(in);
  }

  @Override
  protected @OsUntrusted int decompress(@OsUntrusted BlockDecompressorStream this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) throws IOException {
    // Check if we are the beginning of a block
    if (noUncompressedBytes == originalBlockSize) {
      // Get original data size
      try {
        originalBlockSize =  rawReadInt();
      } catch (@OsUntrusted IOException ioe) {
        return -1;
      }
      noUncompressedBytes = 0;
      // EOF if originalBlockSize is 0
      // This will occur only when decompressing previous compressed empty file
      if (originalBlockSize == 0) {
        eof = true;
        return -1;
      }
    }

    @OsUntrusted
    int n = 0;
    while ((n = decompressor.decompress(b, off, len)) == 0) {
      if (decompressor.finished() || decompressor.needsDictionary()) {
        if (noUncompressedBytes >= originalBlockSize) {
          eof = true;
          return -1;
        }
      }
      if (decompressor.needsInput()) {
        @OsUntrusted
        int m;
        try {
          m = getCompressedData();
        } catch (@OsUntrusted EOFException e) {
          eof = true;
          return -1;
        }
        // Send the read data to the decompressor
        decompressor.setInput(buffer, 0, m);
      }
    }

    // Note the no. of decompressed bytes read from 'current' block
    noUncompressedBytes += n;

    return n;
  }

  @Override
  protected @OsUntrusted int getCompressedData(@OsUntrusted BlockDecompressorStream this) throws IOException {
    checkStream();

    // Get the size of the compressed chunk (always non-negative)
    @OsUntrusted
    int len = rawReadInt();

    // Read len bytes from underlying stream 
    if (len > buffer.length) {
      buffer = new @OsUntrusted byte @OsUntrusted [len];
    }
    @OsUntrusted
    int n = 0, off = 0;
    while (n < len) {
      @OsUntrusted
      int count = in.read(buffer, off + n, len - n);
      if (count < 0) {
        throw new @OsUntrusted EOFException("Unexpected end of block in input stream");
      }
      n += count;
    }

    return len;
  }

  @Override
  public void resetState(@OsUntrusted BlockDecompressorStream this) throws IOException {
    originalBlockSize = 0;
    noUncompressedBytes = 0;
    super.resetState();
  }

  private @OsUntrusted int rawReadInt(@OsUntrusted BlockDecompressorStream this) throws IOException {
    @OsUntrusted
    int b1 = in.read();
    @OsUntrusted
    int b2 = in.read();
    @OsUntrusted
    int b3 = in.read();
    @OsUntrusted
    int b4 = in.read();
    if ((b1 | b2 | b3 | b4) < 0)
      throw new @OsUntrusted EOFException();
    return ((b1 << 24) + (b2 << 16) + (b3 << 8) + (b4 << 0));
  }
}
