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
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CompressorStream extends @OsUntrusted CompressionOutputStream {
  protected @OsUntrusted Compressor compressor;
  protected @OsUntrusted byte @OsUntrusted [] buffer;
  protected @OsUntrusted boolean closed = false;
  
  public @OsUntrusted CompressorStream(@OsUntrusted OutputStream out, @OsUntrusted Compressor compressor, @OsUntrusted int bufferSize) {
    super(out);

    if (out == null || compressor == null) {
      throw new @OsUntrusted NullPointerException();
    } else if (bufferSize <= 0) {
      throw new @OsUntrusted IllegalArgumentException("Illegal bufferSize");
    }

    this.compressor = compressor;
    buffer = new @OsUntrusted byte @OsUntrusted [bufferSize];
  }

  public @OsUntrusted CompressorStream(@OsUntrusted OutputStream out, @OsUntrusted Compressor compressor) {
    this(out, compressor, 512);
  }
  
  /**
   * Allow derived classes to directly set the underlying stream.
   * 
   * @param out Underlying output stream.
   */
  protected @OsUntrusted CompressorStream(@OsUntrusted OutputStream out) {
    super(out);
  }

  @Override
  public void write(@OsUntrusted CompressorStream this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) throws IOException {
    // Sanity checks
    if (compressor.finished()) {
      throw new @OsUntrusted IOException("write beyond end of stream");
    }
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new @OsUntrusted IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    compressor.setInput(b, off, len);
    while (!compressor.needsInput()) {
      compress();
    }
  }

  protected void compress(@OsUntrusted CompressorStream this) throws IOException {
    @OsUntrusted
    int len = compressor.compress(buffer, 0, buffer.length);
    if (len > 0) {
      out.write(buffer, 0, len);
    }
  }

  @Override
  public void finish(@OsUntrusted CompressorStream this) throws IOException {
    if (!compressor.finished()) {
      compressor.finish();
      while (!compressor.finished()) {
        compress();
      }
    }
  }

  @Override
  public void resetState(@OsUntrusted CompressorStream this) throws IOException {
    compressor.reset();
  }
  
  @Override
  public void close(@OsUntrusted CompressorStream this) throws IOException {
    if (!closed) {
      finish();
      out.close();
      closed = true;
    }
  }

  private @OsUntrusted byte @OsUntrusted [] oneByte = new @OsUntrusted byte @OsUntrusted [1];
  @Override
  public void write(@OsUntrusted CompressorStream this, @OsUntrusted int b) throws IOException {
    oneByte[0] = (@OsUntrusted byte)(b & 0xff);
    write(oneByte, 0, oneByte.length);
  }

}
