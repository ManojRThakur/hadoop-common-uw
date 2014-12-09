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
import java.io.Closeable;
import java.io.IOException;

import org.apache.avro.file.SeekableInput;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** Adapts an {@link FSDataInputStream} to Avro's SeekableInput interface. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AvroFSInput implements @OsUntrusted Closeable, @OsUntrusted SeekableInput {
  private final @OsUntrusted FSDataInputStream stream;
  private final @OsUntrusted long len;

  /** Construct given an {@link FSDataInputStream} and its length. */
  public @OsUntrusted AvroFSInput(final @OsUntrusted FSDataInputStream in, final @OsUntrusted long len) {
    this.stream = in;
    this.len = len;
  }

  /** Construct given a {@link FileContext} and a {@link Path}. */
  public @OsUntrusted AvroFSInput(final @OsUntrusted FileContext fc, final @OsUntrusted Path p) throws IOException {
    @OsUntrusted
    FileStatus status = fc.getFileStatus(p);
    this.len = status.getLen();
    this.stream = fc.open(p);
  }

  @Override
  public @OsUntrusted long length(@OsUntrusted AvroFSInput this) {
    return len;
  }

  @Override
  public @OsUntrusted int read(@OsUntrusted AvroFSInput this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) throws IOException {
    return stream.read(b, off, len);
  }

  @Override
  public void seek(@OsUntrusted AvroFSInput this, @OsUntrusted long p) throws IOException {
    stream.seek(p);
  }

  @Override
  public @OsUntrusted long tell(@OsUntrusted AvroFSInput this) throws IOException {
    return stream.getPos();
  }

  @Override
  public void close(@OsUntrusted AvroFSInput this) throws IOException {
    stream.close();
  }
}
