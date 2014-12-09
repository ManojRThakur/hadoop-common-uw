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

package org.apache.hadoop.io.compress.zlib;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.apache.hadoop.io.compress.Decompressor;

/**
 * A wrapper around java.util.zip.Inflater to make it conform 
 * to org.apache.hadoop.io.compress.Decompressor interface.
 * 
 */
public class BuiltInZlibInflater extends @OsUntrusted Inflater implements @OsUntrusted Decompressor {

  public @OsUntrusted BuiltInZlibInflater(@OsUntrusted boolean nowrap) {
    super(nowrap);
  }

  public @OsUntrusted BuiltInZlibInflater() {
    super();
  }

  @Override
  public synchronized @OsUntrusted int decompress(@OsUntrusted BuiltInZlibInflater this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) 
    throws IOException {
    try {
      return super.inflate(b, off, len);
    } catch (@OsUntrusted DataFormatException dfe) {
      throw new @OsUntrusted IOException(dfe.getMessage());
    }
  }
}
