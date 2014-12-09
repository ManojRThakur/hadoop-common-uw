/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.hadoop.io.compress.bzip2;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;

import org.apache.hadoop.io.compress.Decompressor;

/**
 * This is a dummy decompressor for BZip2.
 */
public class BZip2DummyDecompressor implements @OsUntrusted Decompressor {

  @Override
  public @OsUntrusted int decompress(@OsUntrusted BZip2DummyDecompressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) throws IOException {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public void end(@OsUntrusted BZip2DummyDecompressor this) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public @OsUntrusted boolean finished(@OsUntrusted BZip2DummyDecompressor this) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public @OsUntrusted boolean needsDictionary(@OsUntrusted BZip2DummyDecompressor this) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public @OsUntrusted boolean needsInput(@OsUntrusted BZip2DummyDecompressor this) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public @OsUntrusted int getRemaining(@OsUntrusted BZip2DummyDecompressor this) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public void reset(@OsUntrusted BZip2DummyDecompressor this) {
    // do nothing
  }

  @Override
  public void setDictionary(@OsUntrusted BZip2DummyDecompressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public void setInput(@OsUntrusted BZip2DummyDecompressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

}
