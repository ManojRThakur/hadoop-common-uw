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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

/**
 * This is a dummy compressor for BZip2.
 */
public class BZip2DummyCompressor implements @OsUntrusted Compressor {

  @Override
  public @OsUntrusted int compress(@OsUntrusted BZip2DummyCompressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) throws IOException {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public void end(@OsUntrusted BZip2DummyCompressor this) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public void finish(@OsUntrusted BZip2DummyCompressor this) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public @OsUntrusted boolean finished(@OsUntrusted BZip2DummyCompressor this) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public @OsUntrusted long getBytesRead(@OsUntrusted BZip2DummyCompressor this) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public @OsUntrusted long getBytesWritten(@OsUntrusted BZip2DummyCompressor this) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public @OsUntrusted boolean needsInput(@OsUntrusted BZip2DummyCompressor this) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public void reset(@OsUntrusted BZip2DummyCompressor this) {
    // do nothing
  }

  @Override
  public void setDictionary(@OsUntrusted BZip2DummyCompressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public void setInput(@OsUntrusted BZip2DummyCompressor this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) {
    throw new @OsUntrusted UnsupportedOperationException();
  }

  @Override
  public void reinit(@OsUntrusted BZip2DummyCompressor this, @OsUntrusted Configuration conf) {
    // do nothing
  }

}
