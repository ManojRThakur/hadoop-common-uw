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

package org.apache.hadoop.record;

import ostrusted.quals.OsUntrusted;
import java.io.DataInput;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BinaryRecordInput implements @OsUntrusted RecordInput {
    
  private @OsUntrusted DataInput in;
    
  static private class BinaryIndex implements @OsUntrusted Index {
    private @OsUntrusted int nelems;
    private @OsUntrusted BinaryIndex(@OsUntrusted int nelems) {
      this.nelems = nelems;
    }
    @Override
    public @OsUntrusted boolean done(BinaryRecordInput.@OsUntrusted BinaryIndex this) {
      return (nelems <= 0);
    }
    @Override
    public void incr(BinaryRecordInput.@OsUntrusted BinaryIndex this) {
      nelems--;
    }
  }
    
  private @OsUntrusted BinaryRecordInput() {}
    
  private void setDataInput(@OsUntrusted BinaryRecordInput this, @OsUntrusted DataInput inp) {
    this.in = inp;
  }
    
  private static @OsUntrusted ThreadLocal<BinaryRecordInput> bIn = new @OsUntrusted ThreadLocal<BinaryRecordInput>() {
      @Override
      protected synchronized @OsUntrusted BinaryRecordInput initialValue() {
        return new @OsUntrusted BinaryRecordInput();
      }
    };
    
  /**
   * Get a thread-local record input for the supplied DataInput.
   * @param inp data input stream
   * @return binary record input corresponding to the supplied DataInput.
   */
  public static @OsUntrusted BinaryRecordInput get(@OsUntrusted DataInput inp) {
    @OsUntrusted
    BinaryRecordInput bin = (@OsUntrusted BinaryRecordInput) bIn.get();
    bin.setDataInput(inp);
    return bin;
  }
    
  /** Creates a new instance of BinaryRecordInput */
  public @OsUntrusted BinaryRecordInput(@OsUntrusted InputStream strm) {
    this.in = new @OsUntrusted DataInputStream(strm);
  }
    
  /** Creates a new instance of BinaryRecordInput */
  public @OsUntrusted BinaryRecordInput(@OsUntrusted DataInput din) {
    this.in = din;
  }
    
  @Override
  public @OsUntrusted byte readByte(@OsUntrusted BinaryRecordInput this, final @OsUntrusted String tag) throws IOException {
    return in.readByte();
  }
    
  @Override
  public @OsUntrusted boolean readBool(@OsUntrusted BinaryRecordInput this, final @OsUntrusted String tag) throws IOException {
    return in.readBoolean();
  }
    
  @Override
  public @OsUntrusted int readInt(@OsUntrusted BinaryRecordInput this, final @OsUntrusted String tag) throws IOException {
    return Utils.readVInt(in);
  }
    
  @Override
  public @OsUntrusted long readLong(@OsUntrusted BinaryRecordInput this, final @OsUntrusted String tag) throws IOException {
    return Utils.readVLong(in);
  }
    
  @Override
  public @OsUntrusted float readFloat(@OsUntrusted BinaryRecordInput this, final @OsUntrusted String tag) throws IOException {
    return in.readFloat();
  }
    
  @Override
  public @OsUntrusted double readDouble(@OsUntrusted BinaryRecordInput this, final @OsUntrusted String tag) throws IOException {
    return in.readDouble();
  }
    
  @Override
  public @OsUntrusted String readString(@OsUntrusted BinaryRecordInput this, final @OsUntrusted String tag) throws IOException {
    return Utils.fromBinaryString(in);
  }
    
  @Override
  public @OsUntrusted Buffer readBuffer(@OsUntrusted BinaryRecordInput this, final @OsUntrusted String tag) throws IOException {
    final @OsUntrusted int len = Utils.readVInt(in);
    final @OsUntrusted byte @OsUntrusted [] barr = new @OsUntrusted byte @OsUntrusted [len];
    in.readFully(barr);
    return new @OsUntrusted Buffer(barr);
  }
    
  @Override
  public void startRecord(@OsUntrusted BinaryRecordInput this, final @OsUntrusted String tag) throws IOException {
    // no-op
  }
    
  @Override
  public void endRecord(@OsUntrusted BinaryRecordInput this, final @OsUntrusted String tag) throws IOException {
    // no-op
  }
    
  @Override
  public @OsUntrusted Index startVector(@OsUntrusted BinaryRecordInput this, final @OsUntrusted String tag) throws IOException {
    return new @OsUntrusted BinaryIndex(readInt(tag));
  }
    
  @Override
  public void endVector(@OsUntrusted BinaryRecordInput this, final @OsUntrusted String tag) throws IOException {
    // no-op
  }
    
  @Override
  public @OsUntrusted Index startMap(@OsUntrusted BinaryRecordInput this, final @OsUntrusted String tag) throws IOException {
    return new @OsUntrusted BinaryIndex(readInt(tag));
  }
    
  @Override
  public void endMap(@OsUntrusted BinaryRecordInput this, final @OsUntrusted String tag) throws IOException {
    // no-op
  }
}
