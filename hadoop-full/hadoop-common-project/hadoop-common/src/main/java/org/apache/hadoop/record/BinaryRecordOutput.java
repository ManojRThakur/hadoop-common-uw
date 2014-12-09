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
import java.io.IOException;
import java.util.TreeMap;
import java.util.ArrayList;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BinaryRecordOutput implements @OsUntrusted RecordOutput {
    
  private @OsUntrusted DataOutput out;
    
  private @OsUntrusted BinaryRecordOutput() {}
    
  private void setDataOutput(@OsUntrusted BinaryRecordOutput this, @OsUntrusted DataOutput out) {
    this.out = out;
  }
    
  private static @OsUntrusted ThreadLocal<BinaryRecordOutput> bOut = new @OsUntrusted ThreadLocal<BinaryRecordOutput>() {
      @Override
      protected synchronized @OsUntrusted BinaryRecordOutput initialValue() {
        return new @OsUntrusted BinaryRecordOutput();
      }
    };
    
  /**
   * Get a thread-local record output for the supplied DataOutput.
   * @param out data output stream
   * @return binary record output corresponding to the supplied DataOutput.
   */
  public static @OsUntrusted BinaryRecordOutput get(@OsUntrusted DataOutput out) {
    @OsUntrusted
    BinaryRecordOutput bout = (@OsUntrusted BinaryRecordOutput) bOut.get();
    bout.setDataOutput(out);
    return bout;
  }
    
  /** Creates a new instance of BinaryRecordOutput */
  public @OsUntrusted BinaryRecordOutput(@OsUntrusted OutputStream out) {
    this.out = new @OsUntrusted DataOutputStream(out);
  }
    
  /** Creates a new instance of BinaryRecordOutput */
  public @OsUntrusted BinaryRecordOutput(@OsUntrusted DataOutput out) {
    this.out = out;
  }
    
    
  @Override
  public void writeByte(@OsUntrusted BinaryRecordOutput this, @OsUntrusted byte b, @OsUntrusted String tag) throws IOException {
    out.writeByte(b);
  }
    
  @Override
  public void writeBool(@OsUntrusted BinaryRecordOutput this, @OsUntrusted boolean b, @OsUntrusted String tag) throws IOException {
    out.writeBoolean(b);
  }
    
  @Override
  public void writeInt(@OsUntrusted BinaryRecordOutput this, @OsUntrusted int i, @OsUntrusted String tag) throws IOException {
    Utils.writeVInt(out, i);
  }
    
  @Override
  public void writeLong(@OsUntrusted BinaryRecordOutput this, @OsUntrusted long l, @OsUntrusted String tag) throws IOException {
    Utils.writeVLong(out, l);
  }
    
  @Override
  public void writeFloat(@OsUntrusted BinaryRecordOutput this, @OsUntrusted float f, @OsUntrusted String tag) throws IOException {
    out.writeFloat(f);
  }
    
  @Override
  public void writeDouble(@OsUntrusted BinaryRecordOutput this, @OsUntrusted double d, @OsUntrusted String tag) throws IOException {
    out.writeDouble(d);
  }
    
  @Override
  public void writeString(@OsUntrusted BinaryRecordOutput this, @OsUntrusted String s, @OsUntrusted String tag) throws IOException {
    Utils.toBinaryString(out, s);
  }
    
  @Override
  public void writeBuffer(@OsUntrusted BinaryRecordOutput this, @OsUntrusted Buffer buf, @OsUntrusted String tag)
    throws IOException {
    @OsUntrusted
    byte @OsUntrusted [] barr = buf.get();
    @OsUntrusted
    int len = buf.getCount();
    Utils.writeVInt(out, len);
    out.write(barr, 0, len);
  }
    
  @Override
  public void startRecord(@OsUntrusted BinaryRecordOutput this, @OsUntrusted Record r, @OsUntrusted String tag) throws IOException {}
    
  @Override
  public void endRecord(@OsUntrusted BinaryRecordOutput this, @OsUntrusted Record r, @OsUntrusted String tag) throws IOException {}
    
  @Override
  public void startVector(@OsUntrusted BinaryRecordOutput this, @OsUntrusted ArrayList v, @OsUntrusted String tag) throws IOException {
    writeInt(v.size(), tag);
  }
    
  @Override
  public void endVector(@OsUntrusted BinaryRecordOutput this, @OsUntrusted ArrayList v, @OsUntrusted String tag) throws IOException {}
    
  @Override
  public void startMap(@OsUntrusted BinaryRecordOutput this, @OsUntrusted TreeMap v, @OsUntrusted String tag) throws IOException {
    writeInt(v.size(), tag);
  }
    
  @Override
  public void endMap(@OsUntrusted BinaryRecordOutput this, @OsUntrusted TreeMap v, @OsUntrusted String tag) throws IOException {}
    
}
