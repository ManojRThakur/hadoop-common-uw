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
import java.io.PrintStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CsvRecordOutput implements @OsUntrusted RecordOutput {

  private @OsUntrusted PrintStream stream;
  private @OsUntrusted boolean isFirst = true;
    
  private void throwExceptionOnError(@OsUntrusted CsvRecordOutput this, @OsUntrusted String tag) throws IOException {
    if (stream.checkError()) {
      throw new @OsUntrusted IOException("Error serializing "+tag);
    }
  }
 
  private void printCommaUnlessFirst(@OsUntrusted CsvRecordOutput this) {
    if (!isFirst) {
      stream.print(",");
    }
    isFirst = false;
  }
    
  /** Creates a new instance of CsvRecordOutput */
  public @OsUntrusted CsvRecordOutput(@OsUntrusted OutputStream out) {
    try {
      stream = new @OsUntrusted PrintStream(out, true, "UTF-8");
    } catch (@OsUntrusted UnsupportedEncodingException ex) {
      throw new @OsUntrusted RuntimeException(ex);
    }
  }
    
  @Override
  public void writeByte(@OsUntrusted CsvRecordOutput this, @OsUntrusted byte b, @OsUntrusted String tag) throws IOException {
    writeLong((@OsUntrusted long)b, tag);
  }
    
  @Override
  public void writeBool(@OsUntrusted CsvRecordOutput this, @OsUntrusted boolean b, @OsUntrusted String tag) throws IOException {
    printCommaUnlessFirst();
    @OsUntrusted
    String val = b ? "T" : "F";
    stream.print(val);
    throwExceptionOnError(tag);
  }
    
  @Override
  public void writeInt(@OsUntrusted CsvRecordOutput this, @OsUntrusted int i, @OsUntrusted String tag) throws IOException {
    writeLong((@OsUntrusted long)i, tag);
  }
    
  @Override
  public void writeLong(@OsUntrusted CsvRecordOutput this, @OsUntrusted long l, @OsUntrusted String tag) throws IOException {
    printCommaUnlessFirst();
    stream.print(l);
    throwExceptionOnError(tag);
  }
    
  @Override
  public void writeFloat(@OsUntrusted CsvRecordOutput this, @OsUntrusted float f, @OsUntrusted String tag) throws IOException {
    writeDouble((@OsUntrusted double)f, tag);
  }
    
  @Override
  public void writeDouble(@OsUntrusted CsvRecordOutput this, @OsUntrusted double d, @OsUntrusted String tag) throws IOException {
    printCommaUnlessFirst();
    stream.print(d);
    throwExceptionOnError(tag);
  }
    
  @Override
  public void writeString(@OsUntrusted CsvRecordOutput this, @OsUntrusted String s, @OsUntrusted String tag) throws IOException {
    printCommaUnlessFirst();
    stream.print(Utils.toCSVString(s));
    throwExceptionOnError(tag);
  }
    
  @Override
  public void writeBuffer(@OsUntrusted CsvRecordOutput this, @OsUntrusted Buffer buf, @OsUntrusted String tag)
    throws IOException {
    printCommaUnlessFirst();
    stream.print(Utils.toCSVBuffer(buf));
    throwExceptionOnError(tag);
  }
    
  @Override
  public void startRecord(@OsUntrusted CsvRecordOutput this, @OsUntrusted Record r, @OsUntrusted String tag) throws IOException {
    if (tag != null && ! tag.isEmpty()) {
      printCommaUnlessFirst();
      stream.print("s{");
      isFirst = true;
    }
  }
    
  @Override
  public void endRecord(@OsUntrusted CsvRecordOutput this, @OsUntrusted Record r, @OsUntrusted String tag) throws IOException {
    if (tag == null || tag.isEmpty()) {
      stream.print("\n");
      isFirst = true;
    } else {
      stream.print("}");
      isFirst = false;
    }
  }
    
  @Override
  public void startVector(@OsUntrusted CsvRecordOutput this, @OsUntrusted ArrayList v, @OsUntrusted String tag) throws IOException {
    printCommaUnlessFirst();
    stream.print("v{");
    isFirst = true;
  }
    
  @Override
  public void endVector(@OsUntrusted CsvRecordOutput this, @OsUntrusted ArrayList v, @OsUntrusted String tag) throws IOException {
    stream.print("}");
    isFirst = false;
  }
    
  @Override
  public void startMap(@OsUntrusted CsvRecordOutput this, @OsUntrusted TreeMap v, @OsUntrusted String tag) throws IOException {
    printCommaUnlessFirst();
    stream.print("m{");
    isFirst = true;
  }
    
  @Override
  public void endMap(@OsUntrusted CsvRecordOutput this, @OsUntrusted TreeMap v, @OsUntrusted String tag) throws IOException {
    stream.print("}");
    isFirst = false;
  }
}
