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
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.IOException;
import java.io.PushbackReader;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CsvRecordInput implements @OsUntrusted RecordInput {
    
  private @OsUntrusted PushbackReader stream;
    
  private class CsvIndex implements @OsUntrusted Index {
    @Override
    public @OsUntrusted boolean done(@OsUntrusted CsvRecordInput.CsvIndex this) {
      @OsUntrusted
      char c = '\0';
      try {
        c = (@OsUntrusted char) stream.read();
        stream.unread(c);
      } catch (@OsUntrusted IOException ex) {
      }
      return (c == '}') ? true : false;
    }
    @Override
    public void incr(@OsUntrusted CsvRecordInput.CsvIndex this) {}
  }
    
  private void throwExceptionOnError(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    throw new @OsUntrusted IOException("Error deserializing "+tag);
  }
    
  private @OsUntrusted String readField(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    try {
      @OsUntrusted
      StringBuilder buf = new @OsUntrusted StringBuilder();
      while (true) {
        @OsUntrusted
        char c = (@OsUntrusted char) stream.read();
        switch (c) {
        case ',':
          return buf.toString();
        case '}':
        case '\n':
        case '\r':
          stream.unread(c);
          return buf.toString();
        default:
          buf.append(c);
        }
      }
    } catch (@OsUntrusted IOException ex) {
      throw new @OsUntrusted IOException("Error reading "+tag);
    }
  }
    
  /** Creates a new instance of CsvRecordInput */
  public @OsUntrusted CsvRecordInput(@OsUntrusted InputStream in) {
    try {
      stream = new @OsUntrusted PushbackReader(new @OsUntrusted InputStreamReader(in, "UTF-8"));
    } catch (@OsUntrusted UnsupportedEncodingException ex) {
      throw new @OsUntrusted RuntimeException(ex);
    }
  }
    
  @Override
  public @OsUntrusted byte readByte(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    return (@OsUntrusted byte) readLong(tag);
  }
    
  @Override
  public @OsUntrusted boolean readBool(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    String sval = readField(tag);
    return "T".equals(sval) ? true : false;
  }
    
  @Override
  public @OsUntrusted int readInt(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    return (@OsUntrusted int) readLong(tag);
  }
    
  @Override
  public @OsUntrusted long readLong(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    String sval = readField(tag);
    try {
      @OsUntrusted
      long lval = Long.parseLong(sval);
      return lval;
    } catch (@OsUntrusted NumberFormatException ex) {
      throw new @OsUntrusted IOException("Error deserializing "+tag);
    }
  }
    
  @Override
  public @OsUntrusted float readFloat(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    return (@OsUntrusted float) readDouble(tag);
  }
    
  @Override
  public @OsUntrusted double readDouble(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    String sval = readField(tag);
    try {
      @OsUntrusted
      double dval = Double.parseDouble(sval);
      return dval;
    } catch (@OsUntrusted NumberFormatException ex) {
      throw new @OsUntrusted IOException("Error deserializing "+tag);
    }
  }
    
  @Override
  public @OsUntrusted String readString(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    String sval = readField(tag);
    return Utils.fromCSVString(sval);
  }
    
  @Override
  public @OsUntrusted Buffer readBuffer(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    String sval = readField(tag);
    return Utils.fromCSVBuffer(sval);
  }
    
  @Override
  public void startRecord(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    if (tag != null && !tag.isEmpty()) {
      @OsUntrusted
      char c1 = (@OsUntrusted char) stream.read();
      @OsUntrusted
      char c2 = (@OsUntrusted char) stream.read();
      if (c1 != 's' || c2 != '{') {
        throw new @OsUntrusted IOException("Error deserializing "+tag);
      }
    }
  }
    
  @Override
  public void endRecord(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    char c = (@OsUntrusted char) stream.read();
    if (tag == null || tag.isEmpty()) {
      if (c != '\n' && c != '\r') {
        throw new @OsUntrusted IOException("Error deserializing record.");
      } else {
        return;
      }
    }
        
    if (c != '}') {
      throw new @OsUntrusted IOException("Error deserializing "+tag);
    }
    c = (@OsUntrusted char) stream.read();
    if (c != ',') {
      stream.unread(c);
    }
        
    return;
  }
    
  @Override
  public @OsUntrusted Index startVector(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    char c1 = (@OsUntrusted char) stream.read();
    @OsUntrusted
    char c2 = (@OsUntrusted char) stream.read();
    if (c1 != 'v' || c2 != '{') {
      throw new @OsUntrusted IOException("Error deserializing "+tag);
    }
    return new @OsUntrusted CsvIndex();
  }
    
  @Override
  public void endVector(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    char c = (@OsUntrusted char) stream.read();
    if (c != '}') {
      throw new @OsUntrusted IOException("Error deserializing "+tag);
    }
    c = (@OsUntrusted char) stream.read();
    if (c != ',') {
      stream.unread(c);
    }
    return;
  }
    
  @Override
  public @OsUntrusted Index startMap(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    char c1 = (@OsUntrusted char) stream.read();
    @OsUntrusted
    char c2 = (@OsUntrusted char) stream.read();
    if (c1 != 'm' || c2 != '{') {
      throw new @OsUntrusted IOException("Error deserializing "+tag);
    }
    return new @OsUntrusted CsvIndex();
  }
    
  @Override
  public void endMap(@OsUntrusted CsvRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    char c = (@OsUntrusted char) stream.read();
    if (c != '}') {
      throw new @OsUntrusted IOException("Error deserializing "+tag);
    }
    c = (@OsUntrusted char) stream.read();
    if (c != ',') {
      stream.unread(c);
    }
    return;
  }
}
