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
import java.util.Stack;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * XML Serializer.
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class XmlRecordOutput implements @OsUntrusted RecordOutput {

  private @OsUntrusted PrintStream stream;
    
  private @OsUntrusted int indent = 0;
    
  private @OsUntrusted Stack<@OsUntrusted String> compoundStack;
    
  private void putIndent(@OsUntrusted XmlRecordOutput this) {
    @OsUntrusted
    StringBuilder sb = new @OsUntrusted StringBuilder("");
    for (@OsUntrusted int idx = 0; idx < indent; idx++) {
      sb.append("  ");
    }
    stream.print(sb.toString());
  }
    
  private void addIndent(@OsUntrusted XmlRecordOutput this) {
    indent++;
  }
    
  private void closeIndent(@OsUntrusted XmlRecordOutput this) {
    indent--;
  }
    
  private void printBeginEnvelope(@OsUntrusted XmlRecordOutput this, @OsUntrusted String tag) {
    if (!compoundStack.empty()) {
      @OsUntrusted
      String s = compoundStack.peek();
      if ("struct".equals(s)) {
        putIndent();
        stream.print("<member>\n");
        addIndent();
        putIndent();
        stream.print("<name>"+tag+"</name>\n");
        putIndent();
        stream.print("<value>");
      } else if ("vector".equals(s)) {
        stream.print("<value>");
      } else if ("map".equals(s)) {
        stream.print("<value>");
      }
    } else {
      stream.print("<value>");
    }
  }
    
  private void printEndEnvelope(@OsUntrusted XmlRecordOutput this, @OsUntrusted String tag) {
    if (!compoundStack.empty()) {
      @OsUntrusted
      String s = compoundStack.peek();
      if ("struct".equals(s)) {
        stream.print("</value>\n");
        closeIndent();
        putIndent();
        stream.print("</member>\n");
      } else if ("vector".equals(s)) {
        stream.print("</value>\n");
      } else if ("map".equals(s)) {
        stream.print("</value>\n");
      }
    } else {
      stream.print("</value>\n");
    }
  }
    
  private void insideVector(@OsUntrusted XmlRecordOutput this, @OsUntrusted String tag) {
    printBeginEnvelope(tag);
    compoundStack.push("vector");
  }
    
  private void outsideVector(@OsUntrusted XmlRecordOutput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    String s = compoundStack.pop();
    if (!"vector".equals(s)) {
      throw new @OsUntrusted IOException("Error serializing vector.");
    }
    printEndEnvelope(tag);
  }
    
  private void insideMap(@OsUntrusted XmlRecordOutput this, @OsUntrusted String tag) {
    printBeginEnvelope(tag);
    compoundStack.push("map");
  }
    
  private void outsideMap(@OsUntrusted XmlRecordOutput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    String s = compoundStack.pop();
    if (!"map".equals(s)) {
      throw new @OsUntrusted IOException("Error serializing map.");
    }
    printEndEnvelope(tag);
  }
    
  private void insideRecord(@OsUntrusted XmlRecordOutput this, @OsUntrusted String tag) {
    printBeginEnvelope(tag);
    compoundStack.push("struct");
  }
    
  private void outsideRecord(@OsUntrusted XmlRecordOutput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    String s = compoundStack.pop();
    if (!"struct".equals(s)) {
      throw new @OsUntrusted IOException("Error serializing record.");
    }
    printEndEnvelope(tag);
  }
    
  /** Creates a new instance of XmlRecordOutput */
  public @OsUntrusted XmlRecordOutput(@OsUntrusted OutputStream out) {
    try {
      stream = new @OsUntrusted PrintStream(out, true, "UTF-8");
      compoundStack = new @OsUntrusted Stack<@OsUntrusted String>();
    } catch (@OsUntrusted UnsupportedEncodingException ex) {
      throw new @OsUntrusted RuntimeException(ex);
    }
  }
    
  @Override
  public void writeByte(@OsUntrusted XmlRecordOutput this, @OsUntrusted byte b, @OsUntrusted String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<ex:i1>");
    stream.print(Byte.toString(b));
    stream.print("</ex:i1>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void writeBool(@OsUntrusted XmlRecordOutput this, @OsUntrusted boolean b, @OsUntrusted String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<boolean>");
    stream.print(b ? "1" : "0");
    stream.print("</boolean>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void writeInt(@OsUntrusted XmlRecordOutput this, @OsUntrusted int i, @OsUntrusted String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<i4>");
    stream.print(Integer.toString(i));
    stream.print("</i4>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void writeLong(@OsUntrusted XmlRecordOutput this, @OsUntrusted long l, @OsUntrusted String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<ex:i8>");
    stream.print(Long.toString(l));
    stream.print("</ex:i8>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void writeFloat(@OsUntrusted XmlRecordOutput this, @OsUntrusted float f, @OsUntrusted String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<ex:float>");
    stream.print(Float.toString(f));
    stream.print("</ex:float>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void writeDouble(@OsUntrusted XmlRecordOutput this, @OsUntrusted double d, @OsUntrusted String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<double>");
    stream.print(Double.toString(d));
    stream.print("</double>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void writeString(@OsUntrusted XmlRecordOutput this, @OsUntrusted String s, @OsUntrusted String tag) throws IOException {
    printBeginEnvelope(tag);
    stream.print("<string>");
    stream.print(Utils.toXMLString(s));
    stream.print("</string>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void writeBuffer(@OsUntrusted XmlRecordOutput this, @OsUntrusted Buffer buf, @OsUntrusted String tag)
    throws IOException {
    printBeginEnvelope(tag);
    stream.print("<string>");
    stream.print(Utils.toXMLBuffer(buf));
    stream.print("</string>");
    printEndEnvelope(tag);
  }
    
  @Override
  public void startRecord(@OsUntrusted XmlRecordOutput this, @OsUntrusted Record r, @OsUntrusted String tag) throws IOException {
    insideRecord(tag);
    stream.print("<struct>\n");
    addIndent();
  }
    
  @Override
  public void endRecord(@OsUntrusted XmlRecordOutput this, @OsUntrusted Record r, @OsUntrusted String tag) throws IOException {
    closeIndent();
    putIndent();
    stream.print("</struct>");
    outsideRecord(tag);
  }
    
  @Override
  public void startVector(@OsUntrusted XmlRecordOutput this, @OsUntrusted ArrayList v, @OsUntrusted String tag) throws IOException {
    insideVector(tag);
    stream.print("<array>\n");
    addIndent();
  }
    
  @Override
  public void endVector(@OsUntrusted XmlRecordOutput this, @OsUntrusted ArrayList v, @OsUntrusted String tag) throws IOException {
    closeIndent();
    putIndent();
    stream.print("</array>");
    outsideVector(tag);
  }
    
  @Override
  public void startMap(@OsUntrusted XmlRecordOutput this, @OsUntrusted TreeMap v, @OsUntrusted String tag) throws IOException {
    insideMap(tag);
    stream.print("<array>\n");
    addIndent();
  }
    
  @Override
  public void endMap(@OsUntrusted XmlRecordOutput this, @OsUntrusted TreeMap v, @OsUntrusted String tag) throws IOException {
    closeIndent();
    putIndent();
    stream.print("</array>");
    outsideMap(tag);
  }

}
