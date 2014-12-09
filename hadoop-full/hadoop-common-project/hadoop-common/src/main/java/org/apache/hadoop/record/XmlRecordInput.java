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
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.parsers.SAXParser;

/**
 * XML Deserializer.
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class XmlRecordInput implements @OsUntrusted RecordInput {
    
  static private class Value {
    private @OsUntrusted String type;
    private @OsUntrusted StringBuffer sb;
        
    public @OsUntrusted Value(@OsUntrusted String t) {
      type = t;
      sb = new @OsUntrusted StringBuffer();
    }
    public void addChars(XmlRecordInput.@OsUntrusted Value this, @OsUntrusted char @OsUntrusted [] buf, @OsUntrusted int offset, @OsUntrusted int len) {
      sb.append(buf, offset, len);
    }
    public @OsUntrusted String getValue(XmlRecordInput.@OsUntrusted Value this) { return sb.toString(); }
    public @OsUntrusted String getType(XmlRecordInput.@OsUntrusted Value this) { return type; }
  }
    
  private static class XMLParser extends @OsUntrusted DefaultHandler {
    private @OsUntrusted boolean charsValid = false;
        
    private @OsUntrusted ArrayList<@OsUntrusted Value> valList;
        
    private @OsUntrusted XMLParser(@OsUntrusted ArrayList<@OsUntrusted Value> vlist) {
      valList = vlist;
    }
        
    @Override
    public void startDocument(XmlRecordInput.@OsUntrusted XMLParser this) throws SAXException {}
        
    @Override
    public void endDocument(XmlRecordInput.@OsUntrusted XMLParser this) throws SAXException {}
        
    @Override
    public void startElement(XmlRecordInput.@OsUntrusted XMLParser this, @OsUntrusted String ns,
                             @OsUntrusted
                             String sname,
                             @OsUntrusted
                             String qname,
                             @OsUntrusted
                             Attributes attrs) throws SAXException {
      charsValid = false;
      if ("boolean".equals(qname) ||
          "i4".equals(qname) ||
          "int".equals(qname) ||
          "string".equals(qname) ||
          "double".equals(qname) ||
          "ex:i1".equals(qname) ||
          "ex:i8".equals(qname) ||
          "ex:float".equals(qname)) {
        charsValid = true;
        valList.add(new @OsUntrusted Value(qname));
      } else if ("struct".equals(qname) ||
                 "array".equals(qname)) {
        valList.add(new @OsUntrusted Value(qname));
      }
    }
        
    @Override
    public void endElement(XmlRecordInput.@OsUntrusted XMLParser this, @OsUntrusted String ns,
                           @OsUntrusted
                           String sname,
                           @OsUntrusted
                           String qname) throws SAXException {
      charsValid = false;
      if ("struct".equals(qname) ||
          "array".equals(qname)) {
        valList.add(new @OsUntrusted Value("/"+qname));
      }
    }
        
    @Override
    public void characters(XmlRecordInput.@OsUntrusted XMLParser this, @OsUntrusted char buf @OsUntrusted [], @OsUntrusted int offset, @OsUntrusted int len)
      throws SAXException {
      if (charsValid) {
        @OsUntrusted
        Value v = valList.get(valList.size()-1);
        v.addChars(buf, offset, len);
      }
    }
        
  }
    
  private class XmlIndex implements @OsUntrusted Index {
    @Override
    public @OsUntrusted boolean done(@OsUntrusted XmlRecordInput.XmlIndex this) {
      @OsUntrusted
      Value v = valList.get(vIdx);
      if ("/array".equals(v.getType())) {
        valList.set(vIdx, null);
        vIdx++;
        return true;
      } else {
        return false;
      }
    }
    @Override
    public void incr(@OsUntrusted XmlRecordInput.XmlIndex this) {}
  }
    
  private @OsUntrusted ArrayList<@OsUntrusted Value> valList;
  private @OsUntrusted int vLen;
  private @OsUntrusted int vIdx;
    
  private @OsUntrusted Value next(@OsUntrusted XmlRecordInput this) throws IOException {
    if (vIdx < vLen) {
      @OsUntrusted
      Value v = valList.get(vIdx);
      valList.set(vIdx, null);
      vIdx++;
      return v;
    } else {
      throw new @OsUntrusted IOException("Error in deserialization.");
    }
  }
    
  /** Creates a new instance of XmlRecordInput */
  public @OsUntrusted XmlRecordInput(@OsUntrusted InputStream in) {
    try{
      valList = new @OsUntrusted ArrayList<@OsUntrusted Value>();
      @OsUntrusted
      DefaultHandler handler = new @OsUntrusted XMLParser(valList);
      @OsUntrusted
      SAXParserFactory factory = SAXParserFactory.newInstance();
      @OsUntrusted
      SAXParser parser = factory.newSAXParser();
      parser.parse(in, handler);
      vLen = valList.size();
      vIdx = 0;
    } catch (@OsUntrusted Exception ex) {
      throw new @OsUntrusted RuntimeException(ex);
    }
  }
    
  @Override
  public @OsUntrusted byte readByte(@OsUntrusted XmlRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    Value v = next();
    if (!"ex:i1".equals(v.getType())) {
      throw new @OsUntrusted IOException("Error deserializing "+tag+".");
    }
    return Byte.parseByte(v.getValue());
  }
    
  @Override
  public @OsUntrusted boolean readBool(@OsUntrusted XmlRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    Value v = next();
    if (!"boolean".equals(v.getType())) {
      throw new @OsUntrusted IOException("Error deserializing "+tag+".");
    }
    return "1".equals(v.getValue());
  }
    
  @Override
  public @OsUntrusted int readInt(@OsUntrusted XmlRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    Value v = next();
    if (!"i4".equals(v.getType()) &&
        !"int".equals(v.getType())) {
      throw new @OsUntrusted IOException("Error deserializing "+tag+".");
    }
    return Integer.parseInt(v.getValue());
  }
    
  @Override
  public @OsUntrusted long readLong(@OsUntrusted XmlRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    Value v = next();
    if (!"ex:i8".equals(v.getType())) {
      throw new @OsUntrusted IOException("Error deserializing "+tag+".");
    }
    return Long.parseLong(v.getValue());
  }
    
  @Override
  public @OsUntrusted float readFloat(@OsUntrusted XmlRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    Value v = next();
    if (!"ex:float".equals(v.getType())) {
      throw new @OsUntrusted IOException("Error deserializing "+tag+".");
    }
    return Float.parseFloat(v.getValue());
  }
    
  @Override
  public @OsUntrusted double readDouble(@OsUntrusted XmlRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    Value v = next();
    if (!"double".equals(v.getType())) {
      throw new @OsUntrusted IOException("Error deserializing "+tag+".");
    }
    return Double.parseDouble(v.getValue());
  }
    
  @Override
  public @OsUntrusted String readString(@OsUntrusted XmlRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    Value v = next();
    if (!"string".equals(v.getType())) {
      throw new @OsUntrusted IOException("Error deserializing "+tag+".");
    }
    return Utils.fromXMLString(v.getValue());
  }
    
  @Override
  public @OsUntrusted Buffer readBuffer(@OsUntrusted XmlRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    Value v = next();
    if (!"string".equals(v.getType())) {
      throw new @OsUntrusted IOException("Error deserializing "+tag+".");
    }
    return Utils.fromXMLBuffer(v.getValue());
  }
    
  @Override
  public void startRecord(@OsUntrusted XmlRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    Value v = next();
    if (!"struct".equals(v.getType())) {
      throw new @OsUntrusted IOException("Error deserializing "+tag+".");
    }
  }
    
  @Override
  public void endRecord(@OsUntrusted XmlRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    Value v = next();
    if (!"/struct".equals(v.getType())) {
      throw new @OsUntrusted IOException("Error deserializing "+tag+".");
    }
  }
    
  @Override
  public @OsUntrusted Index startVector(@OsUntrusted XmlRecordInput this, @OsUntrusted String tag) throws IOException {
    @OsUntrusted
    Value v = next();
    if (!"array".equals(v.getType())) {
      throw new @OsUntrusted IOException("Error deserializing "+tag+".");
    }
    return new @OsUntrusted XmlIndex();
  }
    
  @Override
  public void endVector(@OsUntrusted XmlRecordInput this, @OsUntrusted String tag) throws IOException {}
    
  @Override
  public @OsUntrusted Index startMap(@OsUntrusted XmlRecordInput this, @OsUntrusted String tag) throws IOException {
    return startVector(tag);
  }
    
  @Override
  public void endMap(@OsUntrusted XmlRecordInput this, @OsUntrusted String tag) throws IOException { endVector(tag); }

}
