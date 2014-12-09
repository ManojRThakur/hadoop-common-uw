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

package org.apache.hadoop.record.meta;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.record.RecordInput;
import org.apache.hadoop.record.RecordOutput;


/** 
 * A record's Type Information object which can read/write itself. 
 * 
 * Type information for a record comprises metadata about the record, 
 * as well as a collection of type information for each field in the record. 
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RecordTypeInfo extends org.apache.hadoop.record.Record 
{

  private @OsUntrusted String name;
  // A RecordTypeInfo is really just a wrapper around StructTypeID
  @OsUntrusted
  StructTypeID sTid;
   // A RecordTypeInfo object is just a collection of TypeInfo objects for each of its fields.  
  //private ArrayList<FieldTypeInfo> typeInfos = new ArrayList<FieldTypeInfo>();
  // we keep a hashmap of struct/record names and their type information, as we need it to 
  // set filters when reading nested structs. This map is used during deserialization.
  //private Map<String, RecordTypeInfo> structRTIs = new HashMap<String, RecordTypeInfo>();

  /**
   * Create an empty RecordTypeInfo object.
   */
  public @OsUntrusted RecordTypeInfo() {
    sTid = new @OsUntrusted StructTypeID();
  }

  /**
   * Create a RecordTypeInfo object representing a record with the given name
   * @param name Name of the record
   */
  public @OsUntrusted RecordTypeInfo(@OsUntrusted String name) {
    this.name = name;
    sTid = new @OsUntrusted StructTypeID();
  }

  /*
   * private constructor
   */
  private @OsUntrusted RecordTypeInfo(@OsUntrusted String name, @OsUntrusted StructTypeID stid) {
    this.sTid = stid;
    this.name = name;
  }
  
  /**
   * return the name of the record
   */
  public @OsUntrusted String getName(@OsUntrusted RecordTypeInfo this) {
    return name;
  }

  /**
   * set the name of the record
   */
  public void setName(@OsUntrusted RecordTypeInfo this, @OsUntrusted String name) {
    this.name = name;
  }

  /**
   * Add a field. 
   * @param fieldName Name of the field
   * @param tid Type ID of the field
   */
  public void addField(@OsUntrusted RecordTypeInfo this, @OsUntrusted String fieldName, @OsUntrusted TypeID tid) {
    sTid.getFieldTypeInfos().add(new @OsUntrusted FieldTypeInfo(fieldName, tid));
  }
  
  private void addAll(@OsUntrusted RecordTypeInfo this, @OsUntrusted Collection<@OsUntrusted FieldTypeInfo> tis) {
    sTid.getFieldTypeInfos().addAll(tis);
  }

  /**
   * Return a collection of field type infos
   */
  public @OsUntrusted Collection<@OsUntrusted FieldTypeInfo> getFieldTypeInfos(@OsUntrusted RecordTypeInfo this) {
    return sTid.getFieldTypeInfos();
  }
  
  /**
   * Return the type info of a nested record. We only consider nesting 
   * to one level. 
   * @param name Name of the nested record
   */
  public @OsUntrusted RecordTypeInfo getNestedStructTypeInfo(@OsUntrusted RecordTypeInfo this, @OsUntrusted String name) {
    @OsUntrusted
    StructTypeID stid = sTid.findStruct(name);
    if (null == stid) return null;
    return new @OsUntrusted RecordTypeInfo(name, stid);
  }

  /**
   * Serialize the type information for a record
   */
  @Override
  public void serialize(@OsUntrusted RecordTypeInfo this, @OsUntrusted RecordOutput rout, @OsUntrusted String tag) throws IOException {
    // write out any header, version info, here
    rout.startRecord(this, tag);
    rout.writeString(name, tag);
    sTid.writeRest(rout, tag);
    rout.endRecord(this, tag);
  }

  /**
   * Deserialize the type information for a record
   */
  @Override
  public void deserialize(@OsUntrusted RecordTypeInfo this, @OsUntrusted RecordInput rin, @OsUntrusted String tag) throws IOException {
    // read in any header, version info 
    rin.startRecord(tag);
    // name
    this.name = rin.readString(tag);
    sTid.read(rin, tag);
    rin.endRecord(tag);
  }
  
  /**
   * This class doesn't implement Comparable as it's not meant to be used 
   * for anything besides de/serializing.
   * So we always throw an exception.
   * Not implemented. Always returns 0 if another RecordTypeInfo is passed in. 
   */
  @Override
  public @OsUntrusted int compareTo (@OsUntrusted RecordTypeInfo this, final @OsUntrusted Object peer_) throws ClassCastException {
    if (!(peer_ instanceof @OsUntrusted RecordTypeInfo)) {
      throw new @OsUntrusted ClassCastException("Comparing different types of records.");
    }
    throw new @OsUntrusted UnsupportedOperationException("compareTo() is not supported");
  }
}
