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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.record.RecordOutput;

/** 
 * Represents a type information for a field, which is made up of its 
 * ID (name) and its type (a TypeID object).
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FieldTypeInfo
{

  private @OsUntrusted String fieldID;
  private @OsUntrusted TypeID typeID;

  /**
   * Construct a FiledTypeInfo with the given field name and the type
   */
  @OsUntrusted
  FieldTypeInfo(@OsUntrusted String fieldID, @OsUntrusted TypeID typeID) {
    this.fieldID = fieldID;
    this.typeID = typeID;
  }

  /**
   * get the field's TypeID object
   */
  public @OsUntrusted TypeID getTypeID(@OsUntrusted FieldTypeInfo this) {
    return typeID;
  }
  
  /**
   * get the field's id (name)
   */
  public @OsUntrusted String getFieldID(@OsUntrusted FieldTypeInfo this) {
    return fieldID;
  }
  
  void write(@OsUntrusted FieldTypeInfo this, @OsUntrusted RecordOutput rout, @OsUntrusted String tag) throws IOException {
    rout.writeString(fieldID, tag);
    typeID.write(rout, tag);
  }
  
  /**
   * Two FieldTypeInfos are equal if ach of their fields matches
   */
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted FieldTypeInfo this, @OsUntrusted Object o) {
    if (this == o) 
      return true;
    if (!(o instanceof @OsUntrusted FieldTypeInfo))
      return false;
    @OsUntrusted
    FieldTypeInfo fti = (@OsUntrusted FieldTypeInfo) o;
    // first check if fieldID matches
    if (!this.fieldID.equals(fti.fieldID)) {
      return false;
    }
    // now see if typeID matches
    return (this.typeID.equals(fti.typeID));
  }
  
  /**
   * We use a basic hashcode implementation, since this class will likely not
   * be used as a hashmap key 
   */
  @Override
  public @OsUntrusted int hashCode(@OsUntrusted FieldTypeInfo this) {
    return 37*17+typeID.hashCode() + 37*17+fieldID.hashCode();
  }
  

  public @OsUntrusted boolean equals(@OsUntrusted FieldTypeInfo this, @OsUntrusted FieldTypeInfo ti) {
    // first check if fieldID matches
    if (!this.fieldID.equals(ti.fieldID)) {
      return false;
    }
    // now see if typeID matches
    return (this.typeID.equals(ti.typeID));
  }

}

