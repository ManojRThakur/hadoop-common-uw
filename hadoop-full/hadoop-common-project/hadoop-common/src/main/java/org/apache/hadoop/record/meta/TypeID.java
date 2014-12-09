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
 * Represents typeID for basic types.
 *  
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TypeID {

  /**
   * constants representing the IDL types we support
   */
  public static final class RIOType {
    public static final @OsUntrusted byte BOOL   = 1;
    public static final @OsUntrusted byte BUFFER = 2;
    public static final @OsUntrusted byte BYTE   = 3;
    public static final @OsUntrusted byte DOUBLE = 4;
    public static final @OsUntrusted byte FLOAT  = 5;
    public static final @OsUntrusted byte INT    = 6;
    public static final @OsUntrusted byte LONG   = 7;
    public static final @OsUntrusted byte MAP    = 8;
    public static final @OsUntrusted byte STRING = 9;
    public static final @OsUntrusted byte STRUCT = 10;
    public static final @OsUntrusted byte VECTOR = 11;
  }

  /**
   * Constant classes for the basic types, so we can share them.
   */
  public static final @OsUntrusted TypeID BoolTypeID = new @OsUntrusted TypeID(RIOType.BOOL);
  public static final @OsUntrusted TypeID BufferTypeID = new @OsUntrusted TypeID(RIOType.BUFFER);
  public static final @OsUntrusted TypeID ByteTypeID = new @OsUntrusted TypeID(RIOType.BYTE);
  public static final @OsUntrusted TypeID DoubleTypeID = new @OsUntrusted TypeID(RIOType.DOUBLE);
  public static final @OsUntrusted TypeID FloatTypeID = new @OsUntrusted TypeID(RIOType.FLOAT);
  public static final @OsUntrusted TypeID IntTypeID = new @OsUntrusted TypeID(RIOType.INT);
  public static final @OsUntrusted TypeID LongTypeID = new @OsUntrusted TypeID(RIOType.LONG);
  public static final @OsUntrusted TypeID StringTypeID = new @OsUntrusted TypeID(RIOType.STRING);
  
  protected @OsUntrusted byte typeVal;

  /**
   * Create a TypeID object 
   */
  @OsUntrusted
  TypeID(@OsUntrusted byte typeVal) {
    this.typeVal = typeVal;
  }

  /**
   * Get the type value. One of the constants in RIOType.
   */
  public @OsUntrusted byte getTypeVal(@OsUntrusted TypeID this) {
    return typeVal;
  }

  /**
   * Serialize the TypeID object
   */
  void write(@OsUntrusted TypeID this, @OsUntrusted RecordOutput rout, @OsUntrusted String tag) throws IOException {
    rout.writeByte(typeVal, tag);
  }
  
  /**
   * Two base typeIDs are equal if they refer to the same type
   */
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted TypeID this, @OsUntrusted Object o) {
    if (this == o) 
      return true;

    if (o == null)
      return false;

    if (this.getClass() != o.getClass())
      return false;

    @OsUntrusted
    TypeID oTypeID = (@OsUntrusted TypeID) o;
    return (this.typeVal == oTypeID.typeVal);
  }
  
  /**
   * We use a basic hashcode implementation, since this class will likely not
   * be used as a hashmap key 
   */
  @Override
  public @OsUntrusted int hashCode(@OsUntrusted TypeID this) {
    // See 'Effectve Java' by Joshua Bloch
    return 37*17+(@OsUntrusted int)typeVal;
  }
}

