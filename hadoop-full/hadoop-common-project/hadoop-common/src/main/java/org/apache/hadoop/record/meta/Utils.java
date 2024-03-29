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
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.record.RecordInput;

/**
 * Various utility functions for Hadooop record I/O platform.
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Utils {
  
  /** Cannot create a new instance of Utils */
  private @OsUntrusted Utils() {
  }
  
  /**
   * read/skip bytes from stream based on a type
   */
  public static void skip(@OsUntrusted RecordInput rin, @OsUntrusted String tag, @OsUntrusted TypeID typeID) throws IOException {
    switch (typeID.typeVal) {
    case TypeID.RIOType.BOOL: 
      rin.readBool(tag);
      break;
    case TypeID.RIOType.BUFFER: 
      rin.readBuffer(tag);
      break;
    case TypeID.RIOType.BYTE: 
      rin.readByte(tag);
      break;
    case TypeID.RIOType.DOUBLE: 
      rin.readDouble(tag);
      break;
    case TypeID.RIOType.FLOAT: 
      rin.readFloat(tag);
      break;
    case TypeID.RIOType.INT: 
      rin.readInt(tag);
      break;
    case TypeID.RIOType.LONG: 
      rin.readLong(tag);
      break;
    case TypeID.RIOType.MAP: 
      org.apache.hadoop.record.Index midx1 = rin.startMap(tag);
      @OsUntrusted
      MapTypeID mtID = (@OsUntrusted MapTypeID) typeID;
      for (; !midx1.done(); midx1.incr()) {
        skip(rin, tag, mtID.getKeyTypeID());
        skip(rin, tag, mtID.getValueTypeID());
      }
      rin.endMap(tag);
      break;
    case TypeID.RIOType.STRING: 
      rin.readString(tag);
      break;
    case TypeID.RIOType.STRUCT:
      rin.startRecord(tag);
      // read past each field in the struct
      @OsUntrusted
      StructTypeID stID = (@OsUntrusted StructTypeID) typeID;
      @OsUntrusted
      Iterator<@OsUntrusted FieldTypeInfo> it = stID.getFieldTypeInfos().iterator();
      while (it.hasNext()) {
        @OsUntrusted
        FieldTypeInfo tInfo = it.next();
        skip(rin, tag, tInfo.getTypeID());
      }
      rin.endRecord(tag);
      break;
    case TypeID.RIOType.VECTOR: 
      org.apache.hadoop.record.Index vidx1 = rin.startVector(tag);
      @OsUntrusted
      VectorTypeID vtID = (@OsUntrusted VectorTypeID) typeID;
      for (; !vidx1.done(); vidx1.incr()) {
        skip(rin, tag, vtID.getElementTypeID());
      }
      rin.endVector(tag);
      break;
    default: 
      // shouldn't be here
      throw new @OsUntrusted IOException("Unknown typeID when skipping bytes");
    }
  }
}
