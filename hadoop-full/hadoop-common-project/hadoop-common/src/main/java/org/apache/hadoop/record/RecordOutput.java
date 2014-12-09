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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Interface that all the serializers have to implement.
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface RecordOutput {
  /**
   * Write a byte to serialized record.
   * @param b Byte to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeByte(@OsUntrusted RecordOutput this, @OsUntrusted byte b, @OsUntrusted String tag) throws IOException;
  
  /**
   * Write a boolean to serialized record.
   * @param b Boolean to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeBool(@OsUntrusted RecordOutput this, @OsUntrusted boolean b, @OsUntrusted String tag) throws IOException;
  
  /**
   * Write an integer to serialized record.
   * @param i Integer to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeInt(@OsUntrusted RecordOutput this, @OsUntrusted int i, @OsUntrusted String tag) throws IOException;
  
  /**
   * Write a long integer to serialized record.
   * @param l Long to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeLong(@OsUntrusted RecordOutput this, @OsUntrusted long l, @OsUntrusted String tag) throws IOException;
  
  /**
   * Write a single-precision float to serialized record.
   * @param f Float to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeFloat(@OsUntrusted RecordOutput this, @OsUntrusted float f, @OsUntrusted String tag) throws IOException;
  
  /**
   * Write a double precision floating point number to serialized record.
   * @param d Double to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeDouble(@OsUntrusted RecordOutput this, @OsUntrusted double d, @OsUntrusted String tag) throws IOException;
  
  /**
   * Write a unicode string to serialized record.
   * @param s String to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeString(@OsUntrusted RecordOutput this, @OsUntrusted String s, @OsUntrusted String tag) throws IOException;
  
  /**
   * Write a buffer to serialized record.
   * @param buf Buffer to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeBuffer(@OsUntrusted RecordOutput this, @OsUntrusted Buffer buf, @OsUntrusted String tag)
    throws IOException;
  
  /**
   * Mark the start of a record to be serialized.
   * @param r Record to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void startRecord(@OsUntrusted RecordOutput this, @OsUntrusted Record r, @OsUntrusted String tag) throws IOException;
  
  /**
   * Mark the end of a serialized record.
   * @param r Record to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void endRecord(@OsUntrusted RecordOutput this, @OsUntrusted Record r, @OsUntrusted String tag) throws IOException;
  
  /**
   * Mark the start of a vector to be serialized.
   * @param v Vector to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void startVector(@OsUntrusted RecordOutput this, @OsUntrusted ArrayList v, @OsUntrusted String tag) throws IOException;
  
  /**
   * Mark the end of a serialized vector.
   * @param v Vector to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void endVector(@OsUntrusted RecordOutput this, @OsUntrusted ArrayList v, @OsUntrusted String tag) throws IOException;
  
  /**
   * Mark the start of a map to be serialized.
   * @param m Map to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void startMap(@OsUntrusted RecordOutput this, @OsUntrusted TreeMap m, @OsUntrusted String tag) throws IOException;
  
  /**
   * Mark the end of a serialized map.
   * @param m Map to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void endMap(@OsUntrusted RecordOutput this, @OsUntrusted TreeMap m, @OsUntrusted String tag) throws IOException;
}
