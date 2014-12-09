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

package org.apache.hadoop.io.serializer;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.InputBuffer;
import org.apache.hadoop.io.RawComparator;

/**
 * <p>
 * A {@link RawComparator} that uses a {@link Deserializer} to deserialize
 * the objects to be compared so that the standard {@link Comparator} can
 * be used to compare them.
 * </p>
 * <p>
 * One may optimize compare-intensive operations by using a custom
 * implementation of {@link RawComparator} that operates directly
 * on byte representations.
 * </p>
 * @param <T>
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public abstract class DeserializerComparator<@OsUntrusted T extends java.lang.@OsUntrusted Object> implements @OsUntrusted RawComparator<T> {
  
  private @OsUntrusted InputBuffer buffer = new @OsUntrusted InputBuffer();
  private @OsUntrusted Deserializer<@OsUntrusted T> deserializer;
  
  private @OsUntrusted T key1;
  private @OsUntrusted T key2;

  protected @OsUntrusted DeserializerComparator(@OsUntrusted Deserializer<@OsUntrusted T> deserializer)
    throws IOException {
    
    this.deserializer = deserializer;
    this.deserializer.open(buffer);
  }

  @Override
  public @OsUntrusted int compare(@OsUntrusted DeserializerComparator<T> this, @OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1, @OsUntrusted byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2) {
    try {
      
      buffer.reset(b1, s1, l1);
      key1 = deserializer.deserialize(key1);
      
      buffer.reset(b2, s2, l2);
      key2 = deserializer.deserialize(key2);
      
    } catch (@OsUntrusted IOException e) {
      throw new @OsUntrusted RuntimeException(e);
    }
    return compare(key1, key2);
  }

}
