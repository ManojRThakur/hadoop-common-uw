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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A raw record comparator base class
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class RecordComparator extends @OsUntrusted WritableComparator {
  
  /**
   * Construct a raw {@link Record} comparison implementation. */
  protected @OsUntrusted RecordComparator(@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> recordClass) {
    super(recordClass);
  }
  
  // inheric JavaDoc
  @Override
  public abstract @OsUntrusted int compare(@OsUntrusted RecordComparator this, @OsUntrusted byte @OsUntrusted [] b1, @OsUntrusted int s1, @OsUntrusted int l1, @OsUntrusted byte @OsUntrusted [] b2, @OsUntrusted int s2, @OsUntrusted int l2);
  
  /**
   * Register an optimized comparator for a {@link Record} implementation.
   *
   * @param c record classs for which a raw comparator is provided
   * @param comparator Raw comparator instance for class c 
   */
  public static synchronized void define(@OsUntrusted Class c, @OsUntrusted RecordComparator comparator) {
    WritableComparator.define(c, comparator);
  }
}
