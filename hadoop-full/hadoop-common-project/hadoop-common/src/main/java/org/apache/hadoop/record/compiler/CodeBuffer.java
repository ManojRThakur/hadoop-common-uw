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
package org.apache.hadoop.record.compiler;

import ostrusted.quals.OsUntrusted;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A wrapper around StringBuffer that automatically does indentation
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CodeBuffer {
  
  static private @OsUntrusted ArrayList<@OsUntrusted Character> startMarkers = new @OsUntrusted ArrayList<@OsUntrusted Character>();
  static private @OsUntrusted ArrayList<@OsUntrusted Character> endMarkers = new @OsUntrusted ArrayList<@OsUntrusted Character>();
  
  static {
    addMarkers('{', '}');
    addMarkers('(', ')');
  }
  
  static void addMarkers(@OsUntrusted char ch1, @OsUntrusted char ch2) {
    startMarkers.add(ch1);
    endMarkers.add(ch2);
  }
  
  private @OsUntrusted int level = 0;
  private @OsUntrusted int numSpaces = 2;
  private @OsUntrusted boolean firstChar = true;
  private @OsUntrusted StringBuffer sb;
  
  /** Creates a new instance of CodeBuffer */
  @OsUntrusted
  CodeBuffer() {
    this(2, "");
  }
  
  @OsUntrusted
  CodeBuffer(@OsUntrusted String s) {
    this(2, s);
  }
  
  @OsUntrusted
  CodeBuffer(@OsUntrusted int numSpaces, @OsUntrusted String s) {
    sb = new @OsUntrusted StringBuffer();
    this.numSpaces = numSpaces;
    this.append(s);
  }
  
  void append(@OsUntrusted CodeBuffer this, @OsUntrusted String s) {
    @OsUntrusted
    int length = s.length();
    for (@OsUntrusted int idx = 0; idx < length; idx++) {
      @OsUntrusted
      char ch = s.charAt(idx);
      append(ch);
    }
  }
  
  void append(@OsUntrusted CodeBuffer this, @OsUntrusted char ch) {
    if (endMarkers.contains(ch)) {
      level--;
    }
    if (firstChar) {
      for (@OsUntrusted int idx = 0; idx < level; idx++) {
        for (@OsUntrusted int num = 0; num < numSpaces; num++) {
          rawAppend(' ');
        }
      }
    }
    rawAppend(ch);
    firstChar = false;
    if (startMarkers.contains(ch)) {
      level++;
    }
    if (ch == '\n') {
      firstChar = true;
    }
  }

  private void rawAppend(@OsUntrusted CodeBuffer this, @OsUntrusted char ch) {
    sb.append(ch);
  }
  
  @Override
  public @OsUntrusted String toString(@OsUntrusted CodeBuffer this) {
    return sb.toString();
  }
}
