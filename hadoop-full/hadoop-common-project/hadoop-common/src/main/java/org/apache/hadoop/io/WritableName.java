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

package org.apache.hadoop.io;

import ostrusted.quals.OsUntrusted;
import java.util.HashMap;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/** Utility to permit renaming of Writable implementation classes without
 * invalidiating files that contain their class name.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class WritableName {
  private static @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>> NAME_TO_CLASS =
    new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>>();
  private static @OsUntrusted HashMap<@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>, @OsUntrusted String> CLASS_TO_NAME =
    new @OsUntrusted HashMap<@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>, @OsUntrusted String>();

  static {                                        // define important types
    WritableName.setName(NullWritable.class, "null");
    WritableName.setName(LongWritable.class, "long");
    WritableName.setName(UTF8.class, "UTF8");
    WritableName.setName(MD5Hash.class, "MD5Hash");
  }

  private @OsUntrusted WritableName() {}                      // no public ctor

  /** Set the name that a class should be known as to something other than the
   * class name. */
  public static synchronized void setName(@OsUntrusted Class writableClass, @OsUntrusted String name) {
    CLASS_TO_NAME.put(writableClass, name);
    NAME_TO_CLASS.put(name, writableClass);
  }

  /** Add an alternate name for a class. */
  public static synchronized void addName(@OsUntrusted Class writableClass, @OsUntrusted String name) {
    NAME_TO_CLASS.put(name, writableClass);
  }

  /** Return the name for a class.  Default is {@link Class#getName()}. */
  public static synchronized @OsUntrusted String getName(@OsUntrusted Class writableClass) {
    @OsUntrusted
    String name = CLASS_TO_NAME.get(writableClass);
    if (name != null)
      return name;
    return writableClass.getName();
  }

  /** Return the class for a name.  Default is {@link Class#forName(String)}.*/
  public static synchronized @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> getClass(@OsUntrusted String name, @OsUntrusted Configuration conf
                                            ) throws IOException {
    @OsUntrusted
    Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> writableClass = NAME_TO_CLASS.get(name);
    if (writableClass != null)
      return writableClass.asSubclass(Writable.class);
    try {
      return conf.getClassByName(name);
    } catch (@OsUntrusted ClassNotFoundException e) {
      @OsUntrusted
      IOException newE = new @OsUntrusted IOException("WritableName can't load class: " + name);
      newE.initCause(e);
      throw newE;
    }
  }

}
