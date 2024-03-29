/*
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

package org.apache.hadoop.util;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class ClassUtil {
  /** 
   * Find a jar that contains a class of the same name, if any.
   * It will return a jar file, even if that is not the first thing
   * on the class path that has a class with the same name.
   * 
   * @param clazz the class to find.
   * @return a jar file that contains the class, or null.
   * @throws IOException
   */
  public static @OsUntrusted String findContainingJar(@OsUntrusted Class clazz) {
    @OsUntrusted
    ClassLoader loader = clazz.getClassLoader();
    @OsUntrusted
    String classFile = clazz.getName().replaceAll("\\.", "/") + ".class";
    try {
      for (@OsUntrusted Enumeration itr = loader.getResources(classFile);
          itr.hasMoreElements();) {
        @OsUntrusted
        URL url = (@OsUntrusted URL) itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          @OsUntrusted
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (@OsUntrusted IOException e) {
      throw new @OsUntrusted RuntimeException(e);
    }
    return null;
  }
}
