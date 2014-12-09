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

package org.apache.hadoop.fs.shell;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Perform shell-like file tests 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class Test extends @OsUntrusted FsCommand {  
  public static void registerCommands(@OsUntrusted CommandFactory factory) {
    factory.addClass(Test.class, "-test");
  }

  public static final @OsUntrusted String NAME = "test";
  public static final @OsUntrusted String USAGE = "-[defsz] <path>";
  public static final @OsUntrusted String DESCRIPTION =
    "Answer various questions about <path>, with result via exit status.\n" +
    "  -d  return 0 if <path> is a directory.\n" +
    "  -e  return 0 if <path> exists.\n" +
    "  -f  return 0 if <path> is a file.\n" +
    "  -s  return 0 if file <path> is greater than zero bytes in size.\n" +
    "  -z  return 0 if file <path> is zero bytes in size.\n" +
    "else, return 1.";

  private @OsUntrusted char flag;
  
  @Override
  protected void processOptions(@OsUntrusted Test this, @OsUntrusted LinkedList<@OsUntrusted String> args) {
    @OsUntrusted
    CommandFormat cf = new @OsUntrusted CommandFormat(1, 1, "e", "d", "f", "s", "z");
    cf.parse(args);
    
    @OsUntrusted
    String @OsUntrusted [] opts = cf.getOpts().toArray(new @OsUntrusted String @OsUntrusted [0]);
    switch (opts.length) {
      case 0:
        throw new @OsUntrusted IllegalArgumentException("No test flag given");
      case 1:
        flag = opts[0].charAt(0);
        break;
      default:
        throw new @OsUntrusted IllegalArgumentException("Only one test flag is allowed");
    }
  }

  @Override
  protected void processPath(@OsUntrusted Test this, @OsUntrusted PathData item) throws IOException {
    @OsUntrusted
    boolean test = false;
    switch (flag) {
      case 'e':
        test = true;
        break;
      case 'd':
        test = item.stat.isDirectory();
        break;
      case 'f':
        test = item.stat.isFile();
        break;
      case 's':
        test = (item.stat.getLen() > 0);
        break;
      case 'z':
        test = (item.stat.getLen() == 0);
        break;
    }
    if (!test) exitCode = 1;
  }

  @Override
  protected void processNonexistentPath(@OsUntrusted Test this, @OsUntrusted PathData item) throws IOException {
    exitCode = 1;
  }
}