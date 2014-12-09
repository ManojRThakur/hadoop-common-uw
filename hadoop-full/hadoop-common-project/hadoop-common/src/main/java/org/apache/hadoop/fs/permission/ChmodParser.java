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
package org.apache.hadoop.fs.permission;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;

/**
 * Parse a permission mode passed in from a chmod command and apply that
 * mode against an existing file.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@SuppressWarnings("ostrusted:cast.unsafe")
public class ChmodParser extends @OsUntrusted PermissionParser {
  private static @OsTrusted Pattern chmodOctalPattern =
          (@OsTrusted Pattern) Pattern.compile("^\\s*[+]?([01]?)([0-7]{3})\\s*$");
  private static @OsTrusted Pattern chmodNormalPattern =
          (@OsTrusted Pattern) Pattern.compile("\\G\\s*([ugoa]*)([+=-]+)([rwxXt]+)([,\\s]*)\\s*");
  
  public @OsUntrusted ChmodParser(@OsUntrusted String modeStr) throws IllegalArgumentException {
    super(modeStr, chmodNormalPattern, chmodOctalPattern);
  }

  /**
   * Apply permission against specified file and determine what the
   * new mode would be
   * @param file File against which to apply mode
   * @return File's new mode if applied.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @OsTrusted short applyNewPermission(@OsUntrusted ChmodParser this, @OsUntrusted FileStatus file) {
    @OsUntrusted
    FsPermission perms = file.getPermission();

    //TODO: Is this iffy?  I assume we want to make FsPermission gotten from a file trusted?
    @OsTrusted int existing = ((@OsTrusted Short) perms.toShort());
    @OsUntrusted
    boolean exeOk = file.isDirectory() || (existing & 0111) != 0;
    
    return (@OsTrusted short) combineModes(existing, exeOk);
  }
}
