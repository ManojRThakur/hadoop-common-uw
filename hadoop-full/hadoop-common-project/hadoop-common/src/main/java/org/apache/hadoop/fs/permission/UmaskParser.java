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

import ostrusted.quals.OsTrusted;
import ostrusted.quals.OsUntrusted;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Parse umask value provided as a string, either in octal or symbolic
 * format and return it as a short value. Umask values are slightly
 * different from standard modes as they cannot specify sticky bit
 * or X.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class UmaskParser extends @OsUntrusted PermissionParser {
  private static @OsUntrusted Pattern chmodOctalPattern = Pattern.compile("^\\s*[+]?()([0-7]{3})\\s*$"); // no leading 1 for sticky bit
  private static @OsUntrusted Pattern umaskSymbolicPattern =    /* not allow X or t */
            Pattern.compile("\\G\\s*([ugoa]*)([+=-]+)([rwx]*)([,\\s]*)\\s*");
  final @OsTrusted short umaskMode;

  @SuppressWarnings({ "untrusted:unsafe.cast", "argument.type.incompatible"})
  public @OsUntrusted UmaskParser(@OsUntrusted String modeStr) throws IllegalArgumentException {
    super(modeStr, umaskSymbolicPattern, chmodOctalPattern);

    umaskMode = (@OsTrusted short) combineModes(0, false);
  }

  /**
   * To be used for file/directory creation only. Symbolic umask is applied
   * relative to file mode creation mask; the permission op characters '+'
   * results in clearing the corresponding bit in the mask, '-' results in bits
   * for indicated permission to be set in the mask.
   * 
   * For octal umask, the specified bits are set in the file mode creation mask.
   * 
   * @return umask
   */
  @SuppressWarnings("untrusted:unsafe.cast")
  public @OsTrusted short getUMask(@OsUntrusted UmaskParser this) {
    if (symbolic) {
      // Return the complement of octal equivalent of umask that was computed
      return (@OsTrusted short) (~umaskMode & 0777);
    }
    return umaskMode;
  }
}
