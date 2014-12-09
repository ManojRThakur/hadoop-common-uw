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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Base class for parsing either chmod permissions or umask permissions.
 * Includes common code needed by either operation as implemented in
 * UmaskParser and ChmodParser classes.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class PermissionParser {
  protected @OsUntrusted boolean symbolic = false;
  protected @OsTrusted short userMode;
  protected @OsTrusted short groupMode;
  protected @OsTrusted short othersMode;
  protected @OsTrusted short stickyMode;
  protected @OsUntrusted char userType = '+';
  protected @OsUntrusted char groupType = '+';
  protected @OsUntrusted char othersType = '+';
  protected @OsUntrusted char stickyBitType = '+';
  
  /**
   * Begin parsing permission stored in modeStr
   * 
   * @param modeStr Permission mode, either octal or symbolic
   * @param symbolic Use-case specific symbolic pattern to match against
   * @throws IllegalArgumentException if unable to parse modeStr
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @OsUntrusted PermissionParser(@OsUntrusted String modeStr, @OsTrusted Pattern symbolic, @OsTrusted Pattern octal)
       throws IllegalArgumentException {
    @OsUntrusted Matcher matcher = null;

    if ((matcher = symbolic.matcher(modeStr)).find()) {
      applyNormalPattern(modeStr, (@OsTrusted Matcher) matcher);
    } else if ((matcher = octal.matcher(modeStr)).matches()) {
      applyOctalPattern(modeStr, (@OsTrusted Matcher) matcher);
    } else {
      throw new @OsUntrusted IllegalArgumentException(modeStr);
    }
  }

  @SuppressWarnings("ostrusted:cast.unsafe")
  private void applyNormalPattern(@OsUntrusted PermissionParser this, @OsUntrusted String modeStr, @OsTrusted Matcher matcher) {
    // Are there multiple permissions stored in one chmod?
    @OsUntrusted
    boolean commaSeperated = false;

    for (@OsUntrusted int i = 0; i < 1 || matcher.end() < modeStr.length(); i++) {
      if (i > 0 && (!commaSeperated || !matcher.find())) {
        throw new @OsUntrusted IllegalArgumentException(modeStr);
      }

      /*
       * groups : 1 : [ugoa]* 2 : [+-=] 3 : [rwxXt]+ 4 : [,\s]*
       */

      @OsUntrusted String str = matcher.group(2);
      @OsTrusted char type = ( @OsTrusted char) str.charAt(str.length() - 1);

      boolean user, group, others, stickyBit;
      user = group = others = stickyBit = false;

      for (@OsUntrusted char c : matcher.group(1).toCharArray()) {
        switch (c) {
        case 'u':
          user = true;
          break;
        case 'g':
          group = true;
          break;
        case 'o':
          others = true;
          break;
        case 'a':
          break;
        default:
          throw new @OsUntrusted RuntimeException("Unexpected");
        }
      }

      if (!(user || group || others)) { // same as specifying 'a'
        user = group = others = true;
      }

      @OsTrusted
      short mode = 0;

      for (@OsUntrusted char c : matcher.group(3).toCharArray()) {
        switch (c) {
        case 'r':
          mode |= 4;
          break;
        case 'w':
          mode |= 2;
          break;
        case 'x':
          mode |= 1;
          break;
        case 'X':
          mode |= 8;
          break;
        case 't':
          stickyBit = true;
          break;
        default:
          throw new @OsUntrusted RuntimeException("Unexpected");
        }
      }

      if (user) {
        userMode = mode;
        userType = type;
      }

      if (group) {
        groupMode = mode;
        groupType = type;
      }

      if (others) {
        othersMode = mode;
        othersType = type;

        stickyMode = (@OsTrusted short) (stickyBit ? 1 : 0);
        stickyBitType = type;
      }

      commaSeperated = matcher.group(4).contains(",");
    }
    symbolic = true;
  }

    @SuppressWarnings("ostrusted:cast.unsafe")
  private @OsTrusted Short trustedShortSubstring( @OsTrusted String str, int start, int end ) {
      return (@OsTrusted Short) Short.valueOf( str.substring( start, end ) );
  }

  //TODO: ALTERNATIVE IS TO MAKE THE OUTPUT OF GROUP BE EQUAL TO THE RECEIVER OF MATCHER
  @SuppressWarnings("ostrusted:cast.unsafe")
  private void applyOctalPattern(@OsUntrusted PermissionParser this, @OsUntrusted String modeStr, @OsTrusted Matcher matcher) {
    userType = groupType = othersType = '=';

    // Check if sticky bit is specified
    @OsTrusted String sb = (@OsTrusted String) matcher.group(1);
    if (!sb.isEmpty()) {
      stickyMode = trustedShortSubstring(sb, 0, 1);
      stickyBitType = '=';
    }

    @OsTrusted String str = (@OsTrusted String) matcher.group(2);
    userMode   = trustedShortSubstring(str, 0, 1 );
    groupMode  = trustedShortSubstring(str, 1, 2 );
    othersMode = trustedShortSubstring(str, 2, 3 );
  }

  protected @OsTrusted int combineModes(@OsUntrusted PermissionParser this, @OsTrusted int existing, @OsUntrusted boolean exeOk) {
    return   combineModeSegments( stickyBitType, stickyMode, ( existing >>> 9 ), false ) << 9 |
             combineModeSegments( userType, userMode,   (existing >>> 6 ) & 7, exeOk   ) << 6 |
             combineModeSegments( groupType, groupMode,  (existing >>> 3 ) & 7, exeOk  ) << 3 |
             combineModeSegments( othersType, othersMode, existing & 7, exeOk);
  }
  
  protected @OsTrusted int combineModeSegments(@OsUntrusted PermissionParser this, @OsUntrusted char type, @OsTrusted int mode,
                                    @OsTrusted
                                    int existing, @OsUntrusted boolean exeOk) {
    @OsUntrusted
    boolean capX = false;

    if ((mode&8) != 0) { // convert X to x;
      capX = true;
      mode &= ~8;
      mode |= 1;
    }

    switch (type) {
    case '+' : mode = mode | existing; break;
    case '-' : mode = (~mode) & existing; break;
    case '=' : break;
    default  : throw new @OsUntrusted RuntimeException("Unexpected");      
    }

    // if X is specified add 'x' only if exeOk or x was already set.
    if (capX && !exeOk && (mode&1) != 0 && (existing&1) == 0) {
      mode &= ~1; // remove x
    }

    return mode;
  }
}
