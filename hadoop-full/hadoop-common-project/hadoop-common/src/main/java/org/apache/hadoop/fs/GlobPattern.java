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

package org.apache.hadoop.fs;

import ostrusted.quals.OsUntrusted;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A class for POSIX glob pattern with brace expansions.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class GlobPattern {
  private static final @OsUntrusted char BACKSLASH = '\\';
  private @OsUntrusted Pattern compiled;
  private @OsUntrusted boolean hasWildcard = false;

  /**
   * Construct the glob pattern object with a glob pattern string
   * @param globPattern the glob pattern string
   */
  public @OsUntrusted GlobPattern(@OsUntrusted String globPattern) {
    set(globPattern);
  }

  /**
   * @return the compiled pattern
   */
  public @OsUntrusted Pattern compiled(@OsUntrusted GlobPattern this) {
    return compiled;
  }

  /**
   * Compile glob pattern string
   * @param globPattern the glob pattern
   * @return the pattern object
   */
  public static @OsUntrusted Pattern compile(@OsUntrusted String globPattern) {
    return new @OsUntrusted GlobPattern(globPattern).compiled();
  }

  /**
   * Match input against the compiled glob pattern
   * @param s input chars
   * @return true for successful matches
   */
  public @OsUntrusted boolean matches(@OsUntrusted GlobPattern this, @OsUntrusted CharSequence s) {
    return compiled.matcher(s).matches();
  }

  /**
   * Set and compile a glob pattern
   * @param glob  the glob pattern string
   */
  public void set(@OsUntrusted GlobPattern this, @OsUntrusted String glob) {
    @OsUntrusted
    StringBuilder regex = new @OsUntrusted StringBuilder();
    @OsUntrusted
    int setOpen = 0;
    @OsUntrusted
    int curlyOpen = 0;
    @OsUntrusted
    int len = glob.length();
    hasWildcard = false;

    for (@OsUntrusted int i = 0; i < len; i++) {
      @OsUntrusted
      char c = glob.charAt(i);

      switch (c) {
        case BACKSLASH:
          if (++i >= len) {
            error("Missing escaped character", glob, i);
          }
          regex.append(c).append(glob.charAt(i));
          continue;
        case '.':
        case '$':
        case '(':
        case ')':
        case '|':
        case '+':
          // escape regex special chars that are not glob special chars
          regex.append(BACKSLASH);
          break;
        case '*':
          regex.append('.');
          hasWildcard = true;
          break;
        case '?':
          regex.append('.');
          hasWildcard = true;
          continue;
        case '{': // start of a group
          regex.append("(?:"); // non-capturing
          curlyOpen++;
          hasWildcard = true;
          continue;
        case ',':
          regex.append(curlyOpen > 0 ? '|' : c);
          continue;
        case '}':
          if (curlyOpen > 0) {
            // end of a group
            curlyOpen--;
            regex.append(")");
            continue;
          }
          break;
        case '[':
          if (setOpen > 0) {
            error("Unclosed character class", glob, i);
          }
          setOpen++;
          hasWildcard = true;
          break;
        case '^': // ^ inside [...] can be unescaped
          if (setOpen == 0) {
            regex.append(BACKSLASH);
          }
          break;
        case '!': // [! needs to be translated to [^
          regex.append(setOpen > 0 && '[' == glob.charAt(i - 1) ? '^' : '!');
          continue;
        case ']':
          // Many set errors like [][] could not be easily detected here,
          // as []], []-] and [-] are all valid POSIX glob and java regex.
          // We'll just let the regex compiler do the real work.
          setOpen = 0;
          break;
        default:
      }
      regex.append(c);
    }

    if (setOpen > 0) {
      error("Unclosed character class", glob, len);
    }
    if (curlyOpen > 0) {
      error("Unclosed group", glob, len);
    }
    compiled = Pattern.compile(regex.toString());
  }

  /**
   * @return true if this is a wildcard pattern (with special chars)
   */
  public @OsUntrusted boolean hasWildcard(@OsUntrusted GlobPattern this) {
    return hasWildcard;
  }

  private static void error(@OsUntrusted String message, @OsUntrusted String pattern, @OsUntrusted int pos) {
    throw new @OsUntrusted PatternSyntaxException(message, pattern, pos);
  }
}
