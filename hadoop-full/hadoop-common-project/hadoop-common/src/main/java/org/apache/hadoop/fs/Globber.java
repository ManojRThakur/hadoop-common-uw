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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class Globber {
  public static final @OsUntrusted Log LOG = LogFactory.getLog(Globber.class.getName());

  private final @OsUntrusted FileSystem fs;
  private final @OsUntrusted FileContext fc;
  private final @OsUntrusted Path pathPattern;
  private final @OsUntrusted PathFilter filter;
  
  public @OsUntrusted Globber(@OsUntrusted FileSystem fs, @OsUntrusted Path pathPattern, @OsUntrusted PathFilter filter) {
    this.fs = fs;
    this.fc = null;
    this.pathPattern = pathPattern;
    this.filter = filter;
  }

  public @OsUntrusted Globber(@OsUntrusted FileContext fc, @OsUntrusted Path pathPattern, @OsUntrusted PathFilter filter) {
    this.fs = null;
    this.fc = fc;
    this.pathPattern = pathPattern;
    this.filter = filter;
  }

  private @OsUntrusted FileStatus getFileStatus(@OsUntrusted Globber this, @OsUntrusted Path path) throws IOException {
    try {
      if (fs != null) {
        return fs.getFileStatus(path);
      } else {
        return fc.getFileStatus(path);
      }
    } catch (@OsUntrusted FileNotFoundException e) {
      return null;
    }
  }

  private @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted Globber this, @OsUntrusted Path path) throws IOException {
    try {
      if (fs != null) {
        return fs.listStatus(path);
      } else {
        return fc.util().listStatus(path);
      }
    } catch (@OsUntrusted FileNotFoundException e) {
      return new @OsUntrusted FileStatus @OsUntrusted [0];
    }
  }

  private @OsUntrusted Path fixRelativePart(@OsUntrusted Globber this, @OsUntrusted Path path) {
    if (fs != null) {
      return fs.fixRelativePart(path);
    } else {
      return fc.fixRelativePart(path);
    }
  }

  /**
   * Convert a path component that contains backslash ecape sequences to a
   * literal string.  This is necessary when you want to explicitly refer to a
   * path that contains globber metacharacters.
   */
  private static @OsUntrusted String unescapePathComponent(@OsUntrusted String name) {
    return name.replaceAll("\\\\(.)", "$1");
  }

  /**
   * Translate an absolute path into a list of path components.
   * We merge double slashes into a single slash here.
   * POSIX root path, i.e. '/', does not get an entry in the list.
   */
  private static @OsUntrusted List<@OsUntrusted String> getPathComponents(@OsUntrusted String path)
      throws IOException {
    @OsUntrusted
    ArrayList<@OsUntrusted String> ret = new @OsUntrusted ArrayList<@OsUntrusted String>();
    for (@OsUntrusted String component : path.split(Path.SEPARATOR)) {
      if (!component.isEmpty()) {
        ret.add(component);
      }
    }
    return ret;
  }

  private @OsUntrusted String schemeFromPath(@OsUntrusted Globber this, @OsUntrusted Path path) throws IOException {
    @OsUntrusted
    String scheme = path.toUri().getScheme();
    if (scheme == null) {
      if (fs != null) {
        scheme = fs.getUri().getScheme();
      } else {
        scheme = fc.getDefaultFileSystem().getUri().getScheme();
      }
    }
    return scheme;
  }

  private @OsUntrusted String authorityFromPath(@OsUntrusted Globber this, @OsUntrusted Path path) throws IOException {
    @OsUntrusted
    String authority = path.toUri().getAuthority();
    if (authority == null) {
      if (fs != null) {
        authority = fs.getUri().getAuthority();
      } else {
        authority = fc.getDefaultFileSystem().getUri().getAuthority();
      }
    }
    return authority ;
  }

  public @OsUntrusted FileStatus @OsUntrusted [] glob(@OsUntrusted Globber this) throws IOException {
    // First we get the scheme and authority of the pattern that was passed
    // in.
    @OsUntrusted
    String scheme = schemeFromPath(pathPattern);
    @OsUntrusted
    String authority = authorityFromPath(pathPattern);

    // Next we strip off everything except the pathname itself, and expand all
    // globs.  Expansion is a process which turns "grouping" clauses,
    // expressed as brackets, into separate path patterns.
    @OsUntrusted
    String pathPatternString = pathPattern.toUri().getPath();
    @OsUntrusted
    List<@OsUntrusted String> flattenedPatterns = GlobExpander.expand(pathPatternString);

    // Now loop over all flattened patterns.  In every case, we'll be trying to
    // match them to entries in the filesystem.
    @OsUntrusted
    ArrayList<@OsUntrusted FileStatus> results = 
        new @OsUntrusted ArrayList<@OsUntrusted FileStatus>(flattenedPatterns.size());
    @OsUntrusted
    boolean sawWildcard = false;
    for (@OsUntrusted String flatPattern : flattenedPatterns) {
      // Get the absolute path for this flattened pattern.  We couldn't do 
      // this prior to flattening because of patterns like {/,a}, where which
      // path you go down influences how the path must be made absolute.
      @OsUntrusted
      Path absPattern = fixRelativePart(new @OsUntrusted Path(
          flatPattern.isEmpty() ? Path.CUR_DIR : flatPattern));
      // Now we break the flattened, absolute pattern into path components.
      // For example, /a/*/c would be broken into the list [a, *, c]
      @OsUntrusted
      List<@OsUntrusted String> components =
          getPathComponents(absPattern.toUri().getPath());
      // Starting out at the root of the filesystem, we try to match
      // filesystem entries against pattern components.
      @OsUntrusted
      ArrayList<@OsUntrusted FileStatus> candidates = new @OsUntrusted ArrayList<@OsUntrusted FileStatus>(1);
      if (Path.WINDOWS && !components.isEmpty()
          && Path.isWindowsAbsolutePath(absPattern.toUri().getPath(), true)) {
        // On Windows the path could begin with a drive letter, e.g. /E:/foo.
        // We will skip matching the drive letter and start from listing the
        // root of the filesystem on that drive.
        @OsUntrusted
        String driveLetter = components.remove(0);
        candidates.add(new @OsUntrusted FileStatus(0, true, 0, 0, 0, new @OsUntrusted Path(scheme,
            authority, Path.SEPARATOR + driveLetter + Path.SEPARATOR)));
      } else {
        candidates.add(new @OsUntrusted FileStatus(0, true, 0, 0, 0,
            new @OsUntrusted Path(scheme, authority, Path.SEPARATOR)));
      }
      
      for (@OsUntrusted int componentIdx = 0; componentIdx < components.size();
          componentIdx++) {
        @OsUntrusted
        ArrayList<@OsUntrusted FileStatus> newCandidates =
            new @OsUntrusted ArrayList<@OsUntrusted FileStatus>(candidates.size());
        @OsUntrusted
        GlobFilter globFilter = new @OsUntrusted GlobFilter(components.get(componentIdx));
        @OsUntrusted
        String component = unescapePathComponent(components.get(componentIdx));
        if (globFilter.hasPattern()) {
          sawWildcard = true;
        }
        if (candidates.isEmpty() && sawWildcard) {
          // Optimization: if there are no more candidates left, stop examining 
          // the path components.  We can only do this if we've already seen
          // a wildcard component-- otherwise, we still need to visit all path 
          // components in case one of them is a wildcard.
          break;
        }
        if ((componentIdx < components.size() - 1) &&
            (!globFilter.hasPattern())) {
          // Optimization: if this is not the terminal path component, and we 
          // are not matching against a glob, assume that it exists.  If it 
          // doesn't exist, we'll find out later when resolving a later glob
          // or the terminal path component.
          for (@OsUntrusted FileStatus candidate : candidates) {
            candidate.setPath(new @OsUntrusted Path(candidate.getPath(), component));
          }
          continue;
        }
        for (@OsUntrusted FileStatus candidate : candidates) {
          if (globFilter.hasPattern()) {
            @OsUntrusted
            FileStatus @OsUntrusted [] children = listStatus(candidate.getPath());
            if (children.length == 1) {
              // If we get back only one result, this could be either a listing
              // of a directory with one entry, or it could reflect the fact
              // that what we listed resolved to a file.
              //
              // Unfortunately, we can't just compare the returned paths to
              // figure this out.  Consider the case where you have /a/b, where
              // b is a symlink to "..".  In that case, listing /a/b will give
              // back "/a/b" again.  If we just went by returned pathname, we'd
              // incorrectly conclude that /a/b was a file and should not match
              // /a/*/*.  So we use getFileStatus of the path we just listed to
              // disambiguate.
              if (!getFileStatus(candidate.getPath()).isDirectory()) {
                continue;
              }
            }
            for (@OsUntrusted FileStatus child : children) {
              // Set the child path based on the parent path.
              child.setPath(new @OsUntrusted Path(candidate.getPath(),
                      child.getPath().getName()));
              if (globFilter.accept(child.getPath())) {
                newCandidates.add(child);
              }
            }
          } else {
            // When dealing with non-glob components, use getFileStatus 
            // instead of listStatus.  This is an optimization, but it also
            // is necessary for correctness in HDFS, since there are some
            // special HDFS directories like .reserved and .snapshot that are
            // not visible to listStatus, but which do exist.  (See HADOOP-9877)
            @OsUntrusted
            FileStatus childStatus = getFileStatus(
                new @OsUntrusted Path(candidate.getPath(), component));
            if (childStatus != null) {
              newCandidates.add(childStatus);
             }
           }
        }
        candidates = newCandidates;
      }
      for (@OsUntrusted FileStatus status : candidates) {
        // HADOOP-3497 semantics: the user-defined filter is applied at the
        // end, once the full path is built up.
        if (filter.accept(status.getPath())) {
          results.add(status);
        }
      }
    }
    /*
     * When the input pattern "looks" like just a simple filename, and we
     * can't find it, we return null rather than an empty array.
     * This is a special case which the shell relies on.
     *
     * To be more precise: if there were no results, AND there were no
     * groupings (aka brackets), and no wildcards in the input (aka stars),
     * we return null.
     */
    if ((!sawWildcard) && results.isEmpty() &&
        (flattenedPatterns.size() <= 1)) {
      return null;
    }
    return results.toArray(new @OsUntrusted FileStatus @OsUntrusted [0]);
  }
}
