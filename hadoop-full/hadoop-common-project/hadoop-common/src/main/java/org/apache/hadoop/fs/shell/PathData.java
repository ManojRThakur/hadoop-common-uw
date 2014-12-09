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
import ostrusted.quals.OsTrusted;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;

/**
 * Encapsulates a Path (path), its FileStatus (stat), and its FileSystem (fs).
 * PathData ensures that the returned path string will be the same as the
 * one passed in during initialization (unlike Path objects which can
 * modify the path string).
 * The stat field will be null if the path does not exist.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

public class PathData implements @OsUntrusted Comparable<@OsUntrusted PathData> {
  protected final @OsUntrusted URI uri;
  public final @OsUntrusted FileSystem fs;
  public final @OsUntrusted Path path;
  public @OsUntrusted FileStatus stat;
  public @OsUntrusted boolean exists;

  /* True if the URI scheme was not present in the pathString but inferred.
   */
  private @OsUntrusted boolean inferredSchemeFromPath = false;

  /**
   *  Pre-compiled regular expressions to detect path formats.
   */
  private static final @OsUntrusted Pattern potentialUri =
      Pattern.compile("^[a-zA-Z][a-zA-Z0-9+-.]+:");
  private static final @OsUntrusted Pattern windowsNonUriAbsolutePath1 =
      Pattern.compile("^/?[a-zA-Z]:\\\\");
  private static final @OsUntrusted Pattern windowsNonUriAbsolutePath2 =
      Pattern.compile("^/?[a-zA-Z]:/");

  /**
   * Creates an object to wrap the given parameters as fields.  The string
   * used to create the path will be recorded since the Path object does not
   * return exactly the same string used to initialize it
   * @param pathString a string for a path
   * @param conf the configuration file
   * @throws IOException if anything goes wrong...
   */
  public @OsUntrusted PathData(@OsUntrusted String pathString, @OsUntrusted Configuration conf) throws IOException {
    this(FileSystem.get(stringToUri(pathString), conf), pathString);
  }
  
  /**
   * Creates an object to wrap the given parameters as fields.  The string
   * used to create the path will be recorded since the Path object does not
   * return exactly the same string used to initialize it
   * @param localPath a local URI
   * @param conf the configuration file
   * @throws IOException if anything goes wrong...
   */
  public @OsUntrusted PathData(@OsUntrusted URI localPath, @OsUntrusted Configuration conf) throws IOException {
    this(FileSystem.getLocal(conf), localPath.getPath());
  }

  /**
   * Looks up the file status for a path.  If the path
   * doesn't exist, then the status will be null
   * @param fs the FileSystem for the path
   * @param pathString a string for a path 
   * @throws IOException if anything goes wrong
   */
  private @OsUntrusted PathData(@OsUntrusted FileSystem fs, @OsUntrusted String pathString) throws IOException {
    this(fs, pathString, lookupStat(fs, pathString, true));
  }

  /**
   * Validates the given Windows path.
   * @param pathString a String of the path suppliued by the user.
   * @return true if the URI scheme was not present in the pathString but
   * inferred; false, otherwise.
   * @throws IOException if anything goes wrong
   */
  private static @OsUntrusted boolean checkIfSchemeInferredFromPath(@OsUntrusted String pathString)
  throws IOException
  {
    if (windowsNonUriAbsolutePath1.matcher(pathString).find()) {
      // Forward slashes disallowed in a backslash-separated path.
      if (pathString.indexOf('/') != -1) {
        throw new @OsUntrusted IOException("Invalid path string " + pathString);
      }

      return true;
    }

    // Is it a forward slash-separated absolute path?
    if (windowsNonUriAbsolutePath2.matcher(pathString).find()) {
      return true;
    }

    // Does it look like a URI? If so then just leave it alone.
    if (potentialUri.matcher(pathString).find()) {
      return false;
    }

    // Looks like a relative path on Windows.
    return false;
  }

  /**
   * Creates an object to wrap the given parameters as fields.  The string
   * used to create the path will be recorded since the Path object does not
   * return exactly the same string used to initialize it.
   * @param fs the FileSystem
   * @param pathString a String of the path
   * @param stat the FileStatus (may be null if the path doesn't exist)
   */
  private @OsUntrusted PathData(@OsUntrusted FileSystem fs, @OsUntrusted String pathString, @OsUntrusted FileStatus stat)
  throws IOException {
    this.fs = fs;
    this.uri = stringToUri(pathString);
    this.path = fs.makeQualified(new @OsUntrusted Path(uri));
    setStat(stat);

    if (Path.WINDOWS) {
      inferredSchemeFromPath = checkIfSchemeInferredFromPath(pathString);
    }
  }

  // need a static method for the ctor above
  /**
   * Get the FileStatus info
   * @param ignoreFNF if true, stat will be null if the path doesn't exist
   * @return FileStatus for the given path
   * @throws IOException if anything goes wrong
   */
  private static
  @OsUntrusted
  FileStatus lookupStat(@OsUntrusted FileSystem fs, @OsUntrusted String pathString, @OsUntrusted boolean ignoreFNF)
  throws IOException {
    @OsUntrusted
    FileStatus status = null;
    try {
      status = fs.getFileStatus(new @OsUntrusted Path(pathString));
    } catch (@OsUntrusted FileNotFoundException e) {
      if (!ignoreFNF) throw new @OsUntrusted PathNotFoundException(pathString);
    }
    // TODO: should consider wrapping other exceptions into Path*Exceptions
    return status;
  }
  
  private void setStat(@OsUntrusted PathData this, @OsUntrusted FileStatus stat) {
    this.stat = stat;
    exists = (stat != null);
  }

  /**
   * Updates the paths's file status
   * @return the updated FileStatus
   * @throws IOException if anything goes wrong...
   */
  public @OsUntrusted FileStatus refreshStatus(@OsUntrusted PathData this) throws IOException {
    @OsUntrusted
    FileStatus status = null;
    try {
      status = lookupStat(fs, toString(), false);
    } finally {
      // always set the status.  the caller must get the correct result
      // if it catches the exception and later interrogates the status
      setStat(status);
    }
    return status;
  }

  protected enum FileTypeRequirement {

@OsUntrusted  SHOULD_NOT_BE_DIRECTORY,  @OsUntrusted  SHOULD_BE_DIRECTORY
  };

  /**
   * Ensure that the file exists and if it is or is not a directory
   * @param typeRequirement Set it to the desired requirement.
   * @throws PathIOException if file doesn't exist or the type does not match
   * what was specified in typeRequirement.
   */
  private void checkIfExists(@OsUntrusted PathData this, @OsUntrusted FileTypeRequirement typeRequirement) 
  throws PathIOException {
    if (!exists) {
      throw new @OsUntrusted PathNotFoundException(toString());      
    }

    if ((typeRequirement == FileTypeRequirement.SHOULD_BE_DIRECTORY)
       && !stat.isDirectory()) {
      throw new @OsUntrusted PathIsNotDirectoryException(toString());
    } else if ((typeRequirement == FileTypeRequirement.SHOULD_NOT_BE_DIRECTORY)
              && stat.isDirectory()) {
      throw new @OsUntrusted PathIsDirectoryException(toString());
    }
  }
  
  /**
   * Returns a new PathData with the given extension.
   * @param extension for the suffix
   * @return PathData
   * @throws IOException shouldn't happen
   */
  public @OsUntrusted PathData suffix(@OsUntrusted PathData this, @OsUntrusted String extension) throws IOException {
    return new @OsUntrusted PathData(fs, this+extension);
  }

  /**
   * Test if the parent directory exists
   * @return boolean indicating parent exists
   * @throws IOException upon unexpected error
   */
  public @OsUntrusted boolean parentExists(@OsUntrusted PathData this) throws IOException {
    return representsDirectory()
        ? fs.exists(path) : fs.exists(path.getParent());
  }

  /**
   * Check if the path represents a directory as determined by the basename
   * being "." or "..", or the path ending with a directory separator 
   * @return boolean if this represents a directory
   */
  public @OsUntrusted boolean representsDirectory(@OsUntrusted PathData this) {
    @OsUntrusted
    String uriPath = uri.getPath();
    @OsUntrusted
    String name = uriPath.substring(uriPath.lastIndexOf("/")+1);
    // Path will munch off the chars that indicate a dir, so there's no way
    // to perform this test except by examining the raw basename we maintain
    return (name.isEmpty() || name.equals(".") || name.equals(".."));
  }
  
  /**
   * Returns a list of PathData objects of the items contained in the given
   * directory.
   * @return list of PathData objects for its children
   * @throws IOException if anything else goes wrong...
   */
  public @OsUntrusted PathData @OsUntrusted [] getDirectoryContents(@OsUntrusted PathData this) throws IOException {
    checkIfExists(FileTypeRequirement.SHOULD_BE_DIRECTORY);
    @OsUntrusted
    FileStatus @OsUntrusted [] stats = fs.listStatus(path);
    @OsUntrusted
    PathData @OsUntrusted [] items = new @OsUntrusted PathData @OsUntrusted [stats.length];
    for (@OsUntrusted int i=0; i < stats.length; i++) {
      // preserve relative paths
      @OsUntrusted
      String child = getStringForChildPath(stats[i].getPath());
      items[i] = new @OsUntrusted PathData(fs, child, stats[i]);
    }
    Arrays.sort(items);
    return items;
  }

  /**
   * Creates a new object for a child entry in this directory
   * @param child the basename will be appended to this object's path
   * @return PathData for the child
   * @throws IOException if this object does not exist or is not a directory
   */
  public @OsUntrusted PathData getPathDataForChild(@OsUntrusted PathData this, @OsUntrusted PathData child) throws IOException {
    checkIfExists(FileTypeRequirement.SHOULD_BE_DIRECTORY);
    return new @OsUntrusted PathData(fs, getStringForChildPath(child.path));
  }

  /**
   * Given a child of this directory, use the directory's path and the child's
   * basename to construct the string to the child.  This preserves relative
   * paths since Path will fully qualify.
   * @param childPath a path contained within this directory
   * @return String of the path relative to this directory
   */
  private @OsUntrusted String getStringForChildPath(@OsUntrusted PathData this, @OsUntrusted Path childPath) {
    @OsUntrusted
    String basename = childPath.getName();
    if (Path.CUR_DIR.equals(toString())) {
      return basename;
    }
    // check getPath() so scheme slashes aren't considered part of the path
    @OsUntrusted
    String separator = uri.getPath().endsWith(Path.SEPARATOR)
        ? "" : Path.SEPARATOR;
    return uriToString(uri, inferredSchemeFromPath) + separator + basename;
  }
  
  protected enum PathType {  @OsUntrusted  HAS_SCHEME,  @OsUntrusted  SCHEMELESS_ABSOLUTE,  @OsUntrusted  RELATIVE };
  
  /**
   * Expand the given path as a glob pattern.  Non-existent paths do not
   * throw an exception because creation commands like touch and mkdir need
   * to create them.  The "stat" field will be null if the path does not
   * exist.
   * @param pattern the pattern to expand as a glob
   * @param conf the hadoop configuration
   * @return list of {@link PathData} objects.  if the pattern is not a glob,
   * and does not exist, the list will contain a single PathData with a null
   * stat 
   * @throws IOException anything else goes wrong...
   */
  public static @OsUntrusted PathData @OsUntrusted [] expandAsGlob(@OsUntrusted String pattern, @OsUntrusted Configuration conf)
  throws IOException {
    @OsUntrusted
    Path globPath = new @OsUntrusted Path(pattern);
    @OsUntrusted
    FileSystem fs = globPath.getFileSystem(conf);    
    @OsUntrusted
    FileStatus @OsUntrusted [] stats = fs.globStatus(globPath);
    @OsUntrusted
    PathData @OsUntrusted [] items = null;
    
    if (stats == null) {
      // remove any quoting in the glob pattern
      pattern = pattern.replaceAll("\\\\(.)", "$1");
      // not a glob & file not found, so add the path with a null stat
      items = new @OsUntrusted PathData @OsUntrusted []{ new @OsUntrusted PathData(fs, pattern, null) };
    } else {
      // figure out what type of glob path was given, will convert globbed
      // paths to match the type to preserve relativity
      @OsUntrusted
      PathType globType;
      @OsUntrusted
      URI globUri = globPath.toUri();
      if (globUri.getScheme() != null) {
        globType = PathType.HAS_SCHEME;
      } else if (!globUri.getPath().isEmpty() &&
                 new @OsUntrusted Path(globUri.getPath()).isAbsolute()) {
        globType = PathType.SCHEMELESS_ABSOLUTE;
      } else {
        globType = PathType.RELATIVE;
      }

      // convert stats to PathData
      items = new @OsUntrusted PathData @OsUntrusted [stats.length];
      @OsUntrusted
      int i=0;
      for (@OsUntrusted FileStatus stat : stats) {
        @OsUntrusted
        URI matchUri = stat.getPath().toUri();
        @OsUntrusted
        String globMatch = null;
        switch (globType) {
          case HAS_SCHEME: // use as-is, but remove authority if necessary
            if (globUri.getAuthority() == null) {
              matchUri = removeAuthority(matchUri);
            }
            globMatch = uriToString(matchUri, false);
            break;
          case SCHEMELESS_ABSOLUTE: // take just the uri's path
            globMatch = matchUri.getPath();
            break;
          case RELATIVE: // make it relative to the current working dir
            @OsUntrusted
            URI cwdUri = fs.getWorkingDirectory().toUri();
            globMatch = relativize(cwdUri, matchUri, stat.isDirectory());
            break;
        }
        items[i++] = new @OsUntrusted PathData(fs, globMatch, stat);
      }
    }
    Arrays.sort(items);
    return items;
  }

  private static @OsUntrusted URI removeAuthority(@OsUntrusted URI uri) {
    try {
      uri = new @OsUntrusted URI(
          uri.getScheme(), "",
          uri.getPath(), uri.getQuery(), uri.getFragment()
      );
    } catch (@OsUntrusted URISyntaxException e) {
      throw new @OsUntrusted IllegalArgumentException(e.getLocalizedMessage());
    }
    return uri;
  }
  
  private static @OsUntrusted String relativize(@OsUntrusted URI cwdUri, @OsUntrusted URI srcUri, @OsUntrusted boolean isDir) {
    @OsUntrusted
    String uriPath = srcUri.getPath();
    @OsUntrusted
    String cwdPath = cwdUri.getPath();
    if (cwdPath.equals(uriPath)) {
      return Path.CUR_DIR;
    }

    // find common ancestor
    @OsUntrusted
    int lastSep = findLongestDirPrefix(cwdPath, uriPath, isDir);
    
    @OsUntrusted
    StringBuilder relPath = new @OsUntrusted StringBuilder();    
    // take the remaining path fragment after the ancestor
    if (lastSep < uriPath.length()) {
      relPath.append(uriPath.substring(lastSep+1));
    }

    // if cwd has a path fragment after the ancestor, convert them to ".."
    if (lastSep < cwdPath.length()) {
      while (lastSep != -1) {
        if (relPath.length() != 0) relPath.insert(0, Path.SEPARATOR);
        relPath.insert(0, "..");
        lastSep = cwdPath.indexOf(Path.SEPARATOR, lastSep+1);
      }
    }
    return relPath.toString();
  }

  private static @OsUntrusted int findLongestDirPrefix(@OsUntrusted String cwd, @OsUntrusted String path, @OsUntrusted boolean isDir) {
    // add the path separator to dirs to simplify finding the longest match
    if (!cwd.endsWith(Path.SEPARATOR)) {
      cwd += Path.SEPARATOR;
    }
    if (isDir && !path.endsWith(Path.SEPARATOR)) {
      path += Path.SEPARATOR;
    }

    // find longest directory prefix 
    @OsUntrusted
    int len = Math.min(cwd.length(), path.length());
    @OsUntrusted
    int lastSep = -1;
    for (@OsUntrusted int i=0; i < len; i++) {
      if (cwd.charAt(i) != path.charAt(i)) break;
      if (cwd.charAt(i) == Path.SEPARATOR_CHAR) lastSep = i;
    }
    return lastSep;
  }
  
  /**
   * Returns the printable version of the path that is either the path
   * as given on the commandline, or the full path
   * @return String of the path
   */
  @Override
  public @OsUntrusted String toString(@OsUntrusted PathData this) {
    return uriToString(uri, inferredSchemeFromPath);
  }
 
  private static @OsUntrusted String uriToString(@OsUntrusted URI uri, @OsUntrusted boolean inferredSchemeFromPath) {
    @OsUntrusted
    String scheme = uri.getScheme();
    // No interpretation of symbols. Just decode % escaped chars.
    @OsUntrusted
    String decodedRemainder = uri.getSchemeSpecificPart();

    // Drop the scheme if it was inferred to ensure fidelity between
    // the input and output path strings.
    if ((scheme == null) || (inferredSchemeFromPath)) {
      if (Path.isWindowsAbsolutePath(decodedRemainder, true)) {
        // Strip the leading '/' added in stringToUri so users see a valid
        // Windows path.
        decodedRemainder = decodedRemainder.substring(1);
      }
      return decodedRemainder;
    } else {
      @OsUntrusted
      StringBuilder buffer = new @OsUntrusted StringBuilder();
      buffer.append(scheme);
      buffer.append(":");
      buffer.append(decodedRemainder);
      return buffer.toString();
    }
  }
  
  /**
   * Get the path to a local file
   * @return File representing the local path
   * @throws IllegalArgumentException if this.fs is not the LocalFileSystem
   */
  public @OsUntrusted File toFile(@OsUntrusted PathData this) {
    if (!(fs instanceof @OsUntrusted LocalFileSystem)) {
       throw new @OsUntrusted IllegalArgumentException("Not a local path: " + path);
    }
    return ((@OsUntrusted LocalFileSystem)fs).pathToFile(path);
  }

  /** Normalize the given Windows path string. This does the following:
   *    1. Adds "file:" scheme for absolute paths.
   *    2. Ensures the scheme-specific part starts with '/' per RFC2396.
   *    3. Replaces backslash path separators with forward slashes.
   *    @param pathString Path string supplied by the user.
   *    @return normalized absolute path string. Returns the input string
   *            if it is not a Windows absolute path.
   */
  private static @OsUntrusted String normalizeWindowsPath(@OsUntrusted String pathString)
  throws IOException
  {
    if (!Path.WINDOWS) {
      return pathString;
    }

    @OsUntrusted
    boolean slashed =
        ((pathString.length() >= 1) && (pathString.charAt(0) == '/'));

    // Is it a backslash-separated absolute path?
    if (windowsNonUriAbsolutePath1.matcher(pathString).find()) {
      // Forward slashes disallowed in a backslash-separated path.
      if (pathString.indexOf('/') != -1) {
        throw new @OsUntrusted IOException("Invalid path string " + pathString);
      }

      pathString = pathString.replace('\\', '/');
      return "file:" + (slashed ? "" : "/") + pathString;
    }

    // Is it a forward slash-separated absolute path?
    if (windowsNonUriAbsolutePath2.matcher(pathString).find()) {
      return "file:" + (slashed ? "" : "/") + pathString;
    }

    // Is it a backslash-separated relative file path (no scheme and
    // no drive-letter specifier)?
    if ((pathString.indexOf(':') == -1) && (pathString.indexOf('\\') != -1)) {
      pathString = pathString.replace('\\', '/');
    }

    return pathString;
  }

  /** Construct a URI from a String with unescaped special characters
   *  that have non-standard semantics. e.g. /, ?, #. A custom parsing
   *  is needed to prevent misbehavior.
   *  @param pathString The input path in string form
   *  @return URI
   */
  private static @OsUntrusted URI stringToUri(@OsUntrusted String pathString) throws IOException {
    // We can't use 'new URI(String)' directly. Since it doesn't do quoting
    // internally, the internal parser may fail or break the string at wrong
    // places. Use of multi-argument ctors will quote those chars for us,
    // but we need to do our own parsing and assembly.
    
    // parse uri components
    @OsUntrusted
    String scheme = null;
    @OsUntrusted
    String authority = null;
    @OsUntrusted
    int start = 0;

    pathString = normalizeWindowsPath(pathString);

    // parse uri scheme, if any
    @OsUntrusted
    int colon = pathString.indexOf(':');
    @OsUntrusted
    int slash = pathString.indexOf('/');
    if (colon > 0 && (slash == colon +1)) {
      // has a non zero-length scheme
      scheme = pathString.substring(0, colon);
      start = colon + 1;
    }

    // parse uri authority, if any
    if (pathString.startsWith("//", start) &&
        (pathString.length()-start > 2)) {
      start += 2;
      @OsUntrusted
      int nextSlash = pathString.indexOf('/', start);
      @OsUntrusted
      int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
      authority = pathString.substring(start, authEnd);
      start = authEnd;
    }
    // uri path is the rest of the string. ? or # are not interpreted,
    // but any occurrence of them will be quoted by the URI ctor.
    @OsUntrusted
    String path = pathString.substring(start, pathString.length());

    // Construct the URI
    try {
      return new @OsUntrusted URI(scheme, authority, path, null, null);
    } catch (@OsUntrusted URISyntaxException e) {
      throw new @OsUntrusted IllegalArgumentException(e);
    }
  }

  @Override
  public @OsUntrusted int compareTo(@OsUntrusted PathData this, @OsUntrusted PathData o) {
    return path.compareTo(((@OsUntrusted PathData)o).path);
  }
  
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted PathData this, @OsUntrusted Object o) {
    return (o != null) &&
           (o instanceof @OsUntrusted PathData) &&
           path.equals(((@OsUntrusted PathData)o).path);
  }
  
  @Override
  public @OsUntrusted int hashCode(@OsUntrusted PathData this) {
    return path.hashCode();
  }
}
