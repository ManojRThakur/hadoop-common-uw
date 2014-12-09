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
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Used primarily by {@link FileContext} to operate on and resolve
 * symlinks in a path. Operations can potentially span multiple
 * {@link AbstractFileSystem}s.
 * 
 * @see FileSystemLinkResolver
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class FSLinkResolver<@OsUntrusted T extends java.lang.@OsUntrusted Object> {

  /**
   * Return a fully-qualified version of the given symlink target if it
   * has no scheme and authority. Partially and fully-qualified paths
   * are returned unmodified.
   * @param pathURI URI of the filesystem of pathWithLink
   * @param pathWithLink Path that contains the symlink
   * @param target The symlink's absolute target
   * @return Fully qualified version of the target.
   */
  public static @OsUntrusted Path qualifySymlinkTarget(final @OsUntrusted URI pathURI,
      @OsUntrusted
      Path pathWithLink, @OsUntrusted Path target) {
    // NB: makeQualified uses the target's scheme and authority, if
    // specified, and the scheme and authority of pathURI, if not.
    final @OsUntrusted URI targetUri = target.toUri();
    final @OsUntrusted String scheme = targetUri.getScheme();
    final @OsUntrusted String auth = targetUri.getAuthority();
    return (scheme == null && auth == null) ? target.makeQualified(pathURI,
        pathWithLink.getParent()) : target;
  }

  /**
   * Generic helper function overridden on instantiation to perform a
   * specific operation on the given file system using the given path
   * which may result in an UnresolvedLinkException.
   * @param fs AbstractFileSystem to perform the operation on.
   * @param p Path given the file system.
   * @return Generic type determined by the specific implementation.
   * @throws UnresolvedLinkException If symbolic link <code>path</code> could
   *           not be resolved
   * @throws IOException an I/O error occurred
   */
  abstract public @OsUntrusted T next(@OsUntrusted FSLinkResolver<T> this, final @OsUntrusted AbstractFileSystem fs, final @OsUntrusted Path p)
      throws IOException, UnresolvedLinkException;

  /**
   * Performs the operation specified by the next function, calling it
   * repeatedly until all symlinks in the given path are resolved.
   * @param fc FileContext used to access file systems.
   * @param path The path to resolve symlinks on.
   * @return Generic type determined by the implementation of next.
   * @throws IOException
   */
  public @OsUntrusted T resolve(@OsUntrusted FSLinkResolver<T> this, final @OsUntrusted FileContext fc, final @OsUntrusted Path path) throws IOException {
    @OsUntrusted
    int count = 0;
    T in = null;
    @OsUntrusted
    Path p = path;
    // NB: More than one AbstractFileSystem can match a scheme, eg 
    // "file" resolves to LocalFs but could have come by RawLocalFs.
    @OsUntrusted
    AbstractFileSystem fs = fc.getFSofPath(p);

    // Loop until all symlinks are resolved or the limit is reached
    for (@OsUntrusted boolean isLink = true; isLink;) {
      try {
        in = next(fs, p);
        isLink = false;
      } catch (@OsUntrusted UnresolvedLinkException e) {
        if (!fc.resolveSymlinks) {
          throw new @OsUntrusted IOException("Path " + path + " contains a symlink"
              + " and symlink resolution is disabled ("
              + CommonConfigurationKeys.FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY + ").", e);
        }
        if (count++ > FsConstants.MAX_PATH_LINKS) {
          throw new @OsUntrusted IOException("Possible cyclic loop while " +
                                "following symbolic link " + path);
        }
        // Resolve the first unresolved path component
        p = qualifySymlinkTarget(fs.getUri(), p, fs.getLinkTarget(p));
        fs = fc.getFSofPath(p);
      }
    }
    return in;
  }
}
