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
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.shell.CopyCommands.CopyFromLocal;

/** Various commands for moving files */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class MoveCommands {
  public static void registerCommands(@OsUntrusted CommandFactory factory) {
    factory.addClass(MoveFromLocal.class, "-moveFromLocal");
    factory.addClass(MoveToLocal.class, "-moveToLocal");
    factory.addClass(Rename.class, "-mv");
  }

  /**
   *  Move local files to a remote filesystem
   */
  public static class MoveFromLocal extends @OsUntrusted CopyFromLocal {
    public static final @OsUntrusted String NAME = "moveFromLocal";
    public static final @OsUntrusted String USAGE = "<localsrc> ... <dst>";
    public static final @OsUntrusted String DESCRIPTION = 
      "Same as -put, except that the source is\n" +
      "deleted after it's copied.";

    @Override
    protected void processPath(MoveCommands.@OsUntrusted MoveFromLocal this, @OsUntrusted PathData src, @OsUntrusted PathData target) throws IOException {
      // unlike copy, don't merge existing dirs during move
      if (target.exists && target.stat.isDirectory()) {
        throw new @OsUntrusted PathExistsException(target.toString());
      }
      super.processPath(src, target);
    }
    
    @Override
    protected void postProcessPath(MoveCommands.@OsUntrusted MoveFromLocal this, @OsUntrusted PathData src) throws IOException {
      if (!src.fs.delete(src.path, false)) {
        // we have no way to know the actual error...
        @OsUntrusted
        PathIOException e = new @OsUntrusted PathIOException(src.toString());
        e.setOperation("remove");
        throw e;
      }
    }
  }

  /**
   *  Move remote files to a local filesystem
   */
  public static class MoveToLocal extends @OsUntrusted FsCommand { 
    public static final @OsUntrusted String NAME = "moveToLocal";
    public static final @OsUntrusted String USAGE = "<src> <localdst>";
    public static final @OsUntrusted String DESCRIPTION = "Not implemented yet";

    @Override
    protected void processOptions(MoveCommands.@OsUntrusted MoveToLocal this, @OsUntrusted LinkedList<@OsUntrusted String> args) throws IOException {
      throw new @OsUntrusted IOException("Option '-moveToLocal' is not implemented yet.");
    }
  }

  /** move/rename paths on the same fileystem */
  public static class Rename extends @OsUntrusted CommandWithDestination {
    public static final @OsUntrusted String NAME = "mv";
    public static final @OsUntrusted String USAGE = "<src> ... <dst>";
    public static final @OsUntrusted String DESCRIPTION = 
      "Move files that match the specified file pattern <src>\n" +
      "to a destination <dst>.  When moving multiple files, the\n" +
      "destination must be a directory.";

    @Override
    protected void processOptions(MoveCommands.@OsUntrusted Rename this, @OsUntrusted LinkedList<@OsUntrusted String> args) throws IOException {
      @OsUntrusted
      CommandFormat cf = new @OsUntrusted CommandFormat(2, Integer.MAX_VALUE);
      cf.parse(args);
      getRemoteDestination(args);
    }

    @Override
    protected void processPath(MoveCommands.@OsUntrusted Rename this, @OsUntrusted PathData src, @OsUntrusted PathData target) throws IOException {
      if (!src.fs.getUri().equals(target.fs.getUri())) {
        throw new @OsUntrusted PathIOException(src.toString(),
            "Does not match target filesystem");
      }
      if (!target.fs.rename(src.path, target.path)) {
        // we have no way to know the actual error...
        throw new @OsUntrusted PathIOException(src.toString());
      }
    }
  }
}
