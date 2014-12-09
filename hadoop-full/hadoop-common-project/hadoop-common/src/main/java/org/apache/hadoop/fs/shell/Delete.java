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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.fs.Trash;

/**
 * Classes that delete paths
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class Delete {
  public static void registerCommands(@OsUntrusted CommandFactory factory) {
    factory.addClass(Rm.class, "-rm");
    factory.addClass(Rmdir.class, "-rmdir");
    factory.addClass(Rmr.class, "-rmr");
    factory.addClass(Expunge.class, "-expunge");
  }

  /** remove non-directory paths */
  public static class Rm extends @OsUntrusted FsCommand {
    public static final @OsUntrusted String NAME = "rm";
    public static final @OsUntrusted String USAGE = "[-f] [-r|-R] [-skipTrash] <src> ...";
    public static final @OsUntrusted String DESCRIPTION =
      "Delete all files that match the specified file pattern.\n" +
      "Equivalent to the Unix command \"rm <src>\"\n" +
      "-skipTrash option bypasses trash, if enabled, and immediately\n" +
      "deletes <src>\n" +
      "  -f     If the file does not exist, do not display a diagnostic\n" +
      "         message or modify the exit status to reflect an error.\n" +
      "  -[rR]  Recursively deletes directories";

    private @OsUntrusted boolean skipTrash = false;
    private @OsUntrusted boolean deleteDirs = false;
    private @OsUntrusted boolean ignoreFNF = false;
    
    @Override
    protected void processOptions(Delete.@OsUntrusted Rm this, @OsUntrusted LinkedList<@OsUntrusted String> args) throws IOException {
      @OsUntrusted
      CommandFormat cf = new @OsUntrusted CommandFormat(
          1, Integer.MAX_VALUE, "f", "r", "R", "skipTrash");
      cf.parse(args);
      ignoreFNF = cf.getOpt("f");
      deleteDirs = cf.getOpt("r") || cf.getOpt("R");
      skipTrash = cf.getOpt("skipTrash");
    }

    @Override
    protected @OsUntrusted List<@OsUntrusted PathData> expandArgument(Delete.@OsUntrusted Rm this, @OsUntrusted String arg) throws IOException {
      try {
        return super.expandArgument(arg);
      } catch (@OsUntrusted PathNotFoundException e) {
        if (!ignoreFNF) {
          throw e;
        }
        // prevent -f on a non-existent glob from failing
        return new @OsUntrusted LinkedList<@OsUntrusted PathData>();
      }
    }

    @Override
    protected void processNonexistentPath(Delete.@OsUntrusted Rm this, @OsUntrusted PathData item) throws IOException {
      if (!ignoreFNF) super.processNonexistentPath(item);
    }

    @Override
    protected void processPath(Delete.@OsUntrusted Rm this, @OsUntrusted PathData item) throws IOException {
      if (item.stat.isDirectory() && !deleteDirs) {
        throw new @OsUntrusted PathIsDirectoryException(item.toString());
      }

      // TODO: if the user wants the trash to be used but there is any
      // problem (ie. creating the trash dir, moving the item to be deleted,
      // etc), then the path will just be deleted because moveToTrash returns
      // false and it falls thru to fs.delete.  this doesn't seem right
      if (moveToTrash(item)) {
        return;
      }
      if (!item.fs.delete(item.path, deleteDirs)) {
        throw new @OsUntrusted PathIOException(item.toString());
      }
      out.println("Deleted " + item);
    }

    private @OsUntrusted boolean moveToTrash(Delete.@OsUntrusted Rm this, @OsUntrusted PathData item) throws IOException {
      @OsUntrusted
      boolean success = false;
      if (!skipTrash) {
        try {
          success = Trash.moveToAppropriateTrash(item.fs, item.path, getConf());
        } catch(@OsUntrusted FileNotFoundException fnfe) {
          throw fnfe;
        } catch (@OsUntrusted IOException ioe) {
            throw new @OsUntrusted IOException(ioe.getMessage() + ". Consider using -skipTrash option", ioe);
        }
      }
      return success;
    }
  }
  
  /** remove any path */
  static class Rmr extends @OsUntrusted Rm {
    public static final @OsUntrusted String NAME = "rmr";
    
    @Override
    protected void processOptions(Delete.@OsUntrusted Rmr this, @OsUntrusted LinkedList<@OsUntrusted String> args) throws IOException {
      args.addFirst("-r");
      super.processOptions(args);
    }

    @Override
    public @OsUntrusted String getReplacementCommand(Delete.@OsUntrusted Rmr this) {
      return "rm -r";
    }
  }

  /** remove only empty directories */
  static class Rmdir extends @OsUntrusted FsCommand {
    public static final @OsUntrusted String NAME = "rmdir";
    public static final @OsUntrusted String USAGE =
      "[--ignore-fail-on-non-empty] <dir> ...";
    public static final @OsUntrusted String DESCRIPTION =
      "Removes the directory entry specified by each directory argument,\n" +
      "provided it is empty.\n"; 
    
    private @OsUntrusted boolean ignoreNonEmpty = false;
    
    @Override
    protected void processOptions(Delete.@OsUntrusted Rmdir this, @OsUntrusted LinkedList<@OsUntrusted String> args) throws IOException {
      @OsUntrusted
      CommandFormat cf = new @OsUntrusted CommandFormat(
          1, Integer.MAX_VALUE, "-ignore-fail-on-non-empty");
      cf.parse(args);
      ignoreNonEmpty = cf.getOpt("-ignore-fail-on-non-empty");
    }

    @Override
    protected void processPath(Delete.@OsUntrusted Rmdir this, @OsUntrusted PathData item) throws IOException {
      if (!item.stat.isDirectory()) {
        throw new @OsUntrusted PathIsNotDirectoryException(item.toString());
      }      
      if (item.fs.listStatus(item.path).length == 0) {
        if (!item.fs.delete(item.path, false)) {
          throw new @OsUntrusted PathIOException(item.toString());
        }
      } else if (!ignoreNonEmpty) {
        throw new @OsUntrusted PathIsNotEmptyDirectoryException(item.toString());
      }
    }
  }

  /** empty the trash */
  static class Expunge extends @OsUntrusted FsCommand {
    public static final @OsUntrusted String NAME = "expunge";
    public static final @OsUntrusted String USAGE = "";
    public static final @OsUntrusted String DESCRIPTION = "Empty the Trash";

    // TODO: should probably allow path arguments for the filesystems
    @Override
    protected void processOptions(Delete.@OsUntrusted Expunge this, @OsUntrusted LinkedList<@OsUntrusted String> args) throws IOException {
      @OsUntrusted
      CommandFormat cf = new @OsUntrusted CommandFormat(0, 0);
      cf.parse(args);
    }

    @Override
    protected void processArguments(Delete.@OsUntrusted Expunge this, @OsUntrusted LinkedList<@OsUntrusted PathData> args)
    throws IOException {
      @OsUntrusted
      Trash trash = new @OsUntrusted Trash(getConf());
      trash.expunge();
      trash.checkpoint();    
    }
  }
}
