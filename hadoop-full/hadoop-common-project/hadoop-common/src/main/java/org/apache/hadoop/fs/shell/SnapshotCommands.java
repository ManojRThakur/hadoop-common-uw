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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotDirectoryException;

import com.google.common.base.Preconditions;

/**
 * Snapshot related operations
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class SnapshotCommands extends @OsUntrusted FsCommand {
  private final static @OsUntrusted String CREATE_SNAPSHOT = "createSnapshot";
  private final static @OsUntrusted String DELETE_SNAPSHOT = "deleteSnapshot";
  private final static @OsUntrusted String RENAME_SNAPSHOT = "renameSnapshot";
  
  public static void registerCommands(@OsUntrusted CommandFactory factory) {
    factory.addClass(CreateSnapshot.class, "-" + CREATE_SNAPSHOT);
    factory.addClass(DeleteSnapshot.class, "-" + DELETE_SNAPSHOT);
    factory.addClass(RenameSnapshot.class, "-" + RENAME_SNAPSHOT);
  }
  
  /**
   *  Create a snapshot
   */
  public static class CreateSnapshot extends @OsUntrusted FsCommand {
    public static final @OsUntrusted String NAME = CREATE_SNAPSHOT;
    public static final @OsUntrusted String USAGE = "<snapshotDir> [<snapshotName>]";
    public static final @OsUntrusted String DESCRIPTION = "Create a snapshot on a directory";

    private @OsUntrusted String snapshotName = null;

    @Override
    protected void processPath(SnapshotCommands.@OsUntrusted CreateSnapshot this, @OsUntrusted PathData item) throws IOException {
      if (!item.stat.isDirectory()) {
        throw new @OsUntrusted PathIsNotDirectoryException(item.toString());
      }
    }
    
    @Override
    protected void processOptions(SnapshotCommands.@OsUntrusted CreateSnapshot this, @OsUntrusted LinkedList<@OsUntrusted String> args) throws IOException {
      if (args.size() == 0) {
        throw new @OsUntrusted IllegalArgumentException("<snapshotDir> is missing.");
      } 
      if (args.size() > 2) {
        throw new @OsUntrusted IllegalArgumentException("Too many arguements.");
      }
      if (args.size() == 2) {
        snapshotName = args.removeLast();
      }
    }

    @Override
    protected void processArguments(SnapshotCommands.@OsUntrusted CreateSnapshot this, @OsUntrusted LinkedList<@OsUntrusted PathData> items)
    throws IOException {
      super.processArguments(items);
      if (numErrors != 0) { // check for error collecting paths
        return;
      }
      assert(items.size() == 1);
      @OsUntrusted
      PathData sroot = items.getFirst();
      @OsUntrusted
      Path snapshotPath = sroot.fs.createSnapshot(sroot.path, snapshotName);
      out.println("Created snapshot " + snapshotPath);
    }    
  }

  /**
   * Delete a snapshot
   */
  public static class DeleteSnapshot extends @OsUntrusted FsCommand {
    public static final @OsUntrusted String NAME = DELETE_SNAPSHOT;
    public static final @OsUntrusted String USAGE = "<snapshotDir> <snapshotName>";
    public static final @OsUntrusted String DESCRIPTION = 
        "Delete a snapshot from a directory";

    private @OsUntrusted String snapshotName;

    @Override
    protected void processPath(SnapshotCommands.@OsUntrusted DeleteSnapshot this, @OsUntrusted PathData item) throws IOException {
      if (!item.stat.isDirectory()) {
        throw new @OsUntrusted PathIsNotDirectoryException(item.toString());
      }
    }

    @Override
    protected void processOptions(SnapshotCommands.@OsUntrusted DeleteSnapshot this, @OsUntrusted LinkedList<@OsUntrusted String> args) throws IOException {
      if (args.size() != 2) {
        throw new @OsUntrusted IOException("args number not 2: " + args.size());
      }
      snapshotName = args.removeLast();
    }

    @Override
    protected void processArguments(SnapshotCommands.@OsUntrusted DeleteSnapshot this, @OsUntrusted LinkedList<@OsUntrusted PathData> items)
        throws IOException {
      super.processArguments(items);
      if (numErrors != 0) { // check for error collecting paths
        return;
      }
      assert (items.size() == 1);
      @OsUntrusted
      PathData sroot = items.getFirst();
      sroot.fs.deleteSnapshot(sroot.path, snapshotName);
    }
  }
  
  /**
   * Rename a snapshot
   */
  public static class RenameSnapshot extends @OsUntrusted FsCommand {
    public static final @OsUntrusted String NAME = RENAME_SNAPSHOT;
    public static final @OsUntrusted String USAGE = "<snapshotDir> <oldName> <newName>";
    public static final @OsUntrusted String DESCRIPTION = 
        "Rename a snapshot from oldName to newName";
    
    private @OsUntrusted String oldName;
    private @OsUntrusted String newName;
    
    @Override
    protected void processPath(SnapshotCommands.@OsUntrusted RenameSnapshot this, @OsUntrusted PathData item) throws IOException {
      if (!item.stat.isDirectory()) {
        throw new @OsUntrusted PathIsNotDirectoryException(item.toString());
      }
    }

    @Override
    protected void processOptions(SnapshotCommands.@OsUntrusted RenameSnapshot this, @OsUntrusted LinkedList<@OsUntrusted String> args) throws IOException {
      if (args.size() != 3) {
        throw new @OsUntrusted IOException("args number not 3: " + args.size());
      }
      newName = args.removeLast();
      oldName = args.removeLast();
    }

    @Override
    protected void processArguments(SnapshotCommands.@OsUntrusted RenameSnapshot this, @OsUntrusted LinkedList<@OsUntrusted PathData> items)
        throws IOException {
      super.processArguments(items);
      if (numErrors != 0) { // check for error collecting paths
        return;
      }
      Preconditions.checkArgument(items.size() == 1);
      @OsUntrusted
      PathData sroot = items.getFirst();
      sroot.fs.renameSnapshot(sroot.path, oldName, newName);
    }
    
  }
}

