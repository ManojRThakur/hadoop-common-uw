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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShellPermissions;
import org.apache.hadoop.fs.Path;

/**
 * Base class for all "hadoop fs" commands
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving

// this class may not look useful now, but it's a placeholder for future
// functionality to act as a registry for fs commands.  currently it's being
// used to implement unnecessary abstract methods in the base class

abstract public class FsCommand extends @OsUntrusted Command {
  /**
   * Register the command classes used by the fs subcommand
   * @param factory where to register the class
   */
  public static void registerCommands(@OsUntrusted CommandFactory factory) {
    factory.registerCommands(CopyCommands.class);
    factory.registerCommands(Count.class);
    factory.registerCommands(Delete.class);
    factory.registerCommands(Display.class);
    factory.registerCommands(FsShellPermissions.class);
    factory.registerCommands(FsUsage.class);
    factory.registerCommands(Ls.class);
    factory.registerCommands(Mkdir.class);
    factory.registerCommands(MoveCommands.class);
    factory.registerCommands(SetReplication.class);
    factory.registerCommands(Stat.class);
    factory.registerCommands(Tail.class);
    factory.registerCommands(Test.class);
    factory.registerCommands(Touch.class);
    factory.registerCommands(SnapshotCommands.class);
  }

  protected @OsUntrusted FsCommand() {}
  
  protected @OsUntrusted FsCommand(@OsUntrusted Configuration conf) {
    super(conf);
  }

  // historical abstract method in Command
  @Override
  public @OsUntrusted String getCommandName(@OsUntrusted FsCommand this) { 
    return getName(); 
  }
  
  // abstract method that normally is invoked by runall() which is
  // overridden below
  @Override
  protected void run(@OsUntrusted FsCommand this, @OsUntrusted Path path) throws IOException {
    throw new @OsUntrusted RuntimeException("not supposed to get here");
  }
  
  /** @deprecated use {@link Command#run(String...argv)} */
  @Deprecated
  @Override
  public @OsUntrusted int runAll(@OsUntrusted FsCommand this) {
    return run(args);
  }
}
