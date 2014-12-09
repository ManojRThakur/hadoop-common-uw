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
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.util.StringUtils;

/**
 * An abstract class for the execution of a file system command
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

abstract public class Command extends @OsUntrusted Configured {
  /** field name indicating the default name of the command */
  public static final @OsUntrusted String COMMAND_NAME_FIELD = "NAME";
  /** field name indicating the command's usage switches and arguments format */
  public static final @OsUntrusted String COMMAND_USAGE_FIELD = "USAGE";
  /** field name indicating the command's long description */
  public static final @OsUntrusted String COMMAND_DESCRIPTION_FIELD = "DESCRIPTION";
    
  protected @OsUntrusted String @OsUntrusted [] args;
  protected @OsUntrusted String name;
  protected @OsUntrusted int exitCode = 0;
  protected @OsUntrusted int numErrors = 0;
  protected @OsUntrusted boolean recursive = false;
  private @OsUntrusted int depth = 0;
  protected @OsUntrusted ArrayList<@OsUntrusted Exception> exceptions = new @OsUntrusted ArrayList<@OsUntrusted Exception>();

  private static final @OsUntrusted Log LOG = LogFactory.getLog(Command.class);

  /** allows stdout to be captured if necessary */
  public @OsUntrusted PrintStream out = System.out;
  /** allows stderr to be captured if necessary */
  public @OsUntrusted PrintStream err = System.err;

  /** Constructor */
  protected @OsUntrusted Command() {
    out = System.out;
    err = System.err;
  }
  
  /** Constructor */
  protected @OsUntrusted Command(@OsUntrusted Configuration conf) {
    super(conf);
  }
  
  /** @return the command's name excluding the leading character - */
  abstract public @OsUntrusted String getCommandName(@OsUntrusted Command this);
  
  protected void setRecursive(@OsUntrusted Command this, @OsUntrusted boolean flag) {
    recursive = flag;
  }
  
  protected @OsUntrusted boolean isRecursive(@OsUntrusted Command this) {
    return recursive;
  }

  protected @OsUntrusted int getDepth(@OsUntrusted Command this) {
    return depth;
  }
  
  /** 
   * Execute the command on the input path
   * 
   * @param path the input path
   * @throws IOException if any error occurs
   */
  abstract protected void run(@OsUntrusted Command this, @OsUntrusted Path path) throws IOException;
  
  /** 
   * For each source path, execute the command
   * 
   * @return 0 if it runs successfully; -1 if it fails
   */
  public @OsUntrusted int runAll(@OsUntrusted Command this) {
    @OsUntrusted
    int exitCode = 0;
    for (@OsUntrusted String src : args) {
      try {
        @OsUntrusted
        PathData @OsUntrusted [] srcs = PathData.expandAsGlob(src, getConf());
        for (@OsUntrusted PathData s : srcs) {
          run(s.path);
        }
      } catch (@OsUntrusted IOException e) {
        exitCode = -1;
        displayError(e);
      }
    }
    return exitCode;
  }

  /**
   * Invokes the command handler.  The default behavior is to process options,
   * expand arguments, and then process each argument.
   * <pre>
   * run
   * |-> {@link #processOptions(LinkedList)}
   * \-> {@link #processRawArguments(LinkedList)}
   *      |-> {@link #expandArguments(LinkedList)}
   *      |   \-> {@link #expandArgument(String)}*
   *      \-> {@link #processArguments(LinkedList)}
   *          |-> {@link #processArgument(PathData)}*
   *          |   |-> {@link #processPathArgument(PathData)}
   *          |   \-> {@link #processPaths(PathData, PathData...)}
   *          |        \-> {@link #processPath(PathData)}*
   *          \-> {@link #processNonexistentPath(PathData)}
   * </pre>
   * Most commands will chose to implement just
   * {@link #processOptions(LinkedList)} and {@link #processPath(PathData)}
   * 
   * @param argv the list of command line arguments
   * @return the exit code for the command
   * @throws IllegalArgumentException if called with invalid arguments
   */
  public @OsUntrusted int run(@OsUntrusted Command this, @OsUntrusted String @OsUntrusted ...argv) {
    @OsUntrusted
    LinkedList<@OsUntrusted String> args = new @OsUntrusted LinkedList<@OsUntrusted String>(Arrays.asList(argv));
    try {
      if (isDeprecated()) {
        displayWarning(
            "DEPRECATED: Please use '"+ getReplacementCommand() + "' instead.");
      }
      processOptions(args);
      processRawArguments(args);
    } catch (@OsUntrusted CommandInterruptException e) {
      displayError("Interrupted");
      return 130;
    } catch (@OsUntrusted IOException e) {
      displayError(e);
    }
    
    return (numErrors == 0) ? exitCode : exitCodeForError();
  }

  /**
   * The exit code to be returned if any errors occur during execution.
   * This method is needed to account for the inconsistency in the exit
   * codes returned by various commands.
   * @return a non-zero exit code
   */
  protected @OsUntrusted int exitCodeForError(@OsUntrusted Command this) { return 1; }
  
  /**
   * Must be implemented by commands to process the command line flags and
   * check the bounds of the remaining arguments.  If an
   * IllegalArgumentException is thrown, the FsShell object will print the
   * short usage of the command.
   * @param args the command line arguments
   * @throws IOException
   */
  protected void processOptions(@OsUntrusted Command this, @OsUntrusted LinkedList<@OsUntrusted String> args) throws IOException {}

  /**
   * Allows commands that don't use paths to handle the raw arguments.
   * Default behavior is to expand the arguments via
   * {@link #expandArguments(LinkedList)} and pass the resulting list to
   * {@link #processArguments(LinkedList)} 
   * @param args the list of argument strings
   * @throws IOException
   */
  protected void processRawArguments(@OsUntrusted Command this, @OsUntrusted LinkedList<@OsUntrusted String> args)
  throws IOException {
    processArguments(expandArguments(args));
  }

  /**
   *  Expands a list of arguments into {@link PathData} objects.  The default
   *  behavior is to call {@link #expandArgument(String)} on each element
   *  which by default globs the argument.  The loop catches IOExceptions,
   *  increments the error count, and displays the exception.
   * @param args strings to expand into {@link PathData} objects
   * @return list of all {@link PathData} objects the arguments
   * @throws IOException if anything goes wrong...
   */
  protected @OsUntrusted LinkedList<@OsUntrusted PathData> expandArguments(@OsUntrusted Command this, @OsUntrusted LinkedList<@OsUntrusted String> args)
  throws IOException {
    @OsUntrusted
    LinkedList<@OsUntrusted PathData> expandedArgs = new @OsUntrusted LinkedList<@OsUntrusted PathData>();
    for (@OsUntrusted String arg : args) {
      try {
        expandedArgs.addAll(expandArgument(arg));
      } catch (@OsUntrusted IOException e) { // other exceptions are probably nasty
        displayError(e);
      }
    }
    return expandedArgs;
  }

  /**
   * Expand the given argument into a list of {@link PathData} objects.
   * The default behavior is to expand globs.  Commands may override to
   * perform other expansions on an argument.
   * @param arg string pattern to expand
   * @return list of {@link PathData} objects
   * @throws IOException if anything goes wrong...
   */
  protected @OsUntrusted List<@OsUntrusted PathData> expandArgument(@OsUntrusted Command this, @OsUntrusted String arg) throws IOException {
    @OsUntrusted
    PathData @OsUntrusted [] items = PathData.expandAsGlob(arg, getConf());
    if (items.length == 0) {
      // it's a glob that failed to match
      throw new @OsUntrusted PathNotFoundException(arg);
    }
    return Arrays.asList(items);
  }

  /**
   *  Processes the command's list of expanded arguments.
   *  {@link #processArgument(PathData)} will be invoked with each item
   *  in the list.  The loop catches IOExceptions, increments the error
   *  count, and displays the exception.
   *  @param args a list of {@link PathData} to process
   *  @throws IOException if anything goes wrong... 
   */
  protected void processArguments(@OsUntrusted Command this, @OsUntrusted LinkedList<@OsUntrusted PathData> args)
  throws IOException {
    for (@OsUntrusted PathData arg : args) {
      try {
        processArgument(arg);
      } catch (@OsUntrusted IOException e) {
        displayError(e);
      }
    }
  }

  /**
   * Processes a {@link PathData} item, calling
   * {@link #processPathArgument(PathData)} or
   * {@link #processNonexistentPath(PathData)} on each item.
   * @param item {@link PathData} item to process
   * @throws IOException if anything goes wrong...
   */
  protected void processArgument(@OsUntrusted Command this, @OsUntrusted PathData item) throws IOException {
    if (item.exists) {
      processPathArgument(item);
    } else {
      processNonexistentPath(item);
    }
  }

  /**
   *  This is the last chance to modify an argument before going into the
   *  (possibly) recursive {@link #processPaths(PathData, PathData...)}
   *  -> {@link #processPath(PathData)} loop.  Ex.  ls and du use this to
   *  expand out directories.
   *  @param item a {@link PathData} representing a path which exists
   *  @throws IOException if anything goes wrong... 
   */
  protected void processPathArgument(@OsUntrusted Command this, @OsUntrusted PathData item) throws IOException {
    // null indicates that the call is not via recursion, ie. there is
    // no parent directory that was expanded
    depth = 0;
    processPaths(null, item);
  }
  
  /**
   *  Provides a hook for handling paths that don't exist.  By default it
   *  will throw an exception.  Primarily overriden by commands that create
   *  paths such as mkdir or touch.
   *  @param item the {@link PathData} that doesn't exist
   *  @throws FileNotFoundException if arg is a path and it doesn't exist
   *  @throws IOException if anything else goes wrong... 
   */
  protected void processNonexistentPath(@OsUntrusted Command this, @OsUntrusted PathData item) throws IOException {
    throw new @OsUntrusted PathNotFoundException(item.toString());
  }

  /**
   *  Iterates over the given expanded paths and invokes
   *  {@link #processPath(PathData)} on each element.  If "recursive" is true,
   *  will do a post-visit DFS on directories.
   *  @param parent if called via a recurse, will be the parent dir, else null
   *  @param items a list of {@link PathData} objects to process
   *  @throws IOException if anything goes wrong...
   */
  protected void processPaths(@OsUntrusted Command this, @OsUntrusted PathData parent, @OsUntrusted PathData @OsUntrusted ... items)
  throws IOException {
    // TODO: this really should be iterative
    for (@OsUntrusted PathData item : items) {
      try {
        processPath(item);
        if (recursive && item.stat.isDirectory()) {
          recursePath(item);
        }
        postProcessPath(item);
      } catch (@OsUntrusted IOException e) {
        displayError(e);
      }
    }
  }

  /**
   * Hook for commands to implement an operation to be applied on each
   * path for the command.  Note implementation of this method is optional
   * if earlier methods in the chain handle the operation.
   * @param item a {@link PathData} object
   * @throws RuntimeException if invoked but not implemented
   * @throws IOException if anything else goes wrong in the user-implementation
   */  
  protected void processPath(@OsUntrusted Command this, @OsUntrusted PathData item) throws IOException {
    throw new @OsUntrusted RuntimeException("processPath() is not implemented");    
  }

  /**
   * Hook for commands to implement an operation to be applied on each
   * path for the command after being processed successfully
   * @param item a {@link PathData} object
   * @throws IOException if anything goes wrong...
   */
  protected void postProcessPath(@OsUntrusted Command this, @OsUntrusted PathData item) throws IOException {    
  }

  /**
   *  Gets the directory listing for a path and invokes
   *  {@link #processPaths(PathData, PathData...)}
   *  @param item {@link PathData} for directory to recurse into
   *  @throws IOException if anything goes wrong...
   */
  protected void recursePath(@OsUntrusted Command this, @OsUntrusted PathData item) throws IOException {
    try {
      depth++;
      processPaths(item, item.getDirectoryContents());
    } finally {
      depth--;
    }
  }

  /**
   * Display an exception prefaced with the command name.  Also increments
   * the error count for the command which will result in a non-zero exit
   * code.
   * @param e exception to display
   */
  public void displayError(@OsUntrusted Command this, @OsUntrusted Exception e) {
    // build up a list of exceptions that occurred
    exceptions.add(e);
    // use runtime so it rips up through the stack and exits out 
    if (e instanceof @OsUntrusted InterruptedIOException) {
      throw new @OsUntrusted CommandInterruptException();
    }
    
    @OsUntrusted
    String errorMessage = e.getLocalizedMessage();
    if (errorMessage == null) {
      // this is an unexpected condition, so dump the whole exception since
      // it's probably a nasty internal error where the backtrace would be
      // useful
      errorMessage = StringUtils.stringifyException(e);
      LOG.debug(errorMessage);
    } else {
      errorMessage = errorMessage.split("\n", 2)[0];
    }
    displayError(errorMessage);
  }
  
  /**
   * Display an error string prefaced with the command name.  Also increments
   * the error count for the command which will result in a non-zero exit
   * code.
   * @param message error message to display
   */
  public void displayError(@OsUntrusted Command this, @OsUntrusted String message) {
    numErrors++;
    displayWarning(message);
  }
  
  /**
   * Display an warning string prefaced with the command name.
   * @param message warning message to display
   */
  public void displayWarning(@OsUntrusted Command this, @OsUntrusted String message) {
    err.println(getName() + ": " + message);
  }
  
  /**
   * The name of the command.  Will first try to use the assigned name
   * else fallback to the command's preferred name
   * @return name of the command
   */
  public @OsUntrusted String getName(@OsUntrusted Command this) {
    return (name == null)
      ? getCommandField(COMMAND_NAME_FIELD)
      : name.startsWith("-") ? name.substring(1) : name;
  }

  /**
   * Define the name of the command.
   * @param name as invoked
   */
  public void setName(@OsUntrusted Command this, @OsUntrusted String name) {
    this.name = name;
  }
  
  /**
   * The short usage suitable for the synopsis
   * @return "name options"
   */
  public @OsUntrusted String getUsage(@OsUntrusted Command this) {
    @OsUntrusted
    String cmd = "-" + getName();
    @OsUntrusted
    String usage = isDeprecated() ? "" : getCommandField(COMMAND_USAGE_FIELD);
    return usage.isEmpty() ? cmd : cmd + " " + usage; 
  }

  /**
   * The long usage suitable for help output
   * @return text of the usage
   */
  public @OsUntrusted String getDescription(@OsUntrusted Command this) {
    return isDeprecated()
      ? "(DEPRECATED) Same as '" + getReplacementCommand() + "'"
      : getCommandField(COMMAND_DESCRIPTION_FIELD);
  }

  /**
   * Is the command deprecated?
   * @return boolean
   */
  public final @OsUntrusted boolean isDeprecated(@OsUntrusted Command this) {
    return (getReplacementCommand() != null);
  }
  
  /**
   * The replacement for a deprecated command
   * @return null if not deprecated, else alternative command
   */
  public @OsUntrusted String getReplacementCommand(@OsUntrusted Command this) {
    return null;
  }

  /**
   * Get a public static class field
   * @param field the field to retrieve
   * @return String of the field
   */
  private @OsUntrusted String getCommandField(@OsUntrusted Command this, @OsUntrusted String field) {
    @OsUntrusted
    String value;
    try {
      @OsUntrusted
      Field f = this.getClass().getDeclaredField(field);
      f.setAccessible(true);
      value = f.get(this).toString();
    } catch (@OsUntrusted Exception e) {
      throw new @OsUntrusted RuntimeException(
          "failed to get " + this.getClass().getSimpleName()+"."+field, e);
    }
    return value;
  }
  
  @SuppressWarnings("serial")
  static class CommandInterruptException extends @OsUntrusted RuntimeException {}
}
