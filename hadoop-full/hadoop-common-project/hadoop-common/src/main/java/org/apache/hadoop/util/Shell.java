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
package org.apache.hadoop.util;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import ostrusted.quals.PolyOsTrusted;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** 
 * A base class for running a Unix command.
 * 
 * <code>Shell</code> can be used to run unix commands like <code>du</code> or
 * <code>df</code>. It also offers facilities to gate commands by 
 * time-intervals.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
abstract public class Shell {
  
  public static final @OsUntrusted Log LOG = LogFactory.getLog(Shell.class);
  
  private static @OsUntrusted boolean IS_JAVA7_OR_ABOVE =
      System.getProperty("java.version").substring(0, 3).compareTo("1.7") >= 0;

  public static @OsUntrusted boolean isJava7OrAbove() {
    return IS_JAVA7_OR_ABOVE;
  }

  /** a Unix command to get the current user's name */
  public final static @OsUntrusted String USER_NAME_COMMAND = "whoami";

  /** Windows CreateProcess synchronization object */
  public static final @OsUntrusted Object WindowsProcessLaunchLock = new @OsUntrusted Object();

  // OSType detection

  public enum OSType {

@OsUntrusted  OS_TYPE_LINUX,

@OsUntrusted  OS_TYPE_WIN,

@OsUntrusted  OS_TYPE_SOLARIS,

@OsUntrusted  OS_TYPE_MAC,

@OsUntrusted  OS_TYPE_FREEBSD,

@OsUntrusted  OS_TYPE_OTHER
  }

  public static final @OsUntrusted OSType osType = getOSType();

  static private @OsUntrusted OSType getOSType() {
    @OsUntrusted
    String osName = System.getProperty("os.name");
    if (osName.startsWith("Windows")) {
      return OSType.OS_TYPE_WIN;
    } else if (osName.contains("SunOS") || osName.contains("Solaris")) {
      return OSType.OS_TYPE_SOLARIS;
    } else if (osName.contains("Mac")) {
      return OSType.OS_TYPE_MAC;
    } else if (osName.contains("FreeBSD")) {
      return OSType.OS_TYPE_FREEBSD;
    } else if (osName.startsWith("Linux")) {
      return OSType.OS_TYPE_LINUX;
    } else {
      // Some other form of Unix
      return OSType.OS_TYPE_OTHER;
    }
  }

  // Helper static vars for each platform
  public static final @OsUntrusted boolean WINDOWS = (osType == OSType.OS_TYPE_WIN);
  public static final @OsUntrusted boolean SOLARIS = (osType == OSType.OS_TYPE_SOLARIS);
  public static final @OsUntrusted boolean MAC     = (osType == OSType.OS_TYPE_MAC);
  public static final @OsUntrusted boolean FREEBSD = (osType == OSType.OS_TYPE_FREEBSD);
  public static final @OsUntrusted boolean LINUX   = (osType == OSType.OS_TYPE_LINUX);
  public static final @OsUntrusted boolean OTHER   = (osType == OSType.OS_TYPE_OTHER);

  /** a Unix command to get the current user's groups list */
  public static @OsTrusted String @OsUntrusted [] getGroupsCommand() {
    return (WINDOWS)? new @OsTrusted String @OsUntrusted []{"cmd", "/c", "groups"}
                    : new @OsTrusted String @OsUntrusted []{"bash", "-c", "groups"};
  }

  /** a Unix command to get a given user's groups list */
  public static @OsTrusted String @OsUntrusted [] getGroupsForUserCommand(final @OsTrusted String user) {
    //'groups username' command return is non-consistent across different unixes
    return (WINDOWS)? new @OsTrusted String @OsUntrusted [] { WINUTILS, "groups", "-F", "\"" + user + "\""}
                    : new @OsTrusted String @OsUntrusted [] {"bash", "-c", "id -Gn " + user};
  }

  /** a Unix command to get a given netgroup's user list */
  public static @OsTrusted String @OsUntrusted [] getUsersForNetgroupCommand(final @OsTrusted String netgroup) {
    //'groups username' command return is non-consistent across different unixes
    return (WINDOWS)? new @OsTrusted String @OsUntrusted [] {"cmd", "/c", "getent netgroup " + netgroup}
                    : new @OsTrusted String @OsUntrusted [] {"bash", "-c", "getent netgroup " + netgroup};
  }

  /** Return a command to get permission information. */
  public static @OsTrusted String @OsUntrusted [] getGetPermissionCommand() {
    return (WINDOWS) ? new @OsTrusted String @OsUntrusted [] { WINUTILS, "ls", "-F" }
                     : new @OsTrusted String @OsUntrusted [] { "/bin/ls", "-ld" };
  }

  /** Return a command to set permission */
  public static @OsTrusted String @OsUntrusted [] getSetPermissionCommand(@OsTrusted String perm, @OsUntrusted boolean recursive) {
    if (recursive) {
      return (WINDOWS) ? new @OsTrusted String @OsUntrusted [] { WINUTILS, "chmod", "-R", perm }
                         : new @OsTrusted String @OsUntrusted [] { "chmod", "-R", perm };
    } else {
      return (WINDOWS) ? new @OsTrusted String @OsUntrusted [] { WINUTILS, "chmod", perm }
                       : new @OsTrusted String @OsUntrusted [] { "chmod", perm };
    }
  }

  /**
   * Return a command to set permission for specific file.
   * 
   * @param perm String permission to set
   * @param recursive boolean true to apply to all sub-directories recursively
   * @param file String file to set
   * @return String[] containing command and arguments
   */
  public static @OsTrusted String @OsUntrusted [] getSetPermissionCommand(@OsTrusted String perm, @OsUntrusted boolean recursive,
                                                 @OsTrusted String file) {
    @OsTrusted String @OsUntrusted [] baseCmd = getSetPermissionCommand(perm, recursive);
    @OsTrusted String @OsUntrusted [] cmdWithFile = Arrays.copyOf(baseCmd, baseCmd.length + 1);
    cmdWithFile[cmdWithFile.length - 1] = file;
    return cmdWithFile;
  }

  /** Return a command to set owner */
  public static @OsTrusted String @OsUntrusted [] getSetOwnerCommand(@OsTrusted String owner) {
    return (WINDOWS) ? new @OsTrusted String @OsUntrusted [] { WINUTILS, "chown", "\"" + owner + "\"" }
                     : new @OsTrusted String @OsUntrusted [] { "chown", owner };
  }
  
  /** Return a command to create symbolic links */
  public static @OsTrusted String @OsUntrusted [] getSymlinkCommand(@OsTrusted String target, @OsTrusted String link) {
    return WINDOWS ? new @OsTrusted String @OsUntrusted [] { WINUTILS, "symlink", link, target }
                   : new @OsTrusted String @OsUntrusted [] { "ln", "-s", target, link };
  }

  /** Return a command to read the target of the a symbolic link*/
  public static @OsTrusted String @OsUntrusted [] getReadlinkCommand(@OsTrusted String link) {
    return WINDOWS ? new @OsTrusted String @OsUntrusted [] { WINUTILS, "readlink", link }
        : new @OsTrusted String @OsUntrusted [] { "readlink", link };
  }

  /** Return a command for determining if process with specified pid is alive. */
  public static @OsUntrusted String @OsUntrusted [] getCheckProcessIsAliveCommand(@OsUntrusted String pid) {
    return Shell.WINDOWS ?
      new @OsUntrusted String @OsUntrusted [] { Shell.WINUTILS, "task", "isAlive", pid } :
      new @OsUntrusted String @OsUntrusted [] { "kill", "-0", isSetsidAvailable ? "-" + pid : pid };
  }

  /** Return a command to send a signal to a given pid */
  public static @OsUntrusted String @OsUntrusted [] getSignalKillCommand(@OsUntrusted int code, @OsUntrusted String pid) {
    return Shell.WINDOWS ? new @OsUntrusted String @OsUntrusted [] { Shell.WINUTILS, "task", "kill", pid } :
      new @OsUntrusted String @OsUntrusted [] { "kill", "-" + code, isSetsidAvailable ? "-" + pid : pid };
  }

  /** Return a regular expression string that match environment variables */
  public static @OsUntrusted String getEnvironmentVariableRegex() {
    return (WINDOWS) ? "%([A-Za-z_][A-Za-z0-9_]*?)%" :
      "\\$([A-Za-z_][A-Za-z0-9_]*)";
  }
  
  /**
   * Returns a File referencing a script with the given basename, inside the
   * given parent directory.  The file extension is inferred by platform: ".cmd"
   * on Windows, or ".sh" otherwise.
   * 
   * @param parent File parent directory
   * @param basename String script file basename
   * @return File referencing the script in the directory
   */
  public static @OsUntrusted File appendScriptExtension(@OsUntrusted File parent, @OsUntrusted String basename) {
    return new @OsUntrusted File(parent, appendScriptExtension(basename));
  }

  /**
   * Returns a script file name with the given basename.  The file extension is
   * inferred by platform: ".cmd" on Windows, or ".sh" otherwise.
   * 
   * @param basename String script file basename
   * @return String script file name
   */
  public static @OsUntrusted String appendScriptExtension(@OsUntrusted String basename) {
    return basename + (WINDOWS ? ".cmd" : ".sh");
  }

  /**
   * Returns a command to run the given script.  The script interpreter is
   * inferred by platform: cmd on Windows or bash otherwise.
   * 
   * @param script File script to run
   * @return String[] command to run the script
   */
  public static @OsUntrusted String @OsUntrusted [] getRunScriptCommand(@OsUntrusted File script) {
    @OsUntrusted
    String absolutePath = script.getAbsolutePath();
    return WINDOWS ? new @OsUntrusted String @OsUntrusted [] { "cmd", "/c", absolutePath } :
      new @OsUntrusted String @OsUntrusted [] { "/bin/bash", absolutePath };
  }

  /** a Unix command to set permission */
  public static final @OsUntrusted String SET_PERMISSION_COMMAND = "chmod";
  /** a Unix command to set owner */
  public static final @OsUntrusted String SET_OWNER_COMMAND = "chown";

  /** a Unix command to set the change user's groups list */
  public static final @OsUntrusted String SET_GROUP_COMMAND = "chgrp";
  /** a Unix command to create a link */
  public static final @OsUntrusted String LINK_COMMAND = "ln";
  /** a Unix command to get a link target */
  public static final @OsUntrusted String READ_LINK_COMMAND = "readlink";

  /**Time after which the executing script would be timedout*/
  protected @OsUntrusted long timeOutInterval = 0L;
  /** If or not script timed out*/
  private @OsUntrusted AtomicBoolean timedOut;


  /** Centralized logic to discover and validate the sanity of the Hadoop 
   *  home directory. Returns either NULL or a directory that exists and 
   *  was specified via either -Dhadoop.home.dir or the HADOOP_HOME ENV 
   *  variable.  This does a lot of work so it should only be called 
   *  privately for initialization once per process.
   **/
  @SuppressWarnings("ostrusted:cast.unsafe")
  //ostrusted, getProperty/getEnv are trusted
  private static @OsTrusted String checkHadoopHome() {

    // first check the Dflag hadoop.home.dir with JVM scope
    @OsTrusted
    String home = (@OsTrusted String) System.getProperty("hadoop.home.dir");

    // fall back to the system/user-global env variable
    if (home == null) {
      home = (@OsTrusted String)  System.getenv("HADOOP_HOME");
    }

    try {
       // couldn't find either setting for hadoop's home directory
       if (home == null) {
         throw new @OsUntrusted IOException("HADOOP_HOME or hadoop.home.dir are not set.");
       }

       if (home.startsWith("\"") && home.endsWith("\"")) {
         home = (@OsTrusted String) home.substring(1, home.length()-1);
       }

       // check that the home setting is actually a directory that exists
       @OsUntrusted
       File homedir = new @OsUntrusted File(home);
       if (!homedir.isAbsolute() || !homedir.exists() || !homedir.isDirectory()) {
         throw new @OsUntrusted IOException("Hadoop home directory " + homedir
           + " does not exist, is not a directory, or is not an absolute path.");
       }

       home = (@OsTrusted String) homedir.getCanonicalPath();

    } catch (@OsUntrusted IOException ioe) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to detect a valid hadoop home directory", ioe);
      }
      home = null;
    }
    
    return home;
  }
  private static @OsTrusted String HADOOP_HOME_DIR = checkHadoopHome();

  // Public getter, throws an exception if HADOOP_HOME failed validation
  // checks and is being referenced downstream.
  public static final @OsUntrusted String getHadoopHome() throws IOException {
    if (HADOOP_HOME_DIR == null) {
      throw new @OsUntrusted IOException("Misconfigured HADOOP_HOME cannot be referenced.");
    }

    return HADOOP_HOME_DIR;
  }

  /** fully qualify the path to a binary that should be in a known hadoop 
   *  bin location. This is primarily useful for disambiguating call-outs 
   *  to executable sub-components of Hadoop to avoid clashes with other 
   *  executables that may be in the path.  Caveat:  this call doesn't 
   *  just format the path to the bin directory.  It also checks for file 
   *  existence of the composed path. The output of this call should be 
   *  cached by callers.
   * */

  @SuppressWarnings("ostrusted:return.type.incompatible")
  //ostrusted, fulExeName is trusted except for executable, hence the @PolyOsTrusted
  public static final @PolyOsTrusted String getQualifiedBinPath(@PolyOsTrusted String executable)
    throws IOException {
    // construct hadoop bin path to the specified executable
    @OsUntrusted
    String fullExeName = HADOOP_HOME_DIR + File.separator + "bin" 
      + File.separator + executable;

    @OsUntrusted
    File exeFile = new @OsUntrusted File(fullExeName);
    if (!exeFile.exists()) {
      throw new @OsUntrusted IOException("Could not locate executable " + fullExeName
        + " in the Hadoop binaries.");
    }

    return exeFile.getCanonicalPath();
  }

  /** a Windows utility to emulate Unix commands */
  public static final @OsTrusted String WINUTILS = getWinUtilsPath();

  public static final @OsTrusted String getWinUtilsPath() {
    @OsTrusted String winUtilsPath = null;

    try {
      if (WINDOWS) {
        winUtilsPath = getQualifiedBinPath("winutils.exe");
      }
    } catch (@OsUntrusted IOException ioe) {
       LOG.error("Failed to locate the winutils binary in the hadoop binary path",
         ioe);
    }

    return winUtilsPath;
  }

  public static final @OsUntrusted boolean isSetsidAvailable = isSetsidSupported();
  private static @OsUntrusted boolean isSetsidSupported() {
    if (Shell.WINDOWS) {
      return false;
    }
    @OsUntrusted
    ShellCommandExecutor shexec = null;
    @OsUntrusted
    boolean setsidSupported = true;
    try {
      @OsTrusted String @OsUntrusted [] args = new @OsTrusted String @OsUntrusted [] {"setsid", "bash", "-c", "echo $$"};
      shexec = new @OsUntrusted ShellCommandExecutor(args);
      shexec.execute();
    } catch (@OsUntrusted IOException ioe) {
      LOG.debug("setsid is not available on this machine. So not using it.");
      setsidSupported = false;
    } finally { // handle the exit code
      if (LOG.isDebugEnabled()) {
        LOG.debug("setsid exited with exit code "
                 + (shexec != null ? shexec.getExitCode() : "(null executor)"));
      }
    }
    return setsidSupported;
  }

  /** Token separator regex used to parse Shell tool outputs */
  public static final @OsUntrusted String TOKEN_SEPARATOR_REGEX
                = WINDOWS ? "[|\n\r]" : "[ \t\n\r\f]";

  private @OsUntrusted long    interval;   // refresh interval in msec
  private @OsUntrusted long    lastTime;   // last time the command was performed
  final private @OsUntrusted boolean redirectErrorStream; // merge stdout and stderr
  private @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> environment; // env for the command execution
  private @OsUntrusted File dir;
  private @OsUntrusted Process process; // sub process used to execute the command
  private @OsUntrusted int exitCode;

  /**If or not script finished executing*/
  private volatile @OsUntrusted AtomicBoolean completed;
  
  public @OsUntrusted Shell() {
    this(0L);
  }
  
  public @OsUntrusted Shell(@OsUntrusted long interval) {
    this(interval, false);
  }

  /**
   * @param interval the minimum duration to wait before re-executing the 
   *        command.
   */
  public @OsUntrusted Shell(@OsUntrusted long interval, @OsUntrusted boolean redirectErrorStream) {
    this.interval = interval;
    this.lastTime = (interval<0) ? 0 : -interval;
    this.redirectErrorStream = redirectErrorStream;
  }
  
  /** set the environment for the command 
   * @param env Mapping of environment variables
   */
  protected void setEnvironment(@OsUntrusted Shell this, @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> env) {
    this.environment = env;
  }

  /** set the working directory 
   * @param dir The directory where the command would be executed
   */
  protected void setWorkingDirectory(@OsUntrusted Shell this, @OsUntrusted File dir) {
    this.dir = dir;
  }

  /** check to see if a command needs to be executed and execute if needed */
  protected void run(@OsUntrusted Shell this) throws IOException {
    if (lastTime + interval > Time.now())
      return;
    exitCode = 0; // reset for next run
    runCommand();
  }

  /** Run a command */
  private void runCommand(@OsUntrusted Shell this) throws IOException { 
    @OsUntrusted
    ProcessBuilder builder = new @OsUntrusted ProcessBuilder(getExecString());
    @OsUntrusted
    Timer timeOutTimer = null;
    @OsUntrusted
    ShellTimeoutTimerTask timeoutTimerTask = null;
    timedOut = new @OsUntrusted AtomicBoolean(false);
    completed = new @OsUntrusted AtomicBoolean(false);
    
    if (environment != null) {
      builder.environment().putAll(this.environment);
    }
    if (dir != null) {
      builder.directory(this.dir);
    }

    builder.redirectErrorStream(redirectErrorStream);
    
    if (Shell.WINDOWS) {
      synchronized (WindowsProcessLaunchLock) {
        // To workaround the race condition issue with child processes
        // inheriting unintended handles during process launch that can
        // lead to hangs on reading output and error streams, we
        // serialize process creation. More info available at:
        // http://support.microsoft.com/kb/315939
        process = builder.start();
      }
    } else {
      process = builder.start();
    }

    if (timeOutInterval > 0) {
      timeOutTimer = new @OsUntrusted Timer("Shell command timeout");
      timeoutTimerTask = new @OsUntrusted ShellTimeoutTimerTask(
          this);
      //One time scheduling.
      timeOutTimer.schedule(timeoutTimerTask, timeOutInterval);
    }
    //ostrusted, standard out and standard in can be considered trusted when coming from an executed process
    final @OsTrusted BufferedReader errReader =
            new @OsTrusted BufferedReader(new @OsUntrusted InputStreamReader(process
                                                     .getErrorStream()));
    @OsTrusted
    BufferedReader inReader = 
            new @OsTrusted BufferedReader(new @OsUntrusted InputStreamReader(process.getInputStream()));
    final @OsUntrusted StringBuffer errMsg = new @OsUntrusted StringBuffer();
    
    // read error and input streams as this would free up the buffers
    // free the error stream buffer
    @OsUntrusted
    Thread errThread = new @OsUntrusted Thread() {
      @Override
      public void run() {
        try {
          @OsUntrusted
          String line = errReader.readLine();
          while((line != null) && !isInterrupted()) {
            errMsg.append(line);
            errMsg.append(System.getProperty("line.separator"));
            line = errReader.readLine();
          }
        } catch(@OsUntrusted IOException ioe) {
          LOG.warn("Error reading the error stream", ioe);
        }
      }
    };
    try {
      errThread.start();
    } catch (@OsUntrusted IllegalStateException ise) { }
    try {
      parseExecResult(inReader); // parse the output
      // clear the input stream buffer
      @OsUntrusted
      String line = inReader.readLine();
      while(line != null) { 
        line = inReader.readLine();
      }
      // wait for the process to finish and check the exit code
      exitCode  = process.waitFor();
      try {
        // make sure that the error thread exits
        errThread.join();
      } catch (@OsUntrusted InterruptedException ie) {
        LOG.warn("Interrupted while reading the error stream", ie);
      }
      completed.set(true);
      //the timeout thread handling
      //taken care in finally block
      if (exitCode != 0) {
        throw new @OsUntrusted ExitCodeException(exitCode, errMsg.toString());
      }
    } catch (@OsUntrusted InterruptedException ie) {
      throw new @OsUntrusted IOException(ie.toString());
    } finally {
      if (timeOutTimer != null) {
        timeOutTimer.cancel();
      }
      // close the input stream
      try {
        inReader.close();
      } catch (@OsUntrusted IOException ioe) {
        LOG.warn("Error while closing the input stream", ioe);
      }
      try {
        if (!completed.get()) {
          errThread.interrupt();
          errThread.join();
        }
      } catch (@OsUntrusted InterruptedException ie) {
        LOG.warn("Interrupted while joining errThread");
      }
      try {
        errReader.close();
      } catch (@OsUntrusted IOException ioe) {
        LOG.warn("Error while closing the error stream", ioe);
      }
      process.destroy();
      lastTime = Time.now();
    }
  }

  /** return an array containing the command name & its parameters */ 
  protected abstract @OsTrusted String @OsUntrusted [] getExecString(@OsUntrusted Shell this);
  
  /** Parse the execution result */
  protected abstract void parseExecResult(@OsUntrusted Shell this, @OsTrusted BufferedReader lines)
  throws IOException;

  /** 
   * Get the environment variable
   */
  public @OsUntrusted String getEnvironment(@OsUntrusted Shell this, @OsUntrusted String env) {
    return environment.get(env);
  }
  
  /** get the current sub-process executing the given command 
   * @return process executing the command
   */
  public @OsUntrusted Process getProcess(@OsUntrusted Shell this) {
    return process;
  }

  /** get the exit code 
   * @return the exit code of the process
   */
  public @OsUntrusted int getExitCode(@OsUntrusted Shell this) {
    return exitCode;
  }

  /**
   * This is an IOException with exit code added.
   */
  public static class ExitCodeException extends @OsUntrusted IOException {
    @OsUntrusted
    int exitCode;
    
    public @OsUntrusted ExitCodeException(@OsUntrusted int exitCode, @OsUntrusted String message) {
      super(message);
      this.exitCode = exitCode;
    }
    
    public @OsUntrusted int getExitCode(Shell.@OsUntrusted ExitCodeException this) {
      return exitCode;
    }
  }
  
  /**
   * A simple shell command executor.
   * 
   * <code>ShellCommandExecutor</code>should be used in cases where the output 
   * of the command needs no explicit parsing and where the command, working 
   * directory and the environment remains unchanged. The output of the command 
   * is stored as-is and is expected to be small.
   */
  public static class ShellCommandExecutor extends @OsUntrusted Shell {
    
    private @OsTrusted String @OsUntrusted [] command;
    private @OsTrusted StringBuffer output;
    
    
    public @OsUntrusted ShellCommandExecutor(@OsTrusted String @OsUntrusted [] execString) {
      this(execString, null);
    }
    
    public @OsUntrusted ShellCommandExecutor(@OsTrusted String @OsUntrusted [] execString, @OsUntrusted File dir) {
      this(execString, dir, null);
    }
   
    public @OsUntrusted ShellCommandExecutor(@OsTrusted String @OsUntrusted [] execString, @OsUntrusted File dir, 
                                 @OsUntrusted
                                 Map<@OsUntrusted String, @OsUntrusted String> env) {
      this(execString, dir, env , 0L);
    }

    /**
     * Create a new instance of the ShellCommandExecutor to execute a command.
     * 
     * @param execString The command to execute with arguments
     * @param dir If not-null, specifies the directory which should be set
     *            as the current working directory for the command.
     *            If null, the current working directory is not modified.
     * @param env If not-null, environment of the command will include the
     *            key-value pairs specified in the map. If null, the current
     *            environment is not modified.
     * @param timeout Specifies the time in milliseconds, after which the
     *                command will be killed and the status marked as timedout.
     *                If 0, the command will not be timed out. 
     */
    public @OsUntrusted ShellCommandExecutor(@OsTrusted String @OsUntrusted [] execString, @OsUntrusted File dir, 
        @OsUntrusted
        Map<@OsUntrusted String, @OsUntrusted String> env, @OsUntrusted long timeout) {
      command = execString.clone();
      if (dir != null) {
        setWorkingDirectory(dir);
      }
      if (env != null) {
        setEnvironment(env);
      }
      timeOutInterval = timeout;
    }
        

    /** Execute the shell command. */
    public void execute(Shell.@OsUntrusted ShellCommandExecutor this) throws IOException {
      this.run();    
    }

    @Override
    public @OsTrusted String @OsUntrusted [] getExecString(Shell.@OsUntrusted ShellCommandExecutor this) {
      return command;
    }

    @Override
    protected void parseExecResult(Shell.@OsUntrusted ShellCommandExecutor this, @OsTrusted BufferedReader lines) throws IOException {
      output = new @OsTrusted StringBuffer();
      @OsTrusted char @OsUntrusted [] buf = new @OsTrusted char @OsUntrusted [512];
      @OsUntrusted
      int nRead;
      while ( (nRead = lines.read(buf, 0, buf.length)) > 0 ) {
        output.append(buf, 0, nRead);
      }
    }
    
    /** Get the output of the shell command.*/
    @SuppressWarnings("ostrusted:cast.unsafe")
    public @OsTrusted String getOutput(Shell.@OsUntrusted ShellCommandExecutor this) {
      return (@OsTrusted String) ( (output == null) ? "" : output.toString() );
    }

    /**
     * Returns the commands of this instance.
     * Arguments with spaces in are presented with quotes round; other
     * arguments are presented raw
     *
     * @return a string representation of the object.
     */
    @Override
    public @OsUntrusted String toString(Shell.@OsUntrusted ShellCommandExecutor this) {
      @OsUntrusted
      StringBuilder builder = new @OsUntrusted StringBuilder();
      @OsUntrusted
      String @OsUntrusted [] args = getExecString();
      for (@OsUntrusted String s : args) {
        if (s.indexOf(' ') >= 0) {
          builder.append('"').append(s).append('"');
        } else {
          builder.append(s);
        }
        builder.append(' ');
      }
      return builder.toString();
    }
  }
  
  /**
   * To check if the passed script to shell command executor timed out or
   * not.
   * 
   * @return if the script timed out.
   */
  public @OsUntrusted boolean isTimedOut(@OsUntrusted Shell this) {
    return timedOut.get();
  }
  
  /**
   * Set if the command has timed out.
   * 
   */
  private void setTimedOut(@OsUntrusted Shell this) {
    this.timedOut.set(true);
  }
  
  /** 
   * Static method to execute a shell command. 
   * Covers most of the simple cases without requiring the user to implement  
   * the <code>Shell</code> interface.
   * @param cmd shell command to execute.
   * @return the output of the executed command.
   */
  public static @OsTrusted String execCommand(@OsTrusted String @OsUntrusted ... cmd) throws IOException {
    return execCommand(null, cmd, 0L);
  }
  
  /** 
   * Static method to execute a shell command. 
   * Covers most of the simple cases without requiring the user to implement  
   * the <code>Shell</code> interface.
   * @param env the map of environment key=value
   * @param cmd shell command to execute.
   * @param timeout time in milliseconds after which script should be marked timeout
   * @return the output of the executed command.o
   */
  
  public static @OsTrusted String execCommand(@OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> env, @OsTrusted String @OsUntrusted [] cmd,
      @OsUntrusted
      long timeout) throws IOException {
    @OsUntrusted
    ShellCommandExecutor exec = new @OsUntrusted ShellCommandExecutor(cmd, null, env, 
                                                          timeout);
    exec.execute();
    return exec.getOutput();
  }

  /** 
   * Static method to execute a shell command. 
   * Covers most of the simple cases without requiring the user to implement  
   * the <code>Shell</code> interface.
   * @param env the map of environment key=value
   * @param cmd shell command to execute.
   * @return the output of the executed command.
   */
  public static @OsUntrusted String execCommand(@OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> env, @OsTrusted String @OsUntrusted ... cmd) 
  throws IOException {
    return execCommand(env, cmd, 0L);
  }
  
  /**
   * Timer which is used to timeout scripts spawned off by shell.
   */
  private static class ShellTimeoutTimerTask extends @OsUntrusted TimerTask {

    private @OsUntrusted Shell shell;

    public @OsUntrusted ShellTimeoutTimerTask(@OsUntrusted Shell shell) {
      this.shell = shell;
    }

    @Override
    public void run(Shell.@OsUntrusted ShellTimeoutTimerTask this) {
      @OsUntrusted
      Process p = shell.getProcess();
      try {
        p.exitValue();
      } catch (@OsUntrusted Exception e) {
        //Process has not terminated.
        //So check if it has completed 
        //if not just destroy it.
        if (p != null && !shell.completed.get()) {
          shell.setTimedOut();
          p.destroy();
        }
      }
    }
  }
}
