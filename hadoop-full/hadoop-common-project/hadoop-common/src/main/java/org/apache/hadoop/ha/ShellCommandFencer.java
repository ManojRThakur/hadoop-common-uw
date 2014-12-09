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
package org.apache.hadoop.ha;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.Shell;

/**
 * Fencing method that runs a shell command. It should be specified
 * in the fencing configuration like:<br>
 * <code>
 *   shell(/path/to/my/script.sh arg1 arg2 ...)
 * </code><br>
 * The string between '(' and ')' is passed directly to a bash shell
 * (cmd.exe on Windows) and may not include any closing parentheses.<p>
 * 
 * The shell command will be run with an environment set up to contain
 * all of the current Hadoop configuration variables, with the '_' character 
 * replacing any '.' characters in the configuration keys.<p>
 * 
 * If the shell command returns an exit code of 0, the fencing is
 * determined to be successful. If it returns any other exit code, the
 * fencing was not successful and the next fencing method in the list
 * will be attempted.<p>
 * 
 * <em>Note:</em> this fencing method does not implement any timeout.
 * If timeouts are necessary, they should be implemented in the shell
 * script itself (eg by forking a subshell to kill its parent in
 * some number of seconds).
 */
public class ShellCommandFencer
  extends @OsUntrusted Configured implements @OsUntrusted FenceMethod {

  /** Length at which to abbreviate command in long messages */
  private static final @OsUntrusted int ABBREV_LENGTH = 20;

  /** Prefix for target parameters added to the environment */
  private static final @OsUntrusted String TARGET_PREFIX = "target_";

  @VisibleForTesting
  static @OsUntrusted Log LOG = LogFactory.getLog(
      ShellCommandFencer.class);

  @Override
  public void checkArgs(@OsUntrusted ShellCommandFencer this, @OsUntrusted String args) throws BadFencingConfigurationException {
    if (args == null || args.isEmpty()) {
      throw new @OsUntrusted BadFencingConfigurationException(
          "No argument passed to 'shell' fencing method");
    }
    // Nothing else we can really check without actually running the command
  }

  @Override
  public @OsUntrusted boolean tryFence(@OsUntrusted ShellCommandFencer this, @OsUntrusted HAServiceTarget target, @OsTrusted String cmd) {
    @OsUntrusted
    ProcessBuilder builder;

    if (!Shell.WINDOWS) {
      builder = new @OsUntrusted ProcessBuilder("bash", "-e", "-c", cmd);
    } else {
      builder = new @OsUntrusted ProcessBuilder("cmd.exe", "/c", cmd);
    }

    setConfAsEnvVars(builder.environment());
    addTargetInfoAsEnvVars(target, builder.environment());

    @OsUntrusted
    Process p;
    try {
      p = builder.start();
      p.getOutputStream().close();
    } catch (@OsUntrusted IOException e) {
      LOG.warn("Unable to execute " + cmd, e);
      return false;
    }
    
    @OsUntrusted
    String pid = tryGetPid(p);
    LOG.info("Launched fencing command '" + cmd + "' with "
        + ((pid != null) ? ("pid " + pid) : "unknown pid"));
    
    @OsUntrusted
    String logPrefix = abbreviate(cmd, ABBREV_LENGTH);
    if (pid != null) {
      logPrefix = "[PID " + pid + "] " + logPrefix;
    }
    
    // Pump logs to stderr
    @OsUntrusted
    StreamPumper errPumper = new @OsUntrusted StreamPumper(
        LOG, logPrefix, p.getErrorStream(),
        StreamPumper.StreamType.STDERR);
    errPumper.start();
    
    @OsUntrusted
    StreamPumper outPumper = new @OsUntrusted StreamPumper(
        LOG, logPrefix, p.getInputStream(),
        StreamPumper.StreamType.STDOUT);
    outPumper.start();
    
    @OsUntrusted
    int rc;
    try {
      rc = p.waitFor();
      errPumper.join();
      outPumper.join();
    } catch (@OsUntrusted InterruptedException ie) {
      LOG.warn("Interrupted while waiting for fencing command: " + cmd);
      return false;
    }
    
    return rc == 0;
  }

  /**
   * Abbreviate a string by putting '...' in the middle of it,
   * in an attempt to keep logs from getting too messy.
   * @param cmd the string to abbreviate
   * @param len maximum length to abbreviate to
   * @return abbreviated string
   */
  static @OsUntrusted String abbreviate(@OsUntrusted String cmd, @OsUntrusted int len) {
    if (cmd.length() > len && len >= 5) {
      @OsUntrusted
      int firstHalf = (len - 3) / 2;
      @OsUntrusted
      int rem = len - firstHalf - 3;
      
      return cmd.substring(0, firstHalf) + 
        "..." + cmd.substring(cmd.length() - rem);
    } else {
      return cmd;
    }
  }
  
  /**
   * Attempt to use evil reflection tricks to determine the
   * pid of a launched process. This is helpful to ops
   * if debugging a fencing process that might have gone
   * wrong. If running on a system or JVM where this doesn't
   * work, it will simply return null.
   */
  private static @OsUntrusted String tryGetPid(@OsUntrusted Process p) {
    try {
      @OsUntrusted
      Class<@OsUntrusted ? extends @OsUntrusted Process> clazz = p.getClass();
      if (clazz.getName().equals("java.lang.UNIXProcess")) {
        @OsUntrusted
        Field f = clazz.getDeclaredField("pid");
        f.setAccessible(true);
        return String.valueOf(f.getInt(p));
      } else {
        LOG.trace("Unable to determine pid for " + p
            + " since it is not a UNIXProcess");
        return null;
      }
    } catch (@OsUntrusted Throwable t) {
      LOG.trace("Unable to determine pid for " + p, t);
      return null;
    }
  }

  /**
   * Set the environment of the subprocess to be the Configuration,
   * with '.'s replaced by '_'s.
   */
  private void setConfAsEnvVars(@OsUntrusted ShellCommandFencer this, @OsUntrusted Map<@OsUntrusted String, @OsTrusted String> env) {
    for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsTrusted String> pair : getConf()) {
      env.put(pair.getKey().replace('.', '_'), pair.getValue());
    }
  }

  /**
   * Add information about the target to the the environment of the
   * subprocess.
   * 
   * @param target
   * @param environment
   */
  private void addTargetInfoAsEnvVars(@OsUntrusted ShellCommandFencer this, @OsUntrusted HAServiceTarget target,
      @OsUntrusted
      Map<@OsUntrusted String, @OsTrusted String> environment) {
    for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted String> e :
         target.getFencingParameters().entrySet()) {
      @OsUntrusted
      String key = TARGET_PREFIX + e.getKey();
      key = key.replace('.', '_');
      @SuppressWarnings("ostrusted:cast.unsafe") // Writing to keys that cant interfere with system environment variables.
      @OsTrusted String trustedValue = (@OsTrusted String) e.getValue();
      environment.put(key, trustedValue);
    }
  }
}
