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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parse the args of a command and check the format of args.
 */
public class CommandFormat {
  final @OsUntrusted int minPar;
  final @OsUntrusted int maxPar;
  final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Boolean> options = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted Boolean>();
  @OsUntrusted
  boolean ignoreUnknownOpts = false;
  
  /**
   * @deprecated use replacement since name is an unused parameter
   * @param name of command, but never used
   * @param min see replacement
   * @param max see replacement
   * @param possibleOpt see replacement
   * @see #CommandFormat(int, int, String...)
   */
  @Deprecated
  public @OsUntrusted CommandFormat(@OsUntrusted String n, @OsUntrusted int min, @OsUntrusted int max, @OsUntrusted String @OsUntrusted ... possibleOpt) {
    this(min, max, possibleOpt);
  }
  
  /**
   * Simple parsing of command line arguments
   * @param min minimum arguments required
   * @param max maximum arguments permitted
   * @param possibleOpt list of the allowed switches
   */
  public @OsUntrusted CommandFormat(@OsUntrusted int min, @OsUntrusted int max, @OsUntrusted String @OsUntrusted ... possibleOpt) {
    minPar = min;
    maxPar = max;
    for (@OsUntrusted String opt : possibleOpt) {
      if (opt == null) {
        ignoreUnknownOpts = true;
      } else {
        options.put(opt, Boolean.FALSE);
      }
    }
  }

  /** Parse parameters starting from the given position
   * Consider using the variant that directly takes a List
   * 
   * @param args an array of input arguments
   * @param pos the position at which starts to parse
   * @return a list of parameters
   */
  public @OsUntrusted List<@OsUntrusted String> parse(@OsUntrusted CommandFormat this, @OsUntrusted String @OsUntrusted [] args, @OsUntrusted int pos) {
    @OsUntrusted
    List<@OsUntrusted String> parameters = new @OsUntrusted ArrayList<@OsUntrusted String>(Arrays.asList(args));
    parameters.subList(0, pos).clear();
    parse(parameters);
    return parameters;
  }

  /** Parse parameters from the given list of args.  The list is
   *  destructively modified to remove the options.
   * 
   * @param args as a list of input arguments
   */
  public void parse(@OsUntrusted CommandFormat this, @OsUntrusted List<@OsUntrusted String> args) {
    @OsUntrusted
    int pos = 0;
    while (pos < args.size()) {
      @OsUntrusted
      String arg = args.get(pos);
      // stop if not an opt, or the stdin arg "-" is found
      if (!arg.startsWith("-") || arg.equals("-")) { 
        break;
      } else if (arg.equals("--")) { // force end of option processing
        args.remove(pos);
        break;
      }
      
      @OsUntrusted
      String opt = arg.substring(1);
      if (options.containsKey(opt)) {
        args.remove(pos);
        options.put(opt, Boolean.TRUE);
      } else if (ignoreUnknownOpts) {
        pos++;
      } else {
        throw new @OsUntrusted UnknownOptionException(arg);
      }
    }
    @OsUntrusted
    int psize = args.size();
    if (psize < minPar) {
      throw new @OsUntrusted NotEnoughArgumentsException(minPar, psize);
    }
    if (psize > maxPar) {
      throw new @OsUntrusted TooManyArgumentsException(maxPar, psize);
    }
  }
  
  /** Return if the option is set or not
   * 
   * @param option String representation of an option
   * @return true is the option is set; false otherwise
   */
  public @OsUntrusted boolean getOpt(@OsUntrusted CommandFormat this, @OsUntrusted String option) {
    return options.containsKey(option) ? options.get(option) : false;
  }
  
  /** Returns all the options that are set
   * 
   * @return Set<String> of the enabled options
   */
  public @OsUntrusted Set<@OsUntrusted String> getOpts(@OsUntrusted CommandFormat this) {
    @OsUntrusted
    Set<@OsUntrusted String> optSet = new @OsUntrusted HashSet<@OsUntrusted String>();
    for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted Boolean> entry : options.entrySet()) {
      if (entry.getValue()) {
        optSet.add(entry.getKey());
      }
    }
    return optSet;
  }
  
  /** Used when the arguments exceed their bounds 
   */
  public static abstract class IllegalNumberOfArgumentsException
  extends @OsUntrusted IllegalArgumentException {
    private static final @OsUntrusted long serialVersionUID = 0L;
    protected @OsUntrusted int expected;
    protected @OsUntrusted int actual;

    protected @OsUntrusted IllegalNumberOfArgumentsException(@OsUntrusted int want, @OsUntrusted int got) {
      expected = want;
      actual = got;
    }

    @Override
    public @OsUntrusted String getMessage(CommandFormat.@OsUntrusted IllegalNumberOfArgumentsException this) {
      return "expected " + expected + " but got " + actual;
    }
  }

  /** Used when too many arguments are supplied to a command
   */
  public static class TooManyArgumentsException
  extends @OsUntrusted IllegalNumberOfArgumentsException {
    private static final @OsUntrusted long serialVersionUID = 0L;

    public @OsUntrusted TooManyArgumentsException(@OsUntrusted int expected, @OsUntrusted int actual) {
      super(expected, actual);
    }

    @Override
    public @OsUntrusted String getMessage(CommandFormat.@OsUntrusted TooManyArgumentsException this) {
      return "Too many arguments: " + super.getMessage();
    }
  }
  
  /** Used when too few arguments are supplied to a command
   */
  public static class NotEnoughArgumentsException
  extends @OsUntrusted IllegalNumberOfArgumentsException {
    private static final @OsUntrusted long serialVersionUID = 0L;

    public @OsUntrusted NotEnoughArgumentsException(@OsUntrusted int expected, @OsUntrusted int actual) {
      super(expected, actual);
    }

    @Override
    public @OsUntrusted String getMessage(CommandFormat.@OsUntrusted NotEnoughArgumentsException this) {
      return "Not enough arguments: " + super.getMessage();
    }
  }
  
  /** Used when an unsupported option is supplied to a command
   */
  public static class UnknownOptionException extends @OsUntrusted IllegalArgumentException {
    private static final @OsUntrusted long serialVersionUID = 0L;
    protected @OsUntrusted String option = null;
    
    public @OsUntrusted UnknownOptionException(@OsUntrusted String unknownOption) {
      super("Illegal option " + unknownOption);
      option = unknownOption;
    }
    
    public @OsUntrusted String getOption(CommandFormat.@OsUntrusted UnknownOptionException this) {
      return option;
    }
  }
}
