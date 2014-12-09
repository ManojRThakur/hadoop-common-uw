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

package org.apache.hadoop.metrics2.util;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Additional helpers (besides guava Preconditions) for programming by contract
 */
@InterfaceAudience.Private
public class Contracts {

  private @OsUntrusted Contracts() {}

  /**
   * Check an argument for false conditions
   * @param <T> type of the argument
   * @param arg the argument to check
   * @param expression  the boolean expression for the condition
   * @param msg the error message if {@code expression} is false
   * @return the argument for convenience
   */
  public static <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted T checkArg(@OsUntrusted T arg, @OsUntrusted boolean expression, @OsUntrusted Object msg) {
    if (!expression) {
      throw new @OsUntrusted IllegalArgumentException(String.valueOf(msg) +": "+ arg);
    }
    return arg;
  }

  /**
   * Check an argument for false conditions
   * @param arg the argument to check
   * @param expression  the boolean expression for the condition
   * @param msg the error message if {@code expression} is false
   * @return the argument for convenience
   */
  public static @OsUntrusted int checkArg(@OsUntrusted int arg, @OsUntrusted boolean expression, @OsUntrusted Object msg) {
    if (!expression) {
      throw new @OsUntrusted IllegalArgumentException(String.valueOf(msg) +": "+ arg);
    }
    return arg;
  }

  /**
   * Check an argument for false conditions
   * @param arg the argument to check
   * @param expression  the boolean expression for the condition
   * @param msg the error message if {@code expression} is false
   * @return the argument for convenience
   */
  public static @OsUntrusted long checkArg(@OsUntrusted long arg, @OsUntrusted boolean expression, @OsUntrusted Object msg) {
    if (!expression) {
      throw new @OsUntrusted IllegalArgumentException(String.valueOf(msg) +": "+ arg);
    }
    return arg;
  }

  /**
   * Check an argument for false conditions
   * @param arg the argument to check
   * @param expression  the boolean expression for the condition
   * @param msg the error message if {@code expression} is false
   * @return the argument for convenience
   */
  public static @OsUntrusted float checkArg(@OsUntrusted float arg, @OsUntrusted boolean expression, @OsUntrusted Object msg) {
    if (!expression) {
      throw new @OsUntrusted IllegalArgumentException(String.valueOf(msg) +": "+ arg);
    }
    return arg;
  }

  /**
   * Check an argument for false conditions
   * @param arg the argument to check
   * @param expression  the boolean expression for the condition
   * @param msg the error message if {@code expression} is false
   * @return the argument for convenience
   */
  public static @OsUntrusted double checkArg(@OsUntrusted double arg, @OsUntrusted boolean expression, @OsUntrusted Object msg) {
    if (!expression) {
      throw new @OsUntrusted IllegalArgumentException(String.valueOf(msg) +": "+ arg);
    }
    return arg;
  }
}
