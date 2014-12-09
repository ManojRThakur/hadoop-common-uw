/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.util;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

/**
 * This class allows generic access to variable length type-safe parameter
 * lists.
 */
public class Options {

  public static abstract class StringOption {
    private final @OsUntrusted String value;
    protected @OsUntrusted StringOption(@OsUntrusted String value) {
      this.value = value;
    }
    public @OsUntrusted String getValue(Options.@OsUntrusted StringOption this) {
      return value;
    }
  }

  public static abstract class ClassOption {
    private final @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> value;
    protected @OsUntrusted ClassOption(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> value) {
      this.value = value;
    }
    public @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> getValue(Options.@OsUntrusted ClassOption this) {
      return value;
    }
  }

  public static abstract class BooleanOption {
    private final @OsUntrusted boolean value;
    protected @OsUntrusted BooleanOption(@OsUntrusted boolean value) {
      this.value = value;
    }
    public @OsUntrusted boolean getValue(Options.@OsUntrusted BooleanOption this) {
      return value;
    }
  }

  public static abstract class IntegerOption {
    private final @OsUntrusted int value;
    protected @OsUntrusted IntegerOption(@OsUntrusted int value) {
      this.value = value;
    }
    public @OsUntrusted int getValue(Options.@OsUntrusted IntegerOption this) {
      return value;
    }
  }

  public static abstract class LongOption {
    private final @OsUntrusted long value;
    protected @OsUntrusted LongOption(@OsUntrusted long value) {
      this.value = value;
    }
    public @OsUntrusted long getValue(Options.@OsUntrusted LongOption this) {
      return value;
    }
  }

  public static abstract class PathOption {
    private final @OsUntrusted Path value;
    protected @OsUntrusted PathOption(@OsUntrusted Path value) {
      this.value = value;
    }
    public @OsUntrusted Path getValue(Options.@OsUntrusted PathOption this) {
      return value;
    }
  }

  public static abstract class FSDataInputStreamOption {
    private final @OsUntrusted FSDataInputStream value;
    protected @OsUntrusted FSDataInputStreamOption(@OsUntrusted FSDataInputStream value) {
      this.value = value;
    }
    public @OsUntrusted FSDataInputStream getValue(Options.@OsUntrusted FSDataInputStreamOption this) {
      return value;
    }
  }

  public static abstract class FSDataOutputStreamOption {
    private final @OsUntrusted FSDataOutputStream value;
    protected @OsUntrusted FSDataOutputStreamOption(@OsUntrusted FSDataOutputStream value) {
      this.value = value;
    }
    public @OsUntrusted FSDataOutputStream getValue(Options.@OsUntrusted FSDataOutputStreamOption this) {
      return value;
    }
  }

  public static abstract class ProgressableOption {
    private final @OsUntrusted Progressable value;
    protected @OsUntrusted ProgressableOption(@OsUntrusted Progressable value) {
      this.value = value;
    }
    public @OsUntrusted Progressable getValue(Options.@OsUntrusted ProgressableOption this) {
      return value;
    }
  }

  /**
   * Find the first option of the required class.
   * @param <T> the static class to find
   * @param <base> the parent class of the array
   * @param cls the dynamic class to find
   * @param opts the list of options to look through
   * @return the first option that matches
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static <@OsUntrusted base extends java.lang.@OsUntrusted Object, @OsUntrusted T extends @OsUntrusted base> @OsUntrusted T getOption(@OsUntrusted Class<@OsUntrusted T> cls, @OsUntrusted base @OsUntrusted [] opts
                                                   ) throws IOException {
    for(@OsUntrusted base o: opts) {
      if (o.getClass() == cls) {
        return (@OsUntrusted T) o;
      }
    }
    return null;
  }

  /**
   * Prepend some new options to the old options
   * @param <T> the type of options
   * @param oldOpts the old options
   * @param newOpts the new options
   * @return a new array of options
   */
  public static <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted T @OsUntrusted [] prependOptions(@OsUntrusted T @OsUntrusted [] oldOpts, @OsUntrusted T @OsUntrusted ... newOpts) {
    // copy the new options to the front of the array
    @OsUntrusted
    T @OsUntrusted [] result = Arrays.copyOf(newOpts, newOpts.length+oldOpts.length);
    // now copy the old options
    System.arraycopy(oldOpts, 0, result, newOpts.length, oldOpts.length);
    return result;
  }
}
