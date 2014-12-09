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
package org.apache.hadoop.fs;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;

/**
 * Exceptions based on standard posix/linux style exceptions for path related
 * errors. Returns an exception with the format "path: standard error string".
 * 
 * This exception corresponds to Error Input/ouput(EIO)
 */
public class PathIOException extends @OsUntrusted IOException {
  static final @OsUntrusted long serialVersionUID = 0L;
  private static final @OsUntrusted String EIO = "Input/output error";
  // NOTE: this really should be a Path, but a Path is buggy and won't
  // return the exact string used to construct the path, and it mangles
  // uris with no authority
  private @OsUntrusted String operation;
  private @OsUntrusted String path;
  private @OsUntrusted String targetPath;

  /**
   * Constructor a generic I/O error exception
   *  @param path for the exception
   */
  public @OsUntrusted PathIOException(@OsUntrusted String path) {
    this(path, EIO, null);
  }

  /**
   * Appends the text of a Throwable to the default error message
   * @param path for the exception
   * @param cause a throwable to extract the error message
   */
  public @OsUntrusted PathIOException(@OsUntrusted String path, @OsUntrusted Throwable cause) {
    this(path, EIO, cause);
  }

  /**
   * Avoid using this method.  Use a subclass of PathIOException if
   * possible.
   * @param path for the exception
   * @param error custom string to use an the error text
   */
  public @OsUntrusted PathIOException(@OsUntrusted String path, @OsUntrusted String error) {
    this(path, error, null);
  }

  protected @OsUntrusted PathIOException(@OsUntrusted String path, @OsUntrusted String error, @OsUntrusted Throwable cause) {
    super(error, cause);
    this.path = path;
  }

  /** Format:
   * cmd: {operation} `path' {to `target'}: error string
   */
  @Override
  public @OsUntrusted String getMessage(@OsUntrusted PathIOException this) {
    @OsUntrusted
    StringBuilder message = new @OsUntrusted StringBuilder();
    if (operation != null) {
      message.append(operation + " ");
    }
    message.append(formatPath(path));
    if (targetPath != null) {
      message.append(" to " + formatPath(targetPath));
    }
    message.append(": " + super.getMessage());
    if (getCause() != null) {
      message.append(": " + getCause().getMessage());
    }
    return message.toString();
  }

  /** @return Path that generated the exception */
  public @OsUntrusted Path getPath(@OsUntrusted PathIOException this)  { return new @OsUntrusted Path(path); }

  /** @return Path if the operation involved copying or moving, else null */
  public @OsUntrusted Path getTargetPath(@OsUntrusted PathIOException this) {
    return (targetPath != null) ? new @OsUntrusted Path(targetPath) : null;
  }    
  
  /**
   * Optional operation that will preface the path
   * @param operation a string
   */
  public void setOperation(@OsUntrusted PathIOException this, @OsUntrusted String operation) {
    this.operation = operation;
  }
  
  /**
   * Optional path if the exception involved two paths, ex. a copy operation
   * @param targetPath the of the operation
   */
  public void setTargetPath(@OsUntrusted PathIOException this, @OsUntrusted String targetPath) {
    this.targetPath = targetPath;
  }
  
  private @OsUntrusted String formatPath(@OsUntrusted PathIOException this, @OsUntrusted String path) {
    return "`" + path + "'";
  }
}