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
package org.apache.hadoop.io.nativeio;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import org.apache.hadoop.util.Shell;


/**
 * An exception generated by a call to the native IO code.
 *
 * These exceptions simply wrap <i>errno</i> result codes on Linux,
 * or the System Error Code on Windows.
 */
public class NativeIOException extends @OsUntrusted IOException {
  private static final @OsUntrusted long serialVersionUID = 1L;

  private @OsUntrusted Errno errno;

  // Java has no unsigned primitive error code. Use a signed 32-bit
  // integer to hold the unsigned 32-bit integer.
  private @OsUntrusted int errorCode;

  public @OsUntrusted NativeIOException(@OsUntrusted String msg, @OsUntrusted Errno errno) {
    super(msg);
    this.errno = errno;
    // Windows error code is always set to ERROR_SUCCESS on Linux,
    // i.e. no failure on Windows
    this.errorCode = 0;
  }

  public @OsUntrusted NativeIOException(@OsUntrusted String msg, @OsUntrusted int errorCode) {
    super(msg);
    this.errorCode = errorCode;
    this.errno = Errno.UNKNOWN;
  }

  public @OsUntrusted long getErrorCode(@OsUntrusted NativeIOException this) {
    return errorCode;
  }

  public @OsUntrusted Errno getErrno(@OsUntrusted NativeIOException this) {
    return errno;
  }

  @Override
  public @OsUntrusted String toString(@OsUntrusted NativeIOException this) {
    if (Shell.WINDOWS)
      return errorCode + ": " + super.getMessage();
    else
      return errno.toString() + ": " + super.getMessage();
  }
}

