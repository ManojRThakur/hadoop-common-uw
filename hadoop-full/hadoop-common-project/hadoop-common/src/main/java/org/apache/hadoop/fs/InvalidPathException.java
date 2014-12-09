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
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Path string is invalid either because it has invalid characters or due to
 * other file system specific reasons.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InvalidPathException extends @OsUntrusted HadoopIllegalArgumentException {
  private static final @OsUntrusted long serialVersionUID = 1L;

  /**
   * Constructs exception with the specified detail message.
   * 
   * @param path invalid path.
   */
  public @OsUntrusted InvalidPathException(final @OsUntrusted String path) {
    super("Invalid path name " + path);
  }

  /**
   * Constructs exception with the specified detail message.
   * 
   * @param path invalid path.
   * @param reason Reason <code>path</code> is invalid
   */
  public @OsUntrusted InvalidPathException(final @OsUntrusted String path, final @OsUntrusted String reason) {
    super("Invalid path " + path
        + (reason == null ? "" : ". (" + reason + ")"));
  }
}
