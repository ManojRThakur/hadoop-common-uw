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
package org.apache.hadoop.io;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** Encapsulate a list of {@link IOException} into an {@link IOException} */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MultipleIOException extends @OsUntrusted IOException {
  /** Require by {@link java.io.Serializable} */
  private static final @OsUntrusted long serialVersionUID = 1L;
  
  private final @OsUntrusted List<@OsUntrusted IOException> exceptions;
  
  /** Constructor is private, use {@link #createIOException(List)}. */
  private @OsUntrusted MultipleIOException(@OsUntrusted List<@OsUntrusted IOException> exceptions) {
    super(exceptions.size() + " exceptions " + exceptions);
    this.exceptions = exceptions;
  }

  /** @return the underlying exceptions */
  public @OsUntrusted List<@OsUntrusted IOException> getExceptions(@OsUntrusted MultipleIOException this) {return exceptions;}

  /** A convenient method to create an {@link IOException}. */
  public static @OsUntrusted IOException createIOException(@OsUntrusted List<@OsUntrusted IOException> exceptions) {
    if (exceptions == null || exceptions.isEmpty()) {
      return null;
    }
    if (exceptions.size() == 1) {
      return exceptions.get(0);
    }
    return new @OsUntrusted MultipleIOException(exceptions);
  }
}