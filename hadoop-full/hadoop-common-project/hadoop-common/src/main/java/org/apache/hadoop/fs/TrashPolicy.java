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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/** 
 * This interface is used for implementing different Trash policies.
 * Provides factory method to create instances of the configured Trash policy.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TrashPolicy extends @OsUntrusted Configured {
  protected @OsUntrusted FileSystem fs; // the FileSystem
  protected @OsUntrusted Path trash; // path to trash directory
  protected @OsUntrusted long deletionInterval; // deletion interval for Emptier

  /**
   * Used to setup the trash policy. Must be implemented by all TrashPolicy
   * implementations
   * @param conf the configuration to be used
   * @param fs the filesystem to be used
   * @param home the home directory
   */
  public abstract void initialize(@OsUntrusted TrashPolicy this, @OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted Path home);

  /**
   * Returns whether the Trash Policy is enabled for this filesystem
   */
  public abstract @OsUntrusted boolean isEnabled(@OsUntrusted TrashPolicy this);

  /** 
   * Move a file or directory to the current trash directory.
   * @return false if the item is already in the trash or trash is disabled
   */ 
  public abstract @OsUntrusted boolean moveToTrash(@OsUntrusted TrashPolicy this, @OsUntrusted Path path) throws IOException;

  /** 
   * Create a trash checkpoint. 
   */
  public abstract void createCheckpoint(@OsUntrusted TrashPolicy this) throws IOException;

  /** 
   * Delete old trash checkpoint(s).
   */
  public abstract void deleteCheckpoint(@OsUntrusted TrashPolicy this) throws IOException;

  /**
   * Get the current working directory of the Trash Policy
   */
  public abstract @OsUntrusted Path getCurrentTrashDir(@OsUntrusted TrashPolicy this);

  /** 
   * Return a {@link Runnable} that periodically empties the trash of all
   * users, intended to be run by the superuser.
   */
  public abstract @OsUntrusted Runnable getEmptier(@OsUntrusted TrashPolicy this) throws IOException;

  /**
   * Get an instance of the configured TrashPolicy based on the value 
   * of the configuration parameter fs.trash.classname.
   *
   * @param conf the configuration to be used
   * @param fs the file system to be used
   * @param home the home directory
   * @return an instance of TrashPolicy
   */
  public static @OsUntrusted TrashPolicy getInstance(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted Path home) {
    @OsUntrusted
    Class<@OsUntrusted ? extends @OsUntrusted TrashPolicy> trashClass = conf.getClass(
        "fs.trash.classname", TrashPolicyDefault.class, TrashPolicy.class);
    @OsUntrusted
    TrashPolicy trash = ReflectionUtils.newInstance(trashClass, conf);
    trash.initialize(conf, fs, home); // initialize TrashPolicy
    return trash;
  }
}
