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
package org.apache.hadoop.fs.permission;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Store permission related information.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class PermissionStatus implements @OsUntrusted Writable {
  static final @OsUntrusted WritableFactory FACTORY = new @OsUntrusted WritableFactory() {
    @Override
    public @OsUntrusted Writable newInstance() { return new @OsUntrusted PermissionStatus(); }
  };
  static {                                      // register a ctor
    WritableFactories.setFactory(PermissionStatus.class, FACTORY);
  }

  /** Create an immutable {@link PermissionStatus} object. */
  public static @OsUntrusted PermissionStatus createImmutable(
      @OsUntrusted
      String user, @OsUntrusted String group, @OsUntrusted FsPermission permission) {
    return new @OsUntrusted PermissionStatus(user, group, permission) {
      @Override
      public @OsUntrusted PermissionStatus applyUMask(@OsUntrusted FsPermission umask) {
        throw new @OsUntrusted UnsupportedOperationException();
      }
      @Override
      public void readFields(@OsUntrusted DataInput in) throws IOException {
        throw new @OsUntrusted UnsupportedOperationException();
      }
    };
  }

  private @OsUntrusted String username;
  private @OsUntrusted String groupname;
  private @OsUntrusted FsPermission permission;

  private @OsUntrusted PermissionStatus() {}

  /** Constructor */
  public @OsUntrusted PermissionStatus(@OsUntrusted String user, @OsUntrusted String group, @OsUntrusted FsPermission permission) {
    username = user;
    groupname = group;
    this.permission = permission;
  }

  /** Return user name */
  public @OsUntrusted String getUserName(@OsUntrusted PermissionStatus this) {return username;}

  /** Return group name */
  public @OsUntrusted String getGroupName(@OsUntrusted PermissionStatus this) {return groupname;}

  /** Return permission */
  public @OsUntrusted FsPermission getPermission(@OsUntrusted PermissionStatus this) {return permission;}

  /**
   * Apply umask.
   * @see FsPermission#applyUMask(FsPermission)
   */
  public @OsUntrusted PermissionStatus applyUMask(@OsUntrusted PermissionStatus this, @OsUntrusted FsPermission umask) {
    permission = permission.applyUMask(umask);
    return this;
  }

  @Override
  public void readFields(@OsUntrusted PermissionStatus this, @OsUntrusted DataInput in) throws IOException {
    username = Text.readString(in, Text.DEFAULT_MAX_LEN);
    groupname = Text.readString(in, Text.DEFAULT_MAX_LEN);
    permission = FsPermission.read(in);
  }

  @Override
  public void write(@OsUntrusted PermissionStatus this, @OsUntrusted DataOutput out) throws IOException {
    write(out, username, groupname, permission);
  }

  /**
   * Create and initialize a {@link PermissionStatus} from {@link DataInput}.
   */
  public static @OsUntrusted PermissionStatus read(@OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    PermissionStatus p = new @OsUntrusted PermissionStatus();
    p.readFields(in);
    return p;
  }

  /**
   * Serialize a {@link PermissionStatus} from its base components.
   */
  public static void write(@OsUntrusted DataOutput out,
                           @OsUntrusted
                           String username, 
                           @OsUntrusted
                           String groupname,
                           @OsUntrusted
                           FsPermission permission) throws IOException {
    Text.writeString(out, username, Text.DEFAULT_MAX_LEN);
    Text.writeString(out, groupname, Text.DEFAULT_MAX_LEN);
    permission.write(out);
  }

  @Override
  public @OsUntrusted String toString(@OsUntrusted PermissionStatus this) {
    return username + ":" + groupname + ":" + permission;
  }
}
