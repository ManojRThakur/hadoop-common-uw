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
package org.apache.hadoop.security;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.io.IOException;
import java.security.Principal;

import javax.security.auth.login.LoginContext;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

/**
 * Save the full and short name of the user as a principal. This allows us to
 * have a single type that we always look for when picking up user names.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
class User implements @OsUntrusted Principal {
  private final @OsTrusted String fullName;
  private final @OsUntrusted String shortName;
  private volatile @OsUntrusted AuthenticationMethod authMethod = null;
  private volatile @OsUntrusted LoginContext login = null;
  private volatile @OsUntrusted long lastLogin = 0;

  public @OsUntrusted User(@OsTrusted String name) {
    this(name, null, null);
  }
  
  public @OsUntrusted User(@OsTrusted String name, @OsUntrusted AuthenticationMethod authMethod, @OsUntrusted LoginContext login) {
    try {
      shortName = new @OsUntrusted HadoopKerberosName(name).getShortName();
    } catch (@OsUntrusted IOException ioe) {
      throw new @OsUntrusted IllegalArgumentException("Illegal principal name " + name, ioe);
    }
    fullName = name;

    this.authMethod = authMethod;
    this.login = login;
  }

  /**
   * Get the full name of the user.
   */
  @Override
  public @OsTrusted String getName(@OsUntrusted User this) {
    return fullName;
  }
  
  /**
   * Get the user name up to the first '/' or '@'
   * @return the leading part of the user name
   */
  public @OsUntrusted String getShortName(@OsUntrusted User this) {
    return shortName;
  }
  
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted User this, @OsUntrusted Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    } else {
      return ((fullName.equals(((@OsUntrusted User) o).fullName)) && (authMethod == ((@OsUntrusted User) o).authMethod));
    }
  }
  
  @Override
  public @OsUntrusted int hashCode(@OsUntrusted User this) {
    return fullName.hashCode();
  }
  
  @Override
  public @OsUntrusted String toString(@OsUntrusted User this) {
    return fullName;
  }

  public void setAuthenticationMethod(@OsUntrusted User this, @OsUntrusted AuthenticationMethod authMethod) {
    this.authMethod = authMethod;
  }

  public @OsUntrusted AuthenticationMethod getAuthenticationMethod(@OsUntrusted User this) {
    return authMethod;
  }
  
  /**
   * Returns login object
   * @return login
   */
  public @OsUntrusted LoginContext getLogin(@OsUntrusted User this) {
    return login;
  }
  
  /**
   * Set the login object
   * @param login
   */
  public void setLogin(@OsUntrusted User this, @OsUntrusted LoginContext login) {
    this.login = login;
  }
  
  /**
   * Set the last login time.
   * @param time the number of milliseconds since the beginning of time
   */
  public void setLastLogin(@OsUntrusted User this, @OsUntrusted long time) {
    lastLogin = time;
  }
  
  /**
   * Get the time of the last login.
   * @return the number of milliseconds since the beginning of time.
   */
  public @OsUntrusted long getLastLogin(@OsUntrusted User this) {
    return lastLogin;
  }
}
