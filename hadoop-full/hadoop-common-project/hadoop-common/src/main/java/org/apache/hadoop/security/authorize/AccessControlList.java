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
package org.apache.hadoop.security.authorize;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import ostrusted.quals.PolyOsTrusted;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;
import java.util.ListIterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.conf.Configuration;

/**
 * Class representing a configured access control list.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class AccessControlList implements @OsUntrusted Writable {

  static {                                      // register a ctor
    WritableFactories.setFactory
    (AccessControlList.class,
      new @OsUntrusted WritableFactory() {
        @Override
        public @OsUntrusted Writable newInstance() { return new @OsUntrusted AccessControlList(); }
      });
  }

  // Indicates an ACL string that represents access to all users
  public static final @OsTrusted String WILDCARD_ACL_VALUE = "*";
  private static final @OsUntrusted int INITIAL_CAPACITY = 256;

  // Set of users who are granted access.
  private @OsUntrusted Set<@OsTrusted String> users;
  // Set of groups which are granted access
  private @OsUntrusted Set<@OsTrusted String> groups;
  // Whether all users are granted access.
  private @OsUntrusted boolean allAllowed;

  private @OsUntrusted Groups groupsMapping = Groups.getUserToGroupsMappingService(new @OsUntrusted Configuration());

  /**
   * This constructor exists primarily for AccessControlList to be Writable.
   */
  public @OsUntrusted AccessControlList() {
  }

  /**
   * Construct a new ACL from a String representation of the same.
   * 
   * The String is a a comma separated list of users and groups.
   * The user list comes first and is separated by a space followed 
   * by the group list. For e.g. "user1,user2 group1,group2"
   * 
   * @param aclString String representation of the ACL
   */
  public @OsUntrusted AccessControlList(@OsTrusted String aclString) {
    buildACL(aclString);
  }

  /**
   * Build ACL from the given string, format of the string is
   * user1,...,userN group1,...,groupN
   *
   * @param aclString build ACL from this string
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  private void buildACL(@OsUntrusted AccessControlList this, @OsTrusted String aclString) {
    users = new @OsUntrusted TreeSet<@OsTrusted String>();
    groups = new @OsUntrusted TreeSet<@OsTrusted String>();
    if (isWildCardACLValue(aclString)) {
      allAllowed = true;
    } else {
      allAllowed = false;
      @OsTrusted String @OsUntrusted [] userGroupStrings = (@OsTrusted String @OsUntrusted []) aclString.split(" ", 2);

      if (userGroupStrings.length >= 1) {
        @OsUntrusted
        List<@OsTrusted String> usersList = new @OsUntrusted LinkedList<@OsTrusted String>(
          Arrays.asList(  (@OsTrusted String @OsUntrusted []) userGroupStrings[0].split(",")) );
        cleanupList(usersList);
        addToSet(users, usersList);
      }
      
      if (userGroupStrings.length == 2) {
        @OsUntrusted List<@OsTrusted String> groupsList = new @OsUntrusted LinkedList<@OsTrusted String>(
          Arrays.asList( (@OsTrusted String @OsUntrusted []) userGroupStrings[1].split(",")));
        cleanupList(groupsList);
        addToSet(groups, groupsList);
        groupsMapping.cacheGroupsAdd(groupsList);
      }
    }
  }
  
  /**
   * Checks whether ACL string contains wildcard
   *
   * @param aclString check this ACL string for wildcard
   * @return true if ACL string contains wildcard false otherwise
   */
  private @OsUntrusted boolean isWildCardACLValue(@OsUntrusted AccessControlList this, @OsUntrusted String aclString) {
    if (aclString.contains(WILDCARD_ACL_VALUE) && 
        aclString.trim().equals(WILDCARD_ACL_VALUE)) {
      return true;
    }
    return false;
  }

  public @OsUntrusted boolean isAllAllowed(@OsUntrusted AccessControlList this) {
    return allAllowed;
  }
  
  /**
   * Add user to the names of users allowed for this service.
   * 
   * @param user
   *          The user name
   */
  public void addUser(@OsUntrusted AccessControlList this, @OsTrusted String user) {
    if (isWildCardACLValue(user)) {
      throw new @OsUntrusted IllegalArgumentException("User " + user + " can not be added");
    }
    if (!isAllAllowed()) {
      users.add(user);
    }
  }

  /**
   * Add group to the names of groups allowed for this service.
   * 
   * @param group
   *          The group name
   */
  public void addGroup(@OsUntrusted AccessControlList this, @OsTrusted String group) {
    if (isWildCardACLValue(group)) {
      throw new @OsUntrusted IllegalArgumentException("Group " + group + " can not be added");
    }
    if (!isAllAllowed()) {
      @OsUntrusted
      List<@OsTrusted String> groupsList = new @OsUntrusted LinkedList<@OsTrusted String>();
      groupsList.add(group);
      groupsMapping.cacheGroupsAdd(groupsList);
      groups.add(group);
    }
  }

  /**
   * Remove user from the names of users allowed for this service.
   * 
   * @param user
   *          The user name
   */
  public void removeUser(@OsUntrusted AccessControlList this, @OsUntrusted String user) {
    if (isWildCardACLValue(user)) {
      throw new @OsUntrusted IllegalArgumentException("User " + user + " can not be removed");
    }
    if (!isAllAllowed()) {
      users.remove(user);
    }
  }

  /**
   * Remove group from the names of groups allowed for this service.
   * 
   * @param group
   *          The group name
   */
  public void removeGroup(@OsUntrusted AccessControlList this, @OsUntrusted String group) {
    if (isWildCardACLValue(group)) {
      throw new @OsUntrusted IllegalArgumentException("Group " + group
          + " can not be removed");
    }
    if (!isAllAllowed()) {
      groups.remove(group);
    }
  }

  /**
   * Get the names of users allowed for this service.
   * @return the set of user names. the set must not be modified.
   */
  @OsUntrusted
  Set<@OsTrusted String> getUsers(@OsUntrusted AccessControlList this) {
    return users;
  }
  
  /**
   * Get the names of user groups allowed for this service.
   * @return the set of group names. the set must not be modified.
   */
  @OsUntrusted
  Set<@OsTrusted String> getGroups(@OsUntrusted AccessControlList this) {
    return groups;
  }

  public @OsUntrusted boolean isUserAllowed(@OsUntrusted AccessControlList this, @OsUntrusted UserGroupInformation ugi) {
    if (allAllowed || users.contains(ugi.getShortUserName())) {
      return true;
    } else {
      for(@OsUntrusted String group: ugi.getGroupNames()) {
        if (groups.contains(group)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Cleanup list, remove empty strings, trim leading/trailing spaces
   *
   * @param list clean this list
   */
  @SuppressWarnings("ostrusted:assignment.type.incompatible")
  private static final void cleanupList(@OsUntrusted List<@PolyOsTrusted String> list) {
    @OsUntrusted
    ListIterator<@OsUntrusted String> i = list.listIterator();
    while(i.hasNext()) {
      @OsUntrusted
      String s = i.next();
      if(s.length() == 0) {
        i.remove();
      } else {
        s = s.trim();
        i.set(s);
      }
    }
  }

  /**
   * Add list to a set
   *
   * @param set add list to this set
   * @param list add items of this list to the set
   */
  @SuppressWarnings("ostrusted:arguments.type.incompatible")
  private static final void addToSet(@OsUntrusted Set<@PolyOsTrusted String> set, @OsUntrusted List<@PolyOsTrusted String> list) {
    for(@OsUntrusted String s : list) {
      set.add(s);
    }
  }

  /**
   * Returns descriptive way of users and groups that are part of this ACL.
   * Use {@link #getAclString()} to get the exact String that can be given to
   * the constructor of AccessControlList to create a new instance.
   */
  @Override
  public @OsUntrusted String toString(@OsUntrusted AccessControlList this) {
    @OsUntrusted
    String str = null;

    if (allAllowed) {
      str = "All users are allowed";
    }
    else if (users.isEmpty() && groups.isEmpty()) {
      str = "No users are allowed";
    }
    else {
      @OsUntrusted
      String usersStr = null;
      @OsUntrusted
      String groupsStr = null;
      if (!users.isEmpty()) {
        usersStr = users.toString();
      }
      if (!groups.isEmpty()) {
        groupsStr = groups.toString();
      }

      if (!users.isEmpty() && !groups.isEmpty()) {
        str = "Users " + usersStr + " and members of the groups "
            + groupsStr + " are allowed";
      }
      else if (!users.isEmpty()) {
        str = "Users " + usersStr + " are allowed";
      }
      else {// users is empty array and groups is nonempty
        str = "Members of the groups "
            + groupsStr + " are allowed";
      }
    }

    return str;
  }

  /**
   * Returns the access control list as a String that can be used for building a
   * new instance by sending it to the constructor of {@link AccessControlList}.
   */
  public @OsUntrusted String getAclString(@OsUntrusted AccessControlList this) {
    @OsUntrusted
    StringBuilder sb = new @OsUntrusted StringBuilder(INITIAL_CAPACITY);
    if (allAllowed) {
      sb.append('*');
    }
    else {
      sb.append(getUsersString());
      sb.append(" ");
      sb.append(getGroupsString());
    }
    return sb.toString();
  }

  /**
   * Serializes the AccessControlList object
   */
  @Override
  public void write(@OsUntrusted AccessControlList this, @OsUntrusted DataOutput out) throws IOException {
    @OsUntrusted
    String aclString = getAclString();
    Text.writeString(out, aclString);
  }

  /**
   * Deserializes the AccessControlList object
   */
  @Override
  //ostrusted, todo: check to see if this is ever called from a Writable reference, if so then we might not have a OsTrusted input
  //ostrusted, and we would need to do sanitation
  @SuppressWarnings( {"ostrusted:cast.unsafe", "override.param.invalid"} )
  public void readFields(@OsUntrusted AccessControlList this, @OsTrusted DataInput in) throws IOException {
    @OsTrusted String aclString = (@OsTrusted String) Text.readString(in);
    buildACL(aclString);
  }

  /**
   * Returns comma-separated concatenated single String of the set 'users'
   *
   * @return comma separated list of users
   */
  private @OsTrusted String getUsersString(@OsUntrusted AccessControlList this) {
    return getString(users);
  }

  /**
   * Returns comma-separated concatenated single String of the set 'groups'
   *
   * @return comma separated list of groups
   */
  private @OsTrusted String getGroupsString(@OsUntrusted AccessControlList this) {
    return getString(groups);
  }

  /**
   * Returns comma-separated concatenated single String of all strings of
   * the given set
   *
   * @param strings set of strings to concatenate
   */
  @SuppressWarnings("ostrusted:return.type.incompatible")
  private @PolyOsTrusted String getString(@OsUntrusted AccessControlList this, @OsUntrusted Set<@PolyOsTrusted String> strings) {
    @OsUntrusted
    StringBuilder sb = new @OsUntrusted StringBuilder(INITIAL_CAPACITY);
    @OsUntrusted
    boolean first = true;
    for(@OsUntrusted String str: strings) {
      if (!first) {
        sb.append(",");
      } else {
        first = false;
      }
      sb.append(str);
    }
    return sb.toString();
  }
}
