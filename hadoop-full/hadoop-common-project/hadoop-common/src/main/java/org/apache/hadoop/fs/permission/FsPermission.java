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
import ostrusted.quals.OsTrusted;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * A class for file/directory permissions.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FsPermission implements @OsUntrusted Writable {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(FsPermission.class);

  static final @OsUntrusted WritableFactory FACTORY = new @OsUntrusted WritableFactory() {
    @Override
    public @OsUntrusted Writable newInstance() { return new @OsUntrusted FsPermission(); }
  };
  static {                                      // register a ctor
    WritableFactories.setFactory(FsPermission.class, FACTORY);
    WritableFactories.setFactory(ImmutableFsPermission.class, FACTORY);
  }

  /** Create an immutable {@link FsPermission} object. */
  public static @OsUntrusted FsPermission createImmutable(@OsUntrusted short permission) {
    return new @OsUntrusted ImmutableFsPermission(permission);
  }

  //POSIX permission style
  private @OsUntrusted FsAction useraction = null;
  private @OsUntrusted FsAction groupaction = null;
  private @OsUntrusted FsAction otheraction = null;
  private @OsUntrusted boolean stickyBit = false;

  private @OsUntrusted FsPermission() {}

  /**
   * Construct by the given {@link FsAction}.
   * @param u user action
   * @param g group action
   * @param o other action
   */
  public @OsUntrusted FsPermission(@OsUntrusted FsAction u, @OsUntrusted FsAction g, @OsUntrusted FsAction o) {
    this(u, g, o, false);
  }

  public @OsUntrusted FsPermission(@OsUntrusted FsAction u, @OsUntrusted FsAction g, @OsUntrusted FsAction o, @OsUntrusted boolean sb) {
    set(u, g, o, sb);
  }

  /**
   * Construct by the given mode.
   * @param mode
   * @see #toShort()
   */
  public @OsUntrusted FsPermission(@OsUntrusted short mode) { fromShort(mode); }

  /**
   * Copy constructor
   * 
   * @param other other permission
   */
  public @OsUntrusted FsPermission(@OsUntrusted FsPermission other) {
    this.useraction = other.useraction;
    this.groupaction = other.groupaction;
    this.otheraction = other.otheraction;
    this.stickyBit = other.stickyBit;
  }
  
  /**
   * Construct by given mode, either in octal or symbolic format.
   * @param mode mode as a string, either in octal or symbolic format
   * @throws IllegalArgumentException if <code>mode</code> is invalid
   */
  public @OsUntrusted FsPermission(@OsUntrusted String mode) {
    this(new @OsUntrusted UmaskParser(mode).getUMask());
  }

  /** Return user {@link FsAction}. */
  public @OsUntrusted FsAction getUserAction(@OsUntrusted FsPermission this) {return useraction;}

  /** Return group {@link FsAction}. */
  public @OsUntrusted FsAction getGroupAction(@OsUntrusted FsPermission this) {return groupaction;}

  /** Return other {@link FsAction}. */
  public @OsUntrusted FsAction getOtherAction(@OsUntrusted FsPermission this) {return otheraction;}

  private void set(@OsUntrusted FsPermission this, @OsUntrusted FsAction u, @OsUntrusted FsAction g, @OsUntrusted FsAction o, @OsUntrusted boolean sb) {
    useraction = u;
    groupaction = g;
    otheraction = o;
    stickyBit = sb;
  }

  public void fromShort(@OsUntrusted FsPermission this, @OsUntrusted short n) {
    @OsUntrusted
    FsAction @OsUntrusted [] v = FsAction.values();

    set(v[(n >>> 6) & 7], v[(n >>> 3) & 7], v[n & 7], (((n >>> 9) & 1) == 1) );
  }

  @Override
  public void write(@OsUntrusted FsPermission this, @OsUntrusted DataOutput out) throws IOException {
    out.writeShort(toShort());
  }

  @Override
  public void readFields(@OsUntrusted FsPermission this, @OsUntrusted DataInput in) throws IOException {
    fromShort(in.readShort());
  }

  /**
   * Create and initialize a {@link FsPermission} from {@link DataInput}.
   */
  public static @OsUntrusted FsPermission read(@OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    FsPermission p = new @OsUntrusted FsPermission();
    p.readFields(in);
    return p;
  }

  /**
   * Encode the object to a short.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  //Short values are considered trusted
  public @OsTrusted short toShort(@OsUntrusted FsPermission this) {
    @OsUntrusted
    int s =  (stickyBit ? 1 << 9 : 0)     |
             (useraction.ordinal() << 6)  |
             (groupaction.ordinal() << 3) |
             otheraction.ordinal();

    return (@OsTrusted short)s;
  }

  @Override
  public @OsUntrusted boolean equals(@OsUntrusted FsPermission this, @OsUntrusted Object obj) {
    if (obj instanceof @OsUntrusted FsPermission) {
      @OsUntrusted
      FsPermission that = (@OsUntrusted FsPermission)obj;
      return this.useraction == that.useraction
          && this.groupaction == that.groupaction
          && this.otheraction == that.otheraction
          && this.stickyBit == that.stickyBit;
    }
    return false;
  }

  @Override
  public @OsUntrusted int hashCode(@OsUntrusted FsPermission this) {return toShort();}

  @Override
  public @OsUntrusted String toString(@OsUntrusted FsPermission this) {
    @OsUntrusted
    String str = useraction.SYMBOL + groupaction.SYMBOL + otheraction.SYMBOL;
    if(stickyBit) {
      @OsUntrusted
      StringBuilder str2 = new @OsUntrusted StringBuilder(str);
      str2.replace(str2.length() - 1, str2.length(),
           otheraction.implies(FsAction.EXECUTE) ? "t" : "T");
      str = str2.toString();
    }

    return str;
  }

  /**
   * Apply a umask to this permission and return a new one.
   *
   * The umask is used by create, mkdir, and other Hadoop filesystem operations.
   * The mode argument for these operations is modified by removing the bits
   * which are set in the umask.  Thus, the umask limits the permissions which
   * newly created files and directories get.
   *
   * @param umask              The umask to use
   * 
   * @return                   The effective permission
   */
  public @OsUntrusted FsPermission applyUMask(@OsUntrusted FsPermission this, @OsUntrusted FsPermission umask) {
    return new @OsUntrusted FsPermission(useraction.and(umask.useraction.not()),
        groupaction.and(umask.groupaction.not()),
        otheraction.and(umask.otheraction.not()));
  }

  /** umask property label deprecated key and code in getUMask method
   *  to accommodate it may be removed in version .23 */
  public static final @OsUntrusted String DEPRECATED_UMASK_LABEL = "dfs.umask"; 
  public static final @OsUntrusted String UMASK_LABEL = 
                  CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY;
  public static final @OsUntrusted int DEFAULT_UMASK = 
                  CommonConfigurationKeys.FS_PERMISSIONS_UMASK_DEFAULT;

  /** 
   * Get the user file creation mask (umask)
   * 
   * {@code UMASK_LABEL} config param has umask value that is either symbolic 
   * or octal.
   * 
   * Symbolic umask is applied relative to file mode creation mask; 
   * the permission op characters '+' clears the corresponding bit in the mask, 
   * '-' sets bits in the mask.
   * 
   * Octal umask, the specified bits are set in the file mode creation mask.
   * 
   * {@code DEPRECATED_UMASK_LABEL} config param has umask value set to decimal.
   */
  public static @OsUntrusted FsPermission getUMask(@OsUntrusted Configuration conf) {
    @OsUntrusted
    int umask = DEFAULT_UMASK;
    
    // To ensure backward compatibility first use the deprecated key.
    // If the deprecated key is not present then check for the new key
    if(conf != null) {
      @OsUntrusted
      String confUmask = conf.get(UMASK_LABEL);
      @OsUntrusted
      int oldUmask = conf.getInt(DEPRECATED_UMASK_LABEL, Integer.MIN_VALUE);
      try {
        if(confUmask != null) {
          umask = new @OsUntrusted UmaskParser(confUmask).getUMask();
        }
      } catch(@OsUntrusted IllegalArgumentException iae) {
        // Provide more explanation for user-facing message
        @OsUntrusted
        String type = iae instanceof @OsUntrusted NumberFormatException ? "decimal"
            : "octal or symbolic";
        @OsUntrusted
        String error = "Unable to parse configuration " + UMASK_LABEL
            + " with value " + confUmask + " as " + type + " umask.";
        LOG.warn(error);
        
        // If oldUmask is not set, then throw the exception
        if (oldUmask == Integer.MIN_VALUE) {
          throw new @OsUntrusted IllegalArgumentException(error);
        }
      }
        
      if(oldUmask != Integer.MIN_VALUE) { // Property was set with old key
        if (umask != oldUmask) {
          LOG.warn(DEPRECATED_UMASK_LABEL
              + " configuration key is deprecated. " + "Convert to "
              + UMASK_LABEL + ", using octal or symbolic umask "
              + "specifications.");
          // Old and new umask values do not match - Use old umask
          umask = oldUmask;
        }
      }
    }
    
    return new @OsUntrusted FsPermission((@OsUntrusted short)umask);
  }

  public @OsUntrusted boolean getStickyBit(@OsUntrusted FsPermission this) {
    return stickyBit;
  }

  /** Set the user file creation mask (umask) */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static void setUMask(@OsUntrusted Configuration conf, @OsUntrusted FsPermission umask) {
    //Integer/short strings are considered trusted
    conf.set(UMASK_LABEL, (@OsTrusted String) String.format("%1$03o", umask.toShort()));
    conf.setInt(DEPRECATED_UMASK_LABEL, (@OsTrusted Short) umask.toShort());
  }

  /**
   * Get the default permission for directory and symlink.
   * In previous versions, this default permission was also used to
   * create files, so files created end up with ugo+x permission.
   * See HADOOP-9155 for detail. 
   * Two new methods are added to solve this, please use 
   * {@link FsPermission#getDirDefault()} for directory, and use
   * {@link FsPermission#getFileDefault()} for file.
   * This method is kept for compatibility.
   */
  public static @OsUntrusted FsPermission getDefault() {
    return new @OsUntrusted FsPermission((@OsUntrusted short)00777);
  }

  /**
   * Get the default permission for directory.
   */
  public static @OsUntrusted FsPermission getDirDefault() {
    return new @OsUntrusted FsPermission((@OsUntrusted short)00777);
  }

  /**
   * Get the default permission for file.
   */
  public static @OsUntrusted FsPermission getFileDefault() {
    return new @OsUntrusted FsPermission((@OsUntrusted short)00666);
  }

  /**
   * Create a FsPermission from a Unix symbolic permission string
   * @param unixSymbolicPermission e.g. "-rw-rw-rw-"
   */
  public static @OsUntrusted FsPermission valueOf(@OsUntrusted String unixSymbolicPermission) {
    if (unixSymbolicPermission == null) {
      return null;
    }
    else if (unixSymbolicPermission.length() != 10) {
      throw new @OsUntrusted IllegalArgumentException("length != 10(unixSymbolicPermission="
          + unixSymbolicPermission + ")");
    }

    @OsTrusted
    int n = 0;
    for(@OsUntrusted int i = 1; i < unixSymbolicPermission.length(); i++) {
      n = n << 1;
      @OsUntrusted
      char c = unixSymbolicPermission.charAt(i);
      n += (c == '-' || c == 'T' || c == 'S') ? 0: 1;
    }

    // Add sticky bit value if set
    if(unixSymbolicPermission.charAt(9) == 't' ||
        unixSymbolicPermission.charAt(9) == 'T')
      n += 01000;

    return new @OsUntrusted FsPermission((@OsUntrusted short)n);
  }
  
  private static class ImmutableFsPermission extends @OsUntrusted FsPermission {
    public @OsUntrusted ImmutableFsPermission(@OsUntrusted short permission) {
      super(permission);
    }
    @Override
    public @OsUntrusted FsPermission applyUMask(FsPermission.@OsUntrusted ImmutableFsPermission this, @OsUntrusted FsPermission umask) {
      throw new @OsUntrusted UnsupportedOperationException();
    }
    @Override
    public void readFields(FsPermission.@OsUntrusted ImmutableFsPermission this, @OsUntrusted DataInput in) throws IOException {
      throw new @OsUntrusted UnsupportedOperationException();
    }    
  }
}
