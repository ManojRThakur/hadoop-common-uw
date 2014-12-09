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
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.SecureIOUtils.AlreadyExistsException;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.Shell;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * JNI wrappers for various native IO-related calls not available in Java.
 * These functions should generally be used alongside a fallback to another
 * more portable mechanism.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NativeIO {
  public static class POSIX {
    // Flags for open() call from bits/fcntl.h
    public static final @OsUntrusted int O_RDONLY   =    00;
    public static final @OsUntrusted int O_WRONLY   =    01;
    public static final @OsUntrusted int O_RDWR     =    02;
    public static final @OsUntrusted int O_CREAT    =  0100;
    public static final @OsUntrusted int O_EXCL     =  0200;
    public static final @OsUntrusted int O_NOCTTY   =  0400;
    public static final @OsUntrusted int O_TRUNC    = 01000;
    public static final @OsUntrusted int O_APPEND   = 02000;
    public static final @OsUntrusted int O_NONBLOCK = 04000;
    public static final @OsUntrusted int O_SYNC   =  010000;
    public static final @OsUntrusted int O_ASYNC  =  020000;
    public static final @OsUntrusted int O_FSYNC = O_SYNC;
    public static final @OsUntrusted int O_NDELAY = O_NONBLOCK;

    // Flags for posix_fadvise() from bits/fcntl.h
    /* No further special treatment.  */
    public static final @OsUntrusted int POSIX_FADV_NORMAL = 0;
    /* Expect random page references.  */
    public static final @OsUntrusted int POSIX_FADV_RANDOM = 1;
    /* Expect sequential page references.  */
    public static final @OsUntrusted int POSIX_FADV_SEQUENTIAL = 2;
    /* Will need these pages.  */
    public static final @OsUntrusted int POSIX_FADV_WILLNEED = 3;
    /* Don't need these pages.  */
    public static final @OsUntrusted int POSIX_FADV_DONTNEED = 4;
    /* Data will be accessed once.  */
    public static final @OsUntrusted int POSIX_FADV_NOREUSE = 5;


    /* Wait upon writeout of all pages
       in the range before performing the
       write.  */
    public static final @OsUntrusted int SYNC_FILE_RANGE_WAIT_BEFORE = 1;
    /* Initiate writeout of all those
       dirty pages in the range which are
       not presently under writeback.  */
    public static final @OsUntrusted int SYNC_FILE_RANGE_WRITE = 2;

    /* Wait upon writeout of all pages in
       the range after performing the
       write.  */
    public static final @OsUntrusted int SYNC_FILE_RANGE_WAIT_AFTER = 4;

    private static final @OsUntrusted Log LOG = LogFactory.getLog(NativeIO.class);

    @VisibleForTesting
    public static @OsUntrusted CacheTracker cacheTracker = null;
    
    private static @OsUntrusted boolean nativeLoaded = false;
    private static @OsUntrusted boolean fadvisePossible = true;
    private static @OsUntrusted boolean syncFileRangePossible = true;

    static final @OsUntrusted String WORKAROUND_NON_THREADSAFE_CALLS_KEY =
      "hadoop.workaround.non.threadsafe.getpwuid";
    static final @OsUntrusted boolean WORKAROUND_NON_THREADSAFE_CALLS_DEFAULT = true;

    private static @OsUntrusted long cacheTimeout = -1;

    public static interface CacheTracker {
      public void fadvise(NativeIO.POSIX.@OsUntrusted CacheTracker this, @OsUntrusted String identifier, @OsUntrusted long offset, @OsUntrusted long len, @OsUntrusted int flags);
    }
    
    static {
      if (NativeCodeLoader.isNativeCodeLoaded()) {
        try {
          @OsUntrusted
          Configuration conf = new @OsUntrusted Configuration();
          workaroundNonThreadSafePasswdCalls = conf.getBoolean(
            WORKAROUND_NON_THREADSAFE_CALLS_KEY,
            WORKAROUND_NON_THREADSAFE_CALLS_DEFAULT);

          initNative();
          nativeLoaded = true;

          cacheTimeout = conf.getLong(
            CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY,
            CommonConfigurationKeys.HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT) *
            1000;
          LOG.debug("Initialized cache for IDs to User/Group mapping with a " +
            " cache timeout of " + cacheTimeout/1000 + " seconds.");

        } catch (@OsUntrusted Throwable t) {
          // This can happen if the user has an older version of libhadoop.so
          // installed - in this case we can continue without native IO
          // after warning
          LOG.error("Unable to initialize NativeIO libraries", t);
        }
      }
    }

    /**
     * Return true if the JNI-based native IO extensions are available.
     */
    public static @OsUntrusted boolean isAvailable() {
      return NativeCodeLoader.isNativeCodeLoaded() && nativeLoaded;
    }

    /** Wrapper around open(2) */
    public static native @OsUntrusted FileDescriptor open(@OsUntrusted String path, @OsUntrusted int flags, @OsUntrusted int mode) throws IOException;
    /** Wrapper around fstat(2) */
    private static native @OsUntrusted Stat fstat(@OsUntrusted FileDescriptor fd) throws IOException;

    /** Native chmod implementation. On UNIX, it is a wrapper around chmod(2) */
    private static native void chmodImpl(@OsUntrusted String path, @OsUntrusted int mode) throws IOException;

    public static void chmod(@OsUntrusted String path, @OsUntrusted int mode) throws IOException {
      if (!Shell.WINDOWS) {
        chmodImpl(path, mode);
      } else {
        try {
          chmodImpl(path, mode);
        } catch (@OsUntrusted NativeIOException nioe) {
          if (nioe.getErrorCode() == 3) {
            throw new @OsUntrusted NativeIOException("No such file or directory",
                Errno.ENOENT);
          } else {
            LOG.warn(String.format("NativeIO.chmod error (%d): %s",
                nioe.getErrorCode(), nioe.getMessage()));
            throw new @OsUntrusted NativeIOException("Unknown error", Errno.UNKNOWN);
          }
        }
      }
    }

    /** Wrapper around posix_fadvise(2) */
    static native void posix_fadvise(
      @OsUntrusted
      FileDescriptor fd, @OsUntrusted long offset, @OsUntrusted long len, @OsUntrusted int flags) throws NativeIOException;

    /** Wrapper around sync_file_range(2) */
    static native void sync_file_range(
      @OsUntrusted
      FileDescriptor fd, @OsUntrusted long offset, @OsUntrusted long nbytes, @OsUntrusted int flags) throws NativeIOException;

    /**
     * Call posix_fadvise on the given file descriptor. See the manpage
     * for this syscall for more information. On systems where this
     * call is not available, does nothing.
     *
     * @throws NativeIOException if there is an error with the syscall
     */
    public static void posixFadviseIfPossible(@OsUntrusted String identifier,
        @OsUntrusted
        FileDescriptor fd, @OsUntrusted long offset, @OsUntrusted long len, @OsUntrusted int flags)
        throws NativeIOException {
      if (cacheTracker != null) {
        cacheTracker.fadvise(identifier, offset, len, flags);
      }
      if (nativeLoaded && fadvisePossible) {
        try {
          posix_fadvise(fd, offset, len, flags);
        } catch (@OsUntrusted UnsupportedOperationException uoe) {
          fadvisePossible = false;
        } catch (@OsUntrusted UnsatisfiedLinkError ule) {
          fadvisePossible = false;
        }
      }
    }

    /**
     * Call sync_file_range on the given file descriptor. See the manpage
     * for this syscall for more information. On systems where this
     * call is not available, does nothing.
     *
     * @throws NativeIOException if there is an error with the syscall
     */
    public static void syncFileRangeIfPossible(
        @OsUntrusted
        FileDescriptor fd, @OsUntrusted long offset, @OsUntrusted long nbytes, @OsUntrusted int flags)
        throws NativeIOException {
      if (nativeLoaded && syncFileRangePossible) {
        try {
          sync_file_range(fd, offset, nbytes, flags);
        } catch (@OsUntrusted UnsupportedOperationException uoe) {
          syncFileRangePossible = false;
        } catch (@OsUntrusted UnsatisfiedLinkError ule) {
          syncFileRangePossible = false;
        }
      }
    }

    /** Linux only methods used for getOwner() implementation */
    private static native @OsUntrusted long getUIDforFDOwnerforOwner(@OsUntrusted FileDescriptor fd) throws IOException;
    private static native @OsUntrusted String getUserName(@OsUntrusted long uid) throws IOException;

    /**
     * Result type of the fstat call
     */
    public static class Stat {
      private @OsUntrusted int ownerId, groupId;
      private @OsUntrusted String owner, group;
      private @OsUntrusted int mode;

      // Mode constants
      public static final @OsUntrusted int S_IFMT = 0170000;      /* type of file */
      public static final @OsUntrusted int   S_IFIFO  = 0010000;  /* named pipe (fifo) */
      public static final @OsUntrusted int   S_IFCHR  = 0020000;  /* character special */
      public static final @OsUntrusted int   S_IFDIR  = 0040000;  /* directory */
      public static final @OsUntrusted int   S_IFBLK  = 0060000;  /* block special */
      public static final @OsUntrusted int   S_IFREG  = 0100000;  /* regular */
      public static final @OsUntrusted int   S_IFLNK  = 0120000;  /* symbolic link */
      public static final @OsUntrusted int   S_IFSOCK = 0140000;  /* socket */
      public static final @OsUntrusted int   S_IFWHT  = 0160000;  /* whiteout */
      public static final @OsUntrusted int S_ISUID = 0004000;  /* set user id on execution */
      public static final @OsUntrusted int S_ISGID = 0002000;  /* set group id on execution */
      public static final @OsUntrusted int S_ISVTX = 0001000;  /* save swapped text even after use */
      public static final @OsUntrusted int S_IRUSR = 0000400;  /* read permission, owner */
      public static final @OsUntrusted int S_IWUSR = 0000200;  /* write permission, owner */
      public static final @OsUntrusted int S_IXUSR = 0000100;  /* execute/search permission, owner */

      @OsUntrusted
      Stat(@OsUntrusted int ownerId, @OsUntrusted int groupId, @OsUntrusted int mode) {
        this.ownerId = ownerId;
        this.groupId = groupId;
        this.mode = mode;
      }
      
      @OsUntrusted
      Stat(@OsUntrusted String owner, @OsUntrusted String group, @OsUntrusted int mode) {
        if (!Shell.WINDOWS) {
          this.owner = owner;
        } else {
          this.owner = stripDomain(owner);
        }
        if (!Shell.WINDOWS) {
          this.group = group;
        } else {
          this.group = stripDomain(group);
        }
        this.mode = mode;
      }
      
      @Override
      public @OsUntrusted String toString(NativeIO.POSIX.@OsUntrusted Stat this) {
        return "Stat(owner='" + owner + "', group='" + group + "'" +
          ", mode=" + mode + ")";
      }

      public @OsUntrusted String getOwner(NativeIO.POSIX.@OsUntrusted Stat this) {
        return owner;
      }
      public @OsUntrusted String getGroup(NativeIO.POSIX.@OsUntrusted Stat this) {
        return group;
      }
      public @OsUntrusted int getMode(NativeIO.POSIX.@OsUntrusted Stat this) {
        return mode;
      }
    }

    /**
     * Returns the file stat for a file descriptor.
     *
     * @param fd file descriptor.
     * @return the file descriptor file stat.
     * @throws IOException thrown if there was an IO error while obtaining the file stat.
     */
    public static @OsUntrusted Stat getFstat(@OsUntrusted FileDescriptor fd) throws IOException {
      @OsUntrusted
      Stat stat = null;
      if (!Shell.WINDOWS) {
        stat = fstat(fd); 
        stat.owner = getName(IdCache.USER, stat.ownerId);
        stat.group = getName(IdCache.GROUP, stat.groupId);
      } else {
        try {
          stat = fstat(fd);
        } catch (@OsUntrusted NativeIOException nioe) {
          if (nioe.getErrorCode() == 6) {
            throw new @OsUntrusted NativeIOException("The handle is invalid.",
                Errno.EBADF);
          } else {
            LOG.warn(String.format("NativeIO.getFstat error (%d): %s",
                nioe.getErrorCode(), nioe.getMessage()));
            throw new @OsUntrusted NativeIOException("Unknown error", Errno.UNKNOWN);
          }
        }
      }
      return stat;
    }

    private static @OsUntrusted String getName(@OsUntrusted IdCache domain, @OsUntrusted int id) throws IOException {
      @OsUntrusted
      Map<@OsUntrusted Integer, @OsUntrusted CachedName> idNameCache = (domain == IdCache.USER)
        ? USER_ID_NAME_CACHE : GROUP_ID_NAME_CACHE;
      @OsUntrusted
      String name;
      @OsUntrusted
      CachedName cachedName = idNameCache.get(id);
      @OsUntrusted
      long now = System.currentTimeMillis();
      if (cachedName != null && (cachedName.timestamp + cacheTimeout) > now) {
        name = cachedName.name;
      } else {
        name = (domain == IdCache.USER) ? getUserName(id) : getGroupName(id);
        if (LOG.isDebugEnabled()) {
          @OsUntrusted
          String type = (domain == IdCache.USER) ? "UserName" : "GroupName";
          LOG.debug("Got " + type + " " + name + " for ID " + id +
            " from the native implementation");
        }
        cachedName = new @OsUntrusted CachedName(name, now);
        idNameCache.put(id, cachedName);
      }
      return name;
    }

    static native @OsUntrusted String getUserName(@OsUntrusted int uid) throws IOException;
    static native @OsUntrusted String getGroupName(@OsUntrusted int uid) throws IOException;

    private static class CachedName {
      final @OsUntrusted long timestamp;
      final @OsUntrusted String name;

      public @OsUntrusted CachedName(@OsUntrusted String name, @OsUntrusted long timestamp) {
        this.name = name;
        this.timestamp = timestamp;
      }
    }

    private static final @OsUntrusted Map<@OsUntrusted Integer, @OsUntrusted CachedName> USER_ID_NAME_CACHE =
      new @OsUntrusted ConcurrentHashMap<@OsUntrusted Integer, @OsUntrusted CachedName>();

    private static final @OsUntrusted Map<@OsUntrusted Integer, @OsUntrusted CachedName> GROUP_ID_NAME_CACHE =
      new @OsUntrusted ConcurrentHashMap<@OsUntrusted Integer, @OsUntrusted CachedName>();

    private enum IdCache {  @OsUntrusted  USER,  @OsUntrusted  GROUP }
  }

  private static @OsUntrusted boolean workaroundNonThreadSafePasswdCalls = false;


  public static class Windows {
    // Flags for CreateFile() call on Windows
    public static final @OsUntrusted long GENERIC_READ = 0x80000000L;
    public static final @OsUntrusted long GENERIC_WRITE = 0x40000000L;

    public static final @OsUntrusted long FILE_SHARE_READ = 0x00000001L;
    public static final @OsUntrusted long FILE_SHARE_WRITE = 0x00000002L;
    public static final @OsUntrusted long FILE_SHARE_DELETE = 0x00000004L;

    public static final @OsUntrusted long CREATE_NEW = 1;
    public static final @OsUntrusted long CREATE_ALWAYS = 2;
    public static final @OsUntrusted long OPEN_EXISTING = 3;
    public static final @OsUntrusted long OPEN_ALWAYS = 4;
    public static final @OsUntrusted long TRUNCATE_EXISTING = 5;

    public static final @OsUntrusted long FILE_BEGIN = 0;
    public static final @OsUntrusted long FILE_CURRENT = 1;
    public static final @OsUntrusted long FILE_END = 2;

    /** Wrapper around CreateFile() on Windows */
    public static native @OsUntrusted FileDescriptor createFile(@OsUntrusted String path,
        @OsUntrusted
        long desiredAccess, @OsUntrusted long shareMode, @OsUntrusted long creationDisposition)
        throws IOException;

    /** Wrapper around SetFilePointer() on Windows */
    public static native @OsUntrusted long setFilePointer(@OsUntrusted FileDescriptor fd,
        @OsUntrusted
        long distanceToMove, @OsUntrusted long moveMethod) throws IOException;

    /** Windows only methods used for getOwner() implementation */
    private static native @OsUntrusted String getOwner(@OsUntrusted FileDescriptor fd) throws IOException;

    /** Supported list of Windows access right flags */
    public static enum AccessRight {

@OsUntrusted  ACCESS_READ (0x0001),      // FILE_READ_DATA

@OsUntrusted  ACCESS_WRITE (0x0002),     // FILE_WRITE_DATA

@OsUntrusted  ACCESS_EXECUTE (0x0020);   // FILE_EXECUTE

      private final @OsUntrusted int accessRight;
      @OsUntrusted
      AccessRight(@OsUntrusted int access) {
        accessRight = access;
      }

      public @OsUntrusted int accessRight(NativeIO.Windows.@OsUntrusted AccessRight this) {
        return accessRight;
      }
    };

    /** Windows only method used to check if the current process has requested
     *  access rights on the given path. */
    private static native @OsUntrusted boolean access0(@OsUntrusted String path, @OsUntrusted int requestedAccess);

    /**
     * Checks whether the current process has desired access rights on
     * the given path.
     * 
     * Longer term this native function can be substituted with JDK7
     * function Files#isReadable, isWritable, isExecutable.
     *
     * @param path input path
     * @param desiredAccess ACCESS_READ, ACCESS_WRITE or ACCESS_EXECUTE
     * @return true if access is allowed
     * @throws IOException I/O exception on error
     */
    public static @OsUntrusted boolean access(@OsUntrusted String path, @OsUntrusted AccessRight desiredAccess)
        throws IOException {
      return access0(path, desiredAccess.accessRight());
    }

    static {
      if (NativeCodeLoader.isNativeCodeLoaded()) {
        try {
          initNative();
          nativeLoaded = true;
        } catch (@OsUntrusted Throwable t) {
          // This can happen if the user has an older version of libhadoop.so
          // installed - in this case we can continue without native IO
          // after warning
          LOG.error("Unable to initialize NativeIO libraries", t);
        }
      }
    }
  }

  private static final @OsUntrusted Log LOG = LogFactory.getLog(NativeIO.class);

  private static @OsUntrusted boolean nativeLoaded = false;

  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      try {
        initNative();
        nativeLoaded = true;
      } catch (@OsUntrusted Throwable t) {
        // This can happen if the user has an older version of libhadoop.so
        // installed - in this case we can continue without native IO
        // after warning
        LOG.error("Unable to initialize NativeIO libraries", t);
      }
    }
  }

  /**
   * Return true if the JNI-based native IO extensions are available.
   */
  public static @OsUntrusted boolean isAvailable() {
    return NativeCodeLoader.isNativeCodeLoaded() && nativeLoaded;
  }

  /** Initialize the JNI method ID and class ID cache */
  private static native void initNative();

  private static class CachedUid {
    final @OsUntrusted long timestamp;
    final @OsUntrusted String username;
    public @OsUntrusted CachedUid(@OsUntrusted String username, @OsUntrusted long timestamp) {
      this.timestamp = timestamp;
      this.username = username;
    }
  }
  private static final @OsUntrusted Map<@OsUntrusted Long, @OsUntrusted CachedUid> uidCache =
      new @OsUntrusted ConcurrentHashMap<@OsUntrusted Long, @OsUntrusted CachedUid>();
  private static @OsUntrusted long cacheTimeout;
  private static @OsUntrusted boolean initialized = false;
  
  /**
   * The Windows logon name has two part, NetBIOS domain name and
   * user account name, of the format DOMAIN\UserName. This method
   * will remove the domain part of the full logon name.
   *
   * @param the full principal name containing the domain
   * @return name with domain removed
   */
  private static @OsUntrusted String stripDomain(@OsUntrusted String name) {
    @OsUntrusted
    int i = name.indexOf('\\');
    if (i != -1)
      name = name.substring(i + 1);
    return name;
  }

  public static @OsUntrusted String getOwner(@OsUntrusted FileDescriptor fd) throws IOException {
    ensureInitialized();
    if (Shell.WINDOWS) {
      @OsUntrusted
      String owner = Windows.getOwner(fd);
      owner = stripDomain(owner);
      return owner;
    } else {
      @OsUntrusted
      long uid = POSIX.getUIDforFDOwnerforOwner(fd);
      @OsUntrusted
      CachedUid cUid = uidCache.get(uid);
      @OsUntrusted
      long now = System.currentTimeMillis();
      if (cUid != null && (cUid.timestamp + cacheTimeout) > now) {
        return cUid.username;
      }
      @OsUntrusted
      String user = POSIX.getUserName(uid);
      LOG.info("Got UserName " + user + " for UID " + uid
          + " from the native implementation");
      cUid = new @OsUntrusted CachedUid(user, now);
      uidCache.put(uid, cUid);
      return user;
    }
  }

  /**
   * Create a FileInputStream that shares delete permission on the
   * file opened, i.e. other process can delete the file the
   * FileInputStream is reading. Only Windows implementation uses
   * the native interface.
   */
  public static @OsUntrusted FileInputStream getShareDeleteFileInputStream(@OsUntrusted File f)
      throws IOException {
    if (!Shell.WINDOWS) {
      // On Linux the default FileInputStream shares delete permission
      // on the file opened.
      //
      return new @OsUntrusted FileInputStream(f);
    } else {
      // Use Windows native interface to create a FileInputStream that
      // shares delete permission on the file opened.
      //
      @OsUntrusted
      FileDescriptor fd = Windows.createFile(
          f.getAbsolutePath(),
          Windows.GENERIC_READ,
          Windows.FILE_SHARE_READ |
              Windows.FILE_SHARE_WRITE |
              Windows.FILE_SHARE_DELETE,
          Windows.OPEN_EXISTING);
      return new @OsUntrusted FileInputStream(fd);
    }
  }

  /**
   * Create a FileInputStream that shares delete permission on the
   * file opened at a given offset, i.e. other process can delete
   * the file the FileInputStream is reading. Only Windows implementation
   * uses the native interface.
   */
  public static @OsUntrusted FileInputStream getShareDeleteFileInputStream(@OsUntrusted File f, @OsUntrusted long seekOffset)
      throws IOException {
    if (!Shell.WINDOWS) {
      @OsUntrusted
      RandomAccessFile rf = new @OsUntrusted RandomAccessFile(f, "r");
      if (seekOffset > 0) {
        rf.seek(seekOffset);
      }
      return new @OsUntrusted FileInputStream(rf.getFD());
    } else {
      // Use Windows native interface to create a FileInputStream that
      // shares delete permission on the file opened, and set it to the
      // given offset.
      //
      @OsUntrusted
      FileDescriptor fd = NativeIO.Windows.createFile(
          f.getAbsolutePath(),
          NativeIO.Windows.GENERIC_READ,
          NativeIO.Windows.FILE_SHARE_READ |
              NativeIO.Windows.FILE_SHARE_WRITE |
              NativeIO.Windows.FILE_SHARE_DELETE,
          NativeIO.Windows.OPEN_EXISTING);
      if (seekOffset > 0)
        NativeIO.Windows.setFilePointer(fd, seekOffset, NativeIO.Windows.FILE_BEGIN);
      return new @OsUntrusted FileInputStream(fd);
    }
  }

  /**
   * Create the specified File for write access, ensuring that it does not exist.
   * @param f the file that we want to create
   * @param permissions we want to have on the file (if security is enabled)
   *
   * @throws AlreadyExistsException if the file already exists
   * @throws IOException if any other error occurred
   */
  public static @OsUntrusted FileOutputStream getCreateForWriteFileOutputStream(@OsUntrusted File f, @OsUntrusted int permissions)
      throws IOException {
    if (!Shell.WINDOWS) {
      // Use the native wrapper around open(2)
      try {
        @OsUntrusted
        FileDescriptor fd = NativeIO.POSIX.open(f.getAbsolutePath(),
            NativeIO.POSIX.O_WRONLY | NativeIO.POSIX.O_CREAT
                | NativeIO.POSIX.O_EXCL, permissions);
        return new @OsUntrusted FileOutputStream(fd);
      } catch (@OsUntrusted NativeIOException nioe) {
        if (nioe.getErrno() == Errno.EEXIST) {
          throw new @OsUntrusted AlreadyExistsException(nioe);
        }
        throw nioe;
      }
    } else {
      // Use the Windows native APIs to create equivalent FileOutputStream
      try {
        @OsUntrusted
        FileDescriptor fd = NativeIO.Windows.createFile(f.getCanonicalPath(),
            NativeIO.Windows.GENERIC_WRITE,
            NativeIO.Windows.FILE_SHARE_DELETE
                | NativeIO.Windows.FILE_SHARE_READ
                | NativeIO.Windows.FILE_SHARE_WRITE,
            NativeIO.Windows.CREATE_NEW);
        NativeIO.POSIX.chmod(f.getCanonicalPath(), permissions);
        return new @OsUntrusted FileOutputStream(fd);
      } catch (@OsUntrusted NativeIOException nioe) {
        if (nioe.getErrorCode() == 80) {
          // ERROR_FILE_EXISTS
          // 80 (0x50)
          // The file exists
          throw new @OsUntrusted AlreadyExistsException(nioe);
        }
        throw nioe;
      }
    }
  }

  private synchronized static void ensureInitialized() {
    if (!initialized) {
      cacheTimeout =
          new @OsUntrusted Configuration().getLong("hadoop.security.uid.cache.secs",
              4*60*60) * 1000;
      LOG.info("Initialized cache for UID to User mapping with a cache" +
          " timeout of " + cacheTimeout/1000 + " seconds.");
      initialized = true;
    }
  }
  
  /**
   * A version of renameTo that throws a descriptive exception when it fails.
   *
   * @param src                  The source path
   * @param dst                  The destination path
   * 
   * @throws NativeIOException   On failure.
   */
  public static void renameTo(@OsUntrusted File src, @OsUntrusted File dst)
      throws IOException {
    if (!nativeLoaded) {
      if (!src.renameTo(dst)) {
        throw new @OsUntrusted IOException("renameTo(src=" + src + ", dst=" +
          dst + ") failed.");
      }
    } else {
      renameTo0(src.getAbsolutePath(), dst.getAbsolutePath());
    }
  }

  /**
   * A version of renameTo that throws a descriptive exception when it fails.
   *
   * @param src                  The source path
   * @param dst                  The destination path
   * 
   * @throws NativeIOException   On failure.
   */
  private static native void renameTo0(@OsUntrusted String src, @OsUntrusted String dst)
      throws NativeIOException;
}
