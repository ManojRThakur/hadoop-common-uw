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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class provides secure APIs for opening and creating files on the local
 * disk. The main issue this class tries to handle is that of symlink traversal.
 * <br/>
 * An example of such an attack is:
 * <ol>
 * <li> Malicious user removes his task's syslog file, and puts a link to the
 * jobToken file of a target user.</li>
 * <li> Malicious user tries to open the syslog file via the servlet on the
 * tasktracker.</li>
 * <li> The tasktracker is unaware of the symlink, and simply streams the contents
 * of the jobToken file. The malicious user can now access potentially sensitive
 * map outputs, etc. of the target user's job.</li>
 * </ol>
 * A similar attack is possible involving task log truncation, but in that case
 * due to an insecure write to a file.
 * <br/>
 */
public class SecureIOUtils {

  /**
   * Ensure that we are set up to run with the appropriate native support code.
   * If security is disabled, and the support code is unavailable, this class
   * still tries its best to be secure, but is vulnerable to some race condition
   * attacks.
   *
   * If security is enabled but the support code is unavailable, throws a
   * RuntimeException since we don't want to run insecurely.
   */
  static {
    @OsUntrusted
    boolean shouldBeSecure = UserGroupInformation.isSecurityEnabled();
    @OsUntrusted
    boolean canBeSecure = NativeIO.isAvailable();

    if (!canBeSecure && shouldBeSecure) {
      throw new @OsUntrusted RuntimeException(
        "Secure IO is not possible without native code extensions.");
    }

    // Pre-cache an instance of the raw FileSystem since we sometimes
    // do secure IO in a shutdown hook, where this call could fail.
    try {
      rawFilesystem = FileSystem.getLocal(new @OsUntrusted Configuration()).getRaw();
    } catch (@OsUntrusted IOException ie) {
      throw new @OsUntrusted RuntimeException(
      "Couldn't obtain an instance of RawLocalFileSystem.");
    }

    // SecureIO just skips security checks in the case that security is
    // disabled
    skipSecurity = !canBeSecure;
  }

  private final static @OsUntrusted boolean skipSecurity;
  private final static @OsUntrusted FileSystem rawFilesystem;

  /**
   * Open the given File for random read access, verifying the expected user/
   * group constraints if security is enabled.
   * 
   * Note that this function provides no additional security checks if hadoop
   * security is disabled, since doing the checks would be too expensive when
   * native libraries are not available.
   * 
   * @param f file that we are trying to open
   * @param mode mode in which we want to open the random access file
   * @param expectedOwner the expected user owner for the file
   * @param expectedGroup the expected group owner for the file
   * @throws IOException if an IO error occurred or if the user/group does
   * not match when security is enabled.
   */
  public static @OsUntrusted RandomAccessFile openForRandomRead(@OsUntrusted File f,
      @OsUntrusted
      String mode, @OsUntrusted String expectedOwner, @OsUntrusted String expectedGroup)
      throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return new @OsUntrusted RandomAccessFile(f, mode);
    }
    return forceSecureOpenForRandomRead(f, mode, expectedOwner, expectedGroup);
  }

  /**
   * Same as openForRandomRead except that it will run even if security is off.
   * This is used by unit tests.
   */
  @VisibleForTesting
  protected static @OsUntrusted RandomAccessFile forceSecureOpenForRandomRead(@OsUntrusted File f,
      @OsUntrusted
      String mode, @OsUntrusted String expectedOwner, @OsUntrusted String expectedGroup)
      throws IOException {
    @OsUntrusted
    RandomAccessFile raf = new @OsUntrusted RandomAccessFile(f, mode);
    @OsUntrusted
    boolean success = false;
    try {
      @OsUntrusted
      Stat stat = NativeIO.POSIX.getFstat(raf.getFD());
      checkStat(f, stat.getOwner(), stat.getGroup(), expectedOwner,
          expectedGroup);
      success = true;
      return raf;
    } finally {
      if (!success) {
        raf.close();
      }
    }
  }

  /**
   * Opens the {@link FSDataInputStream} on the requested file on local file
   * system, verifying the expected user/group constraints if security is
   * enabled.
   * @param file absolute path of the file
   * @param expectedOwner the expected user owner for the file
   * @param expectedGroup the expected group owner for the file
   * @throws IOException if an IO Error occurred or the user/group does not
   * match if security is enabled
   */
  public static @OsUntrusted FSDataInputStream openFSDataInputStream(@OsUntrusted File file,
      @OsUntrusted
      String expectedOwner, @OsUntrusted String expectedGroup) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return rawFilesystem.open(new @OsUntrusted Path(file.getAbsolutePath()));
    }
    return forceSecureOpenFSDataInputStream(file, expectedOwner, expectedGroup);
  }

  /**
   * Same as openFSDataInputStream except that it will run even if security is
   * off. This is used by unit tests.
   */
  @VisibleForTesting
  protected static @OsUntrusted FSDataInputStream forceSecureOpenFSDataInputStream(
      @OsUntrusted
      File file,
      @OsUntrusted
      String expectedOwner, @OsUntrusted String expectedGroup) throws IOException {
    final @OsUntrusted FSDataInputStream in =
        rawFilesystem.open(new @OsUntrusted Path(file.getAbsolutePath()));
    @OsUntrusted
    boolean success = false;
    try {
      @OsUntrusted
      Stat stat = NativeIO.POSIX.getFstat(in.getFileDescriptor());
      checkStat(file, stat.getOwner(), stat.getGroup(), expectedOwner,
          expectedGroup);
      success = true;
      return in;
    } finally {
      if (!success) {
        in.close();
      }
    }
  }

  /**
   * Open the given File for read access, verifying the expected user/group
   * constraints if security is enabled.
   *
   * Note that this function provides no additional checks if Hadoop
   * security is disabled, since doing the checks would be too expensive
   * when native libraries are not available.
   *
   * @param f the file that we are trying to open
   * @param expectedOwner the expected user owner for the file
   * @param expectedGroup the expected group owner for the file
   * @throws IOException if an IO Error occurred, or security is enabled and
   * the user/group does not match
   */
  public static @OsUntrusted FileInputStream openForRead(@OsUntrusted File f, @OsUntrusted String expectedOwner, 
      @OsUntrusted
      String expectedGroup) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return new @OsUntrusted FileInputStream(f);
    }
    return forceSecureOpenForRead(f, expectedOwner, expectedGroup);
  }

  /**
   * Same as openForRead() except that it will run even if security is off.
   * This is used by unit tests.
   */
  @VisibleForTesting
  protected static @OsUntrusted FileInputStream forceSecureOpenForRead(@OsUntrusted File f, @OsUntrusted String expectedOwner,
      @OsUntrusted
      String expectedGroup) throws IOException {

    @OsUntrusted
    FileInputStream fis = new @OsUntrusted FileInputStream(f);
    @OsUntrusted
    boolean success = false;
    try {
      @OsUntrusted
      Stat stat = NativeIO.POSIX.getFstat(fis.getFD());
      checkStat(f, stat.getOwner(), stat.getGroup(), expectedOwner,
          expectedGroup);
      success = true;
      return fis;
    } finally {
      if (!success) {
        fis.close();
      }
    }
  }

  private static @OsUntrusted FileOutputStream insecureCreateForWrite(@OsUntrusted File f,
      @OsUntrusted
      int permissions) throws IOException {
    // If we can't do real security, do a racy exists check followed by an
    // open and chmod
    if (f.exists()) {
      throw new @OsUntrusted AlreadyExistsException("File " + f + " already exists");
    }
    @OsUntrusted
    FileOutputStream fos = new @OsUntrusted FileOutputStream(f);
    @OsUntrusted
    boolean success = false;
    try {
      rawFilesystem.setPermission(new @OsUntrusted Path(f.getAbsolutePath()),
        new @OsUntrusted FsPermission((@OsUntrusted short)permissions));
      success = true;
      return fos;
    } finally {
      if (!success) {
        fos.close();
      }
    }
  }

  /**
   * Open the specified File for write access, ensuring that it does not exist.
   * @param f the file that we want to create
   * @param permissions we want to have on the file (if security is enabled)
   *
   * @throws AlreadyExistsException if the file already exists
   * @throws IOException if any other error occurred
   */
  public static @OsUntrusted FileOutputStream createForWrite(@OsUntrusted File f, @OsUntrusted int permissions)
  throws IOException {
    if (skipSecurity) {
      return insecureCreateForWrite(f, permissions);
    } else {
      return NativeIO.getCreateForWriteFileOutputStream(f, permissions);
    }
  }

  private static void checkStat(@OsUntrusted File f, @OsUntrusted String owner, @OsUntrusted String group, 
      @OsUntrusted
      String expectedOwner, 
      @OsUntrusted
      String expectedGroup) throws IOException {
    @OsUntrusted
    boolean success = true;
    if (expectedOwner != null &&
        !expectedOwner.equals(owner)) {
      if (Path.WINDOWS) {
        @OsUntrusted
        UserGroupInformation ugi =
            UserGroupInformation.createRemoteUser(expectedOwner);
        final @OsUntrusted String adminsGroupString = "Administrators";
        success = owner.equals(adminsGroupString)
            && Arrays.asList(ugi.getGroupNames()).contains(adminsGroupString);
      } else {
        success = false;
      }
    }
    if (!success) {
      throw new @OsUntrusted IOException(
          "Owner '" + owner + "' for path " + f + " did not match " +
              "expected owner '" + expectedOwner + "'");
    }
  }

  /**
   * Signals that an attempt to create a file at a given pathname has failed
   * because another file already existed at that path.
   */
  public static class AlreadyExistsException extends @OsUntrusted IOException {
    private static final @OsUntrusted long serialVersionUID = 1L;

    public @OsUntrusted AlreadyExistsException(@OsUntrusted String msg) {
      super(msg);
    }

    public @OsUntrusted AlreadyExistsException(@OsUntrusted Throwable cause) {
      super(cause);
    }
  }
}
