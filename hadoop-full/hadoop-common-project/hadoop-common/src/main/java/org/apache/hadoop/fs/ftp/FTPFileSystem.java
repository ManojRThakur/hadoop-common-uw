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
package org.apache.hadoop.fs.ftp;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * A {@link FileSystem} backed by an FTP client provided by <a
 * href="http://commons.apache.org/net/">Apache Commons Net</a>.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FTPFileSystem extends @OsUntrusted FileSystem {

  public static final @OsUntrusted Log LOG = LogFactory
      .getLog(FTPFileSystem.class);

  public static final @OsUntrusted int DEFAULT_BUFFER_SIZE = 1024 * 1024;

  public static final @OsUntrusted int DEFAULT_BLOCK_SIZE = 4 * 1024;

  private @OsUntrusted URI uri;

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>ftp</code>
   */
  @Override
  public @OsUntrusted String getScheme(@OsUntrusted FTPFileSystem this) {
    return "ftp";
  }

  @SuppressWarnings("ostrusted:argument.type.incompatible")
  @Override
  public void initialize(@OsUntrusted FTPFileSystem this, @OsUntrusted URI uri, @OsUntrusted Configuration conf) throws IOException { // get
    super.initialize(uri, conf);
    // get host information from uri (overrides info in conf)
    @OsUntrusted
    String host = uri.getHost();
    host = (host == null) ? conf.get("fs.ftp.host", null) : host;
    if (host == null) {
      throw new @OsUntrusted IOException("Invalid host specified");
    }

    //ostrusted, BUZZSAW need to check path before putting in Conf
    conf.set("fs.ftp.host", host);

    // get port information from uri, (overrides info in conf)
    @OsUntrusted
    int port = uri.getPort();
    port = (port == -1) ? FTP.DEFAULT_PORT : port;
    conf.setInt("fs.ftp.host.port", port);

    // get user/password information from URI (overrides info in conf)
    @OsUntrusted
    String userAndPassword = uri.getUserInfo();
    if (userAndPassword == null) {
      userAndPassword = (conf.get("fs.ftp.user." + host, null) + ":" + conf
          .get("fs.ftp.password." + host, null));
      if (userAndPassword == null) {
        throw new @OsUntrusted IOException("Invalid user/passsword specified");
      }
    }
    @OsUntrusted
    String @OsUntrusted [] userPasswdInfo = userAndPassword.split(":");
    conf.set("fs.ftp.user." + host, userPasswdInfo[0]);
    if (userPasswdInfo.length > 1) {
      conf.set("fs.ftp.password." + host, userPasswdInfo[1]);
    } else {
      conf.set("fs.ftp.password." + host, null);
    }
    setConf(conf);
    this.uri = uri;
  }

  /**
   * Connect to the FTP server using configuration parameters *
   * 
   * @return An FTPClient instance
   * @throws IOException
   */
  private @OsUntrusted FTPClient connect(@OsUntrusted FTPFileSystem this) throws IOException {
    @OsUntrusted
    FTPClient client = null;
    @OsUntrusted
    Configuration conf = getConf();
    @OsUntrusted
    String host = conf.get("fs.ftp.host");
    @OsUntrusted
    int port = conf.getInt("fs.ftp.host.port", FTP.DEFAULT_PORT);
    @OsUntrusted
    String user = conf.get("fs.ftp.user." + host);
    @OsUntrusted
    String password = conf.get("fs.ftp.password." + host);
    client = new @OsUntrusted FTPClient();
    client.connect(host, port);
    @OsUntrusted
    int reply = client.getReplyCode();
    if (!FTPReply.isPositiveCompletion(reply)) {
      throw new @OsUntrusted IOException("Server - " + host
          + " refused connection on port - " + port);
    } else if (client.login(user, password)) {
      client.setFileTransferMode(FTP.BLOCK_TRANSFER_MODE);
      client.setFileType(FTP.BINARY_FILE_TYPE);
      client.setBufferSize(DEFAULT_BUFFER_SIZE);
    } else {
      throw new @OsUntrusted IOException("Login failed on server - " + host + ", port - "
          + port);
    }

    return client;
  }

  /**
   * Logout and disconnect the given FTPClient. *
   * 
   * @param client
   * @throws IOException
   */
  private void disconnect(@OsUntrusted FTPFileSystem this, @OsUntrusted FTPClient client) throws IOException {
    if (client != null) {
      if (!client.isConnected()) {
        throw new @OsUntrusted FTPException("Client not connected");
      }
      @OsUntrusted
      boolean logoutSuccess = client.logout();
      client.disconnect();
      if (!logoutSuccess) {
        LOG.warn("Logout failed while disconnecting, error code - "
            + client.getReplyCode());
      }
    }
  }

  /**
   * Resolve against given working directory. *
   * 
   * @param workDir
   * @param path
   * @return
   */
  private @OsUntrusted Path makeAbsolute(@OsUntrusted FTPFileSystem this, @OsUntrusted Path workDir, @OsUntrusted Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new @OsUntrusted Path(workDir, path);
  }

  @Override
  public @OsUntrusted FSDataInputStream open(@OsUntrusted FTPFileSystem this, @OsUntrusted Path file, @OsUntrusted int bufferSize) throws IOException {
    @OsUntrusted
    FTPClient client = connect();
    @OsUntrusted
    Path workDir = new @OsUntrusted Path(client.printWorkingDirectory());
    @OsUntrusted
    Path absolute = makeAbsolute(workDir, file);
    @OsUntrusted
    FileStatus fileStat = getFileStatus(client, absolute);
    if (fileStat.isDirectory()) {
      disconnect(client);
      throw new @OsUntrusted IOException("Path " + file + " is a directory.");
    }
    client.allocate(bufferSize);
    @OsUntrusted
    Path parent = absolute.getParent();
    // Change to parent directory on the
    // server. Only then can we read the
    // file
    // on the server by opening up an InputStream. As a side effect the working
    // directory on the server is changed to the parent directory of the file.
    // The FTP client connection is closed when close() is called on the
    // FSDataInputStream.
    client.changeWorkingDirectory(parent.toUri().getPath());
    @OsUntrusted
    InputStream is = client.retrieveFileStream(file.getName());
    @OsUntrusted
    FSDataInputStream fis = new @OsUntrusted FSDataInputStream(new @OsUntrusted FTPInputStream(is,
        client, statistics));
    if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
      // The ftpClient is an inconsistent state. Must close the stream
      // which in turn will logout and disconnect from FTP server
      fis.close();
      throw new @OsUntrusted IOException("Unable to open file: " + file + ", Aborting");
    }
    return fis;
  }

  /**
   * A stream obtained via this call must be closed before using other APIs of
   * this class or else the invocation will block.
   */
  @Override
  public @OsUntrusted FSDataOutputStream create(@OsUntrusted FTPFileSystem this, @OsUntrusted Path file, @OsUntrusted FsPermission permission,
      @OsUntrusted
      boolean overwrite, @OsUntrusted int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    final @OsUntrusted FTPClient client = connect();
    @OsUntrusted
    Path workDir = new @OsUntrusted Path(client.printWorkingDirectory());
    @OsUntrusted
    Path absolute = makeAbsolute(workDir, file);
    if (exists(client, file)) {
      if (overwrite) {
        delete(client, file);
      } else {
        disconnect(client);
        throw new @OsUntrusted IOException("File already exists: " + file);
      }
    }
    
    @OsUntrusted
    Path parent = absolute.getParent();
    if (parent == null || !mkdirs(client, parent, FsPermission.getDirDefault())) {
      parent = (parent == null) ? new @OsUntrusted Path("/") : parent;
      disconnect(client);
      throw new @OsUntrusted IOException("create(): Mkdirs failed to create: " + parent);
    }
    client.allocate(bufferSize);
    // Change to parent directory on the server. Only then can we write to the
    // file on the server by opening up an OutputStream. As a side effect the
    // working directory on the server is changed to the parent directory of the
    // file. The FTP client connection is closed when close() is called on the
    // FSDataOutputStream.
    client.changeWorkingDirectory(parent.toUri().getPath());
    @OsUntrusted
    FSDataOutputStream fos = new @OsUntrusted FSDataOutputStream(client.storeFileStream(file
        .getName()), statistics) {
      @Override
      public void close() throws IOException {
        super.close();
        if (!client.isConnected()) {
          throw new @OsUntrusted FTPException("Client not connected");
        }
        @OsUntrusted
        boolean cmdCompleted = client.completePendingCommand();
        disconnect(client);
        if (!cmdCompleted) {
          throw new @OsUntrusted FTPException("Could not complete transfer, Reply Code - "
              + client.getReplyCode());
        }
      }
    };
    if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
      // The ftpClient is an inconsistent state. Must close the stream
      // which in turn will logout and disconnect from FTP server
      fos.close();
      throw new @OsUntrusted IOException("Unable to create file: " + file + ", Aborting");
    }
    return fos;
  }

  /** This optional operation is not yet supported. */
  @Override
  public @OsUntrusted FSDataOutputStream append(@OsUntrusted FTPFileSystem this, @OsUntrusted Path f, @OsUntrusted int bufferSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    throw new @OsUntrusted IOException("Not supported");
  }
  
  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private @OsUntrusted boolean exists(@OsUntrusted FTPFileSystem this, @OsUntrusted FTPClient client, @OsUntrusted Path file) {
    try {
      return getFileStatus(client, file) != null;
    } catch (@OsUntrusted FileNotFoundException fnfe) {
      return false;
    } catch (@OsUntrusted IOException ioe) {
      throw new @OsUntrusted FTPException("Failed to get file status", ioe);
    }
  }

  @Override
  public @OsUntrusted boolean delete(@OsUntrusted FTPFileSystem this, @OsUntrusted Path file, @OsUntrusted boolean recursive) throws IOException {
    @OsUntrusted
    FTPClient client = connect();
    try {
      @OsUntrusted
      boolean success = delete(client, file, recursive);
      return success;
    } finally {
      disconnect(client);
    }
  }

  /** @deprecated Use delete(Path, boolean) instead */
  @Deprecated
  private @OsUntrusted boolean delete(@OsUntrusted FTPFileSystem this, @OsUntrusted FTPClient client, @OsUntrusted Path file) throws IOException {
    return delete(client, file, false);
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private @OsUntrusted boolean delete(@OsUntrusted FTPFileSystem this, @OsUntrusted FTPClient client, @OsUntrusted Path file, @OsUntrusted boolean recursive)
      throws IOException {
    @OsUntrusted
    Path workDir = new @OsUntrusted Path(client.printWorkingDirectory());
    @OsUntrusted
    Path absolute = makeAbsolute(workDir, file);
    @OsUntrusted
    String pathName = absolute.toUri().getPath();
    @OsUntrusted
    FileStatus fileStat = getFileStatus(client, absolute);
    if (fileStat.isFile()) {
      return client.deleteFile(pathName);
    }
    @OsUntrusted
    FileStatus @OsUntrusted [] dirEntries = listStatus(client, absolute);
    if (dirEntries != null && dirEntries.length > 0 && !(recursive)) {
      throw new @OsUntrusted IOException("Directory: " + file + " is not empty.");
    }
    if (dirEntries != null) {
      for (@OsUntrusted int i = 0; i < dirEntries.length; i++) {
        delete(client, new @OsUntrusted Path(absolute, dirEntries[i].getPath()), recursive);
      }
    }
    return client.removeDirectory(pathName);
  }

  private @OsUntrusted FsAction getFsAction(@OsUntrusted FTPFileSystem this, @OsUntrusted int accessGroup, @OsUntrusted FTPFile ftpFile) {
    @OsUntrusted
    FsAction action = FsAction.NONE;
    if (ftpFile.hasPermission(accessGroup, FTPFile.READ_PERMISSION)) {
      action.or(FsAction.READ);
    }
    if (ftpFile.hasPermission(accessGroup, FTPFile.WRITE_PERMISSION)) {
      action.or(FsAction.WRITE);
    }
    if (ftpFile.hasPermission(accessGroup, FTPFile.EXECUTE_PERMISSION)) {
      action.or(FsAction.EXECUTE);
    }
    return action;
  }

  private @OsUntrusted FsPermission getPermissions(@OsUntrusted FTPFileSystem this, @OsUntrusted FTPFile ftpFile) {
    @OsUntrusted
    FsAction user, group, others;
    user = getFsAction(FTPFile.USER_ACCESS, ftpFile);
    group = getFsAction(FTPFile.GROUP_ACCESS, ftpFile);
    others = getFsAction(FTPFile.WORLD_ACCESS, ftpFile);
    return new @OsUntrusted FsPermission(user, group, others);
  }

  @Override
  public @OsUntrusted URI getUri(@OsUntrusted FTPFileSystem this) {
    return uri;
  }

  @Override
  public @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted FTPFileSystem this, @OsUntrusted Path file) throws IOException {
    @OsUntrusted
    FTPClient client = connect();
    try {
      @OsUntrusted
      FileStatus @OsUntrusted [] stats = listStatus(client, file);
      return stats;
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted FTPFileSystem this, @OsUntrusted FTPClient client, @OsUntrusted Path file)
      throws IOException {
    @OsUntrusted
    Path workDir = new @OsUntrusted Path(client.printWorkingDirectory());
    @OsUntrusted
    Path absolute = makeAbsolute(workDir, file);
    @OsUntrusted
    FileStatus fileStat = getFileStatus(client, absolute);
    if (fileStat.isFile()) {
      return new @OsUntrusted FileStatus @OsUntrusted [] { fileStat };
    }
    @OsUntrusted
    FTPFile @OsUntrusted [] ftpFiles = client.listFiles(absolute.toUri().getPath());
    @OsUntrusted
    FileStatus @OsUntrusted [] fileStats = new @OsUntrusted FileStatus @OsUntrusted [ftpFiles.length];
    for (@OsUntrusted int i = 0; i < ftpFiles.length; i++) {
      fileStats[i] = getFileStatus(ftpFiles[i], absolute);
    }
    return fileStats;
  }

  @Override
  public @OsUntrusted FileStatus getFileStatus(@OsUntrusted FTPFileSystem this, @OsUntrusted Path file) throws IOException {
    @OsUntrusted
    FTPClient client = connect();
    try {
      @OsUntrusted
      FileStatus status = getFileStatus(client, file);
      return status;
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private @OsUntrusted FileStatus getFileStatus(@OsUntrusted FTPFileSystem this, @OsUntrusted FTPClient client, @OsUntrusted Path file)
      throws IOException {
    @OsUntrusted
    FileStatus fileStat = null;
    @OsUntrusted
    Path workDir = new @OsUntrusted Path(client.printWorkingDirectory());
    @OsUntrusted
    Path absolute = makeAbsolute(workDir, file);
    @OsUntrusted
    Path parentPath = absolute.getParent();
    if (parentPath == null) { // root dir
      @OsUntrusted
      long length = -1; // Length of root dir on server not known
      @OsUntrusted
      boolean isDir = true;
      @OsUntrusted
      int blockReplication = 1;
      @OsUntrusted
      long blockSize = DEFAULT_BLOCK_SIZE; // Block Size not known.
      @OsUntrusted
      long modTime = -1; // Modification time of root dir not known.
      @OsUntrusted
      Path root = new @OsUntrusted Path("/");
      return new @OsUntrusted FileStatus(length, isDir, blockReplication, blockSize,
          modTime, root.makeQualified(this));
    }
    @OsUntrusted
    String pathName = parentPath.toUri().getPath();
    @OsUntrusted
    FTPFile @OsUntrusted [] ftpFiles = client.listFiles(pathName);
    if (ftpFiles != null) {
      for (@OsUntrusted FTPFile ftpFile : ftpFiles) {
        if (ftpFile.getName().equals(file.getName())) { // file found in dir
          fileStat = getFileStatus(ftpFile, parentPath);
          break;
        }
      }
      if (fileStat == null) {
        throw new @OsUntrusted FileNotFoundException("File " + file + " does not exist.");
      }
    } else {
      throw new @OsUntrusted FileNotFoundException("File " + file + " does not exist.");
    }
    return fileStat;
  }

  /**
   * Convert the file information in FTPFile to a {@link FileStatus} object. *
   * 
   * @param ftpFile
   * @param parentPath
   * @return FileStatus
   */
  private @OsUntrusted FileStatus getFileStatus(@OsUntrusted FTPFileSystem this, @OsUntrusted FTPFile ftpFile, @OsUntrusted Path parentPath) {
    @OsUntrusted
    long length = ftpFile.getSize();
    @OsUntrusted
    boolean isDir = ftpFile.isDirectory();
    @OsUntrusted
    int blockReplication = 1;
    // Using default block size since there is no way in FTP client to know of
    // block sizes on server. The assumption could be less than ideal.
    @OsUntrusted
    long blockSize = DEFAULT_BLOCK_SIZE;
    @OsUntrusted
    long modTime = ftpFile.getTimestamp().getTimeInMillis();
    @OsUntrusted
    long accessTime = 0;
    @OsUntrusted
    FsPermission permission = getPermissions(ftpFile);
    @SuppressWarnings("ostrusted:cast.unsafe") // FTP file outside of hadoop commons.  Have to assume that we trust whatever ftp server.
    @OsTrusted
    String user = (@OsTrusted String) ftpFile.getUser();
    @SuppressWarnings("ostrusted:cast.unsafe")
    @OsTrusted
    String group = (@OsTrusted String) ftpFile.getGroup();
    @OsUntrusted
    Path filePath = new @OsUntrusted Path(parentPath, ftpFile.getName());
    return new @OsUntrusted FileStatus(length, isDir, blockReplication, blockSize, modTime,
        accessTime, permission, user, group, filePath.makeQualified(this));
  }

  @Override
  public @OsUntrusted boolean mkdirs(@OsUntrusted FTPFileSystem this, @OsUntrusted Path file, @OsUntrusted FsPermission permission) throws IOException {
    @OsUntrusted
    FTPClient client = connect();
    try {
      @OsUntrusted
      boolean success = mkdirs(client, file, permission);
      return success;
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private @OsUntrusted boolean mkdirs(@OsUntrusted FTPFileSystem this, @OsUntrusted FTPClient client, @OsUntrusted Path file, @OsUntrusted FsPermission permission)
      throws IOException {
    @OsUntrusted
    boolean created = true;
    @OsUntrusted
    Path workDir = new @OsUntrusted Path(client.printWorkingDirectory());
    @OsUntrusted
    Path absolute = makeAbsolute(workDir, file);
    @OsUntrusted
    String pathName = absolute.getName();
    if (!exists(client, absolute)) {
      @OsUntrusted
      Path parent = absolute.getParent();
      created = (parent == null || mkdirs(client, parent, FsPermission
          .getDirDefault()));
      if (created) {
        @OsUntrusted
        String parentDir = parent.toUri().getPath();
        client.changeWorkingDirectory(parentDir);
        created = created && client.makeDirectory(pathName);
      }
    } else if (isFile(client, absolute)) {
      throw new @OsUntrusted IOException(String.format(
          "Can't make directory for path %s since it is a file.", absolute));
    }
    return created;
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   */
  private @OsUntrusted boolean isFile(@OsUntrusted FTPFileSystem this, @OsUntrusted FTPClient client, @OsUntrusted Path file) {
    try {
      return getFileStatus(client, file).isFile();
    } catch (@OsUntrusted FileNotFoundException e) {
      return false; // file does not exist
    } catch (@OsUntrusted IOException ioe) {
      throw new @OsUntrusted FTPException("File check failed", ioe);
    }
  }

  /*
   * Assuming that parent of both source and destination is the same. Is the
   * assumption correct or it is suppose to work like 'move' ?
   */
  @Override
  public @OsUntrusted boolean rename(@OsUntrusted FTPFileSystem this, @OsUntrusted Path src, @OsUntrusted Path dst) throws IOException {
    @OsUntrusted
    FTPClient client = connect();
    try {
      @OsUntrusted
      boolean success = rename(client, src, dst);
      return success;
    } finally {
      disconnect(client);
    }
  }

  /**
   * Convenience method, so that we don't open a new connection when using this
   * method from within another method. Otherwise every API invocation incurs
   * the overhead of opening/closing a TCP connection.
   * 
   * @param client
   * @param src
   * @param dst
   * @return
   * @throws IOException
   */
  private @OsUntrusted boolean rename(@OsUntrusted FTPFileSystem this, @OsUntrusted FTPClient client, @OsUntrusted Path src, @OsUntrusted Path dst)
      throws IOException {
    @OsUntrusted
    Path workDir = new @OsUntrusted Path(client.printWorkingDirectory());
    @OsUntrusted
    Path absoluteSrc = makeAbsolute(workDir, src);
    @OsUntrusted
    Path absoluteDst = makeAbsolute(workDir, dst);
    if (!exists(client, absoluteSrc)) {
      throw new @OsUntrusted IOException("Source path " + src + " does not exist");
    }
    if (exists(client, absoluteDst)) {
      throw new @OsUntrusted IOException("Destination path " + dst
          + " already exist, cannot rename!");
    }
    @OsUntrusted
    String parentSrc = absoluteSrc.getParent().toUri().toString();
    @OsUntrusted
    String parentDst = absoluteDst.getParent().toUri().toString();
    @OsUntrusted
    String from = src.getName();
    @OsUntrusted
    String to = dst.getName();
    if (!parentSrc.equals(parentDst)) {
      throw new @OsUntrusted IOException("Cannot rename parent(source): " + parentSrc
          + ", parent(destination):  " + parentDst);
    }
    client.changeWorkingDirectory(parentSrc);
    @OsUntrusted
    boolean renamed = client.rename(from, to);
    return renamed;
  }

  @Override
  public @OsUntrusted Path getWorkingDirectory(@OsUntrusted FTPFileSystem this) {
    // Return home directory always since we do not maintain state.
    return getHomeDirectory();
  }

  @Override
  public @OsUntrusted Path getHomeDirectory(@OsUntrusted FTPFileSystem this) {
    @OsUntrusted
    FTPClient client = null;
    try {
      client = connect();
      @OsUntrusted
      Path homeDir = new @OsUntrusted Path(client.printWorkingDirectory());
      return homeDir;
    } catch (@OsUntrusted IOException ioe) {
      throw new @OsUntrusted FTPException("Failed to get home directory", ioe);
    } finally {
      try {
        disconnect(client);
      } catch (@OsUntrusted IOException ioe) {
        throw new @OsUntrusted FTPException("Failed to disconnect", ioe);
      }
    }
  }

  @Override
  public void setWorkingDirectory(@OsUntrusted FTPFileSystem this, @OsUntrusted Path newDir) {
    // we do not maintain the working directory state
  }
}
