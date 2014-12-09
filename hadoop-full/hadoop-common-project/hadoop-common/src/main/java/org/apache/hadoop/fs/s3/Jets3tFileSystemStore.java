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

package org.apache.hadoop.fs.s3;

import ostrusted.quals.OsUntrusted;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.INode.FileType;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class Jets3tFileSystemStore implements @OsUntrusted FileSystemStore {
  
  private static final @OsUntrusted String FILE_SYSTEM_NAME = "fs";
  private static final @OsUntrusted String FILE_SYSTEM_VALUE = "Hadoop";

  private static final @OsUntrusted String FILE_SYSTEM_TYPE_NAME = "fs-type";
  private static final @OsUntrusted String FILE_SYSTEM_TYPE_VALUE = "block";

  private static final @OsUntrusted String FILE_SYSTEM_VERSION_NAME = "fs-version";
  private static final @OsUntrusted String FILE_SYSTEM_VERSION_VALUE = "1";
  
  private static final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> METADATA =
    new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted String>();
  
  static {
    METADATA.put(FILE_SYSTEM_NAME, FILE_SYSTEM_VALUE);
    METADATA.put(FILE_SYSTEM_TYPE_NAME, FILE_SYSTEM_TYPE_VALUE);
    METADATA.put(FILE_SYSTEM_VERSION_NAME, FILE_SYSTEM_VERSION_VALUE);
  }

  private static final @OsUntrusted String PATH_DELIMITER = Path.SEPARATOR;
  private static final @OsUntrusted String BLOCK_PREFIX = "block_";

  private @OsUntrusted Configuration conf;
  
  private @OsUntrusted S3Service s3Service;

  private @OsUntrusted S3Bucket bucket;
  
  private @OsUntrusted int bufferSize;
  
  private static final @OsUntrusted Log LOG = 
    LogFactory.getLog(Jets3tFileSystemStore.class.getName());
  
  @Override
  public void initialize(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted URI uri, @OsUntrusted Configuration conf) throws IOException {
    
    this.conf = conf;
    
    @OsUntrusted
    S3Credentials s3Credentials = new @OsUntrusted S3Credentials();
    s3Credentials.initialize(uri, conf);
    try {
      @OsUntrusted
      AWSCredentials awsCredentials =
        new @OsUntrusted AWSCredentials(s3Credentials.getAccessKey(),
            s3Credentials.getSecretAccessKey());
      this.s3Service = new @OsUntrusted RestS3Service(awsCredentials);
    } catch (@OsUntrusted S3ServiceException e) {
      if (e.getCause() instanceof @OsUntrusted IOException) {
        throw (@OsUntrusted IOException) e.getCause();
      }
      throw new @OsUntrusted S3Exception(e);
    }
    bucket = new @OsUntrusted S3Bucket(uri.getHost());

    this.bufferSize = conf.getInt(
                       S3FileSystemConfigKeys.S3_STREAM_BUFFER_SIZE_KEY,
                       S3FileSystemConfigKeys.S3_STREAM_BUFFER_SIZE_DEFAULT
		      );
  }

  @Override
  public @OsUntrusted String getVersion(@OsUntrusted Jets3tFileSystemStore this) throws IOException {
    return FILE_SYSTEM_VERSION_VALUE;
  }

  private void delete(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted String key) throws IOException {
    try {
      s3Service.deleteObject(bucket, key);
    } catch (@OsUntrusted S3ServiceException e) {
      if (e.getCause() instanceof @OsUntrusted IOException) {
        throw (@OsUntrusted IOException) e.getCause();
      }
      throw new @OsUntrusted S3Exception(e);
    }
  }

  @Override
  public void deleteINode(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted Path path) throws IOException {
    delete(pathToKey(path));
  }

  @Override
  public void deleteBlock(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted Block block) throws IOException {
    delete(blockToKey(block));
  }

  @Override
  public @OsUntrusted boolean inodeExists(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted Path path) throws IOException {
    @OsUntrusted
    String key = pathToKey(path);
    @OsUntrusted
    InputStream in = get(key, true);
    if (in == null) {
      if (isRoot(key)) {
        storeINode(path, INode.DIRECTORY_INODE);
        return true;
      } else {
        return false;
      }
    }
    in.close();
    return true;
  }
  
  @Override
  public @OsUntrusted boolean blockExists(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted long blockId) throws IOException {
    @OsUntrusted
    InputStream in = get(blockToKey(blockId), false);
    if (in == null) {
      return false;
    }
    in.close();
    return true;
  }

  private @OsUntrusted InputStream get(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted String key, @OsUntrusted boolean checkMetadata)
      throws IOException {
    
    try {
      @OsUntrusted
      S3Object object = s3Service.getObject(bucket, key);
      if (checkMetadata) {
        checkMetadata(object);
      }
      return object.getDataInputStream();
    } catch (@OsUntrusted S3ServiceException e) {
      if ("NoSuchKey".equals(e.getS3ErrorCode())) {
        return null;
      }
      if (e.getCause() instanceof @OsUntrusted IOException) {
        throw (@OsUntrusted IOException) e.getCause();
      }
      throw new @OsUntrusted S3Exception(e);
    }
  }

  private @OsUntrusted InputStream get(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted String key, @OsUntrusted long byteRangeStart) throws IOException {
    try {
      @OsUntrusted
      S3Object object = s3Service.getObject(bucket, key, null, null, null,
                                            null, byteRangeStart, null);
      return object.getDataInputStream();
    } catch (@OsUntrusted S3ServiceException e) {
      if ("NoSuchKey".equals(e.getS3ErrorCode())) {
        return null;
      }
      if (e.getCause() instanceof @OsUntrusted IOException) {
        throw (@OsUntrusted IOException) e.getCause();
      }
      throw new @OsUntrusted S3Exception(e);
    }
  }

  private void checkMetadata(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted S3Object object) throws S3FileSystemException,
      S3ServiceException {
    
    @OsUntrusted
    String name = (@OsUntrusted String) object.getMetadata(FILE_SYSTEM_NAME);
    if (!FILE_SYSTEM_VALUE.equals(name)) {
      throw new @OsUntrusted S3FileSystemException("Not a Hadoop S3 file.");
    }
    @OsUntrusted
    String type = (@OsUntrusted String) object.getMetadata(FILE_SYSTEM_TYPE_NAME);
    if (!FILE_SYSTEM_TYPE_VALUE.equals(type)) {
      throw new @OsUntrusted S3FileSystemException("Not a block file.");
    }
    @OsUntrusted
    String dataVersion = (@OsUntrusted String) object.getMetadata(FILE_SYSTEM_VERSION_NAME);
    if (!FILE_SYSTEM_VERSION_VALUE.equals(dataVersion)) {
      throw new @OsUntrusted VersionMismatchException(FILE_SYSTEM_VERSION_VALUE,
          dataVersion);
    }
  }

  @Override
  public @OsUntrusted INode retrieveINode(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted Path path) throws IOException {
    @OsUntrusted
    String key = pathToKey(path);
    @OsUntrusted
    InputStream in = get(key, true);
    if (in == null && isRoot(key)) {
      storeINode(path, INode.DIRECTORY_INODE);
      return INode.DIRECTORY_INODE;
    }
    return INode.deserialize(in);
  }

  @Override
  public @OsUntrusted File retrieveBlock(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted Block block, @OsUntrusted long byteRangeStart)
    throws IOException {
    @OsUntrusted
    File fileBlock = null;
    @OsUntrusted
    InputStream in = null;
    @OsUntrusted
    OutputStream out = null;
    try {
      fileBlock = newBackupFile();
      in = get(blockToKey(block), byteRangeStart);
      out = new @OsUntrusted BufferedOutputStream(new @OsUntrusted FileOutputStream(fileBlock));
      @OsUntrusted
      byte @OsUntrusted [] buf = new @OsUntrusted byte @OsUntrusted [bufferSize];
      @OsUntrusted
      int numRead;
      while ((numRead = in.read(buf)) >= 0) {
        out.write(buf, 0, numRead);
      }
      return fileBlock;
    } catch (@OsUntrusted IOException e) {
      // close output stream to file then delete file
      closeQuietly(out);
      out = null; // to prevent a second close
      if (fileBlock != null) {
        @OsUntrusted
        boolean b = fileBlock.delete();
        if (!b) {
          LOG.warn("Ignoring failed delete");
        }
      }
      throw e;
    } finally {
      closeQuietly(out);
      closeQuietly(in);
    }
  }
  
  private @OsUntrusted File newBackupFile(@OsUntrusted Jets3tFileSystemStore this) throws IOException {
    @OsUntrusted
    File dir = new @OsUntrusted File(conf.get("fs.s3.buffer.dir"));
    if (!dir.exists() && !dir.mkdirs()) {
      throw new @OsUntrusted IOException("Cannot create S3 buffer directory: " + dir);
    }
    @OsUntrusted
    File result = File.createTempFile("input-", ".tmp", dir);
    result.deleteOnExit();
    return result;
  }

  @Override
  public @OsUntrusted Set<@OsUntrusted Path> listSubPaths(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted Path path) throws IOException {
    try {
      @OsUntrusted
      String prefix = pathToKey(path);
      if (!prefix.endsWith(PATH_DELIMITER)) {
        prefix += PATH_DELIMITER;
      }
      @OsUntrusted
      S3Object @OsUntrusted [] objects = s3Service.listObjects(bucket, prefix, PATH_DELIMITER);
      @OsUntrusted
      Set<@OsUntrusted Path> prefixes = new @OsUntrusted TreeSet<@OsUntrusted Path>();
      for (@OsUntrusted int i = 0; i < objects.length; i++) {
        prefixes.add(keyToPath(objects[i].getKey()));
      }
      prefixes.remove(path);
      return prefixes;
    } catch (@OsUntrusted S3ServiceException e) {
      if (e.getCause() instanceof @OsUntrusted IOException) {
        throw (@OsUntrusted IOException) e.getCause();
      }
      throw new @OsUntrusted S3Exception(e);
    }
  }
  
  @Override
  public @OsUntrusted Set<@OsUntrusted Path> listDeepSubPaths(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted Path path) throws IOException {
    try {
      @OsUntrusted
      String prefix = pathToKey(path);
      if (!prefix.endsWith(PATH_DELIMITER)) {
        prefix += PATH_DELIMITER;
      }
      @OsUntrusted
      S3Object @OsUntrusted [] objects = s3Service.listObjects(bucket, prefix, null);
      @OsUntrusted
      Set<@OsUntrusted Path> prefixes = new @OsUntrusted TreeSet<@OsUntrusted Path>();
      for (@OsUntrusted int i = 0; i < objects.length; i++) {
        prefixes.add(keyToPath(objects[i].getKey()));
      }
      prefixes.remove(path);
      return prefixes;
    } catch (@OsUntrusted S3ServiceException e) {
      if (e.getCause() instanceof @OsUntrusted IOException) {
        throw (@OsUntrusted IOException) e.getCause();
      }
      throw new @OsUntrusted S3Exception(e);
    }    
  }

  private void put(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted String key, @OsUntrusted InputStream in, @OsUntrusted long length, @OsUntrusted boolean storeMetadata)
      throws IOException {
    
    try {
      @OsUntrusted
      S3Object object = new @OsUntrusted S3Object(key);
      object.setDataInputStream(in);
      object.setContentType("binary/octet-stream");
      object.setContentLength(length);
      if (storeMetadata) {
        object.addAllMetadata(METADATA);
      }
      s3Service.putObject(bucket, object);
    } catch (@OsUntrusted S3ServiceException e) {
      if (e.getCause() instanceof @OsUntrusted IOException) {
        throw (@OsUntrusted IOException) e.getCause();
      }
      throw new @OsUntrusted S3Exception(e);
    }
  }

  @Override
  public void storeINode(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted Path path, @OsUntrusted INode inode) throws IOException {
    put(pathToKey(path), inode.serialize(), inode.getSerializedLength(), true);
  }

  @Override
  public void storeBlock(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted Block block, @OsUntrusted File file) throws IOException {
    @OsUntrusted
    BufferedInputStream in = null;
    try {
      in = new @OsUntrusted BufferedInputStream(new @OsUntrusted FileInputStream(file));
      put(blockToKey(block), in, block.getLength(), false);
    } finally {
      closeQuietly(in);
    }    
  }

  private void closeQuietly(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (@OsUntrusted IOException e) {
        // ignore
      }
    }
  }

  private @OsUntrusted String pathToKey(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted Path path) {
    if (!path.isAbsolute()) {
      throw new @OsUntrusted IllegalArgumentException("Path must be absolute: " + path);
    }
    return path.toUri().getPath();
  }

  private @OsUntrusted Path keyToPath(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted String key) {
    return new @OsUntrusted Path(key);
  }
  
  private @OsUntrusted String blockToKey(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted long blockId) {
    return BLOCK_PREFIX + blockId;
  }

  private @OsUntrusted String blockToKey(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted Block block) {
    return blockToKey(block.getId());
  }

  private @OsUntrusted boolean isRoot(@OsUntrusted Jets3tFileSystemStore this, @OsUntrusted String key) {
    return key.isEmpty() || key.equals("/");
  }

  @Override
  public void purge(@OsUntrusted Jets3tFileSystemStore this) throws IOException {
    try {
      @OsUntrusted
      S3Object @OsUntrusted [] objects = s3Service.listObjects(bucket);
      for (@OsUntrusted int i = 0; i < objects.length; i++) {
        s3Service.deleteObject(bucket, objects[i].getKey());
      }
    } catch (@OsUntrusted S3ServiceException e) {
      if (e.getCause() instanceof @OsUntrusted IOException) {
        throw (@OsUntrusted IOException) e.getCause();
      }
      throw new @OsUntrusted S3Exception(e);
    }
  }

  @Override
  public void dump(@OsUntrusted Jets3tFileSystemStore this) throws IOException {
    @OsUntrusted
    StringBuilder sb = new @OsUntrusted StringBuilder("S3 Filesystem, ");
    sb.append(bucket.getName()).append("\n");
    try {
      @OsUntrusted
      S3Object @OsUntrusted [] objects = s3Service.listObjects(bucket, PATH_DELIMITER, null);
      for (@OsUntrusted int i = 0; i < objects.length; i++) {
        @OsUntrusted
        Path path = keyToPath(objects[i].getKey());
        sb.append(path).append("\n");
        @OsUntrusted
        INode m = retrieveINode(path);
        sb.append("\t").append(m.getFileType()).append("\n");
        if (m.getFileType() == FileType.DIRECTORY) {
          continue;
        }
        for (@OsUntrusted int j = 0; j < m.getBlocks().length; j++) {
          sb.append("\t").append(m.getBlocks()[j]).append("\n");
        }
      }
    } catch (@OsUntrusted S3ServiceException e) {
      if (e.getCause() instanceof @OsUntrusted IOException) {
        throw (@OsUntrusted IOException) e.getCause();
      }
      throw new @OsUntrusted S3Exception(e);
    }
    System.out.println(sb);
  }

}
