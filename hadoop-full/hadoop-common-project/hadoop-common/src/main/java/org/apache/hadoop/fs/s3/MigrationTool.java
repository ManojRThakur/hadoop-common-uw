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
import ostrusted.quals.OsTrusted;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

/**
 * <p>
 * This class is a tool for migrating data from an older to a newer version
 * of an S3 filesystem.
 * </p>
 * <p>
 * All files in the filesystem are migrated by re-writing the block metadata
 * - no datafiles are touched.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MigrationTool extends @OsUntrusted Configured implements @OsUntrusted Tool {
  
  private @OsUntrusted S3Service s3Service;
  private @OsUntrusted S3Bucket bucket;

  // @SuppressWarnings("ostrusted") // cli args are trusted.
  public static void main(@OsTrusted String @OsUntrusted [] args) throws Exception {
    @OsUntrusted
    int res = ToolRunner.run(new @OsUntrusted MigrationTool(), args);
    System.exit(res);
  }
  
  @Override
  public @OsUntrusted int run(@OsUntrusted MigrationTool this, @OsUntrusted String @OsUntrusted [] args) throws Exception {
    
    if (args.length == 0) {
      System.err.println("Usage: MigrationTool <S3 file system URI>");
      System.err.println("\t<S3 file system URI>\tfilesystem to migrate");
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    @OsUntrusted
    URI uri = URI.create(args[0]);
    
    initialize(uri);
    
    @OsUntrusted
    FileSystemStore newStore = new @OsUntrusted Jets3tFileSystemStore();
    newStore.initialize(uri, getConf());
    
    if (get("%2F") != null) { 
      System.err.println("Current version number is [unversioned].");
      System.err.println("Target version number is " +
          newStore.getVersion() + ".");
      @OsUntrusted
      Store oldStore = new @OsUntrusted UnversionedStore();
      migrate(oldStore, newStore);
      return 0;
    } else {
      @OsUntrusted
      S3Object root = get("/");
      if (root != null) {
        @OsUntrusted
        String version = (@OsUntrusted String) root.getMetadata("fs-version");
        if (version == null) {
          System.err.println("Can't detect version - exiting.");
        } else {
          @OsUntrusted
          String newVersion = newStore.getVersion();
          System.err.println("Current version number is " + version + ".");
          System.err.println("Target version number is " + newVersion + ".");
          if (version.equals(newStore.getVersion())) {
            System.err.println("No migration required.");
            return 0;
          }
          // use version number to create Store
          //Store oldStore = ... 
          //migrate(oldStore, newStore);
          System.err.println("Not currently implemented.");
          return 0;
        }
      }
      System.err.println("Can't detect version - exiting.");
      return 0;
    }
    
  }
  
  public void initialize(@OsUntrusted MigrationTool this, @OsUntrusted URI uri) throws IOException {
    
    
    
    try {
      @OsUntrusted
      String accessKey = null;
      @OsUntrusted
      String secretAccessKey = null;
      @OsUntrusted
      String userInfo = uri.getUserInfo();
      if (userInfo != null) {
        @OsUntrusted
        int index = userInfo.indexOf(':');
        if (index != -1) {
          accessKey = userInfo.substring(0, index);
          secretAccessKey = userInfo.substring(index + 1);
        } else {
          accessKey = userInfo;
        }
      }
      if (accessKey == null) {
        accessKey = getConf().get("fs.s3.awsAccessKeyId");
      }
      if (secretAccessKey == null) {
        secretAccessKey = getConf().get("fs.s3.awsSecretAccessKey");
      }
      if (accessKey == null && secretAccessKey == null) {
        throw new @OsUntrusted IllegalArgumentException("AWS " +
                                           "Access Key ID and Secret Access Key " +
                                           "must be specified as the username " +
                                           "or password (respectively) of a s3 URL, " +
                                           "or by setting the " +
                                           "fs.s3.awsAccessKeyId or " +                         
                                           "fs.s3.awsSecretAccessKey properties (respectively).");
      } else if (accessKey == null) {
        throw new @OsUntrusted IllegalArgumentException("AWS " +
                                           "Access Key ID must be specified " +
                                           "as the username of a s3 URL, or by setting the " +
                                           "fs.s3.awsAccessKeyId property.");
      } else if (secretAccessKey == null) {
        throw new @OsUntrusted IllegalArgumentException("AWS " +
                                           "Secret Access Key must be specified " +
                                           "as the password of a s3 URL, or by setting the " +
                                           "fs.s3.awsSecretAccessKey property.");         
      }
      @OsUntrusted
      AWSCredentials awsCredentials =
        new @OsUntrusted AWSCredentials(accessKey, secretAccessKey);
      this.s3Service = new @OsUntrusted RestS3Service(awsCredentials);
    } catch (@OsUntrusted S3ServiceException e) {
      if (e.getCause() instanceof @OsUntrusted IOException) {
        throw (@OsUntrusted IOException) e.getCause();
      }
      throw new @OsUntrusted S3Exception(e);
    }
    bucket = new @OsUntrusted S3Bucket(uri.getHost());
  }
  
  private void migrate(@OsUntrusted MigrationTool this, @OsUntrusted Store oldStore, @OsUntrusted FileSystemStore newStore)
      throws IOException {
    for (@OsUntrusted Path path : oldStore.listAllPaths()) {
      @OsUntrusted
      INode inode = oldStore.retrieveINode(path);
      oldStore.deleteINode(path);
      newStore.storeINode(path, inode);
    }
  }
  
  private @OsUntrusted S3Object get(@OsUntrusted MigrationTool this, @OsUntrusted String key) {
    try {
      return s3Service.getObject(bucket, key);
    } catch (@OsUntrusted S3ServiceException e) {
      if ("NoSuchKey".equals(e.getS3ErrorCode())) {
        return null;
      }
    }
    return null;
  }
  
  interface Store {

    @OsUntrusted
    Set<@OsUntrusted Path> listAllPaths(MigrationTool.@OsUntrusted Store this) throws IOException;
    @OsUntrusted
    INode retrieveINode(MigrationTool.@OsUntrusted Store this, @OsUntrusted Path path) throws IOException;
    void deleteINode(MigrationTool.@OsUntrusted Store this, @OsUntrusted Path path) throws IOException;
    
  }
  
  class UnversionedStore implements @OsUntrusted Store {

    @Override
    public @OsUntrusted Set<@OsUntrusted Path> listAllPaths(@OsUntrusted MigrationTool.UnversionedStore this) throws IOException {
      try {
        @OsUntrusted
        String prefix = urlEncode(Path.SEPARATOR);
        @OsUntrusted
        S3Object @OsUntrusted [] objects = s3Service.listObjects(bucket, prefix, null);
        @OsUntrusted
        Set<@OsUntrusted Path> prefixes = new @OsUntrusted TreeSet<@OsUntrusted Path>();
        for (@OsUntrusted int i = 0; i < objects.length; i++) {
          prefixes.add(keyToPath(objects[i].getKey()));
        }
        return prefixes;
      } catch (@OsUntrusted S3ServiceException e) {
        if (e.getCause() instanceof @OsUntrusted IOException) {
          throw (@OsUntrusted IOException) e.getCause();
        }
        throw new @OsUntrusted S3Exception(e);
      }   
    }

    @Override
    public void deleteINode(@OsUntrusted MigrationTool.UnversionedStore this, @OsUntrusted Path path) throws IOException {
      delete(pathToKey(path));
    }
    
    private void delete(@OsUntrusted MigrationTool.UnversionedStore this, @OsUntrusted String key) throws IOException {
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
    public @OsUntrusted INode retrieveINode(@OsUntrusted MigrationTool.UnversionedStore this, @OsUntrusted Path path) throws IOException {
      return INode.deserialize(get(pathToKey(path)));
    }

    private @OsUntrusted InputStream get(@OsUntrusted MigrationTool.UnversionedStore this, @OsUntrusted String key) throws IOException {
      try {
        @OsUntrusted
        S3Object object = s3Service.getObject(bucket, key);
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
    
    private @OsUntrusted String pathToKey(@OsUntrusted MigrationTool.UnversionedStore this, @OsUntrusted Path path) {
      if (!path.isAbsolute()) {
        throw new @OsUntrusted IllegalArgumentException("Path must be absolute: " + path);
      }
      return urlEncode(path.toUri().getPath());
    }
    
    private @OsUntrusted Path keyToPath(@OsUntrusted MigrationTool.UnversionedStore this, @OsUntrusted String key) {
      return new @OsUntrusted Path(urlDecode(key));
    }

    private @OsUntrusted String urlEncode(@OsUntrusted MigrationTool.UnversionedStore this, @OsUntrusted String s) {
      try {
        return URLEncoder.encode(s, "UTF-8");
      } catch (@OsUntrusted UnsupportedEncodingException e) {
        // Should never happen since every implementation of the Java Platform
        // is required to support UTF-8.
        // See http://java.sun.com/j2se/1.5.0/docs/api/java/nio/charset/Charset.html
        throw new @OsUntrusted IllegalStateException(e);
      }
    }
    
    private @OsUntrusted String urlDecode(@OsUntrusted MigrationTool.UnversionedStore this, @OsUntrusted String s) {
      try {
        return URLDecoder.decode(s, "UTF-8");
      } catch (@OsUntrusted UnsupportedEncodingException e) {
        // Should never happen since every implementation of the Java Platform
        // is required to support UTF-8.
        // See http://java.sun.com/j2se/1.5.0/docs/api/java/nio/charset/Charset.html
        throw new @OsUntrusted IllegalStateException(e);
      }
    }
    
  }
  
}
