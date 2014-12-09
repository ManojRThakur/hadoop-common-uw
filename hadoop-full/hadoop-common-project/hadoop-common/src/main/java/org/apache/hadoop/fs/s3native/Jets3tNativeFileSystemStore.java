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

package org.apache.hadoop.fs.s3native;

import ostrusted.quals.OsUntrusted;
import static org.apache.hadoop.fs.s3native.NativeS3FileSystem.PATH_DELIMITER;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3.S3Credentials;
import org.apache.hadoop.fs.s3.S3Exception;
import org.jets3t.service.S3ObjectsChunk;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class Jets3tNativeFileSystemStore implements @OsUntrusted NativeFileSystemStore {
  
  private @OsUntrusted S3Service s3Service;
  private @OsUntrusted S3Bucket bucket;
  
  @Override
  public void initialize(@OsUntrusted Jets3tNativeFileSystemStore this, @OsUntrusted URI uri, @OsUntrusted Configuration conf) throws IOException {
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
      handleServiceException(e);
    }
    bucket = new @OsUntrusted S3Bucket(uri.getHost());
  }
  
  @Override
  public void storeFile(@OsUntrusted Jets3tNativeFileSystemStore this, @OsUntrusted String key, @OsUntrusted File file, @OsUntrusted byte @OsUntrusted [] md5Hash)
    throws IOException {
    
    @OsUntrusted
    BufferedInputStream in = null;
    try {
      in = new @OsUntrusted BufferedInputStream(new @OsUntrusted FileInputStream(file));
      @OsUntrusted
      S3Object object = new @OsUntrusted S3Object(key);
      object.setDataInputStream(in);
      object.setContentType("binary/octet-stream");
      object.setContentLength(file.length());
      if (md5Hash != null) {
        object.setMd5Hash(md5Hash);
      }
      s3Service.putObject(bucket, object);
    } catch (@OsUntrusted S3ServiceException e) {
      handleServiceException(e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (@OsUntrusted IOException e) {
          // ignore
        }
      }
    }
  }

  @Override
  public void storeEmptyFile(@OsUntrusted Jets3tNativeFileSystemStore this, @OsUntrusted String key) throws IOException {
    try {
      @OsUntrusted
      S3Object object = new @OsUntrusted S3Object(key);
      object.setDataInputStream(new @OsUntrusted ByteArrayInputStream(new @OsUntrusted byte @OsUntrusted [0]));
      object.setContentType("binary/octet-stream");
      object.setContentLength(0);
      s3Service.putObject(bucket, object);
    } catch (@OsUntrusted S3ServiceException e) {
      handleServiceException(e);
    }
  }
  
  @Override
  public @OsUntrusted FileMetadata retrieveMetadata(@OsUntrusted Jets3tNativeFileSystemStore this, @OsUntrusted String key) throws IOException {
    try {
      @OsUntrusted
      S3Object object = s3Service.getObjectDetails(bucket, key);
      return new @OsUntrusted FileMetadata(key, object.getContentLength(),
          object.getLastModifiedDate().getTime());
    } catch (@OsUntrusted S3ServiceException e) {
      // Following is brittle. Is there a better way?
      if (e.getMessage().contains("ResponseCode=404")) {
        return null;
      }
      handleServiceException(e);
      return null; //never returned - keep compiler happy
    }
  }
  
  @Override
  public @OsUntrusted InputStream retrieve(@OsUntrusted Jets3tNativeFileSystemStore this, @OsUntrusted String key) throws IOException {
    try {
      @OsUntrusted
      S3Object object = s3Service.getObject(bucket, key);
      return object.getDataInputStream();
    } catch (@OsUntrusted S3ServiceException e) {
      handleServiceException(key, e);
      return null; //never returned - keep compiler happy
    }
  }
  
  @Override
  public @OsUntrusted InputStream retrieve(@OsUntrusted Jets3tNativeFileSystemStore this, @OsUntrusted String key, @OsUntrusted long byteRangeStart)
    throws IOException {
    try {
      @OsUntrusted
      S3Object object = s3Service.getObject(bucket, key, null, null, null,
                                            null, byteRangeStart, null);
      return object.getDataInputStream();
    } catch (@OsUntrusted S3ServiceException e) {
      handleServiceException(key, e);
      return null; //never returned - keep compiler happy
    }
  }

  @Override
  public @OsUntrusted PartialListing list(@OsUntrusted Jets3tNativeFileSystemStore this, @OsUntrusted String prefix, @OsUntrusted int maxListingLength)
    throws IOException {
    return list(prefix, maxListingLength, null, false);
  }
  
  @Override
  public @OsUntrusted PartialListing list(@OsUntrusted Jets3tNativeFileSystemStore this, @OsUntrusted String prefix, @OsUntrusted int maxListingLength, @OsUntrusted String priorLastKey,
      @OsUntrusted
      boolean recurse) throws IOException {

    return list(prefix, recurse ? null : PATH_DELIMITER, maxListingLength, priorLastKey);
  }


  private @OsUntrusted PartialListing list(@OsUntrusted Jets3tNativeFileSystemStore this, @OsUntrusted String prefix, @OsUntrusted String delimiter,
      @OsUntrusted
      int maxListingLength, @OsUntrusted String priorLastKey) throws IOException {
    try {
      if (prefix.length() > 0 && !prefix.endsWith(PATH_DELIMITER)) {
        prefix += PATH_DELIMITER;
      }
      @OsUntrusted
      S3ObjectsChunk chunk = s3Service.listObjectsChunked(bucket.getName(),
          prefix, delimiter, maxListingLength, priorLastKey);
      
      @OsUntrusted
      FileMetadata @OsUntrusted [] fileMetadata =
        new @OsUntrusted FileMetadata @OsUntrusted [chunk.getObjects().length];
      for (@OsUntrusted int i = 0; i < fileMetadata.length; i++) {
        @OsUntrusted
        S3Object object = chunk.getObjects()[i];
        fileMetadata[i] = new @OsUntrusted FileMetadata(object.getKey(),
            object.getContentLength(), object.getLastModifiedDate().getTime());
      }
      return new @OsUntrusted PartialListing(chunk.getPriorLastKey(), fileMetadata,
          chunk.getCommonPrefixes());
    } catch (@OsUntrusted S3ServiceException e) {
      handleServiceException(e);
      return null; //never returned - keep compiler happy
    }
  }

  @Override
  public void delete(@OsUntrusted Jets3tNativeFileSystemStore this, @OsUntrusted String key) throws IOException {
    try {
      s3Service.deleteObject(bucket, key);
    } catch (@OsUntrusted S3ServiceException e) {
      handleServiceException(key, e);
    }
  }
  
  @Override
  public void copy(@OsUntrusted Jets3tNativeFileSystemStore this, @OsUntrusted String srcKey, @OsUntrusted String dstKey) throws IOException {
    try {
      s3Service.copyObject(bucket.getName(), srcKey, bucket.getName(),
          new @OsUntrusted S3Object(dstKey), false);
    } catch (@OsUntrusted S3ServiceException e) {
      handleServiceException(srcKey, e);
    }
  }

  @Override
  public void purge(@OsUntrusted Jets3tNativeFileSystemStore this, @OsUntrusted String prefix) throws IOException {
    try {
      @OsUntrusted
      S3Object @OsUntrusted [] objects = s3Service.listObjects(bucket, prefix, null);
      for (@OsUntrusted S3Object object : objects) {
        s3Service.deleteObject(bucket, object.getKey());
      }
    } catch (@OsUntrusted S3ServiceException e) {
      handleServiceException(e);
    }
  }

  @Override
  public void dump(@OsUntrusted Jets3tNativeFileSystemStore this) throws IOException {
    @OsUntrusted
    StringBuilder sb = new @OsUntrusted StringBuilder("S3 Native Filesystem, ");
    sb.append(bucket.getName()).append("\n");
    try {
      @OsUntrusted
      S3Object @OsUntrusted [] objects = s3Service.listObjects(bucket);
      for (@OsUntrusted S3Object object : objects) {
        sb.append(object.getKey()).append("\n");
      }
    } catch (@OsUntrusted S3ServiceException e) {
      handleServiceException(e);
    }
    System.out.println(sb);
  }

  private void handleServiceException(@OsUntrusted Jets3tNativeFileSystemStore this, @OsUntrusted String key, @OsUntrusted S3ServiceException e) throws IOException {
    if ("NoSuchKey".equals(e.getS3ErrorCode())) {
      throw new @OsUntrusted FileNotFoundException("Key '" + key + "' does not exist in S3");
    } else {
      handleServiceException(e);
    }
  }

  private void handleServiceException(@OsUntrusted Jets3tNativeFileSystemStore this, @OsUntrusted S3ServiceException e) throws IOException {
    if (e.getCause() instanceof @OsUntrusted IOException) {
      throw (@OsUntrusted IOException) e.getCause();
    }
    else {
      throw new @OsUntrusted S3Exception(e);
    }
  }
}
