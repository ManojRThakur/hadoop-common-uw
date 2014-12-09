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
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * <p>
 * Extracts AWS credentials from the filesystem URI or configuration.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class S3Credentials {
  
  private @OsUntrusted String accessKey;
  private @OsUntrusted String secretAccessKey; 

  /**
   * @throws IllegalArgumentException if credentials for S3 cannot be
   * determined.
   */
  public void initialize(@OsUntrusted S3Credentials this, @OsUntrusted URI uri, @OsUntrusted Configuration conf) {
    if (uri.getHost() == null) {
      throw new @OsUntrusted IllegalArgumentException("Invalid hostname in URI " + uri);
    }
    
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
    
    @OsUntrusted
    String scheme = uri.getScheme();
    @OsUntrusted
    String accessKeyProperty = String.format("fs.%s.awsAccessKeyId", scheme);
    @OsUntrusted
    String secretAccessKeyProperty =
      String.format("fs.%s.awsSecretAccessKey", scheme);
    if (accessKey == null) {
      accessKey = conf.get(accessKeyProperty);
    }
    if (secretAccessKey == null) {
      secretAccessKey = conf.get(secretAccessKeyProperty);
    }
    if (accessKey == null && secretAccessKey == null) {
      throw new @OsUntrusted IllegalArgumentException("AWS " +
                                         "Access Key ID and Secret Access " +
                                         "Key must be specified as the " +
                                         "username or password " +
                                         "(respectively) of a " + scheme +
                                         " URL, or by setting the " +
                                         accessKeyProperty + " or " +
                                         secretAccessKeyProperty +
                                         " properties (respectively).");
    } else if (accessKey == null) {
      throw new @OsUntrusted IllegalArgumentException("AWS " +
                                         "Access Key ID must be specified " +
                                         "as the username of a " + scheme +
                                         " URL, or by setting the " +
                                         accessKeyProperty + " property.");
    } else if (secretAccessKey == null) {
      throw new @OsUntrusted IllegalArgumentException("AWS " +
                                         "Secret Access Key must be " +
                                         "specified as the password of a " +
                                         scheme + " URL, or by setting the " +
                                         secretAccessKeyProperty +
                                         " property.");       
    }

  }
  
  public @OsUntrusted String getAccessKey(@OsUntrusted S3Credentials this) {
    return accessKey;
  }
  
  public @OsUntrusted String getSecretAccessKey(@OsUntrusted S3Credentials this) {
    return secretAccessKey;
  }
}