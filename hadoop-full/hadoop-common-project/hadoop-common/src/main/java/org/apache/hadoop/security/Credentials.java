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
import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * A class that provides the facilities of reading and writing 
 * secret keys and Tokens.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class Credentials implements @OsUntrusted Writable {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(Credentials.class);

  private  @OsUntrusted Map<@OsUntrusted Text, @OsUntrusted byte @OsUntrusted []> secretKeysMap = new @OsUntrusted HashMap<@OsUntrusted Text, @OsUntrusted byte @OsUntrusted []>();
  private  @OsUntrusted Map<@OsUntrusted Text, @OsUntrusted Token<@OsUntrusted ? extends @OsUntrusted TokenIdentifier>> tokenMap = 
    new @OsUntrusted HashMap<@OsUntrusted Text, @OsUntrusted Token<@OsUntrusted ? extends @OsUntrusted TokenIdentifier>>(); 

  /**
   * Create an empty credentials instance
   */
  public @OsUntrusted Credentials() {
  }
  
  /**
   * Create a copy of the given credentials
   * @param credentials to copy
   */
  public @OsUntrusted Credentials(@OsUntrusted Credentials credentials) {
    this.addAll(credentials);
  }
  
  /**
   * Returns the key bytes for the alias
   * @param alias the alias for the key
   * @return key for this alias
   */
  public @OsUntrusted byte @OsUntrusted [] getSecretKey(@OsUntrusted Credentials this, @OsUntrusted Text alias) {
    return secretKeysMap.get(alias);
  }
  
  /**
   * Returns the Token object for the alias
   * @param alias the alias for the Token
   * @return token for this alias
   */
  public @OsUntrusted Token<@OsUntrusted ? extends @OsUntrusted TokenIdentifier> getToken(@OsUntrusted Credentials this, @OsUntrusted Text alias) {
    return tokenMap.get(alias);
  }
  
  /**
   * Add a token in the storage (in memory)
   * @param alias the alias for the key
   * @param t the token object
   */
  public void addToken(@OsUntrusted Credentials this, @OsUntrusted Text alias, @OsUntrusted Token<@OsUntrusted ? extends @OsUntrusted TokenIdentifier> t) {
    if (t != null) {
      tokenMap.put(alias, t);
    } else {
      LOG.warn("Null token ignored for " + alias);
    }
  }
  
  /**
   * Return all the tokens in the in-memory map
   */
  public @OsUntrusted Collection<@OsUntrusted Token<@OsUntrusted ? extends @OsUntrusted TokenIdentifier>> getAllTokens(@OsUntrusted Credentials this) {
    return tokenMap.values();
  }
  
  /**
   * @return number of Tokens in the in-memory map
   */
  public @OsUntrusted int numberOfTokens(@OsUntrusted Credentials this) {
    return tokenMap.size();
  }
  
  /**
   * @return number of keys in the in-memory map
   */
  public @OsUntrusted int numberOfSecretKeys(@OsUntrusted Credentials this) {
    return secretKeysMap.size();
  }
  
  /**
   * Set the key for an alias
   * @param alias the alias for the key
   * @param key the key bytes
   */
  public void addSecretKey(@OsUntrusted Credentials this, @OsUntrusted Text alias, @OsUntrusted byte @OsUntrusted [] key) {
    secretKeysMap.put(alias, key);
  }
 
  /**
   * Convenience method for reading a token storage file, and loading the Tokens
   * therein in the passed UGI
   * @param filename
   * @param conf
   * @throws IOException
   */
  public static @OsUntrusted Credentials readTokenStorageFile(@OsUntrusted Path filename, @OsUntrusted Configuration conf)
  throws IOException {
    @OsUntrusted
    FSDataInputStream in = null;
    @OsUntrusted
    Credentials credentials = new @OsUntrusted Credentials();
    try {
      in = filename.getFileSystem(conf).open(filename);
      credentials.readTokenStorageStream(in);
      in.close();
      return credentials;
    } catch(@OsUntrusted IOException ioe) {
      throw new @OsUntrusted IOException("Exception reading " + filename, ioe);
    } finally {
      IOUtils.cleanup(LOG, in);
    }
  }

  /**
   * Convenience method for reading a token storage file, and loading the Tokens
   * therein in the passed UGI
   * @param filename
   * @param conf
   * @throws IOException
   */
  public static @OsUntrusted Credentials readTokenStorageFile(@OsUntrusted File filename, @OsUntrusted Configuration conf)
      throws IOException {
    @OsUntrusted
    DataInputStream in = null;
    @OsUntrusted
    Credentials credentials = new @OsUntrusted Credentials();
    try {
      in = new @OsUntrusted DataInputStream(new @OsUntrusted BufferedInputStream(
          new @OsUntrusted FileInputStream(filename)));
      credentials.readTokenStorageStream(in);
      return credentials;
    } catch(@OsUntrusted IOException ioe) {
      throw new @OsUntrusted IOException("Exception reading " + filename, ioe);
    } finally {
      IOUtils.cleanup(LOG, in);
    }
  }
  
  /**
   * Convenience method for reading a token storage file directly from a 
   * datainputstream
   */
  public void readTokenStorageStream(@OsUntrusted Credentials this, @OsUntrusted DataInputStream in) throws IOException {
    @OsUntrusted
    byte @OsUntrusted [] magic = new @OsUntrusted byte @OsUntrusted [TOKEN_STORAGE_MAGIC.length];
    in.readFully(magic);
    if (!Arrays.equals(magic, TOKEN_STORAGE_MAGIC)) {
      throw new @OsUntrusted IOException("Bad header found in token storage.");
    }
    @OsUntrusted
    byte version = in.readByte();
    if (version != TOKEN_STORAGE_VERSION) {
      throw new @OsUntrusted IOException("Unknown version " + version + 
                            " in token storage.");
    }
    readFields(in);
  }
  
  private static final @OsUntrusted byte @OsUntrusted [] TOKEN_STORAGE_MAGIC = "HDTS".getBytes();
  private static final @OsUntrusted byte TOKEN_STORAGE_VERSION = 0;
  
  public void writeTokenStorageToStream(@OsUntrusted Credentials this, @OsUntrusted DataOutputStream os)
    throws IOException {
    os.write(TOKEN_STORAGE_MAGIC);
    os.write(TOKEN_STORAGE_VERSION);
    write(os);
  }

  public void writeTokenStorageFile(@OsUntrusted Credentials this, @OsUntrusted Path filename, 
                                    @OsUntrusted
                                    Configuration conf) throws IOException {
    @OsUntrusted
    FSDataOutputStream os = filename.getFileSystem(conf).create(filename);
    writeTokenStorageToStream(os);
    os.close();
  }

  /**
   * Stores all the keys to DataOutput
   * @param out
   * @throws IOException
   */
  @Override
  public void write(@OsUntrusted Credentials this, @OsUntrusted DataOutput out) throws IOException {
    // write out tokens first
    WritableUtils.writeVInt(out, tokenMap.size());
    for(Map.@OsUntrusted Entry<@OsUntrusted Text, 
        @OsUntrusted
        Token<@OsUntrusted ? extends @OsUntrusted TokenIdentifier>> e: tokenMap.entrySet()) {
      e.getKey().write(out);
      e.getValue().write(out);
    }
    
    // now write out secret keys
    WritableUtils.writeVInt(out, secretKeysMap.size());
    for(Map.@OsUntrusted Entry<@OsUntrusted Text, @OsUntrusted byte @OsUntrusted []> e : secretKeysMap.entrySet()) {
      e.getKey().write(out);
      WritableUtils.writeVInt(out, e.getValue().length);
      out.write(e.getValue());
    }
  }
  
  /**
   * Loads all the keys
   * @param in
   * @throws IOException
   */
  @Override
  public void readFields(@OsUntrusted Credentials this, @OsUntrusted DataInput in) throws IOException {
    secretKeysMap.clear();
    tokenMap.clear();
    
    @OsUntrusted
    int size = WritableUtils.readVInt(in);
    for(@OsUntrusted int i=0; i<size; i++) {
      @OsUntrusted
      Text alias = new @OsUntrusted Text();
      alias.readFields(in);
      @OsUntrusted
      Token<@OsUntrusted ? extends @OsUntrusted TokenIdentifier> t = new @OsUntrusted Token<@OsUntrusted TokenIdentifier>();
      t.readFields(in);
      tokenMap.put(alias, t);
    }
    
    size = WritableUtils.readVInt(in);
    for(@OsUntrusted int i=0; i<size; i++) {
      @OsUntrusted
      Text alias = new @OsUntrusted Text();
      alias.readFields(in);
      @OsUntrusted
      int len = WritableUtils.readVInt(in);
      @OsUntrusted
      byte @OsUntrusted [] value = new @OsUntrusted byte @OsUntrusted [len];
      in.readFully(value);
      secretKeysMap.put(alias, value);
    }
  }
 
  /**
   * Copy all of the credentials from one credential object into another.
   * Existing secrets and tokens are overwritten.
   * @param other the credentials to copy
   */
  public void addAll(@OsUntrusted Credentials this, @OsUntrusted Credentials other) {
    addAll(other, true);
  }

  /**
   * Copy all of the credentials from one credential object into another.
   * Existing secrets and tokens are not overwritten.
   * @param other the credentials to copy
   */
  public void mergeAll(@OsUntrusted Credentials this, @OsUntrusted Credentials other) {
    addAll(other, false);
  }

  private void addAll(@OsUntrusted Credentials this, @OsUntrusted Credentials other, @OsUntrusted boolean overwrite) {
    for(Map.@OsUntrusted Entry<@OsUntrusted Text, @OsUntrusted byte @OsUntrusted []> secret: other.secretKeysMap.entrySet()) {
      @OsUntrusted
      Text key = secret.getKey();
      if (!secretKeysMap.containsKey(key) || overwrite) {
        secretKeysMap.put(key, secret.getValue());
      }
    }
    for(Map.@OsUntrusted Entry<@OsUntrusted Text, @OsUntrusted Token<@OsUntrusted ? extends @OsUntrusted TokenIdentifier>> token: other.tokenMap.entrySet()){
      @OsUntrusted
      Text key = token.getKey();
      if (!tokenMap.containsKey(key) || overwrite) {
        tokenMap.put(key, token.getValue());
      }
    }
  }
}
