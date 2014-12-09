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

package org.apache.hadoop.security.token;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * The client-side form of the token.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class Token<@OsUntrusted T extends @OsUntrusted TokenIdentifier> implements @OsUntrusted Writable {
  public static final @OsUntrusted Log LOG = LogFactory.getLog(Token.class);
  
  private static @OsUntrusted Map<@OsUntrusted Text, @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted TokenIdentifier>> tokenKindMap;
  
  private @OsUntrusted byte @OsUntrusted [] identifier;
  private @OsUntrusted byte @OsUntrusted [] password;
  private @OsUntrusted Text kind;
  private @OsUntrusted Text service;
  private @OsUntrusted TokenRenewer renewer;
  
  /**
   * Construct a token given a token identifier and a secret manager for the
   * type of the token identifier.
   * @param id the token identifier
   * @param mgr the secret manager
   */
  public @OsUntrusted Token(@OsUntrusted T id, @OsUntrusted SecretManager<@OsUntrusted T> mgr) {
    password = mgr.createPassword(id);
    identifier = id.getBytes();
    kind = id.getKind();
    service = new @OsUntrusted Text();
  }
 
  /**
   * Construct a token from the components.
   * @param identifier the token identifier
   * @param password the token's password
   * @param kind the kind of token
   * @param service the service for this token
   */
  public @OsUntrusted Token(@OsUntrusted byte @OsUntrusted [] identifier, @OsUntrusted byte @OsUntrusted [] password, @OsUntrusted Text kind, @OsUntrusted Text service) {
    this.identifier = identifier;
    this.password = password;
    this.kind = kind;
    this.service = service;
  }

  /**
   * Default constructor
   */
  public @OsUntrusted Token() {
    identifier = new @OsUntrusted byte @OsUntrusted [0];
    password = new @OsUntrusted byte @OsUntrusted [0];
    kind = new @OsUntrusted Text();
    service = new @OsUntrusted Text();
  }

  /**
   * Clone a token.
   * @param other the token to clone
   */
  public @OsUntrusted Token(@OsUntrusted Token<@OsUntrusted T> other) {
    this.identifier = other.identifier;
    this.password = other.password;
    this.kind = other.kind;
    this.service = other.service;
  }

  /**
   * Get the token identifier's byte representation
   * @return the token identifier's byte representation
   */
  public @OsUntrusted byte @OsUntrusted [] getIdentifier(@OsUntrusted Token<T> this) {
    return identifier;
  }
  
  private static synchronized @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted TokenIdentifier>
      getClassForIdentifier(@OsUntrusted Text kind) {
    if (tokenKindMap == null) {
      tokenKindMap = Maps.newHashMap();
      for (@OsUntrusted TokenIdentifier id : ServiceLoader.load(TokenIdentifier.class)) {
        tokenKindMap.put(id.getKind(), id.getClass());
      }
    }
    @OsUntrusted
    Class<@OsUntrusted ? extends @OsUntrusted TokenIdentifier> cls = tokenKindMap.get(kind);
    if (cls == null) {
      LOG.warn("Cannot find class for token kind " + kind);
       return null;
    }
    return cls;
  }
  
  /**
   * Get the token identifier object, or null if it could not be constructed
   * (because the class could not be loaded, for example).
   * @return the token identifier, or null
   * @throws IOException 
   */
  @SuppressWarnings("unchecked")
  public @OsUntrusted T decodeIdentifier(@OsUntrusted Token<T> this) throws IOException {
    @OsUntrusted
    Class<@OsUntrusted ? extends @OsUntrusted TokenIdentifier> cls = getClassForIdentifier(getKind());
    if (cls == null) {
      return null;
    }
    @OsUntrusted
    TokenIdentifier tokenIdentifier = ReflectionUtils.newInstance(cls, null);
    @OsUntrusted
    ByteArrayInputStream buf = new @OsUntrusted ByteArrayInputStream(identifier);
    @OsUntrusted
    DataInputStream in = new @OsUntrusted DataInputStream(buf);  
    tokenIdentifier.readFields(in);
    in.close();
    return (T) tokenIdentifier;
  }
  
  /**
   * Get the token password/secret
   * @return the token password/secret
   */
  public @OsUntrusted byte @OsUntrusted [] getPassword(@OsUntrusted Token<T> this) {
    return password;
  }
  
  /**
   * Get the token kind
   * @return the kind of the token
   */
  public synchronized @OsUntrusted Text getKind(@OsUntrusted Token<T> this) {
    return kind;
  }

  /**
   * Set the token kind. This is only intended to be used by services that
   * wrap another service's token, such as HFTP wrapping HDFS.
   * @param newKind
   */
  @InterfaceAudience.Private
  public synchronized void setKind(@OsUntrusted Token<T> this, @OsUntrusted Text newKind) {
    kind = newKind;
    renewer = null;
  }

  /**
   * Get the service on which the token is supposed to be used
   * @return the service name
   */
  public @OsUntrusted Text getService(@OsUntrusted Token<T> this) {
    return service;
  }
  
  /**
   * Set the service on which the token is supposed to be used
   * @param newService the service name
   */
  public void setService(@OsUntrusted Token<T> this, @OsUntrusted Text newService) {
    service = newService;
  }

  /**
   * Indicates whether the token is a clone.  Used by HA failover proxy
   * to indicate a token should not be visible to the user via
   * UGI.getCredentials()
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class PrivateToken<@OsUntrusted T extends @OsUntrusted TokenIdentifier> extends @OsUntrusted Token<T> {
    public @OsUntrusted PrivateToken(@OsUntrusted Token<@OsUntrusted T> token) {
      super(token);
    }
  }

  @Override
  public void readFields(@OsUntrusted Token<T> this, @OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    int len = WritableUtils.readVInt(in);
    if (identifier == null || identifier.length != len) {
      identifier = new @OsUntrusted byte @OsUntrusted [len];
    }
    in.readFully(identifier);
    len = WritableUtils.readVInt(in);
    if (password == null || password.length != len) {
      password = new @OsUntrusted byte @OsUntrusted [len];
    }
    in.readFully(password);
    kind.readFields(in);
    service.readFields(in);
  }

  @Override
  public void write(@OsUntrusted Token<T> this, @OsUntrusted DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, identifier.length);
    out.write(identifier);
    WritableUtils.writeVInt(out, password.length);
    out.write(password);
    kind.write(out);
    service.write(out);
  }

  /**
   * Generate a string with the url-quoted base64 encoded serialized form
   * of the Writable.
   * @param obj the object to serialize
   * @return the encoded string
   * @throws IOException
   */
  private static @OsUntrusted String encodeWritable(@OsUntrusted Writable obj) throws IOException {
    @OsUntrusted
    DataOutputBuffer buf = new @OsUntrusted DataOutputBuffer();
    obj.write(buf);
    @OsUntrusted
    Base64 encoder = new @OsUntrusted Base64(0, null, true);
    @OsUntrusted
    byte @OsUntrusted [] raw = new @OsUntrusted byte @OsUntrusted [buf.getLength()];
    System.arraycopy(buf.getData(), 0, raw, 0, buf.getLength());
    return encoder.encodeToString(raw);
  }
  
  /**
   * Modify the writable to the value from the newValue
   * @param obj the object to read into
   * @param newValue the string with the url-safe base64 encoded bytes
   * @throws IOException
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  private static void decodeWritable(@OsUntrusted Writable obj, 
                                     @OsTrusted String newValue) throws IOException {
    @OsUntrusted
    Base64 decoder = new @OsUntrusted Base64(0, null, true);

    DataInputBuffer buf = new DataInputBuffer();
    @OsUntrusted byte @OsUntrusted [] decoded = decoder.decode(newValue);
    buf.reset(decoded, decoded.length);

    //safe because we just read from newValue a trusted value, included manual inspection of DataInputBuffer
    //which is backed by a byteArrayInputStream
    obj.readFields((@OsTrusted DataInputBuffer)  buf);
  }

  /**
   * Encode this token as a url safe string
   * @return the encoded string
   * @throws IOException
   */
  public @OsUntrusted String encodeToUrlString(@OsUntrusted Token<T> this) throws IOException {
    return encodeWritable(this);
  }
  
  /**
   * Decode the given url safe string into this token.
   * @param newValue the encoded string
   * @throws IOException
   */
  //ostrusted this might be to stringent for the given API
  public void decodeFromUrlString(@OsUntrusted Token<T> this, @OsTrusted String newValue) throws IOException {
    decodeWritable(this, newValue);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public @OsUntrusted boolean equals(@OsUntrusted Token<T> this, @OsUntrusted Object right) {
    if (this == right) {
      return true;
    } else if (right == null || getClass() != right.getClass()) {
      return false;
    } else {
      @OsUntrusted
      Token<T> r = (@OsUntrusted Token<T>) right;
      return Arrays.equals(identifier, r.identifier) &&
             Arrays.equals(password, r.password) &&
             kind.equals(r.kind) &&
             service.equals(r.service);
    }
  }
  
  @Override
  public @OsUntrusted int hashCode(@OsUntrusted Token<T> this) {
    return WritableComparator.hashBytes(identifier, identifier.length);
  }
  
  private static void addBinaryBuffer(@OsUntrusted StringBuilder buffer, @OsUntrusted byte @OsUntrusted [] bytes) {
    for (@OsUntrusted int idx = 0; idx < bytes.length; idx++) {
      // if not the first, put a blank separator in
      if (idx != 0) {
        buffer.append(' ');
      }
      @OsUntrusted
      String num = Integer.toHexString(0xff & bytes[idx]);
      // if it is only one digit, add a leading 0.
      if (num.length() < 2) {
        buffer.append('0');
      }
      buffer.append(num);
    }
  }
  
  private void identifierToString(@OsUntrusted Token<T> this, @OsUntrusted StringBuilder buffer) {
    T id = null;
    try {
      id = decodeIdentifier();
    } catch (@OsUntrusted IOException e) {
      // handle in the finally block
    } finally {
      if (id != null) {
        buffer.append("(").append(id).append(")");
      } else {
        addBinaryBuffer(buffer, identifier);
      }
    }
  }

  @Override
  public @OsUntrusted String toString(@OsUntrusted Token<T> this) {
    @OsUntrusted
    StringBuilder buffer = new @OsUntrusted StringBuilder();
    buffer.append("Kind: ");
    buffer.append(kind.toString());
    buffer.append(", Service: ");
    buffer.append(service.toString());
    buffer.append(", Ident: ");
    identifierToString(buffer);
    return buffer.toString();
  }
  
  private static @OsUntrusted ServiceLoader<@OsUntrusted TokenRenewer> renewers =
      ServiceLoader.load(TokenRenewer.class);

  private synchronized @OsUntrusted TokenRenewer getRenewer(@OsUntrusted Token<T> this) throws IOException {
    if (renewer != null) {
      return renewer;
    }
    renewer = TRIVIAL_RENEWER;
    synchronized (renewers) {
      for (@OsUntrusted TokenRenewer canidate : renewers) {
        if (canidate.handleKind(this.kind)) {
          renewer = canidate;
          return renewer;
        }
      }
    }
    LOG.warn("No TokenRenewer defined for token kind " + this.kind);
    return renewer;
  }

  /**
   * Is this token managed so that it can be renewed or cancelled?
   * @return true, if it can be renewed and cancelled.
   */
  public @OsUntrusted boolean isManaged(@OsUntrusted Token<T> this) throws IOException {
    return getRenewer().isManaged(this);
  }

  /**
   * Renew this delegation token
   * @return the new expiration time
   * @throws IOException
   * @throws InterruptedException
   */
  public @OsUntrusted long renew(@OsUntrusted Token<T> this, @OsUntrusted Configuration conf
                    ) throws IOException, InterruptedException {
    return getRenewer().renew(this, conf);
  }
  
  /**
   * Cancel this delegation token
   * @throws IOException
   * @throws InterruptedException
   */
  public void cancel(@OsUntrusted Token<T> this, @OsUntrusted Configuration conf
                     ) throws IOException, InterruptedException {
    getRenewer().cancel(this, conf);
  }
  
  /**
   * A trivial renewer for token kinds that aren't managed. Sub-classes need
   * to implement getKind for their token kind.
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  @InterfaceStability.Evolving
  public static class TrivialRenewer extends @OsUntrusted TokenRenewer {
    
    // define the kind for this renewer
    protected @OsUntrusted Text getKind(Token.@OsUntrusted TrivialRenewer this) {
      return null;
    }

    @Override
    public @OsUntrusted boolean handleKind(Token.@OsUntrusted TrivialRenewer this, @OsUntrusted Text kind) {
      return kind.equals(getKind());
    }

    @Override
    public @OsUntrusted boolean isManaged(Token.@OsUntrusted TrivialRenewer this, @OsUntrusted Token<@OsUntrusted ? extends java.lang.@OsUntrusted Object> token) {
      return false;
    }

    @Override
    public @OsUntrusted long renew(Token.@OsUntrusted TrivialRenewer this, @OsUntrusted Token<@OsUntrusted ? extends java.lang.@OsUntrusted Object> token, @OsUntrusted Configuration conf) {
      throw new @OsUntrusted UnsupportedOperationException("Token renewal is not supported "+
                                              " for " + token.kind + " tokens");
    }

    @Override
    public void cancel(Token.@OsUntrusted TrivialRenewer this, @OsUntrusted Token<@OsUntrusted ? extends java.lang.@OsUntrusted Object> token, @OsUntrusted Configuration conf) throws IOException,
        InterruptedException {
      throw new @OsUntrusted UnsupportedOperationException("Token cancel is not supported " +
          " for " + token.kind + " tokens");
    }

  }
  private static final @OsUntrusted TokenRenewer TRIVIAL_RENEWER = new @OsUntrusted TrivialRenewer();
}
