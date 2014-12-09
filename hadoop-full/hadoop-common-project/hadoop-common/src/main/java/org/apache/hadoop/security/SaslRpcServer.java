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
import ostrusted.quals.OsTrusted;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.security.Security;
import java.util.Map;
import java.util.TreeMap;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.Server.Connection;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * A utility class for dealing with SASL on RPC server
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SaslRpcServer {
  public static final @OsUntrusted Log LOG = LogFactory.getLog(SaslRpcServer.class);
  public static final @OsUntrusted String SASL_DEFAULT_REALM = "default";
  public static final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> SASL_PROPS = 
      new @OsUntrusted TreeMap<@OsUntrusted String, @OsUntrusted String>();

  public static enum QualityOfProtection {

@OsUntrusted  AUTHENTICATION("auth"),

@OsUntrusted  INTEGRITY("auth-int"),

@OsUntrusted  PRIVACY("auth-conf");
    
    public final @OsUntrusted String saslQop;
    
    private @OsUntrusted QualityOfProtection(@OsUntrusted String saslQop) {
      this.saslQop = saslQop;
    }
    
    public @OsUntrusted String getSaslQop(SaslRpcServer.@OsUntrusted QualityOfProtection this) {
      return saslQop;
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public @OsUntrusted AuthMethod authMethod;
  public @OsUntrusted String mechanism;
  public @OsUntrusted String protocol;
  public @OsUntrusted String serverId;
  
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public @OsUntrusted SaslRpcServer(@OsUntrusted AuthMethod authMethod) throws IOException {
    this.authMethod = authMethod;
    mechanism = authMethod.getMechanismName();    
    switch (authMethod) {
      case SIMPLE: {
        return; // no sasl for simple
      }
      case TOKEN: {
        protocol = "";
        serverId = SaslRpcServer.SASL_DEFAULT_REALM;
        break;
      }
      case KERBEROS: {
        @OsUntrusted
        String fullName = UserGroupInformation.getCurrentUser().getUserName();
        if (LOG.isDebugEnabled())
          LOG.debug("Kerberos principal name is " + fullName);
        // don't use KerberosName because we don't want auth_to_local
        @OsUntrusted
        String @OsUntrusted [] parts = fullName.split("[/@]", 3);
        protocol = parts[0];
        // should verify service host is present here rather than in create()
        // but lazy tests are using a UGI that isn't a SPN...
        serverId = (parts.length < 2) ? "" : parts[1];
        break;
      }
      default:
        // we should never be able to get here
        throw new @OsUntrusted AccessControlException(
            "Server does not support SASL " + authMethod);
    }
  }
  
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public @OsUntrusted SaslServer create(@OsUntrusted SaslRpcServer this, @OsUntrusted Connection connection,
                           @OsUntrusted
                           SecretManager<@OsUntrusted TokenIdentifier> secretManager
      ) throws IOException, InterruptedException {
    @OsUntrusted
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    final @OsUntrusted CallbackHandler callback;
    switch (authMethod) {
      case TOKEN: {
        callback = new @OsUntrusted SaslDigestCallbackHandler(secretManager, connection);
        break;
      }
      case KERBEROS: {
        if (serverId.isEmpty()) {
          throw new @OsUntrusted AccessControlException(
              "Kerberos principal name does NOT have the expected "
                  + "hostname part: " + ugi.getUserName());
        }
        callback = new @OsUntrusted SaslGssCallbackHandler();
        break;
      }
      default:
        // we should never be able to get here
        throw new @OsUntrusted AccessControlException(
            "Server does not support SASL " + authMethod);
    }
    
    @OsUntrusted
    SaslServer saslServer = ugi.doAs(
        new @OsUntrusted PrivilegedExceptionAction<@OsUntrusted SaslServer>() {
          @Override
          public @OsUntrusted SaslServer run() throws SaslException  {
            return Sasl.createSaslServer(mechanism, protocol, serverId,
                SaslRpcServer.SASL_PROPS, callback);
          }
        });
    if (saslServer == null) {
      throw new @OsUntrusted AccessControlException(
          "Unable to find SASL server implementation for " + mechanism);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created SASL server with mechanism = " + mechanism);
    }
    return saslServer;
  }

  public static void init(@OsUntrusted Configuration conf) {
    @OsUntrusted
    QualityOfProtection saslQOP = QualityOfProtection.AUTHENTICATION;
    @OsUntrusted
    String rpcProtection = conf.get("hadoop.rpc.protection",
        QualityOfProtection.AUTHENTICATION.name().toLowerCase());
    if (QualityOfProtection.INTEGRITY.name().toLowerCase()
        .equals(rpcProtection)) {
      saslQOP = QualityOfProtection.INTEGRITY;
    } else if (QualityOfProtection.PRIVACY.name().toLowerCase().equals(
        rpcProtection)) {
      saslQOP = QualityOfProtection.PRIVACY;
    }
    
    SASL_PROPS.put(Sasl.QOP, saslQOP.getSaslQop());
    SASL_PROPS.put(Sasl.SERVER_AUTH, "true");
    Security.addProvider(new SaslPlainServer.@OsUntrusted SecurityProvider());
  }
  
  static @OsUntrusted String encodeIdentifier(@OsUntrusted byte @OsUntrusted [] identifier) {
    return new @OsUntrusted String(Base64.encodeBase64(identifier));
  }

  static @OsUntrusted byte @OsUntrusted [] decodeIdentifier(@OsUntrusted String identifier) {
    return Base64.decodeBase64(identifier.getBytes());
  }

  public static <@OsUntrusted T extends @OsUntrusted TokenIdentifier> @OsUntrusted T getIdentifier(@OsUntrusted String id,
      @OsUntrusted
      SecretManager<@OsUntrusted T> secretManager) throws InvalidToken {
    @OsUntrusted
    byte @OsUntrusted [] tokenId = decodeIdentifier(id);
    @OsUntrusted
    T tokenIdentifier = secretManager.createIdentifier();
    try {
      tokenIdentifier.readFields(new @OsUntrusted DataInputStream(new @OsUntrusted ByteArrayInputStream(
          tokenId)));
    } catch (@OsUntrusted IOException e) {
      throw (@OsUntrusted InvalidToken) new @OsUntrusted InvalidToken(
          "Can't de-serialize tokenIdentifier").initCause(e);
    }
    return tokenIdentifier;
  }

  static @OsUntrusted char @OsUntrusted [] encodePassword(@OsUntrusted byte @OsUntrusted [] password) {
    return new @OsUntrusted String(Base64.encodeBase64(password)).toCharArray();
  }

  /** Splitting fully qualified Kerberos name into parts */
  public static @OsUntrusted String @OsUntrusted [] splitKerberosName(@OsUntrusted String fullName) {
    return fullName.split("[/@]");
  }

  /** Authentication method */
  @InterfaceStability.Evolving
  public static enum AuthMethod {

@OsUntrusted  SIMPLE((@OsTrusted byte) 80, ""),

@OsUntrusted  KERBEROS((@OsTrusted byte) 81, "GSSAPI"),
    @Deprecated

@OsUntrusted  DIGEST((@OsTrusted byte) 82, "DIGEST-MD5"),

@OsUntrusted  TOKEN((@OsTrusted byte) 82, "DIGEST-MD5"),

@OsUntrusted  PLAIN((@OsTrusted byte) 83, "PLAIN");

    /** The code for this method. */
    public final @OsTrusted byte code;
    public final @OsTrusted String mechanismName;

    private @OsUntrusted AuthMethod(@OsTrusted byte code, @OsTrusted String mechanismName) {
      this.code = code;
      this.mechanismName = mechanismName;
    }

    private static final @OsUntrusted int FIRST_CODE = values()[0].code;

    /** Return the object represented by the code. */
    private static @OsUntrusted AuthMethod valueOf(byte code) {
      final @OsUntrusted int i = (code & 0xff) - FIRST_CODE;
      return i < 0 || i >= values().length ? null : values()[i];
    }

    /** Return the SASL mechanism name */
    public @OsTrusted String getMechanismName(SaslRpcServer.@OsUntrusted AuthMethod this) {
      return mechanismName;
    }

    /** Read from in */
    public static @OsUntrusted AuthMethod read(@OsUntrusted DataInput in) throws IOException {
      return valueOf(in.readByte());
    }

    /** Write to out */
    public void write(SaslRpcServer.@OsUntrusted AuthMethod this, @OsUntrusted DataOutput out) throws IOException {
      out.write(code);
    }
  };

  /** CallbackHandler for SASL DIGEST-MD5 mechanism */
  @InterfaceStability.Evolving
  public static class SaslDigestCallbackHandler implements @OsUntrusted CallbackHandler {
    private @OsUntrusted SecretManager<@OsUntrusted TokenIdentifier> secretManager;
    private @OsUntrusted Server.@OsUntrusted Connection connection; 
    
    public @OsUntrusted SaslDigestCallbackHandler(
        @OsUntrusted
        SecretManager<@OsUntrusted TokenIdentifier> secretManager,
        @OsUntrusted
        Server.@OsUntrusted Connection connection) {
      this.secretManager = secretManager;
      this.connection = connection;
    }

    private @OsUntrusted char @OsUntrusted [] getPassword(SaslRpcServer.@OsUntrusted SaslDigestCallbackHandler this, @OsUntrusted TokenIdentifier tokenid) throws InvalidToken,
        StandbyException, RetriableException, IOException {
      return encodePassword(secretManager.retriableRetrievePassword(tokenid));
    }

    @Override
    public void handle(SaslRpcServer.@OsUntrusted SaslDigestCallbackHandler this, @OsUntrusted Callback @OsUntrusted [] callbacks) throws InvalidToken,
        UnsupportedCallbackException, StandbyException, RetriableException,
        IOException {
      @OsUntrusted
      NameCallback nc = null;
      @OsUntrusted
      PasswordCallback pc = null;
      @OsUntrusted
      AuthorizeCallback ac = null;
      for (@OsUntrusted Callback callback : callbacks) {
        if (callback instanceof @OsUntrusted AuthorizeCallback) {
          ac = (@OsUntrusted AuthorizeCallback) callback;
        } else if (callback instanceof @OsUntrusted NameCallback) {
          nc = (@OsUntrusted NameCallback) callback;
        } else if (callback instanceof @OsUntrusted PasswordCallback) {
          pc = (@OsUntrusted PasswordCallback) callback;
        } else if (callback instanceof @OsUntrusted RealmCallback) {
          continue; // realm is ignored
        } else {
          throw new @OsUntrusted UnsupportedCallbackException(callback,
              "Unrecognized SASL DIGEST-MD5 Callback");
        }
      }
      if (pc != null) {
        @OsUntrusted
        TokenIdentifier tokenIdentifier = getIdentifier(nc.getDefaultName(),
            secretManager);
        @OsUntrusted
        char @OsUntrusted [] password = getPassword(tokenIdentifier);
        @OsUntrusted
        UserGroupInformation user = null;
        user = tokenIdentifier.getUser(); // may throw exception
        connection.attemptingUser = user;
        
        if (LOG.isDebugEnabled()) {
          LOG.debug("SASL server DIGEST-MD5 callback: setting password "
              + "for client: " + tokenIdentifier.getUser());
        }
        pc.setPassword(password);
      }
      if (ac != null) {
        @OsUntrusted
        String authid = ac.getAuthenticationID();
        @OsUntrusted
        String authzid = ac.getAuthorizationID();
        if (authid.equals(authzid)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          if (LOG.isDebugEnabled()) {
            @OsUntrusted
            String username =
              getIdentifier(authzid, secretManager).getUser().getUserName();
            LOG.debug("SASL server DIGEST-MD5 callback: setting "
                + "canonicalized client ID: " + username);
          }
          ac.setAuthorizedID(authzid);
        }
      }
    }
  }

  /** CallbackHandler for SASL GSSAPI Kerberos mechanism */
  @InterfaceStability.Evolving
  public static class SaslGssCallbackHandler implements @OsUntrusted CallbackHandler {

    @Override
    public void handle(SaslRpcServer.@OsUntrusted SaslGssCallbackHandler this, @OsUntrusted Callback @OsUntrusted [] callbacks) throws
        UnsupportedCallbackException {
      @OsUntrusted
      AuthorizeCallback ac = null;
      for (@OsUntrusted Callback callback : callbacks) {
        if (callback instanceof @OsUntrusted AuthorizeCallback) {
          ac = (@OsUntrusted AuthorizeCallback) callback;
        } else {
          throw new @OsUntrusted UnsupportedCallbackException(callback,
              "Unrecognized SASL GSSAPI Callback");
        }
      }
      if (ac != null) {
        @OsUntrusted
        String authid = ac.getAuthenticationID();
        @OsUntrusted
        String authzid = ac.getAuthorizationID();
        if (authid.equals(authzid)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          if (LOG.isDebugEnabled())
            LOG.debug("SASL server GSSAPI callback: setting "
                + "canonicalized client ID: " + authzid);
          ac.setAuthorizedID(authzid);
        }
      }
    }
  }
}
