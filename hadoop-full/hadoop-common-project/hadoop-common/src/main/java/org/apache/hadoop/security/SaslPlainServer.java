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
import java.security.Provider;
import java.util.Map;

import javax.security.auth.callback.*;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SaslPlainServer implements @OsUntrusted SaslServer {
  @SuppressWarnings("serial")
  public static class SecurityProvider extends @OsUntrusted Provider {
    public @OsUntrusted SecurityProvider() {
      super("SaslPlainServer", 1.0, "SASL PLAIN Authentication Server");
      put("SaslServerFactory.PLAIN",
          SaslPlainServerFactory.class.getName());
    }
  }

  public static class SaslPlainServerFactory implements @OsUntrusted SaslServerFactory {
    @Override
    public @OsUntrusted SaslServer createSaslServer(SaslPlainServer.@OsUntrusted SaslPlainServerFactory this, @OsUntrusted String mechanism, @OsUntrusted String protocol,
        @OsUntrusted
        String serverName, @OsUntrusted Map<@OsUntrusted String, @OsUntrusted ? extends java.lang.@OsUntrusted Object> props, @OsUntrusted CallbackHandler cbh)
            throws SaslException {
      return "PLAIN".equals(mechanism) ? new @OsUntrusted SaslPlainServer(cbh) : null; 
    }
    @Override
    public @OsUntrusted String @OsUntrusted [] getMechanismNames(SaslPlainServer.@OsUntrusted SaslPlainServerFactory this, @OsUntrusted Map<@OsUntrusted String, @OsUntrusted ? extends java.lang.@OsUntrusted Object> props){
      return (props == null) || "false".equals(props.get(Sasl.POLICY_NOPLAINTEXT))
          ? new @OsUntrusted String @OsUntrusted []{"PLAIN"}
          : new @OsUntrusted String @OsUntrusted [0];
    }
  }
  
  private @OsUntrusted CallbackHandler cbh;
  private @OsUntrusted boolean completed;
  private @OsUntrusted String authz;
  
  @OsUntrusted
  SaslPlainServer(@OsUntrusted CallbackHandler callback) {
    this.cbh = callback;
  }

  @Override
  public @OsUntrusted String getMechanismName(@OsUntrusted SaslPlainServer this) {
    return "PLAIN";
  }
  
  @Override
  public @OsUntrusted byte @OsUntrusted [] evaluateResponse(@OsUntrusted SaslPlainServer this, @OsUntrusted byte @OsUntrusted [] response) throws SaslException {
    if (completed) {
      throw new @OsUntrusted IllegalStateException("PLAIN authentication has completed");
    }
    if (response == null) {
      throw new @OsUntrusted IllegalArgumentException("Received null response");
    }
    try {
      @OsUntrusted
      String payload;
      try {
        payload = new @OsUntrusted String(response, "UTF-8");
      } catch (@OsUntrusted Exception e) {
        throw new @OsUntrusted IllegalArgumentException("Received corrupt response", e);
      }
      // [ authz, authn, password ]
      @OsUntrusted
      String @OsUntrusted [] parts = payload.split("\u0000", 3);
      if (parts.length != 3) {
        throw new @OsUntrusted IllegalArgumentException("Received corrupt response");
      }
      if (parts[0].isEmpty()) { // authz = authn
        parts[0] = parts[1];
      }
      
      @OsUntrusted
      NameCallback nc = new @OsUntrusted NameCallback("SASL PLAIN");
      nc.setName(parts[1]);
      @OsUntrusted
      PasswordCallback pc = new @OsUntrusted PasswordCallback("SASL PLAIN", false);
      pc.setPassword(parts[2].toCharArray());
      @OsUntrusted
      AuthorizeCallback ac = new @OsUntrusted AuthorizeCallback(parts[1], parts[0]);
      cbh.handle(new @OsUntrusted Callback @OsUntrusted []{nc, pc, ac});      
      if (ac.isAuthorized()) {
        authz = ac.getAuthorizedID();
      }
    } catch (@OsUntrusted Exception e) {
      throw new @OsUntrusted SaslException("PLAIN auth failed: " + e.getMessage());
    } finally {
      completed = true;
    }
    return null;
  }

  private void throwIfNotComplete(@OsUntrusted SaslPlainServer this) {
    if (!completed) {
      throw new @OsUntrusted IllegalStateException("PLAIN authentication not completed");
    }
  }
  
  @Override
  public @OsUntrusted boolean isComplete(@OsUntrusted SaslPlainServer this) {
    return completed;
  }

  @Override
  public @OsUntrusted String getAuthorizationID(@OsUntrusted SaslPlainServer this) {
    throwIfNotComplete();
    return authz;
  }
  
  @Override
  public @OsUntrusted Object getNegotiatedProperty(@OsUntrusted SaslPlainServer this, @OsUntrusted String propName) {
    throwIfNotComplete();      
    return Sasl.QOP.equals(propName) ? "auth" : null;
  }
  
  @Override
  public @OsUntrusted byte @OsUntrusted [] wrap(@OsUntrusted SaslPlainServer this, @OsUntrusted byte @OsUntrusted [] outgoing, @OsUntrusted int offset, @OsUntrusted int len)
      throws SaslException {
    throwIfNotComplete();
    throw new @OsUntrusted IllegalStateException(
        "PLAIN supports neither integrity nor privacy");      
  }
  
  @Override
  public @OsUntrusted byte @OsUntrusted [] unwrap(@OsUntrusted SaslPlainServer this, @OsUntrusted byte @OsUntrusted [] incoming, @OsUntrusted int offset, @OsUntrusted int len)
      throws SaslException {
    throwIfNotComplete();
    throw new @OsUntrusted IllegalStateException(
        "PLAIN supports neither integrity nor privacy");      
  }
  
  @Override
  public void dispose(@OsUntrusted SaslPlainServer this) throws SaslException {
    cbh = null;
    authz = null;
  }
}