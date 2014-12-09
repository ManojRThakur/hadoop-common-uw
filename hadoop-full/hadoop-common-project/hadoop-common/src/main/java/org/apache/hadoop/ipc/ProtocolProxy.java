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

package org.apache.hadoop.ipc;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashSet;


/**
 * a class wraps around a server's proxy, 
 * containing a list of its supported methods.
 * 
 * A list of methods with a value of null indicates that the client and server
 * have the same protocol.
 */
public class ProtocolProxy<@OsUntrusted T extends java.lang.@OsUntrusted Object> {
  private @OsUntrusted Class<@OsUntrusted T> protocol;
  private @OsUntrusted T proxy;
  private @OsUntrusted HashSet<@OsUntrusted Integer> serverMethods = null;
  final private @OsUntrusted boolean supportServerMethodCheck;
  private @OsUntrusted boolean serverMethodsFetched = false;
  
  /**
   * Constructor
   * 
   * @param protocol protocol class
   * @param proxy its proxy
   * @param supportServerMethodCheck If false proxy will never fetch server
   *        methods and isMethodSupported will always return true. If true,
   *        server methods will be fetched for the first call to 
   *        isMethodSupported. 
   */
  public @OsUntrusted ProtocolProxy(@OsUntrusted Class<@OsUntrusted T> protocol, @OsUntrusted T proxy,
      @OsUntrusted
      boolean supportServerMethodCheck) {
    this.protocol = protocol;
    this.proxy = proxy;
    this.supportServerMethodCheck = supportServerMethodCheck;
  }
  
  private void fetchServerMethods(@OsUntrusted ProtocolProxy<T> this, @OsUntrusted Method method) throws IOException {
    @OsUntrusted
    long clientVersion;
    clientVersion = RPC.getProtocolVersion(method.getDeclaringClass());
    @OsUntrusted
    int clientMethodsHash = ProtocolSignature.getFingerprint(method
        .getDeclaringClass().getMethods());
    @OsUntrusted
    ProtocolSignature serverInfo = ((@OsUntrusted VersionedProtocol) proxy)
        .getProtocolSignature(RPC.getProtocolName(protocol), clientVersion,
            clientMethodsHash);
    @OsUntrusted
    long serverVersion = serverInfo.getVersion();
    if (serverVersion != clientVersion) {
      throw new RPC.@OsUntrusted VersionMismatch(protocol.getName(), clientVersion,
          serverVersion);
    }
    @OsUntrusted
    int @OsUntrusted [] serverMethodsCodes = serverInfo.getMethods();
    if (serverMethodsCodes != null) {
      serverMethods = new @OsUntrusted HashSet<@OsUntrusted Integer>(serverMethodsCodes.length);
      for (@OsUntrusted int m : serverMethodsCodes) {
        this.serverMethods.add(Integer.valueOf(m));
      }
    }
    serverMethodsFetched = true;
  }

  /*
   * Get the proxy
   */
  public @OsUntrusted T getProxy(@OsUntrusted ProtocolProxy<T> this) {
    return proxy;
  }
  
  /**
   * Check if a method is supported by the server or not
   * 
   * @param methodName a method's name in String format
   * @param parameterTypes a method's parameter types
   * @return true if the method is supported by the server
   */
  public synchronized @OsUntrusted boolean isMethodSupported(@OsUntrusted ProtocolProxy<T> this, @OsUntrusted String methodName,
                                   @OsUntrusted
                                   Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> @OsUntrusted ... parameterTypes)
  throws IOException {
    if (!supportServerMethodCheck) {
      return true;
    }
    @OsUntrusted
    Method method;
    try {
      method = protocol.getDeclaredMethod(methodName, parameterTypes);
    } catch (@OsUntrusted SecurityException e) {
      throw new @OsUntrusted IOException(e);
    } catch (@OsUntrusted NoSuchMethodException e) {
      throw new @OsUntrusted IOException(e);
    }
    if (!serverMethodsFetched) {
      fetchServerMethods(method);
    }
    if (serverMethods == null) { // client & server have the same protocol
      return true;
    }
    return serverMethods.contains(
        Integer.valueOf(ProtocolSignature.getFingerprint(method)));
  }
}