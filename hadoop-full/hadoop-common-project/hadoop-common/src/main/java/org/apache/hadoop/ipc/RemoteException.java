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
import java.lang.reflect.Constructor;

import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.xml.sax.Attributes;

public class RemoteException extends @OsUntrusted IOException {
  /** For java.io.Serializable */
  private static final @OsUntrusted long serialVersionUID = 1L;
  private final @OsUntrusted int errorCode;

  private @OsUntrusted String className;
  
  public @OsUntrusted RemoteException(@OsUntrusted String className, @OsUntrusted String msg) {
    super(msg);
    this.className = className;
    errorCode = -1;
  }
  
  public @OsUntrusted RemoteException(@OsUntrusted String className, @OsUntrusted String msg, @OsUntrusted RpcErrorCodeProto erCode) {
    super(msg);
    this.className = className;
    if (erCode != null)
      errorCode = erCode.getNumber();
    else 
      errorCode = -1;
  }
  
  public @OsUntrusted String getClassName(@OsUntrusted RemoteException this) {
    return className;
  }
  
  public @OsUntrusted RpcErrorCodeProto getErrorCode(@OsUntrusted RemoteException this) {
    return RpcErrorCodeProto.valueOf(errorCode);
  }

  /**
   * If this remote exception wraps up one of the lookupTypes
   * then return this exception.
   * <p>
   * Unwraps any IOException.
   * 
   * @param lookupTypes the desired exception class.
   * @return IOException, which is either the lookupClass exception or this.
   */
  public @OsUntrusted IOException unwrapRemoteException(@OsUntrusted RemoteException this, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> @OsUntrusted ... lookupTypes) {
    if(lookupTypes == null)
      return this;
    for(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> lookupClass : lookupTypes) {
      if(!lookupClass.getName().equals(getClassName()))
        continue;
      try {
        return instantiateException(lookupClass.asSubclass(IOException.class));
      } catch(@OsUntrusted Exception e) {
        // cannot instantiate lookupClass, just return this
        return this;
      }
    }
    // wrapped up exception is not in lookupTypes, just return this
    return this;
  }

  /**
   * Instantiate and return the exception wrapped up by this remote exception.
   * 
   * <p> This unwraps any <code>Throwable</code> that has a constructor taking
   * a <code>String</code> as a parameter.
   * Otherwise it returns this.
   * 
   * @return <code>Throwable
   */
  public @OsUntrusted IOException unwrapRemoteException(@OsUntrusted RemoteException this) {
    try {
      @OsUntrusted
      Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> realClass = Class.forName(getClassName());
      return instantiateException(realClass.asSubclass(IOException.class));
    } catch(@OsUntrusted Exception e) {
      // cannot instantiate the original exception, just return this
    }
    return this;
  }

  private @OsUntrusted IOException instantiateException(@OsUntrusted RemoteException this, @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted IOException> cls)
      throws Exception {
    @OsUntrusted
    Constructor<@OsUntrusted ? extends @OsUntrusted IOException> cn = cls.getConstructor(String.class);
    cn.setAccessible(true);
    @OsUntrusted
    IOException ex = cn.newInstance(this.getMessage());
    ex.initCause(this);
    return ex;
  }

  /** Create RemoteException from attributes */
  public static @OsUntrusted RemoteException valueOf(@OsUntrusted Attributes attrs) {
    return new @OsUntrusted RemoteException(attrs.getValue("class"),
        attrs.getValue("message")); 
  }

  @Override
  public @OsUntrusted String toString(@OsUntrusted RemoteException this) {
    return getClass().getName() + "(" + className + "): " + getMessage();
  }
}
