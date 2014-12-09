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
package org.apache.hadoop.fs;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.net.URLStreamHandlerFactory;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * Factory for URL stream handlers.
 * 
 * There is only one handler whose job is to create UrlConnections. A
 * FsUrlConnection relies on FileSystem to choose the appropriate FS
 * implementation.
 * 
 * Before returning our handler, we make sure that FileSystem knows an
 * implementation for the requested scheme/protocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FsUrlStreamHandlerFactory implements
    @OsUntrusted
    URLStreamHandlerFactory {

  // The configuration holds supported FS implementation class names.
  private @OsUntrusted Configuration conf;

  // This map stores whether a protocol is know or not by FileSystem
  private @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Boolean> protocols = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted Boolean>();

  // The URL Stream handler
  private java.net.URLStreamHandler handler;

  public @OsUntrusted FsUrlStreamHandlerFactory() {
    this(new @OsUntrusted Configuration());
  }

  public @OsUntrusted FsUrlStreamHandlerFactory(@OsUntrusted Configuration conf) {
    this.conf = new @OsUntrusted Configuration(conf);
    // force init of FileSystem code to avoid HADOOP-9041
    try {
      FileSystem.getFileSystemClass("file", conf);
    } catch (@OsUntrusted IOException io) {
      throw new @OsUntrusted RuntimeException(io);
    }
    this.handler = new @OsUntrusted FsUrlStreamHandler(this.conf);
  }

  @Override
  public java.net.URLStreamHandler createURLStreamHandler(@OsUntrusted FsUrlStreamHandlerFactory this, @OsUntrusted String protocol) {
    if (!protocols.containsKey(protocol)) {
      @OsUntrusted
      boolean known = true;
      try {
        FileSystem.getFileSystemClass(protocol, conf);
      }
      catch (@OsUntrusted IOException ex) {
        known = false;
      }
      protocols.put(protocol, known);
    }
    if (protocols.get(protocol)) {
      return handler;
    } else {
      // FileSystem does not know the protocol, let the VM handle this
      return null;
    }
  }

}
