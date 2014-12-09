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

package org.apache.hadoop.io.serializer.avro;

import ostrusted.quals.OsUntrusted;
import java.util.HashSet;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Serialization for Avro Reflect classes. For a class to be accepted by this 
 * serialization, it must either be in the package list configured via 
 * <code>avro.reflect.pkgs</code> or implement 
 * {@link AvroReflectSerializable} interface.
 *
 */
@SuppressWarnings("unchecked")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AvroReflectSerialization extends @OsUntrusted AvroSerialization<@OsUntrusted Object>{

  /**
   * Key to configure packages that contain classes to be serialized and 
   * deserialized using this class. Multiple packages can be specified using 
   * comma-separated list.
   */
  @InterfaceAudience.Private
  public static final @OsUntrusted String AVRO_REFLECT_PACKAGES = "avro.reflect.pkgs";

  private @OsUntrusted Set<@OsUntrusted String> packages; 

  @InterfaceAudience.Private
  @Override
  public synchronized @OsUntrusted boolean accept(@OsUntrusted AvroReflectSerialization this, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> c) {
    if (packages == null) {
      getPackages();
    }
    return AvroReflectSerializable.class.isAssignableFrom(c) ||
      (c.getPackage() != null && packages.contains(c.getPackage().getName()));
  }

  private void getPackages(@OsUntrusted AvroReflectSerialization this) {
    @OsUntrusted
    String @OsUntrusted [] pkgList  = getConf().getStrings(AVRO_REFLECT_PACKAGES);
    packages = new @OsUntrusted HashSet<@OsUntrusted String>();
    if (pkgList != null) {
      for (@OsUntrusted String pkg : pkgList) {
        packages.add(pkg.trim());
      }
    }
  }

  @InterfaceAudience.Private
  @Override
  public @OsUntrusted DatumReader getReader(@OsUntrusted AvroReflectSerialization this, @OsUntrusted Class<@OsUntrusted Object> clazz) {
    try {
      return new @OsUntrusted ReflectDatumReader(clazz);
    } catch (@OsUntrusted Exception e) {
      throw new @OsUntrusted RuntimeException(e);
    }
  }

  @InterfaceAudience.Private
  @Override
  public @OsUntrusted Schema getSchema(@OsUntrusted AvroReflectSerialization this, @OsUntrusted Object t) {
    return ReflectData.get().getSchema(t.getClass());
  }

  @InterfaceAudience.Private
  @Override
  public @OsUntrusted DatumWriter getWriter(@OsUntrusted AvroReflectSerialization this, @OsUntrusted Class<@OsUntrusted Object> clazz) {
    return new @OsUntrusted ReflectDatumWriter();
  }

}
