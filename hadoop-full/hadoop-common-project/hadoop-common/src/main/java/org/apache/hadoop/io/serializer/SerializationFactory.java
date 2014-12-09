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

package org.apache.hadoop.io.serializer;

import ostrusted.quals.OsUntrusted;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.serializer.avro.AvroReflectSerialization;
import org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * <p>
 * A factory for {@link Serialization}s.
 * </p>
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SerializationFactory extends @OsUntrusted Configured {

  static final @OsUntrusted Log LOG =
    LogFactory.getLog(SerializationFactory.class.getName());

  private @OsUntrusted List<@OsUntrusted Serialization<@OsUntrusted ? extends java.lang.@OsUntrusted Object>> serializations = new @OsUntrusted ArrayList<@OsUntrusted Serialization<@OsUntrusted ? extends java.lang.@OsUntrusted Object>>();

  /**
   * <p>
   * Serializations are found by reading the <code>io.serializations</code>
   * property from <code>conf</code>, which is a comma-delimited list of
   * classnames.
   * </p>
   */
  public @OsUntrusted SerializationFactory(@OsUntrusted Configuration conf) {
    super(conf);
    if (conf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY).equals("")) {
      LOG.warn("Serialization for various data types may not be available. Please configure "
          + CommonConfigurationKeys.IO_SERIALIZATIONS_KEY
          + " properly to have serialization support (it is currently not set).");
    } else {
      for (@OsUntrusted String serializerName : conf.getStrings(
          CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, new @OsUntrusted String @OsUntrusted [] {
              WritableSerialization.class.getName(),
              AvroSpecificSerialization.class.getName(),
              AvroReflectSerialization.class.getName() })) {
        add(conf, serializerName);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void add(@OsUntrusted SerializationFactory this, @OsUntrusted Configuration conf, @OsUntrusted String serializationName) {
    try {
      @OsUntrusted
      Class<@OsUntrusted ? extends @OsUntrusted Serialization> serializionClass =
        (@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Serialization>) conf.getClassByName(serializationName);
      serializations.add((@OsUntrusted Serialization)
      ReflectionUtils.newInstance(serializionClass, getConf()));
    } catch (@OsUntrusted ClassNotFoundException e) {
      LOG.warn("Serialization class not found: ", e);
    }
  }

  public <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted Serializer<@OsUntrusted T> getSerializer(@OsUntrusted SerializationFactory this, @OsUntrusted Class<@OsUntrusted T> c) {
    @OsUntrusted
    Serialization<@OsUntrusted T> serializer = getSerialization(c);
    if (serializer != null) {
      return serializer.getSerializer(c);
    }
    return null;
  }

  public <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted Deserializer<@OsUntrusted T> getDeserializer(@OsUntrusted SerializationFactory this, @OsUntrusted Class<@OsUntrusted T> c) {
    @OsUntrusted
    Serialization<@OsUntrusted T> serializer = getSerialization(c);
    if (serializer != null) {
      return serializer.getDeserializer(c);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public <@OsUntrusted T extends java.lang.@OsUntrusted Object> @OsUntrusted Serialization<@OsUntrusted T> getSerialization(@OsUntrusted SerializationFactory this, @OsUntrusted Class<@OsUntrusted T> c) {
    for (@OsUntrusted Serialization serialization : serializations) {
      if (serialization.accept(c)) {
        return (@OsUntrusted Serialization<@OsUntrusted T>) serialization;
      }
    }
    return null;
  }

}
