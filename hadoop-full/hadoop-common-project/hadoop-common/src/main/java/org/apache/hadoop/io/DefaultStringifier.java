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

package org.apache.hadoop.io;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.io.IOException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.GenericsUtil;

/**
 * DefaultStringifier is the default implementation of the {@link Stringifier}
 * interface which stringifies the objects using base64 encoding of the
 * serialized version of the objects. The {@link Serializer} and
 * {@link Deserializer} are obtained from the {@link SerializationFactory}.
 * <br>
 * DefaultStringifier offers convenience methods to store/load objects to/from
 * the configuration.
 * 
 * @param <T> the class of the objects to stringify
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DefaultStringifier<@OsUntrusted T extends java.lang.@OsUntrusted Object> implements @OsUntrusted Stringifier<T> {

  private static final @OsUntrusted String SEPARATOR = ",";

  private @OsUntrusted Serializer<@OsUntrusted T> serializer;

  private @OsUntrusted Deserializer<@OsUntrusted T> deserializer;

  private @OsUntrusted DataInputBuffer inBuf;

  private @OsUntrusted DataOutputBuffer outBuf;

  public @OsUntrusted DefaultStringifier(@OsUntrusted Configuration conf, @OsUntrusted Class<@OsUntrusted T> c) {

    @OsUntrusted
    SerializationFactory factory = new @OsUntrusted SerializationFactory(conf);
    this.serializer = factory.getSerializer(c);
    this.deserializer = factory.getDeserializer(c);
    this.inBuf = new @OsUntrusted DataInputBuffer();
    this.outBuf = new @OsUntrusted DataOutputBuffer();
    try {
      serializer.open(outBuf);
      deserializer.open(inBuf);
    } catch (@OsUntrusted IOException ex) {
      throw new @OsUntrusted RuntimeException(ex);
    }
  }

  @Override
  public @OsUntrusted T fromString(@OsUntrusted DefaultStringifier<T> this, @OsUntrusted String str) throws IOException {
    try {
      @OsUntrusted
      byte @OsUntrusted [] bytes = Base64.decodeBase64(str.getBytes("UTF-8"));
      inBuf.reset(bytes, bytes.length);
      T restored = deserializer.deserialize(null);
      return restored;
    } catch (@OsUntrusted UnsupportedCharsetException ex) {
      throw new @OsUntrusted IOException(ex.toString());
    }
  }

  @Override
  public @OsTrusted String toString(@OsUntrusted DefaultStringifier<T> this, @OsUntrusted T obj) throws IOException {
    outBuf.reset();
    serializer.serialize(obj);
    @OsUntrusted
    byte @OsUntrusted [] buf = new @OsUntrusted byte @OsUntrusted [outBuf.getLength()];
    System.arraycopy(outBuf.getData(), 0, buf, 0, buf.length);
    //OSTrusted, we're considering Base64 trusted
    return new @OsTrusted String(Base64.encodeBase64(buf));
  }

  @Override
  public void close(@OsUntrusted DefaultStringifier<T> this) throws IOException {
    inBuf.close();
    outBuf.close();
    deserializer.close();
    serializer.close();
  }

  /**
   * Stores the item in the configuration with the given keyName.
   * 
   * @param <K>  the class of the item
   * @param conf the configuration to store
   * @param item the object to be stored
   * @param keyName the name of the key to use
   * @throws IOException : forwards Exceptions from the underlying 
   * {@link Serialization} classes. 
   */
  public static <@OsUntrusted K extends java.lang.@OsUntrusted Object> void store(@OsUntrusted Configuration conf, @OsUntrusted K item, @OsUntrusted String keyName)
  throws IOException {

    @OsUntrusted
    DefaultStringifier<@OsUntrusted K> stringifier = new @OsUntrusted DefaultStringifier<@OsUntrusted K>(conf,
        GenericsUtil.getClass(item));
    conf.set(keyName, stringifier.toString(item));
    stringifier.close();
  }

  /**
   * Restores the object from the configuration.
   * 
   * @param <K> the class of the item
   * @param conf the configuration to use
   * @param keyName the name of the key to use
   * @param itemClass the class of the item
   * @return restored object
   * @throws IOException : forwards Exceptions from the underlying 
   * {@link Serialization} classes.
   */
  public static <@OsUntrusted K extends java.lang.@OsUntrusted Object> @OsUntrusted K load(@OsUntrusted Configuration conf, @OsUntrusted String keyName,
      @OsUntrusted
      Class<@OsUntrusted K> itemClass) throws IOException {
    @OsUntrusted
    DefaultStringifier<@OsUntrusted K> stringifier = new @OsUntrusted DefaultStringifier<@OsUntrusted K>(conf,
        itemClass);
    try {
      @OsUntrusted
      String itemStr = conf.get(keyName);
      return stringifier.fromString(itemStr);
    } finally {
      stringifier.close();
    }
  }

  /**
   * Stores the array of items in the configuration with the given keyName.
   * 
   * @param <K> the class of the item
   * @param conf the configuration to use 
   * @param items the objects to be stored
   * @param keyName the name of the key to use
   * @throws IndexOutOfBoundsException if the items array is empty
   * @throws IOException : forwards Exceptions from the underlying 
   * {@link Serialization} classes.         
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static <@OsUntrusted K extends java.lang.@OsUntrusted Object> void storeArray(@OsUntrusted Configuration conf, @OsTrusted K @OsUntrusted [] items,
      @OsUntrusted
      String keyName) throws IOException {

    @OsUntrusted
    DefaultStringifier<@OsUntrusted K> stringifier = new @OsUntrusted DefaultStringifier<@OsUntrusted K>(conf, 
        GenericsUtil.getClass(items[0]));
    try {
      @OsUntrusted StringBuilder builder = new @OsUntrusted StringBuilder();
      for ( K item : items) {
        builder.append(stringifier.toString(item)).append(SEPARATOR);
      }
      //ostrusted, items are trusted, only entry into this builder
      conf.set(keyName, (@OsTrusted String) builder.toString());
    }
    finally {
      stringifier.close();
    }
  }

  /**
   * Restores the array of objects from the configuration.
   * 
   * @param <K> the class of the item
   * @param conf the configuration to use
   * @param keyName the name of the key to use
   * @param itemClass the class of the item
   * @return restored object
   * @throws IOException : forwards Exceptions from the underlying 
   * {@link Serialization} classes.
   */
  public static <@OsUntrusted K extends java.lang.@OsUntrusted Object> @OsUntrusted K @OsUntrusted [] loadArray(@OsUntrusted Configuration conf, @OsUntrusted String keyName,
      @OsUntrusted
      Class<@OsUntrusted K> itemClass) throws IOException {
    @OsUntrusted
    DefaultStringifier<@OsUntrusted K> stringifier = new @OsUntrusted DefaultStringifier<@OsUntrusted K>(conf,
        itemClass);
    try {
      @OsUntrusted
      String itemStr = conf.get(keyName);
      @OsUntrusted
      ArrayList<@OsUntrusted K> list = new @OsUntrusted ArrayList<@OsUntrusted K>();
      @OsUntrusted
      String @OsUntrusted [] parts = itemStr.split(SEPARATOR);

      for (@OsUntrusted String part : parts) {
        if (!part.isEmpty())
          list.add(stringifier.fromString(part));
      }

      return GenericsUtil.toArray(itemClass, list);
    }
    finally {
      stringifier.close();
    }
  }

}
