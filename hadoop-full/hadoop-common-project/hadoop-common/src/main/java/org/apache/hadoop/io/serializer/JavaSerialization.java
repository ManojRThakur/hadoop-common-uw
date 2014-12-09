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
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * <p>
 * An experimental {@link Serialization} for Java {@link Serializable} classes.
 * </p>
 * @see JavaSerializationComparator
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class JavaSerialization implements @OsUntrusted Serialization<@OsUntrusted Serializable> {

  static class JavaSerializationDeserializer<@OsUntrusted T extends @OsUntrusted Serializable>
    implements @OsUntrusted Deserializer<T> {

    private @OsUntrusted ObjectInputStream ois;

    @Override
    public void open(JavaSerialization.@OsUntrusted JavaSerializationDeserializer<T> this, @OsUntrusted InputStream in) throws IOException {
      ois = new @OsUntrusted ObjectInputStream(in) {
        @Override protected void readStreamHeader() {
          // no header
        }
      };
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public @OsUntrusted T deserialize(JavaSerialization.@OsUntrusted JavaSerializationDeserializer<T> this, @OsUntrusted T object) throws IOException {
      try {
        // ignore passed-in object
        return (T) ois.readObject();
      } catch (@OsUntrusted ClassNotFoundException e) {
        throw new @OsUntrusted IOException(e.toString());
      }
    }

    @Override
    public void close(JavaSerialization.@OsUntrusted JavaSerializationDeserializer<T> this) throws IOException {
      ois.close();
    }

  }

  static class JavaSerializationSerializer
    implements @OsUntrusted Serializer<@OsUntrusted Serializable> {

    private @OsUntrusted ObjectOutputStream oos;

    @Override
    public void open(JavaSerialization.@OsUntrusted JavaSerializationSerializer this, @OsUntrusted OutputStream out) throws IOException {
      oos = new @OsUntrusted ObjectOutputStream(out) {
        @Override protected void writeStreamHeader() {
          // no header
        }
      };
    }

    @Override
    public void serialize(JavaSerialization.@OsUntrusted JavaSerializationSerializer this, @OsUntrusted Serializable object) throws IOException {
      oos.reset(); // clear (class) back-references
      oos.writeObject(object);
    }

    @Override
    public void close(JavaSerialization.@OsUntrusted JavaSerializationSerializer this) throws IOException {
      oos.close();
    }

  }

  @Override
  @InterfaceAudience.Private
  public @OsUntrusted boolean accept(@OsUntrusted JavaSerialization this, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> c) {
    return Serializable.class.isAssignableFrom(c);
  }

  @Override
  @InterfaceAudience.Private
  public @OsUntrusted Deserializer<@OsUntrusted Serializable> getDeserializer(@OsUntrusted JavaSerialization this, @OsUntrusted Class<@OsUntrusted Serializable> c) {
    return new @OsUntrusted JavaSerializationDeserializer<@OsUntrusted Serializable>();
  }

  @Override
  @InterfaceAudience.Private
  public @OsUntrusted Serializer<@OsUntrusted Serializable> getSerializer(@OsUntrusted JavaSerialization this, @OsUntrusted Class<@OsUntrusted Serializable> c) {
    return new @OsUntrusted JavaSerializationSerializer();
  }

}
