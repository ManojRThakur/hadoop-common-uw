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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * Base class for providing serialization to Avro types.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AvroSerialization<@OsUntrusted T> extends @OsUntrusted Configured 
	implements @OsUntrusted Serialization<@OsUntrusted T>{
  
  @InterfaceAudience.Private
  public static final @OsUntrusted String AVRO_SCHEMA_KEY = "Avro-Schema";

  @Override
  @InterfaceAudience.Private
  public @OsUntrusted Deserializer<@OsUntrusted T> getDeserializer(@OsUntrusted AvroSerialization<T> this, @OsUntrusted Class<@OsUntrusted T> c) {
    return new @OsUntrusted AvroDeserializer(c);
  }

  @Override
  @InterfaceAudience.Private
  public @OsUntrusted Serializer<@OsUntrusted T> getSerializer(@OsUntrusted AvroSerialization<T> this, @OsUntrusted Class<@OsUntrusted T> c) {
    return new @OsUntrusted AvroSerializer(c);
  }

  /**
   * Return an Avro Schema instance for the given class.
   */
  @InterfaceAudience.Private
  public abstract @OsUntrusted Schema getSchema(@OsUntrusted AvroSerialization<T> this, @OsUntrusted T t);

  /**
   * Create and return Avro DatumWriter for the given class.
   */
  @InterfaceAudience.Private
  public abstract @OsUntrusted DatumWriter<@OsUntrusted T> getWriter(@OsUntrusted AvroSerialization<T> this, @OsUntrusted Class<@OsUntrusted T> clazz);

  /**
   * Create and return Avro DatumReader for the given class.
   */
  @InterfaceAudience.Private
  public abstract @OsUntrusted DatumReader<@OsUntrusted T> getReader(@OsUntrusted AvroSerialization<T> this, @OsUntrusted Class<@OsUntrusted T> clazz);

  class AvroSerializer implements @OsUntrusted Serializer<T> {

    private @OsUntrusted DatumWriter<@OsUntrusted T> writer;
    private @OsUntrusted BinaryEncoder encoder;
    private @OsUntrusted OutputStream outStream;

    @OsUntrusted
    AvroSerializer(@OsUntrusted Class<@OsUntrusted T> clazz) {
      this.writer = getWriter(clazz);
    }

    @Override
    public void close(@OsUntrusted AvroSerialization<T>.AvroSerializer this) throws IOException {
      encoder.flush();
      outStream.close();
    }

    @Override
    public void open(@OsUntrusted AvroSerialization<T>.AvroSerializer this, @OsUntrusted OutputStream out) throws IOException {
      outStream = out;
      encoder = EncoderFactory.get().binaryEncoder(out, encoder);
    }

    @Override
    public void serialize(@OsUntrusted AvroSerialization<T>.AvroSerializer this, @OsUntrusted T t) throws IOException {
      writer.setSchema(getSchema(t));
      writer.write(t, encoder);
    }

  }

  class AvroDeserializer implements @OsUntrusted Deserializer<T> {

    private @OsUntrusted DatumReader<@OsUntrusted T> reader;
    private @OsUntrusted BinaryDecoder decoder;
    private @OsUntrusted InputStream inStream;

    @OsUntrusted
    AvroDeserializer(@OsUntrusted Class<@OsUntrusted T> clazz) {
      this.reader = getReader(clazz);
    }

    @Override
    public void close(@OsUntrusted AvroSerialization<T>.AvroDeserializer this) throws IOException {
      inStream.close();
    }

    @Override
    public @OsUntrusted T deserialize(@OsUntrusted AvroSerialization<T>.AvroDeserializer this, @OsUntrusted T t) throws IOException {
      return reader.read(t, decoder);
    }

    @Override
    public void open(@OsUntrusted AvroSerialization<T>.AvroDeserializer this, @OsUntrusted InputStream in) throws IOException {
      inStream = in;
      decoder = DecoderFactory.get().binaryDecoder(in, decoder);
    }

  }

}
