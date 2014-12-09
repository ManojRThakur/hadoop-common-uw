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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A {@link Serialization} for {@link Writable}s that delegates to
 * {@link Writable#write(java.io.DataOutput)} and
 * {@link Writable#readFields(java.io.DataInput)}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class WritableSerialization extends @OsUntrusted Configured
	implements @OsUntrusted Serialization<@OsUntrusted Writable> {
  static class WritableDeserializer extends @OsUntrusted Configured
  	implements @OsUntrusted Deserializer<@OsUntrusted Writable> {

    private @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> writableClass;
    private @OsUntrusted DataInputStream dataIn;
    
    public @OsUntrusted WritableDeserializer(@OsUntrusted Configuration conf, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> c) {
      setConf(conf);
      this.writableClass = c;
    }
    
    @Override
    public void open(WritableSerialization.@OsUntrusted WritableDeserializer this, @OsUntrusted InputStream in) {
      if (in instanceof @OsUntrusted DataInputStream) {
        dataIn = (@OsUntrusted DataInputStream) in;
      } else {
        dataIn = new @OsUntrusted DataInputStream(in);
      }
    }
    
    @Override
    public @OsUntrusted Writable deserialize(WritableSerialization.@OsUntrusted WritableDeserializer this, @OsUntrusted Writable w) throws IOException {
      @OsUntrusted
      Writable writable;
      if (w == null) {
        writable 
          = (@OsUntrusted Writable) ReflectionUtils.newInstance(writableClass, getConf());
      } else {
        writable = w;
      }
      writable.readFields(dataIn);
      return writable;
    }

    @Override
    public void close(WritableSerialization.@OsUntrusted WritableDeserializer this) throws IOException {
      dataIn.close();
    }
    
  }
  
  static class WritableSerializer extends @OsUntrusted Configured implements
  	@OsUntrusted
  	Serializer<@OsUntrusted Writable> {
    
    private @OsUntrusted DataOutputStream dataOut;
    
    @Override
    public void open(WritableSerialization.@OsUntrusted WritableSerializer this, @OsUntrusted OutputStream out) {
      if (out instanceof @OsUntrusted DataOutputStream) {
        dataOut = (@OsUntrusted DataOutputStream) out;
      } else {
        dataOut = new @OsUntrusted DataOutputStream(out);
      }
    }

    @Override
    public void serialize(WritableSerialization.@OsUntrusted WritableSerializer this, @OsUntrusted Writable w) throws IOException {
      w.write(dataOut);
    }

    @Override
    public void close(WritableSerialization.@OsUntrusted WritableSerializer this) throws IOException {
      dataOut.close();
    }

  }

  @InterfaceAudience.Private
  @Override
  public @OsUntrusted boolean accept(@OsUntrusted WritableSerialization this, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> c) {
    return Writable.class.isAssignableFrom(c);
  }

  @InterfaceAudience.Private
  @Override
  public @OsUntrusted Serializer<@OsUntrusted Writable> getSerializer(@OsUntrusted WritableSerialization this, @OsUntrusted Class<@OsUntrusted Writable> c) {
    return new @OsUntrusted WritableSerializer();
  }
  
  @InterfaceAudience.Private
  @Override
  public @OsUntrusted Deserializer<@OsUntrusted Writable> getDeserializer(@OsUntrusted WritableSerialization this, @OsUntrusted Class<@OsUntrusted Writable> c) {
    return new @OsUntrusted WritableDeserializer(getConf(), c);
  }

}
