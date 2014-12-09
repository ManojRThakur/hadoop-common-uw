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
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** A base-class for Writables which store themselves compressed and lazily
 * inflate on field access.  This is useful for large objects whose fields are
 * not be altered during a map or reduce operation: leaving the field data
 * compressed makes copying the instance from one file to another much
 * faster. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CompressedWritable implements @OsUntrusted Writable {
  // if non-null, the compressed field data of this instance.
  private @OsUntrusted byte @OsUntrusted [] compressed;

  public @OsUntrusted CompressedWritable() {}

  @Override
  public final void readFields(@OsUntrusted CompressedWritable this, @OsUntrusted DataInput in) throws IOException {
    compressed = new @OsUntrusted byte @OsUntrusted [in.readInt()];
    in.readFully(compressed, 0, compressed.length);
  }

  /** Must be called by all methods which access fields to ensure that the data
   * has been uncompressed. */
  protected void ensureInflated(@OsUntrusted CompressedWritable this) {
    if (compressed != null) {
      try {
        @OsUntrusted
        ByteArrayInputStream deflated = new @OsUntrusted ByteArrayInputStream(compressed);
        @OsUntrusted
        DataInput inflater =
          new @OsUntrusted DataInputStream(new @OsUntrusted InflaterInputStream(deflated));
        readFieldsCompressed(inflater);
        compressed = null;
      } catch (@OsUntrusted IOException e) {
        throw new @OsUntrusted RuntimeException(e);
      }
    }
  }

  /** Subclasses implement this instead of {@link #readFields(DataInput)}. */
  protected abstract void readFieldsCompressed(@OsUntrusted CompressedWritable this, @OsUntrusted DataInput in)
    throws IOException;

  @Override
  public final void write(@OsUntrusted CompressedWritable this, @OsUntrusted DataOutput out) throws IOException {
    if (compressed == null) {
      @OsUntrusted
      ByteArrayOutputStream deflated = new @OsUntrusted ByteArrayOutputStream();
      @OsUntrusted
      Deflater deflater = new @OsUntrusted Deflater(Deflater.BEST_SPEED);
      @OsUntrusted
      DataOutputStream dout =
        new @OsUntrusted DataOutputStream(new @OsUntrusted DeflaterOutputStream(deflated, deflater));
      writeCompressed(dout);
      dout.close();
      deflater.end();
      compressed = deflated.toByteArray();
    }
    out.writeInt(compressed.length);
    out.write(compressed);
  }

  /** Subclasses implement this instead of {@link #write(DataOutput)}. */
  protected abstract void writeCompressed(@OsUntrusted CompressedWritable this, @OsUntrusted DataOutput out) throws IOException;

}
