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

package org.apache.hadoop.record.compiler;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/**
 * Code generator for "buffer" type.
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JBuffer extends @OsUntrusted JCompType {
  
  class JavaBuffer extends @OsUntrusted JavaCompType {
    
    @OsUntrusted
    JavaBuffer() {
      super("org.apache.hadoop.record.Buffer", "Buffer", 
          "org.apache.hadoop.record.Buffer", "TypeID.RIOType.BUFFER");
    }
    
    @Override
    @OsUntrusted
    String getTypeIDObjectString(@OsUntrusted JBuffer.JavaBuffer this) {
      return "org.apache.hadoop.record.meta.TypeID.BufferTypeID";
    }

    @Override
    void genCompareTo(@OsUntrusted JBuffer.JavaBuffer this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname, @OsUntrusted String other) {
      cb.append(Consts.RIO_PREFIX + "ret = "+fname+".compareTo("+other+");\n");
    }
    
    @Override
    void genEquals(@OsUntrusted JBuffer.JavaBuffer this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname, @OsUntrusted String peer) {
      cb.append(Consts.RIO_PREFIX + "ret = "+fname+".equals("+peer+");\n");
    }
    
    @Override
    void genHashCode(@OsUntrusted JBuffer.JavaBuffer this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append(Consts.RIO_PREFIX + "ret = "+fname+".hashCode();\n");
    }
    
    @Override
    void genSlurpBytes(@OsUntrusted JBuffer.JavaBuffer this, @OsUntrusted CodeBuffer cb, @OsUntrusted String b, @OsUntrusted String s, @OsUntrusted String l) {
      cb.append("{\n");
      cb.append("int i = org.apache.hadoop.record.Utils.readVInt("+
                b+", "+s+");\n");
      cb.append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
      cb.append(s+" += z+i; "+l+" -= (z+i);\n");
      cb.append("}\n");
    }
    
    @Override
    void genCompareBytes(@OsUntrusted JBuffer.JavaBuffer this, @OsUntrusted CodeBuffer cb) {
      cb.append("{\n");
      cb.append("int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n");
      cb.append("int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n");
      cb.append("int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);\n");
      cb.append("int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);\n");
      cb.append("s1+=z1; s2+=z2; l1-=z1; l2-=z2;\n");
      cb.append("int r1 = org.apache.hadoop.record.Utils.compareBytes(b1,s1,i1,b2,s2,i2);\n");
      cb.append("if (r1 != 0) { return (r1<0)?-1:0; }\n");
      cb.append("s1+=i1; s2+=i2; l1-=i1; l1-=i2;\n");
      cb.append("}\n");
    }
  }
  
  class CppBuffer extends @OsUntrusted CppCompType {
    
    @OsUntrusted
    CppBuffer() {
      super(" ::std::string");
    }
    
    @Override
    void genGetSet(@OsUntrusted JBuffer.CppBuffer this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append("virtual const "+getType()+"& get"+toCamelCase(fname)+"() const {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
      cb.append("virtual "+getType()+"& get"+toCamelCase(fname)+"() {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
    }
    
    @Override
    @OsUntrusted
    String getTypeIDObjectString(@OsUntrusted JBuffer.CppBuffer this) {
      return "new ::hadoop::TypeID(::hadoop::RIOTYPE_BUFFER)";
    }

  }
  /** Creates a new instance of JBuffer */
  public @OsUntrusted JBuffer() {
    setJavaType(new @OsUntrusted JavaBuffer());
    setCppType(new @OsUntrusted CppBuffer());
    setCType(new @OsUntrusted CCompType());
  }
  
  @Override
  @OsUntrusted
  String getSignature(@OsUntrusted JBuffer this) {
    return "B";
  }
}
