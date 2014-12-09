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
 * Code generator for "byte" type.
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JByte extends @OsUntrusted JType {
  
  class JavaByte extends @OsUntrusted JavaType {
    
    @OsUntrusted
    JavaByte() {
      super("byte", "Byte", "Byte", "TypeID.RIOType.BYTE");
    }
    
    @Override
    @OsUntrusted
    String getTypeIDObjectString(@OsUntrusted JByte.JavaByte this) {
      return "org.apache.hadoop.record.meta.TypeID.ByteTypeID";
    }

    @Override
    void genSlurpBytes(@OsUntrusted JByte.JavaByte this, @OsUntrusted CodeBuffer cb, @OsUntrusted String b, @OsUntrusted String s, @OsUntrusted String l) {
      cb.append("{\n");
      cb.append("if ("+l+"<1) {\n");
      cb.append("throw new java.io.IOException(\"Byte is exactly 1 byte."+
                " Provided buffer is smaller.\");\n");
      cb.append("}\n");
      cb.append(s+"++; "+l+"--;\n");
      cb.append("}\n");
    }
    
    @Override
    void genCompareBytes(@OsUntrusted JByte.JavaByte this, @OsUntrusted CodeBuffer cb) {
      cb.append("{\n");
      cb.append("if (l1<1 || l2<1) {\n");
      cb.append("throw new java.io.IOException(\"Byte is exactly 1 byte."+
                " Provided buffer is smaller.\");\n");
      cb.append("}\n");
      cb.append("if (b1[s1] != b2[s2]) {\n");
      cb.append("return (b1[s1]<b2[s2])?-1:0;\n");
      cb.append("}\n");
      cb.append("s1++; s2++; l1--; l2--;\n");
      cb.append("}\n");
    }
  }
  
  class CppByte extends @OsUntrusted CppType {
    
    @OsUntrusted
    CppByte() {
      super("int8_t");
    }
    
    @Override
    @OsUntrusted
    String getTypeIDObjectString(@OsUntrusted JByte.CppByte this) {
      return "new ::hadoop::TypeID(::hadoop::RIOTYPE_BYTE)";
    }
  }

  public @OsUntrusted JByte() {
    setJavaType(new @OsUntrusted JavaByte());
    setCppType(new @OsUntrusted CppByte());
    setCType(new @OsUntrusted CType());
  }
  
  @Override
  @OsUntrusted
  String getSignature(@OsUntrusted JByte this) {
    return "b";
  }
}
