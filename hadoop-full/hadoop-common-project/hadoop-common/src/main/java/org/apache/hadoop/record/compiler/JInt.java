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
 * Code generator for "int" type
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JInt extends @OsUntrusted JType {
  
  class JavaInt extends @OsUntrusted JavaType {
    
    @OsUntrusted
    JavaInt() {
      super("int", "Int", "Integer", "TypeID.RIOType.INT");
    }
    
    @Override
    @OsUntrusted
    String getTypeIDObjectString(@OsUntrusted JInt.JavaInt this) {
      return "org.apache.hadoop.record.meta.TypeID.IntTypeID";
    }

    @Override
    void genSlurpBytes(@OsUntrusted JInt.JavaInt this, @OsUntrusted CodeBuffer cb, @OsUntrusted String b, @OsUntrusted String s, @OsUntrusted String l) {
      cb.append("{\n");
      cb.append("int i = org.apache.hadoop.record.Utils.readVInt("+b+", "+s+");\n");
      cb.append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
      cb.append(s+"+=z; "+l+"-=z;\n");
      cb.append("}\n");
    }
    
    @Override
    void genCompareBytes(@OsUntrusted JInt.JavaInt this, @OsUntrusted CodeBuffer cb) {
      cb.append("{\n");
      cb.append("int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n");
      cb.append("int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n");
      cb.append("if (i1 != i2) {\n");
      cb.append("return ((i1-i2) < 0) ? -1 : 0;\n");
      cb.append("}\n");
      cb.append("int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);\n");
      cb.append("int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);\n");
      cb.append("s1+=z1; s2+=z2; l1-=z1; l2-=z2;\n");
      cb.append("}\n");
    }
  }

  class CppInt extends @OsUntrusted CppType {
    
    @OsUntrusted
    CppInt() {
      super("int32_t");
    }
    
    @Override
    @OsUntrusted
    String getTypeIDObjectString(@OsUntrusted JInt.CppInt this) {
      return "new ::hadoop::TypeID(::hadoop::RIOTYPE_INT)";
    }
  }

  /** Creates a new instance of JInt */
  public @OsUntrusted JInt() {
    setJavaType(new @OsUntrusted JavaInt());
    setCppType(new @OsUntrusted CppInt());
    setCType(new @OsUntrusted CType());
  }
  
  @Override
  @OsUntrusted
  String getSignature(@OsUntrusted JInt this) {
    return "i";
  }
}
