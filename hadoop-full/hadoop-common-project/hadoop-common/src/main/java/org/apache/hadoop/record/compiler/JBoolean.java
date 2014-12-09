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
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JBoolean extends @OsUntrusted JType {
  
  class JavaBoolean extends @OsUntrusted JType.@OsUntrusted JavaType {
    
    @OsUntrusted
    JavaBoolean() {
      super("boolean", "Bool", "Boolean", "TypeID.RIOType.BOOL");
    }
    
    @Override
    void genCompareTo(@OsUntrusted JBoolean.JavaBoolean this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname, @OsUntrusted String other) {
      cb.append(Consts.RIO_PREFIX + "ret = ("+fname+" == "+other+")? 0 : ("+
          fname+"?1:-1);\n");
    }
    
    @Override
    @OsUntrusted
    String getTypeIDObjectString(@OsUntrusted JBoolean.JavaBoolean this) {
      return "org.apache.hadoop.record.meta.TypeID.BoolTypeID";
    }

    @Override
    void genHashCode(@OsUntrusted JBoolean.JavaBoolean this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append(Consts.RIO_PREFIX + "ret = ("+fname+")?0:1;\n");
    }
    
    // In Binary format, boolean is written as byte. true = 1, false = 0
    @Override
    void genSlurpBytes(@OsUntrusted JBoolean.JavaBoolean this, @OsUntrusted CodeBuffer cb, @OsUntrusted String b, @OsUntrusted String s, @OsUntrusted String l) {
      cb.append("{\n");
      cb.append("if ("+l+"<1) {\n");
      cb.append("throw new java.io.IOException(\"Boolean is exactly 1 byte."+
                " Provided buffer is smaller.\");\n");
      cb.append("}\n");
      cb.append(s+"++; "+l+"--;\n");
      cb.append("}\n");
    }
    
    // In Binary format, boolean is written as byte. true = 1, false = 0
    @Override
    void genCompareBytes(@OsUntrusted JBoolean.JavaBoolean this, @OsUntrusted CodeBuffer cb) {
      cb.append("{\n");
      cb.append("if (l1<1 || l2<1) {\n");
      cb.append("throw new java.io.IOException(\"Boolean is exactly 1 byte."+
                " Provided buffer is smaller.\");\n");
      cb.append("}\n");
      cb.append("if (b1[s1] != b2[s2]) {\n");
      cb.append("return (b1[s1]<b2[s2])? -1 : 0;\n");
      cb.append("}\n");
      cb.append("s1++; s2++; l1--; l2--;\n");
      cb.append("}\n");
    }
  }
  
  class CppBoolean extends @OsUntrusted CppType {
    
    @OsUntrusted
    CppBoolean() {
      super("bool");
    }
    
    @Override
    @OsUntrusted
    String getTypeIDObjectString(@OsUntrusted JBoolean.CppBoolean this) {
      return "new ::hadoop::TypeID(::hadoop::RIOTYPE_BOOL)";
    }
  }

  /** Creates a new instance of JBoolean */
  public @OsUntrusted JBoolean() {
    setJavaType(new @OsUntrusted JavaBoolean());
    setCppType(new @OsUntrusted CppBoolean());
    setCType(new @OsUntrusted CType());
  }
  
  @Override
  @OsUntrusted
  String getSignature(@OsUntrusted JBoolean this) {
    return "z";
  }
}
