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
public class JString extends @OsUntrusted JCompType {
    
  class JavaString extends @OsUntrusted JavaCompType {
    
    @OsUntrusted
    JavaString() {
      super("String", "String", "String", "TypeID.RIOType.STRING");
    }
    
    @Override
    @OsUntrusted
    String getTypeIDObjectString(@OsUntrusted JString.JavaString this) {
      return "org.apache.hadoop.record.meta.TypeID.StringTypeID";
    }

    @Override
    void genSlurpBytes(@OsUntrusted JString.JavaString this, @OsUntrusted CodeBuffer cb, @OsUntrusted String b, @OsUntrusted String s, @OsUntrusted String l) {
      cb.append("{\n");
      cb.append("int i = org.apache.hadoop.record.Utils.readVInt("+b+", "+s+");\n");
      cb.append("int z = org.apache.hadoop.record.Utils.getVIntSize(i);\n");
      cb.append(s+"+=(z+i); "+l+"-= (z+i);\n");
      cb.append("}\n");
    }
    
    @Override
    void genCompareBytes(@OsUntrusted JString.JavaString this, @OsUntrusted CodeBuffer cb) {
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
    
    @Override
    void genClone(@OsUntrusted JString.JavaString this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append(Consts.RIO_PREFIX + "other."+fname+" = this."+fname+";\n");
    }
  }

  class CppString extends @OsUntrusted CppCompType {
    
    @OsUntrusted
    CppString() {
      super("::std::string");
    }
    
    @Override
    @OsUntrusted
    String getTypeIDObjectString(@OsUntrusted JString.CppString this) {
      return "new ::hadoop::TypeID(::hadoop::RIOTYPE_STRING)";
    }
  }
  
  /** Creates a new instance of JString */
  public @OsUntrusted JString() {
    setJavaType(new @OsUntrusted JavaString());
    setCppType(new @OsUntrusted CppString());
    setCType(new @OsUntrusted CCompType());
  }
    
  @Override
  @OsUntrusted
  String getSignature(@OsUntrusted JString this) {
    return "s";
  }
}