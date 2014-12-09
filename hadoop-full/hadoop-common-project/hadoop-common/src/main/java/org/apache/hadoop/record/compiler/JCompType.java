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


/**
 * Abstract base class for all the "compound" types such as ustring,
 * buffer, vector, map, and record.
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
abstract class JCompType extends @OsUntrusted JType {
  
  abstract class JavaCompType extends @OsUntrusted JavaType {
    
    @OsUntrusted
    JavaCompType(@OsUntrusted String type, @OsUntrusted String suffix, @OsUntrusted String wrapper, 
        @OsUntrusted
        String typeIDByteString) { 
      super(type, suffix, wrapper, typeIDByteString);
    }
    
    @Override
    void genCompareTo(@OsUntrusted JCompType.JavaCompType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname, @OsUntrusted String other) {
      cb.append(Consts.RIO_PREFIX + "ret = "+fname+".compareTo("+other+");\n");
    }
    
    @Override
    void genEquals(@OsUntrusted JCompType.JavaCompType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname, @OsUntrusted String peer) {
      cb.append(Consts.RIO_PREFIX + "ret = "+fname+".equals("+peer+");\n");
    }
    
    @Override
    void genHashCode(@OsUntrusted JCompType.JavaCompType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append(Consts.RIO_PREFIX + "ret = "+fname+".hashCode();\n");
    }
    
    @Override
    void genClone(@OsUntrusted JCompType.JavaCompType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append(Consts.RIO_PREFIX + "other."+fname+" = ("+getType()+") this."+
          fname+".clone();\n");
    }
  }
  
  abstract class CppCompType extends @OsUntrusted CppType {
    
    @OsUntrusted
    CppCompType(@OsUntrusted String type) {
      super(type);
    }
    
    @Override
    void genGetSet(@OsUntrusted JCompType.CppCompType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append("virtual const "+getType()+"& get"+toCamelCase(fname)+"() const {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
      cb.append("virtual "+getType()+"& get"+toCamelCase(fname)+"() {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
    }
  }
  
  class CCompType extends @OsUntrusted CType {
    
  }
}
