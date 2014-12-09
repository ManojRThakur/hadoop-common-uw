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
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/**
 * Abstract Base class for all types supported by Hadoop Record I/O.
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
abstract public class JType {
  
  static @OsUntrusted String toCamelCase(@OsUntrusted String name) {
    @OsUntrusted
    char firstChar = name.charAt(0);
    if (Character.isLowerCase(firstChar)) {
      return ""+Character.toUpperCase(firstChar) + name.substring(1);
    }
    return name;
  }
  
  @OsUntrusted
  JavaType javaType;
  @OsUntrusted
  CppType cppType;
  @OsUntrusted
  CType cType;
  
  abstract class JavaType {
    private @OsUntrusted String name;
    private @OsUntrusted String methodSuffix;
    private @OsUntrusted String wrapper;
    private @OsUntrusted String typeIDByteString; // points to TypeID.RIOType 
    
    @OsUntrusted
    JavaType(@OsUntrusted String javaname,
        @OsUntrusted
        String suffix,
        @OsUntrusted
        String wrapper, 
        @OsUntrusted
        String typeIDByteString) { 
      this.name = javaname;
      this.methodSuffix = suffix;
      this.wrapper = wrapper;
      this.typeIDByteString = typeIDByteString;
    }

    void genDecl(@OsUntrusted JType.JavaType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append("private "+name+" "+fname+";\n");
    }
    
    void genStaticTypeInfo(@OsUntrusted JType.JavaType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append(Consts.RTI_VAR + ".addField(\"" + fname + "\", " +
          getTypeIDObjectString() + ");\n");
    }
    
    abstract @OsUntrusted String getTypeIDObjectString(@OsUntrusted JType.JavaType this);
    
    void genSetRTIFilter(@OsUntrusted JType.JavaType this, @OsUntrusted CodeBuffer cb, @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Integer> nestedStructMap) {
      // do nothing by default
      return;
    }

    /*void genRtiFieldCondition(CodeBuffer cb, String fname, int ct) {
      cb.append("if ((tInfo.fieldID.equals(\"" + fname + "\")) && (typeVal ==" +
          " org.apache.hadoop.record.meta." + getTypeIDByteString() + ")) {\n");
      cb.append("rtiFilterFields[i] = " + ct + ";\n");
      cb.append("}\n");
    }

    void genRtiNestedFieldCondition(CodeBuffer cb, String varName, int ct) {
      cb.append("if (" + varName + ".getElementTypeID().getTypeVal() == " +
          "org.apache.hadoop.record.meta." + getTypeIDByteString() + 
          ") {\n");
      cb.append("rtiFilterFields[i] = " + ct + ";\n");
      cb.append("}\n");  
    }*/

    void genConstructorParam(@OsUntrusted JType.JavaType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append("final "+name+" "+fname);
    }
    
    void genGetSet(@OsUntrusted JType.JavaType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append("public "+name+" get"+toCamelCase(fname)+"() {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
      cb.append("public void set"+toCamelCase(fname)+"(final "+name+" "+fname+") {\n");
      cb.append("this."+fname+"="+fname+";\n");
      cb.append("}\n");
    }
    
    @OsUntrusted
    String getType(@OsUntrusted JType.JavaType this) {
      return name;
    }
    
    @OsUntrusted
    String getWrapperType(@OsUntrusted JType.JavaType this) {
      return wrapper;
    }
    
    @OsUntrusted
    String getMethodSuffix(@OsUntrusted JType.JavaType this) {
      return methodSuffix;
    }
    
    @OsUntrusted
    String getTypeIDByteString(@OsUntrusted JType.JavaType this) {
      return typeIDByteString;
    }
    
    void genWriteMethod(@OsUntrusted JType.JavaType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname, @OsUntrusted String tag) {
      cb.append(Consts.RECORD_OUTPUT + ".write"+methodSuffix + 
          "("+fname+",\""+tag+"\");\n");
    }
    
    void genReadMethod(@OsUntrusted JType.JavaType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname, @OsUntrusted String tag, @OsUntrusted boolean decl) {
      if (decl) {
        cb.append(name+" "+fname+";\n");
      }
      cb.append(fname+"=" + Consts.RECORD_INPUT + ".read" + 
          methodSuffix+"(\""+tag+"\");\n");
    }
    
    void genCompareTo(@OsUntrusted JType.JavaType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname, @OsUntrusted String other) {
      cb.append(Consts.RIO_PREFIX + "ret = ("+fname+" == "+other+")? 0 :(("+
          fname+"<"+other+")?-1:1);\n");
    }
    
    abstract void genCompareBytes(@OsUntrusted JType.JavaType this, @OsUntrusted CodeBuffer cb);
    
    abstract void genSlurpBytes(@OsUntrusted JType.JavaType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String b, @OsUntrusted String s, @OsUntrusted String l);
    
    void genEquals(@OsUntrusted JType.JavaType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname, @OsUntrusted String peer) {
      cb.append(Consts.RIO_PREFIX + "ret = ("+fname+"=="+peer+");\n");
    }
    
    void genHashCode(@OsUntrusted JType.JavaType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append(Consts.RIO_PREFIX + "ret = (int)"+fname+";\n");
    }
    
    void genConstructorSet(@OsUntrusted JType.JavaType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append("this."+fname+" = "+fname+";\n");
    }
    
    void genClone(@OsUntrusted JType.JavaType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append(Consts.RIO_PREFIX + "other."+fname+" = this."+fname+";\n");
    }
  }
  
  abstract class CppType {
    private @OsUntrusted String name;
    
    @OsUntrusted
    CppType(@OsUntrusted String cppname) {
      name = cppname;
    }
    
    void genDecl(@OsUntrusted JType.CppType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append(name+" "+fname+";\n");
    }
    
    void genStaticTypeInfo(@OsUntrusted JType.CppType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append("p->addField(new ::std::string(\"" + 
          fname + "\"), " + getTypeIDObjectString() + ");\n");
    }
    
    void genGetSet(@OsUntrusted JType.CppType this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname) {
      cb.append("virtual "+name+" get"+toCamelCase(fname)+"() const {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
      cb.append("virtual void set"+toCamelCase(fname)+"("+name+" m_) {\n");
      cb.append(fname+"=m_;\n");
      cb.append("}\n");
    }
    
    abstract @OsUntrusted String getTypeIDObjectString(@OsUntrusted JType.CppType this);

    void genSetRTIFilter(@OsUntrusted JType.CppType this, @OsUntrusted CodeBuffer cb) {
      // do nothing by default
      return;
    }

    @OsUntrusted
    String getType(@OsUntrusted JType.CppType this) {
      return name;
    }
  }
  
  class CType {
    
  }
  
  abstract @OsUntrusted String getSignature(@OsUntrusted JType this);
  
  void setJavaType(@OsUntrusted JType this, @OsUntrusted JavaType jType) {
    this.javaType = jType;
  }
  
  @OsUntrusted
  JavaType getJavaType(@OsUntrusted JType this) {
    return javaType;
  }
  
  void setCppType(@OsUntrusted JType this, @OsUntrusted CppType cppType) {
    this.cppType = cppType;
  }
  
  @OsUntrusted
  CppType getCppType(@OsUntrusted JType this) {
    return cppType;
  }
  
  void setCType(@OsUntrusted JType this, @OsUntrusted CType cType) {
    this.cType = cType;
  }
  
  @OsUntrusted
  CType getCType(@OsUntrusted JType this) {
    return cType;
  }
}
