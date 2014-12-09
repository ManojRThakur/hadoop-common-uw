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
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class JVector extends @OsUntrusted JCompType {
  
  static private @OsUntrusted int level = 0;
  
  static private @OsUntrusted String getId(@OsUntrusted String id) { return id+getLevel(); }
  
  static private @OsUntrusted String getLevel() { return Integer.toString(level); }
  
  static private void incrLevel() { level++; }
  
  static private void decrLevel() { level--; }
  
  private @OsUntrusted JType type;
  
  class JavaVector extends @OsUntrusted JavaCompType {
    
    private @OsUntrusted JType.@OsUntrusted JavaType element;
    
    @OsUntrusted
    JavaVector(@OsUntrusted JType.@OsUntrusted JavaType t) {
      super("java.util.ArrayList<"+t.getWrapperType()+">",
            "Vector", "java.util.ArrayList<"+t.getWrapperType()+">",
            "TypeID.RIOType.VECTOR");
      element = t;
    }
    
    @Override
    @OsUntrusted
    String getTypeIDObjectString(@OsUntrusted JVector.JavaVector this) {
      return "new org.apache.hadoop.record.meta.VectorTypeID(" + 
      element.getTypeIDObjectString() + ")";
    }

    @Override
    void genSetRTIFilter(@OsUntrusted JVector.JavaVector this, @OsUntrusted CodeBuffer cb, @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Integer> nestedStructMap) {
      element.genSetRTIFilter(cb, nestedStructMap);
    }

    @Override
    void genCompareTo(@OsUntrusted JVector.JavaVector this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname, @OsUntrusted String other) {
      cb.append("{\n");
      incrLevel();
      cb.append("int "+getId(Consts.RIO_PREFIX + "len1")+" = "+fname+
          ".size();\n");
      cb.append("int "+getId(Consts.RIO_PREFIX + "len2")+" = "+other+
          ".size();\n");
      cb.append("for(int "+getId(Consts.RIO_PREFIX + "vidx")+" = 0; "+
          getId(Consts.RIO_PREFIX + "vidx")+"<"+getId(Consts.RIO_PREFIX + "len1")+
          " && "+getId(Consts.RIO_PREFIX + "vidx")+"<"+
          getId(Consts.RIO_PREFIX + "len2")+"; "+
          getId(Consts.RIO_PREFIX + "vidx")+"++) {\n");
      cb.append(element.getType()+" "+getId(Consts.RIO_PREFIX + "e1")+
                " = "+fname+
                ".get("+getId(Consts.RIO_PREFIX + "vidx")+");\n");
      cb.append(element.getType()+" "+getId(Consts.RIO_PREFIX + "e2")+
                " = "+other+
                ".get("+getId(Consts.RIO_PREFIX + "vidx")+");\n");
      element.genCompareTo(cb, getId(Consts.RIO_PREFIX + "e1"), 
          getId(Consts.RIO_PREFIX + "e2"));
      cb.append("if (" + Consts.RIO_PREFIX + "ret != 0) { return " +
          Consts.RIO_PREFIX + "ret; }\n");
      cb.append("}\n");
      cb.append(Consts.RIO_PREFIX + "ret = ("+getId(Consts.RIO_PREFIX + "len1")+
          " - "+getId(Consts.RIO_PREFIX + "len2")+");\n");
      decrLevel();
      cb.append("}\n");
    }
    
    @Override
    void genReadMethod(@OsUntrusted JVector.JavaVector this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname, @OsUntrusted String tag, @OsUntrusted boolean decl) {
      if (decl) {
        cb.append(getType()+" "+fname+";\n");
      }
      cb.append("{\n");
      incrLevel();
      cb.append("org.apache.hadoop.record.Index "+
          getId(Consts.RIO_PREFIX + "vidx")+" = " + 
          Consts.RECORD_INPUT + ".startVector(\""+tag+"\");\n");
      cb.append(fname+"=new "+getType()+"();\n");
      cb.append("for (; !"+getId(Consts.RIO_PREFIX + "vidx")+".done(); " + 
          getId(Consts.RIO_PREFIX + "vidx")+".incr()) {\n");
      element.genReadMethod(cb, getId(Consts.RIO_PREFIX + "e"), 
          getId(Consts.RIO_PREFIX + "e"), true);
      cb.append(fname+".add("+getId(Consts.RIO_PREFIX + "e")+");\n");
      cb.append("}\n");
      cb.append(Consts.RECORD_INPUT + ".endVector(\""+tag+"\");\n");
      decrLevel();
      cb.append("}\n");
    }
    
    @Override
    void genWriteMethod(@OsUntrusted JVector.JavaVector this, @OsUntrusted CodeBuffer cb, @OsUntrusted String fname, @OsUntrusted String tag) {
      cb.append("{\n");
      incrLevel();
      cb.append(Consts.RECORD_OUTPUT + ".startVector("+fname+",\""+tag+"\");\n");
      cb.append("int "+getId(Consts.RIO_PREFIX + "len")+" = "+fname+".size();\n");
      cb.append("for(int "+getId(Consts.RIO_PREFIX + "vidx")+" = 0; " + 
          getId(Consts.RIO_PREFIX + "vidx")+"<"+getId(Consts.RIO_PREFIX + "len")+
          "; "+getId(Consts.RIO_PREFIX + "vidx")+"++) {\n");
      cb.append(element.getType()+" "+getId(Consts.RIO_PREFIX + "e")+" = "+
          fname+".get("+getId(Consts.RIO_PREFIX + "vidx")+");\n");
      element.genWriteMethod(cb, getId(Consts.RIO_PREFIX + "e"), 
          getId(Consts.RIO_PREFIX + "e"));
      cb.append("}\n");
      cb.append(Consts.RECORD_OUTPUT + ".endVector("+fname+",\""+tag+"\");\n");
      cb.append("}\n");
      decrLevel();
    }
    
    @Override
    void genSlurpBytes(@OsUntrusted JVector.JavaVector this, @OsUntrusted CodeBuffer cb, @OsUntrusted String b, @OsUntrusted String s, @OsUntrusted String l) {
      cb.append("{\n");
      incrLevel();
      cb.append("int "+getId("vi")+
                " = org.apache.hadoop.record.Utils.readVInt("+b+", "+s+");\n");
      cb.append("int "+getId("vz")+
                " = org.apache.hadoop.record.Utils.getVIntSize("+getId("vi")+");\n");
      cb.append(s+"+="+getId("vz")+"; "+l+"-="+getId("vz")+";\n");
      cb.append("for (int "+getId("vidx")+" = 0; "+getId("vidx")+
                " < "+getId("vi")+"; "+getId("vidx")+"++)");
      element.genSlurpBytes(cb, b, s, l);
      decrLevel();
      cb.append("}\n");
    }
    
    @Override
    void genCompareBytes(@OsUntrusted JVector.JavaVector this, @OsUntrusted CodeBuffer cb) {
      cb.append("{\n");
      incrLevel();
      cb.append("int "+getId("vi1")+
                " = org.apache.hadoop.record.Utils.readVInt(b1, s1);\n");
      cb.append("int "+getId("vi2")+
                " = org.apache.hadoop.record.Utils.readVInt(b2, s2);\n");
      cb.append("int "+getId("vz1")+
                " = org.apache.hadoop.record.Utils.getVIntSize("+getId("vi1")+");\n");
      cb.append("int "+getId("vz2")+
                " = org.apache.hadoop.record.Utils.getVIntSize("+getId("vi2")+");\n");
      cb.append("s1+="+getId("vz1")+"; s2+="+getId("vz2")+
                "; l1-="+getId("vz1")+"; l2-="+getId("vz2")+";\n");
      cb.append("for (int "+getId("vidx")+" = 0; "+getId("vidx")+
                " < "+getId("vi1")+" && "+getId("vidx")+" < "+getId("vi2")+
                "; "+getId("vidx")+"++)");
      element.genCompareBytes(cb);
      cb.append("if ("+getId("vi1")+" != "+getId("vi2")+
                ") { return ("+getId("vi1")+"<"+getId("vi2")+")?-1:0; }\n");
      decrLevel();
      cb.append("}\n");
    }
  }
  
  class CppVector extends @OsUntrusted CppCompType {
    
    private @OsUntrusted JType.@OsUntrusted CppType element;
    
    @OsUntrusted
    CppVector(@OsUntrusted JType.@OsUntrusted CppType t) {
      super("::std::vector< "+t.getType()+" >");
      element = t;
    }
    
    @Override
    @OsUntrusted
    String getTypeIDObjectString(@OsUntrusted JVector.CppVector this) {
      return "new ::hadoop::VectorTypeID(" +    
      element.getTypeIDObjectString() + ")";
    }

    @Override
    void genSetRTIFilter(@OsUntrusted JVector.CppVector this, @OsUntrusted CodeBuffer cb) {
      element.genSetRTIFilter(cb);
    }

  }
  
  /** Creates a new instance of JVector */
  public @OsUntrusted JVector(@OsUntrusted JType t) {
    type = t;
    setJavaType(new @OsUntrusted JavaVector(t.getJavaType()));
    setCppType(new @OsUntrusted CppVector(t.getCppType()));
    setCType(new @OsUntrusted CCompType());
  }
  
  @Override
  @OsUntrusted
  String getSignature(@OsUntrusted JVector this) {
    return "[" + type.getSignature() + "]";
  }
}
