/* Generated By:JavaCC: Do not edit this line. Rcc.java */
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

package org.apache.hadoop.record.compiler.generated;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.record.compiler.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.io.File;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Rcc implements @OsUntrusted RccConstants {
  private static @OsUntrusted String language = "java";
  private static @OsUntrusted String destDir = ".";
  private static @OsUntrusted ArrayList<@OsUntrusted String> recFiles = new @OsUntrusted ArrayList<@OsUntrusted String>();
  private static @OsUntrusted ArrayList<@OsUntrusted String> cmdargs = new @OsUntrusted ArrayList<@OsUntrusted String>();
  private static @OsUntrusted JFile curFile;
  private static @OsUntrusted Hashtable<@OsUntrusted String, @OsUntrusted JRecord> recTab;
  private static @OsUntrusted String curDir = ".";
  private static @OsUntrusted String curFileName;
  private static @OsUntrusted String curModuleName;

  public static void main(@OsUntrusted String @OsUntrusted [] args) {
    System.exit(driver(args));
  }

  public static void usage() {
    System.err.println("Usage: rcc --language [java|c++] ddl-files");
  }

  public static @OsUntrusted int driver(@OsUntrusted String @OsUntrusted [] args) {
    for (@OsUntrusted int i=0; i<args.length; i++) {
      if ("-l".equalsIgnoreCase(args[i]) ||
          "--language".equalsIgnoreCase(args[i])) {
        language = args[i+1].toLowerCase();
        i++;
      } else if ("-d".equalsIgnoreCase(args[i]) ||
                 "--destdir".equalsIgnoreCase(args[i])) {
        destDir = args[i+1];
        i++;
      } else if (args[i].startsWith("-")) {
        @OsUntrusted
        String arg = args[i].substring(1);
        if (arg.startsWith("-")) {
          arg = arg.substring(1);
        }
        cmdargs.add(arg.toLowerCase());
      } else {
        recFiles.add(args[i]);
      }
    }
    if (recFiles.size() == 0) {
      usage();
      return 1;
    }
    for (@OsUntrusted int i=0; i<recFiles.size(); i++) {
      curFileName = recFiles.get(i);
      @OsUntrusted
      File file = new @OsUntrusted File(curFileName);
      try {
        @OsUntrusted
        FileReader reader = new @OsUntrusted FileReader(file);
        @OsUntrusted
        Rcc parser = new @OsUntrusted Rcc(reader);
        try {
          recTab = new @OsUntrusted Hashtable<@OsUntrusted String, @OsUntrusted JRecord>();
          curFile = parser.Input();
        } catch (@OsUntrusted ParseException e) {
          System.err.println(e.toString());
          return 1;
        }
        try {
          reader.close();
        } catch (@OsUntrusted IOException e) {
        }
      } catch (@OsUntrusted FileNotFoundException e) {
        System.err.println("File " + recFiles.get(i) +
                           " Not found.");
        return 1;
      }
      try {
        @OsUntrusted
        int retCode = curFile.genCode(language, destDir, cmdargs);
        if (retCode != 0) { return retCode; }
      } catch (@OsUntrusted IOException e) {
        System.err.println(e.toString());
        return 1;
      }
    }
    return 0;
  }

  final public @OsUntrusted JFile Input(@OsUntrusted Rcc this) throws ParseException {
    @OsUntrusted
    ArrayList<@OsUntrusted JFile> ilist = new @OsUntrusted ArrayList<@OsUntrusted JFile>();
    @OsUntrusted
    ArrayList<@OsUntrusted JRecord> rlist = new @OsUntrusted ArrayList<@OsUntrusted JRecord>();
    @OsUntrusted
    JFile i;
    @OsUntrusted
    ArrayList<@OsUntrusted JRecord> l;
    label_1:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case INCLUDE_TKN:
        i = Include();
        ilist.add(i);
        break;
      case MODULE_TKN:
        l = Module();
        rlist.addAll(l);
        break;
      default:
        jj_la1[0] = jj_gen;
        jj_consume_token(-1);
        throw new @OsUntrusted ParseException();
      }
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case MODULE_TKN:
      case INCLUDE_TKN:
        ;
        break;
      default:
        jj_la1[1] = jj_gen;
        break label_1;
      }
    }
    jj_consume_token(0);
    {if (true) return new @OsUntrusted JFile(curFileName, ilist, rlist);}
    throw new @OsUntrusted Error("Missing return statement in function");
  }

  final public @OsUntrusted JFile Include(@OsUntrusted Rcc this) throws ParseException {
    @OsUntrusted
    String fname;
    @OsUntrusted
    Token t;
    jj_consume_token(INCLUDE_TKN);
    t = jj_consume_token(CSTRING_TKN);
    @OsUntrusted
    JFile ret = null;
    fname = t.image.replaceAll("^\"", "").replaceAll("\"$","");
    @OsUntrusted
    File file = new @OsUntrusted File(curDir, fname);
    @OsUntrusted
    String tmpDir = curDir;
    @OsUntrusted
    String tmpFile = curFileName;
    curDir = file.getParent();
    curFileName = file.getName();
    try {
      @OsUntrusted
      FileReader reader = new @OsUntrusted FileReader(file);
      @OsUntrusted
      Rcc parser = new @OsUntrusted Rcc(reader);
      try {
        ret = parser.Input();
        System.out.println(fname + " Parsed Successfully");
      } catch (@OsUntrusted ParseException e) {
        System.out.println(e.toString());
        System.exit(1);
      }
      try {
        reader.close();
      } catch (@OsUntrusted IOException e) {
      }
    } catch (@OsUntrusted FileNotFoundException e) {
      System.out.println("File " + fname +
                         " Not found.");
      System.exit(1);
    }
    curDir = tmpDir;
    curFileName = tmpFile;
    {if (true) return ret;}
    throw new @OsUntrusted Error("Missing return statement in function");
  }

  final public @OsUntrusted ArrayList<@OsUntrusted JRecord> Module(@OsUntrusted Rcc this) throws ParseException {
    @OsUntrusted
    String mName;
    @OsUntrusted
    ArrayList<@OsUntrusted JRecord> rlist;
    jj_consume_token(MODULE_TKN);
    mName = ModuleName();
    curModuleName = mName;
    jj_consume_token(LBRACE_TKN);
    rlist = RecordList();
    jj_consume_token(RBRACE_TKN);
    {if (true) return rlist;}
    throw new @OsUntrusted Error("Missing return statement in function");
  }

  final public @OsUntrusted String ModuleName(@OsUntrusted Rcc this) throws ParseException {
    @OsUntrusted
    String name = "";
    @OsUntrusted
    Token t;
    t = jj_consume_token(IDENT_TKN);
    name += t.image;
    label_2:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case DOT_TKN:
        ;
        break;
      default:
        jj_la1[2] = jj_gen;
        break label_2;
      }
      jj_consume_token(DOT_TKN);
      t = jj_consume_token(IDENT_TKN);
      name += "." + t.image;
    }
    {if (true) return name;}
    throw new @OsUntrusted Error("Missing return statement in function");
  }

  final public @OsUntrusted ArrayList<@OsUntrusted JRecord> RecordList(@OsUntrusted Rcc this) throws ParseException {
    @OsUntrusted
    ArrayList<@OsUntrusted JRecord> rlist = new @OsUntrusted ArrayList<@OsUntrusted JRecord>();
    @OsUntrusted
    JRecord r;
    label_3:
    while (true) {
      r = Record();
      rlist.add(r);
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case RECORD_TKN:
        ;
        break;
      default:
        jj_la1[3] = jj_gen;
        break label_3;
      }
    }
    {if (true) return rlist;}
    throw new @OsUntrusted Error("Missing return statement in function");
  }

  final public @OsUntrusted JRecord Record(@OsUntrusted Rcc this) throws ParseException {
    @OsUntrusted
    String rname;
    @OsUntrusted
    ArrayList<@OsUntrusted JField<@OsUntrusted JType>> flist = new @OsUntrusted ArrayList<@OsUntrusted JField<@OsUntrusted JType>>();
    @OsUntrusted
    Token t;
    @OsUntrusted
    JField<@OsUntrusted JType> f;
    jj_consume_token(RECORD_TKN);
    t = jj_consume_token(IDENT_TKN);
    rname = t.image;
    jj_consume_token(LBRACE_TKN);
    label_4:
    while (true) {
      f = Field();
      flist.add(f);
      jj_consume_token(SEMICOLON_TKN);
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case BYTE_TKN:
      case BOOLEAN_TKN:
      case INT_TKN:
      case LONG_TKN:
      case FLOAT_TKN:
      case DOUBLE_TKN:
      case USTRING_TKN:
      case BUFFER_TKN:
      case VECTOR_TKN:
      case MAP_TKN:
      case IDENT_TKN:
        ;
        break;
      default:
        jj_la1[4] = jj_gen;
        break label_4;
      }
    }
    jj_consume_token(RBRACE_TKN);
    @OsUntrusted
    String fqn = curModuleName + "." + rname;
    @OsUntrusted
    JRecord r = new @OsUntrusted JRecord(fqn, flist);
    recTab.put(fqn, r);
    {if (true) return r;}
    throw new @OsUntrusted Error("Missing return statement in function");
  }

  final public @OsUntrusted JField<@OsUntrusted JType> Field(@OsUntrusted Rcc this) throws ParseException {
    @OsUntrusted
    JType jt;
    @OsUntrusted
    Token t;
    jt = Type();
    t = jj_consume_token(IDENT_TKN);
    {if (true) return new @OsUntrusted JField<@OsUntrusted JType>(t.image, jt);}
    throw new @OsUntrusted Error("Missing return statement in function");
  }

  final public @OsUntrusted JType Type(@OsUntrusted Rcc this) throws ParseException {
    @OsUntrusted
    JType jt;
    @OsUntrusted
    Token t;
    @OsUntrusted
    String rname;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case MAP_TKN:
      jt = Map();
      {if (true) return jt;}
      break;
    case VECTOR_TKN:
      jt = Vector();
      {if (true) return jt;}
      break;
    case BYTE_TKN:
      jj_consume_token(BYTE_TKN);
      {if (true) return new @OsUntrusted JByte();}
      break;
    case BOOLEAN_TKN:
      jj_consume_token(BOOLEAN_TKN);
      {if (true) return new @OsUntrusted JBoolean();}
      break;
    case INT_TKN:
      jj_consume_token(INT_TKN);
      {if (true) return new @OsUntrusted JInt();}
      break;
    case LONG_TKN:
      jj_consume_token(LONG_TKN);
      {if (true) return new @OsUntrusted JLong();}
      break;
    case FLOAT_TKN:
      jj_consume_token(FLOAT_TKN);
      {if (true) return new @OsUntrusted JFloat();}
      break;
    case DOUBLE_TKN:
      jj_consume_token(DOUBLE_TKN);
      {if (true) return new @OsUntrusted JDouble();}
      break;
    case USTRING_TKN:
      jj_consume_token(USTRING_TKN);
      {if (true) return new @OsUntrusted JString();}
      break;
    case BUFFER_TKN:
      jj_consume_token(BUFFER_TKN);
      {if (true) return new @OsUntrusted JBuffer();}
      break;
    case IDENT_TKN:
      rname = ModuleName();
      if (rname.indexOf('.', 0) < 0) {
        rname = curModuleName + "." + rname;
      }
      @OsUntrusted
      JRecord r = recTab.get(rname);
      if (r == null) {
        System.out.println("Type " + rname + " not known. Exiting.");
        System.exit(1);
      }
      {if (true) return r;}
      break;
    default:
      jj_la1[5] = jj_gen;
      jj_consume_token(-1);
      throw new @OsUntrusted ParseException();
    }
    throw new @OsUntrusted Error("Missing return statement in function");
  }

  final public @OsUntrusted JMap Map(@OsUntrusted Rcc this) throws ParseException {
    @OsUntrusted
    JType jt1;
    @OsUntrusted
    JType jt2;
    jj_consume_token(MAP_TKN);
    jj_consume_token(LT_TKN);
    jt1 = Type();
    jj_consume_token(COMMA_TKN);
    jt2 = Type();
    jj_consume_token(GT_TKN);
    {if (true) return new @OsUntrusted JMap(jt1, jt2);}
    throw new @OsUntrusted Error("Missing return statement in function");
  }

  final public @OsUntrusted JVector Vector(@OsUntrusted Rcc this) throws ParseException {
    @OsUntrusted
    JType jt;
    jj_consume_token(VECTOR_TKN);
    jj_consume_token(LT_TKN);
    jt = Type();
    jj_consume_token(GT_TKN);
    {if (true) return new @OsUntrusted JVector(jt);}
    throw new @OsUntrusted Error("Missing return statement in function");
  }

  public @OsUntrusted RccTokenManager token_source;
  @OsUntrusted
  SimpleCharStream jj_input_stream;
  public @OsUntrusted Token token;
  public @OsUntrusted Token jj_nt;
  private @OsUntrusted int jj_ntk;
  private @OsUntrusted int jj_gen;
  final private @OsUntrusted int @OsUntrusted [] jj_la1 = new @OsUntrusted int @OsUntrusted [6];
  static private @OsUntrusted int @OsUntrusted [] jj_la1_0;
  static private @OsUntrusted int @OsUntrusted [] jj_la1_1;
  static {
    jj_la1_0();
    jj_la1_1();
  }
  private static void jj_la1_0() {
    jj_la1_0 = new @OsUntrusted int @OsUntrusted [] {0x2800, 0x2800, 0x40000000, 0x1000, 0xffc000, 0xffc000,};
  }
  private static void jj_la1_1() {
    jj_la1_1 = new @OsUntrusted int @OsUntrusted [] {0x0, 0x0, 0x0, 0x0, 0x1, 0x1,};
  }

  public @OsUntrusted Rcc(java.io.InputStream stream) {
    this(stream, null);
  }
  public @OsUntrusted Rcc(java.io.InputStream stream, @OsUntrusted String encoding) {
    try { jj_input_stream = new @OsUntrusted SimpleCharStream(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new @OsUntrusted RuntimeException(e); }
    token_source = new @OsUntrusted RccTokenManager(jj_input_stream);
    token = new @OsUntrusted Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (@OsUntrusted int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  public void ReInit(@OsUntrusted Rcc this, java.io.InputStream stream) {
    ReInit(stream, null);
  }
  public void ReInit(@OsUntrusted Rcc this, java.io.InputStream stream, @OsUntrusted String encoding) {
    try { jj_input_stream.ReInit(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new @OsUntrusted RuntimeException(e); }
    token_source.ReInit(jj_input_stream);
    token = new @OsUntrusted Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (@OsUntrusted int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  public @OsUntrusted Rcc(java.io.Reader stream) {
    jj_input_stream = new @OsUntrusted SimpleCharStream(stream, 1, 1);
    token_source = new @OsUntrusted RccTokenManager(jj_input_stream);
    token = new @OsUntrusted Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (@OsUntrusted int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  public void ReInit(@OsUntrusted Rcc this, java.io.Reader stream) {
    jj_input_stream.ReInit(stream, 1, 1);
    token_source.ReInit(jj_input_stream);
    token = new @OsUntrusted Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (@OsUntrusted int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  public @OsUntrusted Rcc(@OsUntrusted RccTokenManager tm) {
    token_source = tm;
    token = new @OsUntrusted Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (@OsUntrusted int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  public void ReInit(@OsUntrusted Rcc this, @OsUntrusted RccTokenManager tm) {
    token_source = tm;
    token = new @OsUntrusted Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (@OsUntrusted int i = 0; i < 6; i++) jj_la1[i] = -1;
  }

  final private @OsUntrusted Token jj_consume_token(@OsUntrusted Rcc this, @OsUntrusted int kind) throws ParseException {
    @OsUntrusted
    Token oldToken;
    if ((oldToken = token).next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    if (token.kind == kind) {
      jj_gen++;
      return token;
    }
    token = oldToken;
    jj_kind = kind;
    throw generateParseException();
  }

  final public @OsUntrusted Token getNextToken(@OsUntrusted Rcc this) {
    if (token.next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    jj_gen++;
    return token;
  }

  final public @OsUntrusted Token getToken(@OsUntrusted Rcc this, @OsUntrusted int index) {
    @OsUntrusted
    Token t = token;
    for (@OsUntrusted int i = 0; i < index; i++) {
      if (t.next != null) t = t.next;
      else t = t.next = token_source.getNextToken();
    }
    return t;
  }

  final private @OsUntrusted int jj_ntk(@OsUntrusted Rcc this) {
    if ((jj_nt=token.next) == null)
      return (jj_ntk = (token.next=token_source.getNextToken()).kind);
    else
      return (jj_ntk = jj_nt.kind);
  }

  private java.util.Vector<@OsUntrusted int @OsUntrusted []> jj_expentries = new java.util.Vector<@OsUntrusted int @OsUntrusted []>();
  private @OsUntrusted int @OsUntrusted [] jj_expentry;
  private @OsUntrusted int jj_kind = -1;

  public @OsUntrusted ParseException generateParseException(@OsUntrusted Rcc this) {
    jj_expentries.removeAllElements();
    @OsUntrusted
    boolean @OsUntrusted [] la1tokens = new @OsUntrusted boolean @OsUntrusted [33];
    for (@OsUntrusted int i = 0; i < 33; i++) {
      la1tokens[i] = false;
    }
    if (jj_kind >= 0) {
      la1tokens[jj_kind] = true;
      jj_kind = -1;
    }
    for (@OsUntrusted int i = 0; i < 6; i++) {
      if (jj_la1[i] == jj_gen) {
        for (@OsUntrusted int j = 0; j < 32; j++) {
          if ((jj_la1_0[i] & (1<<j)) != 0) {
            la1tokens[j] = true;
          }
          if ((jj_la1_1[i] & (1<<j)) != 0) {
            la1tokens[32+j] = true;
          }
        }
      }
    }
    for (@OsUntrusted int i = 0; i < 33; i++) {
      if (la1tokens[i]) {
        jj_expentry = new @OsUntrusted int @OsUntrusted [1];
        jj_expentry[0] = i;
        jj_expentries.addElement(jj_expentry);
      }
    }
    @OsUntrusted
    int @OsUntrusted [] @OsUntrusted [] exptokseq = new @OsUntrusted int @OsUntrusted [jj_expentries.size()] @OsUntrusted [];
    for (@OsUntrusted int i = 0; i < jj_expentries.size(); i++) {
      exptokseq[i] = jj_expentries.elementAt(i);
    }
    return new @OsUntrusted ParseException(token, exptokseq, tokenImage);
  }

  final public void enable_tracing(@OsUntrusted Rcc this) {
  }

  final public void disable_tracing(@OsUntrusted Rcc this) {
  }

}
