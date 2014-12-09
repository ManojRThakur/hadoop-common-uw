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
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.io.*;
import java.util.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.ProtoUtil;

import com.google.protobuf.Message;

/** A polymorphic Writable that writes an instance with it's class name.
 * Handles arrays, strings and primitive types without a Writable wrapper.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ObjectWritable implements @OsUntrusted Writable, @OsUntrusted Configurable {

  private @OsUntrusted Class declaredClass;
  private @OsUntrusted Object instance;
  private @OsUntrusted Configuration conf;

  public @OsUntrusted ObjectWritable() {}
  
  public @OsUntrusted ObjectWritable(@OsUntrusted Object instance) {
    set(instance);
  }

  public @OsUntrusted ObjectWritable(@OsUntrusted Class declaredClass, @OsUntrusted Object instance) {
    this.declaredClass = declaredClass;
    this.instance = instance;
  }

  /** Return the instance, or null if none. */
  public @OsUntrusted Object get(@OsUntrusted ObjectWritable this) { return instance; }
  
  /** Return the class this is meant to be. */
  public @OsUntrusted Class getDeclaredClass(@OsUntrusted ObjectWritable this) { return declaredClass; }
  
  /** Reset the instance. */
  public void set(@OsUntrusted ObjectWritable this, @OsUntrusted Object instance) {
    this.declaredClass = instance.getClass();
    this.instance = instance;
  }
  
  @Override
  public @OsUntrusted String toString(@OsUntrusted ObjectWritable this) {
    return "OW[class=" + declaredClass + ",value=" + instance + "]";
  }

  
  @Override
  public void readFields(@OsUntrusted ObjectWritable this, @OsUntrusted DataInput in) throws IOException {
    readObject(in, this, this.conf);
  }
  
  @Override
  public void write(@OsUntrusted ObjectWritable this, @OsUntrusted DataOutput out) throws IOException {
    writeObject(out, instance, declaredClass, conf);
  }

  private static final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>> PRIMITIVE_NAMES = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>>();
  static {
    PRIMITIVE_NAMES.put("boolean", Boolean.TYPE);
    PRIMITIVE_NAMES.put("byte", Byte.TYPE);
    PRIMITIVE_NAMES.put("char", Character.TYPE);
    PRIMITIVE_NAMES.put("short", Short.TYPE);
    PRIMITIVE_NAMES.put("int", Integer.TYPE);
    PRIMITIVE_NAMES.put("long", Long.TYPE);
    PRIMITIVE_NAMES.put("float", Float.TYPE);
    PRIMITIVE_NAMES.put("double", Double.TYPE);
    PRIMITIVE_NAMES.put("void", Void.TYPE);
  }

  private static class NullInstance extends @OsUntrusted Configured implements @OsUntrusted Writable {
    private @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> declaredClass;
    public @OsUntrusted NullInstance() { super(null); }
    public @OsUntrusted NullInstance(@OsUntrusted Class declaredClass, @OsUntrusted Configuration conf) {
      super(conf);
      this.declaredClass = declaredClass;
    }
    @Override
    public void readFields(ObjectWritable.@OsUntrusted NullInstance this, @OsUntrusted DataInput in) throws IOException {
      @OsUntrusted
      String className = UTF8.readString(in);
      declaredClass = PRIMITIVE_NAMES.get(className);
      if (declaredClass == null) {
        try {
          declaredClass = getConf().getClassByName(className);
        } catch (@OsUntrusted ClassNotFoundException e) {
          throw new @OsUntrusted RuntimeException(e.toString());
        }
      }
    }
    @Override
    public void write(ObjectWritable.@OsUntrusted NullInstance this, @OsUntrusted DataOutput out) throws IOException {
      UTF8.writeString(out, declaredClass.getName());
    }
  }

  /** Write a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding. */
  public static void writeObject(@OsUntrusted DataOutput out, @OsUntrusted Object instance,
                                 @OsUntrusted
                                 Class declaredClass, 
                                 @OsUntrusted
                                 Configuration conf) throws IOException {
    writeObject(out, instance, declaredClass, conf, false);
  }
  
    /** 
     * Write a {@link Writable}, {@link String}, primitive type, or an array of
     * the preceding.  
     * 
     * @param allowCompactArrays - set true for RPC and internal or intra-cluster
     * usages.  Set false for inter-cluster, File, and other persisted output 
     * usages, to preserve the ability to interchange files with other clusters 
     * that may not be running the same version of software.  Sometime in ~2013 
     * we can consider removing this parameter and always using the compact format.
     */
    public static void writeObject(@OsUntrusted DataOutput out, @OsUntrusted Object instance,
        @OsUntrusted
        Class declaredClass, @OsUntrusted Configuration conf, @OsUntrusted boolean allowCompactArrays) 
    throws IOException {

    if (instance == null) {                       // null
      instance = new @OsUntrusted NullInstance(declaredClass, conf);
      declaredClass = Writable.class;
    }
    
    // Special case: must come before writing out the declaredClass.
    // If this is an eligible array of primitives,
    // wrap it in an ArrayPrimitiveWritable$Internal wrapper class.
    if (allowCompactArrays && declaredClass.isArray()
        && instance.getClass().getName().equals(declaredClass.getName())
        && instance.getClass().getComponentType().isPrimitive()) {
      instance = new ArrayPrimitiveWritable.@OsUntrusted Internal(instance);
      declaredClass = ArrayPrimitiveWritable.Internal.class;
    }

    UTF8.writeString(out, declaredClass.getName()); // always write declared

    if (declaredClass.isArray()) {     // non-primitive or non-compact array
      @OsUntrusted
      int length = Array.getLength(instance);
      out.writeInt(length);
      for (@OsUntrusted int i = 0; i < length; i++) {
        writeObject(out, Array.get(instance, i),
            declaredClass.getComponentType(), conf, allowCompactArrays);
      }
      
    } else if (declaredClass == ArrayPrimitiveWritable.Internal.class) {
      ((ArrayPrimitiveWritable.@OsUntrusted Internal) instance).write(out);
      
    } else if (declaredClass == String.class) {   // String
      UTF8.writeString(out, (@OsUntrusted String)instance);
      
    } else if (declaredClass.isPrimitive()) {     // primitive type

      if (declaredClass == Boolean.TYPE) {        // boolean
        out.writeBoolean(((@OsUntrusted Boolean)instance).booleanValue());
      } else if (declaredClass == Character.TYPE) { // char
        out.writeChar(((@OsUntrusted Character)instance).charValue());
      } else if (declaredClass == Byte.TYPE) {    // byte
        out.writeByte(((@OsUntrusted Byte)instance).byteValue());
      } else if (declaredClass == Short.TYPE) {   // short
        out.writeShort(((@OsUntrusted Short)instance).shortValue());
      } else if (declaredClass == Integer.TYPE) { // int
        out.writeInt(((@OsUntrusted Integer)instance).intValue());
      } else if (declaredClass == Long.TYPE) {    // long
        out.writeLong(((@OsUntrusted Long)instance).longValue());
      } else if (declaredClass == Float.TYPE) {   // float
        out.writeFloat(((@OsUntrusted Float)instance).floatValue());
      } else if (declaredClass == Double.TYPE) {  // double
        out.writeDouble(((@OsUntrusted Double)instance).doubleValue());
      } else if (declaredClass == Void.TYPE) {    // void
      } else {
        throw new @OsUntrusted IllegalArgumentException("Not a primitive: "+declaredClass);
      }
    } else if (declaredClass.isEnum()) {         // enum
      UTF8.writeString(out, ((@OsUntrusted Enum)instance).name());
    } else if (Writable.class.isAssignableFrom(declaredClass)) { // Writable
      UTF8.writeString(out, instance.getClass().getName());
      ((@OsUntrusted Writable)instance).write(out);

    } else if (Message.class.isAssignableFrom(declaredClass)) {
      ((@OsUntrusted Message)instance).writeDelimitedTo(
          DataOutputOutputStream.constructOutputStream(out));
    } else {
      throw new @OsUntrusted IOException("Can't write: "+instance+" as "+declaredClass);
    }
  }
  
  
  /** Read a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding. */
  public static @OsUntrusted Object readObject(@OsUntrusted DataInput in, @OsUntrusted Configuration conf)
    throws IOException {
    return readObject(in, null, conf);
  }
    
  /** Read a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding. */
  @SuppressWarnings("unchecked")
  public static @OsUntrusted Object readObject(@OsUntrusted DataInput in, @OsUntrusted ObjectWritable objectWritable, @OsUntrusted Configuration conf)
    throws IOException {
    @OsUntrusted
    String className = UTF8.readString(in);
    @OsUntrusted
    Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> declaredClass = PRIMITIVE_NAMES.get(className);
    if (declaredClass == null) {
      declaredClass = loadClass(conf, className);
    }
    
    @OsUntrusted
    Object instance;
    
    if (declaredClass.isPrimitive()) {            // primitive types

      if (declaredClass == Boolean.TYPE) {             // boolean
        instance = Boolean.valueOf(in.readBoolean());
      } else if (declaredClass == Character.TYPE) {    // char
        instance = Character.valueOf(in.readChar());
      } else if (declaredClass == Byte.TYPE) {         // byte
        instance = Byte.valueOf(in.readByte());
      } else if (declaredClass == Short.TYPE) {        // short
        instance = Short.valueOf(in.readShort());
      } else if (declaredClass == Integer.TYPE) {      // int
        instance = Integer.valueOf(in.readInt());
      } else if (declaredClass == Long.TYPE) {         // long
        instance = Long.valueOf(in.readLong());
      } else if (declaredClass == Float.TYPE) {        // float
        instance = Float.valueOf(in.readFloat());
      } else if (declaredClass == Double.TYPE) {       // double
        instance = Double.valueOf(in.readDouble());
      } else if (declaredClass == Void.TYPE) {         // void
        instance = null;
      } else {
        throw new @OsUntrusted IllegalArgumentException("Not a primitive: "+declaredClass);
      }

    } else if (declaredClass.isArray()) {              // array
      @OsUntrusted
      int length = in.readInt();
      instance = Array.newInstance(declaredClass.getComponentType(), length);
      for (@OsUntrusted int i = 0; i < length; i++) {
        Array.set(instance, i, readObject(in, conf));
      }
      
    } else if (declaredClass == ArrayPrimitiveWritable.Internal.class) {
      // Read and unwrap ArrayPrimitiveWritable$Internal array.
      // Always allow the read, even if write is disabled by allowCompactArrays.
      ArrayPrimitiveWritable.@OsUntrusted Internal temp = 
          new ArrayPrimitiveWritable.@OsUntrusted Internal();
      temp.readFields(in);
      instance = temp.get();
      declaredClass = instance.getClass();

    } else if (declaredClass == String.class) {        // String
      instance = UTF8.readString(in);
    } else if (declaredClass.isEnum()) {         // enum
      instance = Enum.valueOf((@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted Enum>) declaredClass, UTF8.readString(in));
    } else if (Message.class.isAssignableFrom(declaredClass)) {
      instance = tryInstantiateProtobuf(declaredClass, in);
    } else {                                      // Writable
      @OsUntrusted
      Class instanceClass = null;
      @OsUntrusted
      String str = UTF8.readString(in);
      instanceClass = loadClass(conf, str);
      
      @OsUntrusted
      Writable writable = WritableFactories.newInstance(instanceClass, conf);
      writable.readFields(in);
      instance = writable;

      if (instanceClass == NullInstance.class) {  // null
        declaredClass = ((@OsUntrusted NullInstance)instance).declaredClass;
        instance = null;
      }
    }

    if (objectWritable != null) {                 // store values
      objectWritable.declaredClass = declaredClass;
      objectWritable.instance = instance;
    }

    return instance;
      
  }

  /**
   * Try to instantiate a protocol buffer of the given message class
   * from the given input stream.
   * 
   * @param protoClass the class of the generated protocol buffer
   * @param dataIn the input stream to read from
   * @return the instantiated Message instance
   * @throws IOException if an IO problem occurs
   */
  private static @OsUntrusted Message tryInstantiateProtobuf(
      @OsUntrusted
      Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> protoClass,
      @OsUntrusted
      DataInput dataIn) throws IOException {

    try {
      if (dataIn instanceof @OsUntrusted InputStream) {
        // We can use the built-in parseDelimitedFrom and not have to re-copy
        // the data
        @OsUntrusted
        Method parseMethod = getStaticProtobufMethod(protoClass,
            "parseDelimitedFrom", InputStream.class);
        return (@OsUntrusted Message)parseMethod.invoke(null, (@OsUntrusted InputStream)dataIn);
      } else {
        // Have to read it into a buffer first, since protobuf doesn't deal
        // with the DataInput interface directly.
        
        // Read the size delimiter that writeDelimitedTo writes
        @OsUntrusted
        int size = ProtoUtil.readRawVarint32(dataIn);
        if (size < 0) {
          throw new @OsUntrusted IOException("Invalid size: " + size);
        }
      
        @OsUntrusted
        byte @OsUntrusted [] data = new @OsUntrusted byte @OsUntrusted [size];
        dataIn.readFully(data);
        @OsUntrusted
        Method parseMethod = getStaticProtobufMethod(protoClass,
            "parseFrom", byte[].class);
        return (@OsUntrusted Message)parseMethod.invoke(null, data);
      }
    } catch (@OsUntrusted InvocationTargetException e) {
      
      if (e.getCause() instanceof @OsUntrusted IOException) {
        throw (IOException)e.getCause();
      } else {
        throw new @OsUntrusted IOException(e.getCause());
      }
    } catch (@OsUntrusted IllegalAccessException iae) {
      throw new @OsUntrusted AssertionError("Could not access parse method in " +
          protoClass);
    }
  }

  static @OsUntrusted Method getStaticProtobufMethod(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> declaredClass, @OsUntrusted String method,
      @OsUntrusted
      Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> @OsUntrusted ... args) {

    try {
      return declaredClass.getMethod(method, args);
    } catch (@OsUntrusted Exception e) {
      // This is a bug in Hadoop - protobufs should all have this static method
      throw new @OsUntrusted AssertionError("Protocol buffer class " + declaredClass +
          " does not have an accessible parseFrom(InputStream) method!");
    }
  }

  /**
   * Find and load the class with given name <tt>className</tt> by first finding
   * it in the specified <tt>conf</tt>. If the specified <tt>conf</tt> is null,
   * try load it directly.
   */
  public static @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> loadClass(@OsUntrusted Configuration conf, @OsUntrusted String className) {
    @OsUntrusted
    Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> declaredClass = null;
    try {
      if (conf != null)
        declaredClass = conf.getClassByName(className);
      else
        declaredClass = Class.forName(className);
    } catch (@OsUntrusted ClassNotFoundException e) {
      throw new @OsUntrusted RuntimeException("readObject can't find class " + className,
          e);
    }
    return declaredClass;
  }

  @Override
  public void setConf(@OsUntrusted ObjectWritable this, @OsUntrusted Configuration conf) {
    this.conf = conf;
  }

  @Override
  public @OsUntrusted Configuration getConf(@OsUntrusted ObjectWritable this) {
    return this.conf;
  }
  
}
