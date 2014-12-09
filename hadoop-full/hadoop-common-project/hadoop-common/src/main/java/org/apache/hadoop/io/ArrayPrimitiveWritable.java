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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is a wrapper class.  It wraps a Writable implementation around
 * an array of primitives (e.g., int[], long[], etc.), with optimized 
 * wire format, and without creating new objects per element.
 * 
 * This is a wrapper class only; it does not make a copy of the 
 * underlying array.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ArrayPrimitiveWritable implements @OsUntrusted Writable {
  
  //componentType is determined from the component type of the value array 
  //during a "set" operation.  It must be primitive.
  private @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> componentType = null; 
  //declaredComponentType need not be declared, but if you do (by using the
  //ArrayPrimitiveWritable(Class<?>) constructor), it will provide typechecking
  //for all "set" operations.
  private @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> declaredComponentType = null;
  private @OsUntrusted int length;
  private @OsUntrusted Object value; //must be an array of <componentType>[length]
  
  private static final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>> PRIMITIVE_NAMES = 
    new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>>(16);
  static {
    PRIMITIVE_NAMES.put(boolean.class.getName(), boolean.class);
    PRIMITIVE_NAMES.put(byte.class.getName(), byte.class);
    PRIMITIVE_NAMES.put(char.class.getName(), char.class);
    PRIMITIVE_NAMES.put(short.class.getName(), short.class);
    PRIMITIVE_NAMES.put(int.class.getName(), int.class);
    PRIMITIVE_NAMES.put(long.class.getName(), long.class);
    PRIMITIVE_NAMES.put(float.class.getName(), float.class);
    PRIMITIVE_NAMES.put(double.class.getName(), double.class);
  }
  
  private static @OsUntrusted Class<@OsUntrusted ?> getPrimitiveClass(@OsUntrusted String className) {
    return PRIMITIVE_NAMES.get(className);
  }
  
  private static void checkPrimitive(@OsUntrusted Class<@OsUntrusted ?> componentType) {
    if (componentType == null) { 
      throw new @OsUntrusted HadoopIllegalArgumentException("null component type not allowed"); 
    }
    if (! PRIMITIVE_NAMES.containsKey(componentType.getName())) {
      throw new @OsUntrusted HadoopIllegalArgumentException("input array component type "
          + componentType.getName() + " is not a candidate primitive type");
    }
  }
  
  private void checkDeclaredComponentType(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted Class<@OsUntrusted ?> componentType) {
    if ((declaredComponentType != null) 
        && (componentType != declaredComponentType)) {
      throw new @OsUntrusted HadoopIllegalArgumentException("input array component type "
          + componentType.getName() + " does not match declared type "
          + declaredComponentType.getName());     
    }
  }
  
  private static void checkArray(@OsUntrusted Object value) {
    if (value == null) { 
      throw new @OsUntrusted HadoopIllegalArgumentException("null value not allowed"); 
    }
    if (! value.getClass().isArray()) {
      throw new @OsUntrusted HadoopIllegalArgumentException("non-array value of class "
          + value.getClass() + " not allowed");             
    }
  }
  
  /**
   * Construct an empty instance, for use during Writable read
   */
  public @OsUntrusted ArrayPrimitiveWritable() {
    //empty constructor
  }
  
  /**
   * Construct an instance of known type but no value yet
   * for use with type-specific wrapper classes
   */
  public @OsUntrusted ArrayPrimitiveWritable(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> componentType) {
    checkPrimitive(componentType);
    this.declaredComponentType = componentType;
  }
  
  /**
   * Wrap an existing array of primitives
   * @param value - array of primitives
   */
  public @OsUntrusted ArrayPrimitiveWritable(@OsUntrusted Object value) {
    set(value);
  }
  
  /**
   * Get the original array.  
   * Client must cast it back to type componentType[]
   * (or may use type-specific wrapper classes).
   * @return - original array as Object
   */
  public @OsUntrusted Object get(@OsUntrusted ArrayPrimitiveWritable this) { return value; }
  
  public @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> getComponentType(@OsUntrusted ArrayPrimitiveWritable this) { return componentType; }
  
  public @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> getDeclaredComponentType(@OsUntrusted ArrayPrimitiveWritable this) { return declaredComponentType; }
  
  public @OsUntrusted boolean isDeclaredComponentType(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> componentType) {
    return componentType == declaredComponentType;
  }
  
  public void set(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted Object value) {
    checkArray(value);
    @OsUntrusted
    Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> componentType = value.getClass().getComponentType();
    checkPrimitive(componentType);
    checkDeclaredComponentType(componentType);
    this.componentType = componentType;
    this.value = value;
    this.length = Array.getLength(value);
  }
  
  /**
   * Do not use this class.
   * This is an internal class, purely for ObjectWritable to use as
   * a label class for transparent conversions of arrays of primitives
   * during wire protocol reads and writes.
   */
  static class Internal extends @OsUntrusted ArrayPrimitiveWritable {
    @OsUntrusted
    Internal() {             //use for reads
      super(); 
    }
    
    @OsUntrusted
    Internal(@OsUntrusted Object value) { //use for writes
      super(value);
    }
  } //end Internal subclass declaration

  /* 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  @SuppressWarnings("deprecation")
  public void write(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataOutput out) throws IOException {
    // write componentType 
    UTF8.writeString(out, componentType.getName());      
    // write length
    out.writeInt(length);

    // do the inner loop.  Walk the decision tree only once.
    if (componentType == Boolean.TYPE) {          // boolean
      writeBooleanArray(out);
    } else if (componentType == Character.TYPE) { // char
      writeCharArray(out);
    } else if (componentType == Byte.TYPE) {      // byte
      writeByteArray(out);
    } else if (componentType == Short.TYPE) {     // short
      writeShortArray(out);
    } else if (componentType == Integer.TYPE) {   // int
      writeIntArray(out);
    } else if (componentType == Long.TYPE) {      // long
      writeLongArray(out);
    } else if (componentType == Float.TYPE) {     // float
      writeFloatArray(out);
    } else if (componentType == Double.TYPE) {    // double
      writeDoubleArray(out);
    } else {
      throw new @OsUntrusted IOException("Component type " + componentType.toString()
          + " is set as the output type, but no encoding is implemented for this type.");
    }
  }

  /* 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataInput in) throws IOException {
    
    // read and set the component type of the array
    @SuppressWarnings("deprecation")
    @OsUntrusted
    String className = UTF8.readString(in);
    @OsUntrusted
    Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> componentType = getPrimitiveClass(className);
    if (componentType == null) {
      throw new @OsUntrusted IOException("encoded array component type "
          + className + " is not a candidate primitive type");
    }
    checkDeclaredComponentType(componentType);
    this.componentType = componentType;
  
    // read and set the length of the array
    @OsUntrusted
    int length = in.readInt();
    if (length < 0) {
      throw new @OsUntrusted IOException("encoded array length is negative " + length);
    }
    this.length = length;
    
    // construct and read in the array
    value = Array.newInstance(componentType, length);

    // do the inner loop.  Walk the decision tree only once.
    if (componentType == Boolean.TYPE) {             // boolean
      readBooleanArray(in);
    } else if (componentType == Character.TYPE) {    // char
      readCharArray(in);
    } else if (componentType == Byte.TYPE) {         // byte
      readByteArray(in);
    } else if (componentType == Short.TYPE) {        // short
      readShortArray(in);
    } else if (componentType == Integer.TYPE) {      // int
      readIntArray(in);
    } else if (componentType == Long.TYPE) {         // long
      readLongArray(in);
    } else if (componentType == Float.TYPE) {        // float
      readFloatArray(in);
    } else if (componentType == Double.TYPE) {       // double
      readDoubleArray(in);
    } else {
      throw new @OsUntrusted IOException("Encoded type " + className
          + " converted to valid component type " + componentType.toString()
          + " but no encoding is implemented for this type.");
    }
  }
  
  //For efficient implementation, there's no way around
  //the following massive code duplication.
  
  private void writeBooleanArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataOutput out) throws IOException {
    @OsUntrusted
    boolean @OsUntrusted [] v = (@OsUntrusted boolean @OsUntrusted []) value;
    for (@OsUntrusted int i = 0; i < length; i++)
      out.writeBoolean(v[i]);
  }
  
  private void writeCharArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataOutput out) throws IOException {
    @OsUntrusted
    char @OsUntrusted [] v = (@OsUntrusted char @OsUntrusted []) value;
    for (@OsUntrusted int i = 0; i < length; i++)
      out.writeChar(v[i]);
  }
  
  private void writeByteArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataOutput out) throws IOException {
    out.write((@OsUntrusted byte @OsUntrusted []) value, 0, length);
  }
  
  private void writeShortArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataOutput out) throws IOException {
    @OsUntrusted
    short @OsUntrusted [] v = (@OsUntrusted short @OsUntrusted []) value;
    for (@OsUntrusted int i = 0; i < length; i++)
      out.writeShort(v[i]);
  }
  
  private void writeIntArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataOutput out) throws IOException {
    @OsUntrusted
    int @OsUntrusted [] v = (@OsUntrusted int @OsUntrusted []) value;
    for (@OsUntrusted int i = 0; i < length; i++)
      out.writeInt(v[i]);
  }
  
  private void writeLongArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataOutput out) throws IOException {
    @OsUntrusted
    long @OsUntrusted [] v = (@OsUntrusted long @OsUntrusted []) value;
    for (@OsUntrusted int i = 0; i < length; i++)
      out.writeLong(v[i]);
  }
  
  private void writeFloatArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataOutput out) throws IOException {
    @OsUntrusted
    float @OsUntrusted [] v = (@OsUntrusted float @OsUntrusted []) value;
    for (@OsUntrusted int i = 0; i < length; i++)
      out.writeFloat(v[i]);
  }
  
  private void writeDoubleArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataOutput out) throws IOException {
    @OsUntrusted
    double @OsUntrusted [] v = (@OsUntrusted double @OsUntrusted []) value;
    for (@OsUntrusted int i = 0; i < length; i++)
      out.writeDouble(v[i]);
  }

  private void readBooleanArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    boolean @OsUntrusted [] v = (@OsUntrusted boolean @OsUntrusted []) value;
    for (@OsUntrusted int i = 0; i < length; i++)
      v[i] = in.readBoolean(); 
  }
  
  private void readCharArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    char @OsUntrusted [] v = (@OsUntrusted char @OsUntrusted []) value;
    for (@OsUntrusted int i = 0; i < length; i++)
      v[i] = in.readChar(); 
  }
  
  private void readByteArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataInput in) throws IOException {
    in.readFully((@OsUntrusted byte @OsUntrusted []) value, 0, length);
  }
  
  private void readShortArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    short @OsUntrusted [] v = (@OsUntrusted short @OsUntrusted []) value;
    for (@OsUntrusted int i = 0; i < length; i++)
      v[i] = in.readShort(); 
  }
  
  private void readIntArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    int @OsUntrusted [] v = (@OsUntrusted int @OsUntrusted []) value;
    for (@OsUntrusted int i = 0; i < length; i++)
      v[i] = in.readInt(); 
  }
  
  private void readLongArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    long @OsUntrusted [] v = (@OsUntrusted long @OsUntrusted []) value;
    for (@OsUntrusted int i = 0; i < length; i++)
      v[i] = in.readLong(); 
  }
  
  private void readFloatArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    float @OsUntrusted [] v = (@OsUntrusted float @OsUntrusted []) value;
    for (@OsUntrusted int i = 0; i < length; i++)
      v[i] = in.readFloat(); 
  }
  
  private void readDoubleArray(@OsUntrusted ArrayPrimitiveWritable this, @OsUntrusted DataInput in) throws IOException {
    @OsUntrusted
    double @OsUntrusted [] v = (@OsUntrusted double @OsUntrusted []) value;
    for (@OsUntrusted int i = 0; i < length; i++)
      v[i] = in.readDouble(); 
  }
}

