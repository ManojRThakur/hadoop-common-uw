/*
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
package org.apache.hadoop.io.compress;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A factory that will find the correct codec for a given filename.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CompressionCodecFactory {

  public static final @OsUntrusted Log LOG =
    LogFactory.getLog(CompressionCodecFactory.class.getName());
  
  private static final @OsUntrusted ServiceLoader<@OsUntrusted CompressionCodec> CODEC_PROVIDERS =
    ServiceLoader.load(CompressionCodec.class);

  /**
   * A map from the reversed filename suffixes to the codecs.
   * This is probably overkill, because the maps should be small, but it 
   * automatically supports finding the longest matching suffix. 
   */
  private @OsUntrusted SortedMap<@OsUntrusted String, @OsUntrusted CompressionCodec> codecs = null;

    /**
     * A map from the reversed filename suffixes to the codecs.
     * This is probably overkill, because the maps should be small, but it
     * automatically supports finding the longest matching suffix.
     */
    private @OsUntrusted Map<@OsUntrusted String, @OsUntrusted CompressionCodec> codecsByName = null;

  /**
   * A map from class names to the codecs
   */
  private @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted CompressionCodec> codecsByClassName = null;

  private void addCodec(@OsUntrusted CompressionCodecFactory this, @OsUntrusted CompressionCodec codec) {
    @OsUntrusted
    String suffix = codec.getDefaultExtension();
    codecs.put(new @OsUntrusted StringBuilder(suffix).reverse().toString(), codec);
    codecsByClassName.put(codec.getClass().getCanonicalName(), codec);

    @OsUntrusted
    String codecName = codec.getClass().getSimpleName();
    codecsByName.put(codecName.toLowerCase(), codec);
    if (codecName.endsWith("Codec")) {
      codecName = codecName.substring(0, codecName.length() - "Codec".length());
      codecsByName.put(codecName.toLowerCase(), codec);
    }
  }

  /**
   * Print the extension map out as a string.
   */
  @Override
  public @OsUntrusted String toString(@OsUntrusted CompressionCodecFactory this) {
    @OsUntrusted
    StringBuilder buf = new @OsUntrusted StringBuilder();
    @OsUntrusted
    Iterator<Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted CompressionCodec>> itr = 
      codecs.entrySet().iterator();
    buf.append("{ ");
    if (itr.hasNext()) {
      Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted CompressionCodec> entry = itr.next();
      buf.append(entry.getKey());
      buf.append(": ");
      buf.append(entry.getValue().getClass().getName());
      while (itr.hasNext()) {
        entry = itr.next();
        buf.append(", ");
        buf.append(entry.getKey());
        buf.append(": ");
        buf.append(entry.getValue().getClass().getName());
      }
    }
    buf.append(" }");
    return buf.toString();
  }

  /**
   * Get the list of codecs discovered via a Java ServiceLoader, or
   * listed in the configuration. Codecs specified in configuration come
   * later in the returned list, and are considered to override those
   * from the ServiceLoader.
   * @param conf the configuration to look in
   * @return a list of the {@link CompressionCodec} classes
   */
  public static @OsUntrusted List<@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted CompressionCodec>> getCodecClasses(@OsUntrusted Configuration conf) {
    @OsUntrusted
    List<@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted CompressionCodec>> result
      = new @OsUntrusted ArrayList<@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted CompressionCodec>>();
    // Add codec classes discovered via service loading
    synchronized (CODEC_PROVIDERS) {
      // CODEC_PROVIDERS is a lazy collection. Synchronize so it is
      // thread-safe. See HADOOP-8406.
      for (@OsUntrusted CompressionCodec codec : CODEC_PROVIDERS) {
        result.add(codec.getClass());
      }
    }
    // Add codec classes from configuration
    @OsUntrusted
    String codecsString = conf.get("io.compression.codecs");
    if (codecsString != null) {
      @OsUntrusted
      StringTokenizer codecSplit = new @OsUntrusted StringTokenizer(codecsString, ",");
      while (codecSplit.hasMoreElements()) {
        @OsUntrusted
        String codecSubstring = codecSplit.nextToken().trim();
        if (codecSubstring.length() != 0) {
          try {
            @OsUntrusted
            Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> cls = conf.getClassByName(codecSubstring);
            if (!CompressionCodec.class.isAssignableFrom(cls)) {
              throw new @OsUntrusted IllegalArgumentException("Class " + codecSubstring +
                                                 " is not a CompressionCodec");
            }
            result.add(cls.asSubclass(CompressionCodec.class));
          } catch (@OsUntrusted ClassNotFoundException ex) {
            throw new @OsUntrusted IllegalArgumentException("Compression codec " + 
                                               codecSubstring + " not found.",
                                               ex);
          }
        }
      }
    }
    return result;
  }
  
  /**
   * Sets a list of codec classes in the configuration. In addition to any
   * classes specified using this method, {@link CompressionCodec} classes on
   * the classpath are discovered using a Java ServiceLoader.
   * @param conf the configuration to modify
   * @param classes the list of classes to set
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static void setCodecClasses(@OsUntrusted Configuration conf,
                                     @OsUntrusted
                                     List<@OsUntrusted Class> classes) {
    @OsUntrusted
    StringBuilder buf = new @OsUntrusted StringBuilder();
    @OsUntrusted
    Iterator<@OsUntrusted Class> itr = classes.iterator();
    if (itr.hasNext()) {
      @OsUntrusted
      Class cls = itr.next();
      buf.append(cls.getName());
      while(itr.hasNext()) {
        buf.append(',');
        buf.append(itr.next().getName());
      }
    }

    //ostrusted, all strings are classnames separated by commas
    conf.set("io.compression.codecs", (@OsTrusted String) buf.toString());
  }
  
  /**
   * Find the codecs specified in the config value io.compression.codecs 
   * and register them. Defaults to gzip and deflate.
   */
  public @OsUntrusted CompressionCodecFactory(@OsUntrusted Configuration conf) {
    codecs = new @OsUntrusted TreeMap<@OsUntrusted String, @OsUntrusted CompressionCodec>();
    codecsByClassName = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted CompressionCodec>();
    codecsByName = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted CompressionCodec>();
    @OsUntrusted
    List<@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted CompressionCodec>> codecClasses = getCodecClasses(conf);
    if (codecClasses == null || codecClasses.isEmpty()) {
      addCodec(new @OsUntrusted GzipCodec());
      addCodec(new @OsUntrusted DefaultCodec());      
    } else {
      for (@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted CompressionCodec> codecClass : codecClasses) {
        addCodec(ReflectionUtils.newInstance(codecClass, conf));
      }
    }
  }
  
  /**
   * Find the relevant compression codec for the given file based on its
   * filename suffix.
   * @param file the filename to check
   * @return the codec object
   */
  public @OsUntrusted CompressionCodec getCodec(@OsUntrusted CompressionCodecFactory this, @OsUntrusted Path file) {
    @OsUntrusted
    CompressionCodec result = null;
    if (codecs != null) {
      @OsUntrusted
      String filename = file.getName();
      @OsUntrusted
      String reversedFilename = new @OsUntrusted StringBuilder(filename).reverse().toString();
      @OsUntrusted
      SortedMap<@OsUntrusted String, @OsUntrusted CompressionCodec> subMap = 
        codecs.headMap(reversedFilename);
      if (!subMap.isEmpty()) {
        @OsUntrusted
        String potentialSuffix = subMap.lastKey();
        if (reversedFilename.startsWith(potentialSuffix)) {
          result = codecs.get(potentialSuffix);
        }
      }
    }
    return result;
  }
  
  /**
   * Find the relevant compression codec for the codec's canonical class name.
   * @param classname the canonical class name of the codec
   * @return the codec object
   */
  public @OsUntrusted CompressionCodec getCodecByClassName(@OsUntrusted CompressionCodecFactory this, @OsUntrusted String classname) {
    if (codecsByClassName == null) {
      return null;
    }
    return codecsByClassName.get(classname);
  }

    /**
     * Find the relevant compression codec for the codec's canonical class name
     * or by codec alias.
     * <p/>
     * Codec aliases are case insensitive.
     * <p/>
     * The code alias is the short class name (without the package name).
     * If the short class name ends with 'Codec', then there are two aliases for
     * the codec, the complete short class name and the short class name without
     * the 'Codec' ending. For example for the 'GzipCodec' codec class name the
     * alias are 'gzip' and 'gzipcodec'.
     *
     * @param codecName the canonical class name of the codec
     * @return the codec object
     */
    public @OsUntrusted CompressionCodec getCodecByName(@OsUntrusted CompressionCodecFactory this, @OsUntrusted String codecName) {
      if (codecsByClassName == null) {
        return null;
      }
      @OsUntrusted
      CompressionCodec codec = getCodecByClassName(codecName);
      if (codec == null) {
        // trying to get the codec by name in case the name was specified instead a class
        codec = codecsByName.get(codecName.toLowerCase());
      }
      return codec;
    }

    /**
     * Find the relevant compression codec for the codec's canonical class name
     * or by codec alias and returns its implemetation class.
     * <p/>
     * Codec aliases are case insensitive.
     * <p/>
     * The code alias is the short class name (without the package name).
     * If the short class name ends with 'Codec', then there are two aliases for
     * the codec, the complete short class name and the short class name without
     * the 'Codec' ending. For example for the 'GzipCodec' codec class name the
     * alias are 'gzip' and 'gzipcodec'.
     *
     * @param codecName the canonical class name of the codec
     * @return the codec class
     */
    public @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted CompressionCodec> getCodecClassByName(@OsUntrusted CompressionCodecFactory this, @OsUntrusted String codecName) {
      @OsUntrusted
      CompressionCodec codec = getCodecByName(codecName);
      if (codec == null) {
        return null;
      }
      return codec.getClass();
    }

  /**
   * Removes a suffix from a filename, if it has it.
   * @param filename the filename to strip
   * @param suffix the suffix to remove
   * @return the shortened filename
   */
  public static @OsUntrusted String removeSuffix(@OsUntrusted String filename, @OsUntrusted String suffix) {
    if (filename.endsWith(suffix)) {
      return filename.substring(0, filename.length() - suffix.length());
    }
    return filename;
  }
  
  /**
   * A little test program.
   * @param args
   */
  public static void main(@OsUntrusted String @OsUntrusted [] args) throws Exception {
    @OsUntrusted
    Configuration conf = new @OsUntrusted Configuration();
    @OsUntrusted
    CompressionCodecFactory factory = new @OsUntrusted CompressionCodecFactory(conf);
    @OsUntrusted
    boolean encode = false;
    for(@OsUntrusted int i=0; i < args.length; ++i) {
      if ("-in".equals(args[i])) {
        encode = true;
      } else if ("-out".equals(args[i])) {
        encode = false;
      } else {
        @OsUntrusted
        CompressionCodec codec = factory.getCodec(new @OsUntrusted Path(args[i]));
        if (codec == null) {
          System.out.println("Codec for " + args[i] + " not found.");
        } else { 
          if (encode) {
            @OsUntrusted
            CompressionOutputStream out = null;
            java.io.InputStream in = null;
            try {
              out = codec.createOutputStream(
                  new java.io.FileOutputStream(args[i]));
              @OsUntrusted
              byte @OsUntrusted [] buffer = new @OsUntrusted byte @OsUntrusted [100];
              @OsUntrusted
              String inFilename = removeSuffix(args[i], 
                  codec.getDefaultExtension());
              in = new java.io.FileInputStream(inFilename);
              @OsUntrusted
              int len = in.read(buffer);
              while (len > 0) {
                out.write(buffer, 0, len);
                len = in.read(buffer);
              }
            } finally {
              if(out != null) { out.close(); }
              if(in  != null) { in.close(); }
            }
          } else {
            @OsUntrusted
            CompressionInputStream in = null;
            try {
              in = codec.createInputStream(
                  new java.io.FileInputStream(args[i]));
              @OsUntrusted
              byte @OsUntrusted [] buffer = new @OsUntrusted byte @OsUntrusted [100];
              @OsUntrusted
              int len = in.read(buffer);
              while (len > 0) {
                System.out.write(buffer, 0, len);
                len = in.read(buffer);
              }
            } finally {
              if(in != null) { in.close(); }
            }
          }
        }
      }
    }
  }
}
