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

package org.apache.hadoop.conf;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.WeakHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;
import com.google.common.base.Preconditions;
import ostrusted.quals.PolyOsTrusted;

/** 
 * Provides access to configuration parameters.
 *
 * <h4 id="Resources">Resources</h4>
 *
 * <p>Configurations are specified by resources. A resource contains a set of
 * name/value pairs as XML data. Each resource is named by either a 
 * <code>String</code> or by a {@link Path}. If named by a <code>String</code>, 
 * then the classpath is examined for a file with that name.  If named by a 
 * <code>Path</code>, then the local filesystem is examined directly, without 
 * referring to the classpath.
 *
 * <p>Unless explicitly turned off, Hadoop by default specifies two 
 * resources, loaded in-order from the classpath: <ol>
 * <li><tt><a href="{@docRoot}/../core-default.html">core-default.xml</a>
 * </tt>: Read-only defaults for hadoop.</li>
 * <li><tt>core-site.xml</tt>: Site-specific configuration for a given hadoop
 * installation.</li>
 * </ol>
 * Applications may add additional resources, which are loaded
 * subsequent to these resources in the order they are added.
 * 
 * <h4 id="FinalParams">Final Parameters</h4>
 *
 * <p>Configuration parameters may be declared <i>final</i>. 
 * Once a resource declares a value final, no subsequently-loaded 
 * resource can alter that value.  
 * For example, one might define a final parameter with:
 * <tt><pre>
 *  &lt;property&gt;
 *    &lt;name&gt;dfs.hosts.include&lt;/name&gt;
 *    &lt;value&gt;/etc/hadoop/conf/hosts.include&lt;/value&gt;
 *    <b>&lt;final&gt;true&lt;/final&gt;</b>
 *  &lt;/property&gt;</pre></tt>
 *
 * Administrators typically define parameters as final in 
 * <tt>core-site.xml</tt> for values that user applications may not alter.
 *
 * <h4 id="VariableExpansion">Variable Expansion</h4>
 *
 * <p>Value strings are first processed for <i>variable expansion</i>. The
 * available properties are:<ol>
 * <li>Other properties defined in this Configuration; and, if a name is
 * undefined here,</li>
 * <li>Properties in {@link System#getProperties()}.</li>
 * </ol>
 *
 * <p>For example, if a configuration resource contains the following property
 * definitions: 
 * <tt><pre>
 *  &lt;property&gt;
 *    &lt;name&gt;basedir&lt;/name&gt;
 *    &lt;value&gt;/user/${<i>user.name</i>}&lt;/value&gt;
 *  &lt;/property&gt;
 *  
 *  &lt;property&gt;
 *    &lt;name&gt;tempdir&lt;/name&gt;
 *    &lt;value&gt;${<i>basedir</i>}/tmp&lt;/value&gt;
 *  &lt;/property&gt;</pre></tt>
 *
 * When <tt>conf.get("tempdir")</tt> is called, then <tt>${<i>basedir</i>}</tt>
 * will be resolved to another property in this Configuration, while
 * <tt>${<i>user.name</i>}</tt> would then ordinarily be resolved to the value
 * of the System property with that name.
 * By default, warnings will be given to any deprecated configuration 
 * parameters and these are suppressible by configuring
 * <tt>log4j.logger.org.apache.hadoop.conf.Configuration.deprecation</tt> in
 * log4j.properties file.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Configuration implements @OsUntrusted Iterable<Map.@OsUntrusted Entry<@OsUntrusted String, @OsTrusted String>>,
                                      @OsUntrusted Writable {
  private static final @OsUntrusted Log LOG =
    LogFactory.getLog(Configuration.class);

  private static final @OsUntrusted Log LOG_DEPRECATION =
    LogFactory.getLog("org.apache.hadoop.conf.Configuration.deprecation");

  private @OsUntrusted boolean quietmode = true;
  
  private static class Resource {
    private final @OsUntrusted Object resource;
    private final @OsUntrusted String name;
    
    public @OsUntrusted Resource(@OsUntrusted Object resource) {
      this(resource, resource.toString());
    }
    
    public @OsUntrusted Resource(@OsUntrusted Object resource, @OsUntrusted String name) {
      this.resource = resource;
      this.name = name;
    }
    
    public @OsUntrusted String getName(Configuration.@OsUntrusted Resource this){
      return name;
    }
    
    public @OsUntrusted Object getResource(Configuration.@OsUntrusted Resource this) {
      return resource;
    }
    
    @Override
    public @OsUntrusted String toString(Configuration.@OsUntrusted Resource this) {
      return name;
    }
  }
  
  /**
   * List of configuration resources.
   */
  private @OsUntrusted ArrayList<@OsTrusted Resource> resources = new @OsUntrusted ArrayList<@OsTrusted Resource>();
  
  /**
   * The value reported as the setting resource when a key is set
   * by code rather than a file resource by dumpConfiguration.
   */
  static final @OsUntrusted String UNKNOWN_RESOURCE = "Unknown";


  /**
   * List of configuration parameters marked <b>final</b>. 
   */
  private @OsUntrusted Set<@OsUntrusted String> finalParameters = new @OsUntrusted HashSet<@OsUntrusted String>();
  
  private @OsUntrusted boolean loadDefaults = true;
  
  /**
   * Configuration objects
   */
  private static final @OsUntrusted WeakHashMap<@OsUntrusted Configuration, @OsUntrusted Object> REGISTRY = 
    new @OsUntrusted WeakHashMap<@OsUntrusted Configuration, @OsUntrusted Object>();
  
  /**
   * List of default Resources. Resources are loaded in the order of the list 
   * entries
   */
  private static final @OsUntrusted CopyOnWriteArrayList<@OsTrusted String> defaultResources =
    new @OsUntrusted CopyOnWriteArrayList<@OsTrusted String>();

  private static final @OsUntrusted Map<@OsUntrusted ClassLoader, @OsUntrusted Map<@OsUntrusted String, @OsUntrusted WeakReference<@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>>>>
    CACHE_CLASSES = new @OsUntrusted WeakHashMap<@OsUntrusted ClassLoader, @OsUntrusted Map<@OsUntrusted String, @OsUntrusted WeakReference<@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>>>>();

  /**
   * Sentinel value to store negative cache results in {@link #CACHE_CLASSES}.
   */
  private static final @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> NEGATIVE_CACHE_SENTINEL =
    NegativeCacheSentinel.class;

  /**
   * Stores the mapping of key to the resource which modifies or loads 
   * the key most recently
   */
  private @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted String @OsUntrusted []> updatingResource;
 
  /**
   * Class to keep the information about the keys which replace the deprecated
   * ones.
   * 
   * This class stores the new keys which replace the deprecated keys and also
   * gives a provision to have a custom message for each of the deprecated key
   * that is being replaced. It also provides method to get the appropriate
   * warning message which can be logged whenever the deprecated key is used.
   */
  private static class DeprecatedKeyInfo {
    private @OsUntrusted String @OsUntrusted [] newKeys;
    private @OsUntrusted String customMessage;
    private @OsUntrusted boolean accessed;
    @OsUntrusted
    DeprecatedKeyInfo(@OsUntrusted String @OsUntrusted [] newKeys, @OsUntrusted String customMessage) {
      this.newKeys = newKeys;
      this.customMessage = customMessage;
      accessed = false;
    }

    /**
     * Method to provide the warning message. It gives the custom message if
     * non-null, and default message otherwise.
     * @param key the associated deprecated key.
     * @return message that is to be logged when a deprecated key is used.
     */
    private final @OsUntrusted String getWarningMessage(Configuration.@OsUntrusted DeprecatedKeyInfo this, @OsUntrusted String key) {
      @OsUntrusted
      String warningMessage;
      if(customMessage == null) {
        @OsUntrusted
        StringBuilder message = new @OsUntrusted StringBuilder(key);
        @OsUntrusted
        String deprecatedKeySuffix = " is deprecated. Instead, use ";
        message.append(deprecatedKeySuffix);
        for (@OsUntrusted int i = 0; i < newKeys.length; i++) {
          message.append(newKeys[i]);
          if(i != newKeys.length-1) {
            message.append(", ");
          }
        }
        warningMessage = message.toString();
      }
      else {
        warningMessage = customMessage;
      }
      accessed = true;
      return warningMessage;
    }
  }
  
  /**
   * Stores the deprecated keys, the new keys which replace the deprecated keys
   * and custom message(if any provided).
   */
  private static @OsUntrusted Map<@OsUntrusted String, @OsUntrusted DeprecatedKeyInfo> deprecatedKeyMap = 
      new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted DeprecatedKeyInfo>();
  
  /**
   * Stores a mapping from superseding keys to the keys which they deprecate.
   */
  private static @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> reverseDeprecatedKeyMap =
      new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted String>();

  /**
   * Adds the deprecated key to the deprecation map.
   * It does not override any existing entries in the deprecation map.
   * This is to be used only by the developers in order to add deprecation of
   * keys, and attempts to call this method after loading resources once,
   * would lead to <tt>UnsupportedOperationException</tt>
   * 
   * If a key is deprecated in favor of multiple keys, they are all treated as 
   * aliases of each other, and setting any one of them resets all the others 
   * to the new value.
   * 
   * @param key
   * @param newKeys
   * @param customMessage
   * @deprecated use {@link #addDeprecation(String key, String newKey,
      String customMessage)} instead
   */
  @Deprecated
  public synchronized static void addDeprecation(@OsUntrusted String key, String[] newKeys,
      @OsUntrusted
      String customMessage) {
    if (key == null || key.length() == 0 ||
        newKeys == null || newKeys.length == 0) {
      throw new @OsUntrusted IllegalArgumentException();
    }
    if (!isDeprecated(key)) {
      @OsUntrusted
      DeprecatedKeyInfo newKeyInfo;
      newKeyInfo = new @OsUntrusted DeprecatedKeyInfo(newKeys, customMessage);
      deprecatedKeyMap.put(key, newKeyInfo);
      for (@OsUntrusted String newKey : newKeys) {
        reverseDeprecatedKeyMap.put(newKey, key);
      }
    }
  }
  
  /**
   * Adds the deprecated key to the deprecation map.
   * It does not override any existing entries in the deprecation map.
   * This is to be used only by the developers in order to add deprecation of
   * keys, and attempts to call this method after loading resources once,
   * would lead to <tt>UnsupportedOperationException</tt>
   * 
   * @param key
   * @param newKey
   * @param customMessage
   */
  public synchronized static void addDeprecation(@OsUntrusted String key, @OsUntrusted String newKey,
	      @OsUntrusted
	      String customMessage) {
	  addDeprecation(key, new @OsUntrusted String @OsUntrusted [] {newKey}, customMessage);
  }

  /**
   * Adds the deprecated key to the deprecation map when no custom message
   * is provided.
   * It does not override any existing entries in the deprecation map.
   * This is to be used only by the developers in order to add deprecation of
   * keys, and attempts to call this method after loading resources once,
   * would lead to <tt>UnsupportedOperationException</tt>
   * 
   * If a key is deprecated in favor of multiple keys, they are all treated as 
   * aliases of each other, and setting any one of them resets all the others 
   * to the new value.
   * 
   * @param key Key that is to be deprecated
   * @param newKeys list of keys that take up the values of deprecated key
   * @deprecated use {@link #addDeprecation(String key, String newKey)} instead
   */
  @Deprecated
  public synchronized static void addDeprecation(@OsUntrusted String key, @OsUntrusted String @OsUntrusted [] newKeys) {
    addDeprecation(key, newKeys, null);
  }
  
  /**
   * Adds the deprecated key to the deprecation map when no custom message
   * is provided.
   * It does not override any existing entries in the deprecation map.
   * This is to be used only by the developers in order to add deprecation of
   * keys, and attempts to call this method after loading resources once,
   * would lead to <tt>UnsupportedOperationException</tt>
   * 
   * @param key Key that is to be deprecated
   * @param newKey key that takes up the value of deprecated key
   */
  public synchronized static void addDeprecation(@OsUntrusted String key, @OsUntrusted String newKey) {
	addDeprecation(key, new @OsUntrusted String @OsUntrusted [] {newKey}, null);
  }
  
  /**
   * checks whether the given <code>key</code> is deprecated.
   * 
   * @param key the parameter which is to be checked for deprecation
   * @return <code>true</code> if the key is deprecated and 
   *         <code>false</code> otherwise.
   */
  public static @OsUntrusted boolean isDeprecated(@OsUntrusted String key) {
    return deprecatedKeyMap.containsKey(key);
  }

  /**
   * Returns the alternate name for a key if the property name is deprecated
   * or if deprecates a property name.
   *
   * @param name property name.
   * @return alternate name.
   */
  private @OsUntrusted String @OsUntrusted [] getAlternateNames(@OsUntrusted Configuration this, @OsUntrusted String name) {
    @OsUntrusted
    String altNames @OsUntrusted [] = null;
    @OsUntrusted
    DeprecatedKeyInfo keyInfo = deprecatedKeyMap.get(name);
    if (keyInfo == null) {
      altNames = (reverseDeprecatedKeyMap.get(name) != null ) ? 
        new @OsUntrusted String @OsUntrusted [] {reverseDeprecatedKeyMap.get(name)} : null;
      if(altNames != null && altNames.length > 0) {
    	//To help look for other new configs for this deprecated config
    	keyInfo = deprecatedKeyMap.get(altNames[0]);
      }      
    } 
    if(keyInfo != null && keyInfo.newKeys.length > 0) {
      @OsUntrusted
      List<@OsUntrusted String> list = new @OsUntrusted ArrayList<@OsUntrusted String>(); 
      if(altNames != null) {
    	  list.addAll(Arrays.asList(altNames));
      }
      list.addAll(Arrays.asList(keyInfo.newKeys));
      altNames = list.toArray(new @OsUntrusted String @OsUntrusted [list.size()]);
    }
    return altNames;
  }

  /**
   * Checks for the presence of the property <code>name</code> in the
   * deprecation map. Returns the first of the list of new keys if present
   * in the deprecation map or the <code>name</code> itself. If the property
   * is not presently set but the property map contains an entry for the
   * deprecated key, the value of the deprecated key is set as the value for
   * the provided property name.
   *
   * @param name the property name
   * @return the first property in the list of properties mapping
   *         the <code>name</code> or the <code>name</code> itself.
   */
  private @OsUntrusted String @OsUntrusted [] handleDeprecation(@OsUntrusted Configuration this, @OsUntrusted String name) {
    @OsUntrusted
    ArrayList<@OsUntrusted String > names = new @OsUntrusted ArrayList<@OsUntrusted String>();
	if (isDeprecated(name)) {
      @OsUntrusted
      DeprecatedKeyInfo keyInfo = deprecatedKeyMap.get(name);
      warnOnceIfDeprecated(name);
      for (@OsUntrusted String newKey : keyInfo.newKeys) {
        if(newKey != null) {
          names.add(newKey);
        }
      }
    }
    if(names.size() == 0) {
    	names.add(name);
    }
    for(@OsUntrusted String n : names) {
	  @OsUntrusted
	  String deprecatedKey = reverseDeprecatedKeyMap.get(n);
	  if (deprecatedKey != null && !getOverlay().containsKey(n) &&
	      getOverlay().containsKey(deprecatedKey)) {
	    getProps().setProperty(n, getOverlay().getProperty(deprecatedKey));
	    getOverlay().setProperty(n, getOverlay().getProperty(deprecatedKey));
	  }
    }
    return names.toArray(new @OsUntrusted String @OsUntrusted [names.size()]);
  }
 
  private void handleDeprecation(@OsUntrusted Configuration this) {
    LOG.debug("Handling deprecation for all properties in config...");
    @OsUntrusted
    Set<@OsUntrusted Object> keys = new @OsUntrusted HashSet<@OsUntrusted Object>();
    keys.addAll(getProps().keySet());
    for (@OsUntrusted Object item: keys) {
      LOG.debug("Handling deprecation for " + (@OsUntrusted String)item);
      handleDeprecation((@OsUntrusted String)item);
    }
  }
 
  static{
    //print deprecation warning if hadoop-site.xml is found in classpath
    @OsUntrusted
    ClassLoader cL = Thread.currentThread().getContextClassLoader();
    if (cL == null) {
      cL = Configuration.class.getClassLoader();
    }
    if(cL.getResource("hadoop-site.xml")!=null) {
      LOG.warn("DEPRECATED: hadoop-site.xml found in the classpath. " +
          "Usage of hadoop-site.xml is deprecated. Instead use core-site.xml, "
          + "mapred-site.xml and hdfs-site.xml to override properties of " +
          "core-default.xml, mapred-default.xml and hdfs-default.xml " +
          "respectively");
    }
    addDefaultResource("core-default.xml");
    addDefaultResource("core-site.xml");
    //Add code for managing deprecated key mapping
    //for example
    //addDeprecation("oldKey1",new String[]{"newkey1","newkey2"});
    //adds deprecation for oldKey1 to two new keys(newkey1, newkey2).
    //so get or set of oldKey1 will correctly populate/access values of 
    //newkey1 and newkey2
    addDeprecatedKeys();
  }
  
  private @OsUntrusted Properties properties;
  private @OsUntrusted Properties overlay;
  private @OsUntrusted ClassLoader classLoader;
  {
    classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = Configuration.class.getClassLoader();
    }
  }
  
  /** A new configuration. */
  public @OsUntrusted Configuration() {
    this(true);
  }

  /** A new configuration where the behavior of reading from the default 
   * resources can be turned off.
   * 
   * If the parameter {@code loadDefaults} is false, the new instance
   * will not load resources from the default files. 
   * @param loadDefaults specifies whether to load from the default files
   */
  public @OsUntrusted Configuration(@OsUntrusted boolean loadDefaults) {
    this.loadDefaults = loadDefaults;
    updatingResource = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted String @OsUntrusted []>();
    synchronized(Configuration.class) {
      REGISTRY.put(this, null);
    }
  }
  
  /** 
   * A new configuration with the same settings cloned from another.
   * 
   * @param other the configuration from which to clone settings.
   */
  @SuppressWarnings({"unchecked", "ostrusted:cast.unsafe"})
  public @OsUntrusted Configuration(@OsUntrusted Configuration other) {
   //ostrusted, this is safe since the other.resources must be safe
   this.resources = (@OsUntrusted ArrayList<@OsTrusted Resource>) other.resources.clone();
   synchronized(other) {
     if (other.properties != null) {
       this.properties = (@OsUntrusted Properties)other.properties.clone();
     }

     if (other.overlay!=null) {
       this.overlay = (@OsUntrusted Properties)other.overlay.clone();
     }

     this.updatingResource = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted String @OsUntrusted []>(other.updatingResource);
   }
   
    this.finalParameters = new @OsUntrusted HashSet<@OsUntrusted String>(other.finalParameters);
    synchronized(Configuration.class) {
      REGISTRY.put(this, null);
    }
    this.classLoader = other.classLoader;
    this.loadDefaults = other.loadDefaults;
    setQuietMode(other.getQuietMode());
  }
  
  /**
   * Add a default resource. Resources are loaded in the order of the resources 
   * added.
   * @param name file name. File should be present in the classpath.
   */
  public static synchronized void addDefaultResource(@OsTrusted String name) {
    if(!defaultResources.contains(name)) {
      defaultResources.add(name);
      for(@OsUntrusted Configuration conf : REGISTRY.keySet()) {
        if(conf.loadDefaults) {
          conf.reloadConfiguration();
        }
      }
    }
  }

  /**
   * Add a configuration resource. 
   * 
   * The properties of this resource will override properties of previously 
   * added resources, unless they were marked <a href="#Final">final</a>. 
   * 
   * @param name resource to be added, the classpath is examined for a file 
   *             with that name.
   */
  public void addResource(@OsUntrusted Configuration this, @OsTrusted String name) {
    addResourceObject(new @OsTrusted Resource(name));
  }

  /**
   * Add a configuration resource. 
   * 
   * The properties of this resource will override properties of previously 
   * added resources, unless they were marked <a href="#Final">final</a>. 
   * 
   * @param url url of the resource to be added, the local filesystem is 
   *            examined directly to find the resource, without referring to 
   *            the classpath.
   */
  public void addResource(@OsUntrusted Configuration this, @OsTrusted URL url) {
    addResourceObject(new @OsTrusted Resource(url));
  }

  /**
   * Add a configuration resource. 
   * 
   * The properties of this resource will override properties of previously 
   * added resources, unless they were marked <a href="#Final">final</a>. 
   * 
   * @param file file-path of resource to be added, the local filesystem is
   *             examined directly to find the resource, without referring to 
   *             the classpath.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  //TODO: I ensure that the Hadoop Common Location that use this, force the input path objects to @Trusted
  //TODO: This was exercising a checker framework bug
  public void addResource(@OsUntrusted Configuration this, Path file) {
    addResourceObject((@OsTrusted Resource) new Resource(file));
  }

  /**
   * Add a configuration resource. 
   * 
   * The properties of this resource will override properties of previously 
   * added resources, unless they were marked <a href="#Final">final</a>. 
   * 
   * WARNING: The contents of the InputStream will be cached, by this method. 
   * So use this sparingly because it does increase the memory consumption.
   * 
   * @param in InputStream to deserialize the object from. In will be read from
   * when a get or set is called next.  After it is read the stream will be
   * closed. 
   */
  public void addResource(@OsUntrusted Configuration this, @OsTrusted InputStream in) {
    addResourceObject(new @OsTrusted Resource(in));
  }

  /**
   * Add a configuration resource. 
   * 
   * The properties of this resource will override properties of previously 
   * added resources, unless they were marked <a href="#Final">final</a>. 
   * 
   * @param in InputStream to deserialize the object from.
   * @param name the name of the resource because InputStream.toString is not
   * very descriptive some times.  
   */
  public void addResource(@OsUntrusted Configuration this, @OsTrusted InputStream in, @OsUntrusted String name) {
    addResourceObject(new @OsTrusted Resource(in, name));
  }
  
  
  /**
   * Reload configuration from previously added resources.
   *
   * This method will clear all the configuration read from the added 
   * resources, and final parameters. This will make the resources to 
   * be read again before accessing the values. Values that are added
   * via set methods will overlay values read from the resources.
   */
  public synchronized void reloadConfiguration(@OsUntrusted Configuration this) {
    properties = null;                            // trigger reload
    finalParameters.clear();                      // clear site-limits
  }
  
  private synchronized void addResourceObject(@OsUntrusted Configuration this, @OsTrusted Resource resource) {
    resources.add(resource);                      // add to resources
    reloadConfiguration();
  }
  
  private static @OsUntrusted Pattern varPat = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");
  private static @OsUntrusted int MAX_SUBST = 20;

  @SuppressWarnings("ostrusted:cast.unsafe")
  private @OsTrusted String substituteVars(@OsUntrusted Configuration this, @OsTrusted String expr) {
    if (expr == null) {
      return null;
    }
    @OsUntrusted Matcher match = varPat.matcher("");
    @OsTrusted String eval = expr;
    @OsUntrusted Set<@OsTrusted String> evalSet = new @OsUntrusted HashSet<@OsTrusted String>();
    for(@OsUntrusted int s=0; s<MAX_SUBST; s++) {
      if (evalSet.contains(eval)) {
        // Cyclic resolution pattern detected. Return current expression.
        return eval;
      }
      evalSet.add(eval);
      match.reset(eval);
      if (!match.find()) {
        return eval;
      }
      @OsUntrusted String var = match.group();
      var = var.substring(2, var.length()-1); // remove ${ .. }
      @OsTrusted String val = null;
      try {
        val = (@OsTrusted String) System.getProperty(var);
      } catch(@OsUntrusted SecurityException se) {
        LOG.warn("Unexpected SecurityException in Configuration", se);
      }
      if (val == null) {
        val = getRaw(var);
      }
      if (val == null) {
        return eval; // return literal ${var}: var is unbound
      }
      // substitute
      eval = ( (@OsTrusted String) eval.substring(0, match.start() ) ) + val + ( (@OsTrusted String) eval.substring(match.end()) );
    }
    throw new @OsUntrusted IllegalStateException("Variable substitution depth too large: " 
                                    + MAX_SUBST + " " + expr);
  }
  
  /**
   * Get the value of the <code>name</code> property, <code>null</code> if
   * no such property exists. If the key is deprecated, it returns the value of
   * the first key which replaces the deprecated key and is not null
   * 
   * Values are processed for <a href="#VariableExpansion">variable expansion</a> 
   * before being returned. 
   * 
   * @param name the property name.
   * @return the value of the <code>name</code> or its replacing property, 
   *         or null if no such property exists.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @OsTrusted String get(@OsUntrusted Configuration this, @OsUntrusted String name) {
    @OsUntrusted
    String @OsUntrusted [] names = handleDeprecation(name);
    @OsTrusted  String result = null;
    for(@OsUntrusted String n : names) {
      result = substituteVars( (@OsTrusted String) getProps().getProperty(n));
    }
    return result;
  }
  
  /**
   * Get the value of the <code>name</code> property as a trimmed <code>String</code>, 
   * <code>null</code> if no such property exists. 
   * If the key is deprecated, it returns the value of
   * the first key which replaces the deprecated key and is not null
   * 
   * Values are processed for <a href="#VariableExpansion">variable expansion</a> 
   * before being returned. 
   * 
   * @param name the property name.
   * @return the value of the <code>name</code> or its replacing property, 
   *         or null if no such property exists.
   */
  public @OsUntrusted String getTrimmed(@OsUntrusted Configuration this, @OsUntrusted String name) {
    @OsUntrusted
    String value = get(name);
    
    if (null == value) {
      return null;
    } else {
      return value.trim();
    }
  }
  
  /**
   * Get the value of the <code>name</code> property as a trimmed <code>String</code>, 
   * <code>defaultValue</code> if no such property exists. 
   * See @{Configuration#getTrimmed} for more details.
   * 
   * @param name          the property name.
   * @param defaultValue  the property default value.
   * @return              the value of the <code>name</code> or defaultValue
   *                      if it is not set.
   */
  public @OsUntrusted String getTrimmed(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted String defaultValue) {
    @OsUntrusted
    String ret = getTrimmed(name);
    return ret == null ? defaultValue : ret;
  }

  /**
   * Get the value of the <code>name</code> property, without doing
   * <a href="#VariableExpansion">variable expansion</a>.If the key is 
   * deprecated, it returns the value of the first key which replaces 
   * the deprecated key and is not null.
   * 
   * @param name the property name.
   * @return the value of the <code>name</code> property or 
   *         its replacing property and null if no such property exists.
   */
  @SuppressWarnings("trusted:cast.unsafe")
  public @OsTrusted String getRaw(@OsUntrusted Configuration this, @OsUntrusted String name) {
    @OsUntrusted
    String @OsUntrusted [] names = handleDeprecation(name);
    @OsTrusted String result = null;
    for(@OsUntrusted String n : names) {
      result = (@OsTrusted String) getProps().getProperty(n);
    }
    return result;
  }

  /** 
   * Set the <code>value</code> of the <code>name</code> property. If 
   * <code>name</code> is deprecated or there is a deprecated name associated to it,
   * it sets the value to both names.
   * 
   * @param name property name.
   * @param value property value.
   */
  public void set(@OsUntrusted Configuration this, @OsUntrusted String name, @OsTrusted String value) {
    set(name, value, null);
  }
  
  /** 
   * Set the <code>value</code> of the <code>name</code> property. If 
   * <code>name</code> is deprecated or there is a deprecated name associated to it,
   * it sets the value to both names.
   * 
   * @param name property name.
   * @param value property value.
   * @param source the place that this configuration value came from 
   * (For debugging).
   * @throws IllegalArgumentException when the value or name is null.
   */
  //TODO: ostrusted  we should make properties.put polymorphic with respect to the receiver type of properties
  public void set(@OsUntrusted Configuration this, @OsUntrusted String name, @OsTrusted String value, @OsUntrusted String source) {
    Preconditions.checkArgument( name != null,"Property name must not be null");
    Preconditions.checkArgument( value != null, "Property value must not be null");

    if (deprecatedKeyMap.isEmpty()) {
      getProps();
    }
    getOverlay().setProperty(name, value);
    getProps().setProperty(name, value);
    if(source == null) {
      updatingResource.put(name, new @OsUntrusted String @OsUntrusted [] {"programatically"});
    } else {
      updatingResource.put(name, new @OsUntrusted String @OsUntrusted [] {source});
    }
    @OsUntrusted
    String @OsUntrusted [] altNames = getAlternateNames(name);
    if (altNames != null && altNames.length > 0) {
      @OsUntrusted
      String altSource = "because " + name + " is deprecated";
      for(@OsUntrusted String altName : altNames) {
        if(!altName.equals(name)) {
          getOverlay().setProperty(altName, value);
          getProps().setProperty(altName, value);
          updatingResource.put(altName, new @OsUntrusted String @OsUntrusted [] {altSource});
        }
      }
    }
    warnOnceIfDeprecated(name);
  }

  private void warnOnceIfDeprecated(@OsUntrusted Configuration this, @OsUntrusted String name) {
    @OsUntrusted
    DeprecatedKeyInfo keyInfo = deprecatedKeyMap.get(name);
    if (keyInfo != null && !keyInfo.accessed) {
      LOG_DEPRECATION.info(keyInfo.getWarningMessage(name));
    }
  }

  /**
   * Unset a previously set property.
   */
  public synchronized void unset(@OsUntrusted Configuration this, @OsUntrusted String name) {
    @OsUntrusted
    String @OsUntrusted [] altNames = getAlternateNames(name);
    getOverlay().remove(name);
    getProps().remove(name);
    if (altNames !=null && altNames.length > 0) {
      for(@OsUntrusted String altName : altNames) {
    	getOverlay().remove(altName);
    	getProps().remove(altName);
      }
    }
  }

  /**
   * Sets a property if it is currently unset.
   * @param name the property name
   * @param value the new value
   */
  public synchronized void setIfUnset(@OsUntrusted Configuration this, @OsUntrusted String name, @OsTrusted String value) {
    if (get(name) == null) {
      set(name, value);
    }
  }
  
  private synchronized @OsUntrusted Properties getOverlay(@OsUntrusted Configuration this) {
    if (overlay==null){
      overlay=new @OsUntrusted Properties();
    }
    return overlay;
  }

  /** 
   * Get the value of the <code>name</code>. If the key is deprecated,
   * it returns the value of the first key which replaces the deprecated key
   * and is not null.
   * If no such property exists,
   * then <code>defaultValue</code> is returned.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @return property value, or <code>defaultValue</code> if the property 
   *         doesn't exist.                    
   */
  @SuppressWarnings("ostrusted:cast.unsafe")  //Should be PolyOsTrusted with defaultValue
  public @PolyOsTrusted String get(@OsUntrusted Configuration this, @OsUntrusted String name, @PolyOsTrusted String defaultValue) {
    @OsUntrusted
    String @OsUntrusted [] names = handleDeprecation(name);
    @OsTrusted
    String result = null;
    for(@OsUntrusted String n : names) {
      result = substituteVars( ( (@OsTrusted String) getProps().getProperty(n, defaultValue) ));
    }
    return result;
  }

  /** 
   * Get the value of the <code>name</code> property as an <code>int</code>.
   *   
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>int</code>,
   * then an error is thrown.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as an <code>int</code>, 
   *         or <code>defaultValue</code>. 
   */
  public @OsUntrusted int getInt(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted int defaultValue) {
    @OsUntrusted
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    @OsUntrusted
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Integer.parseInt(hexString, 16);
    }
    return Integer.parseInt(valueString);
  }
  
  /**
   * Get the value of the <code>name</code> property as a set of comma-delimited
   * <code>int</code> values.
   * 
   * If no such property exists, an empty array is returned.
   * 
   * @param name property name
   * @return property value interpreted as an array of comma-delimited
   *         <code>int</code> values
   */
  public @OsUntrusted int @OsUntrusted [] getInts(@OsUntrusted Configuration this, @OsUntrusted String name) {
    @OsUntrusted
    String @OsUntrusted [] strings = getTrimmedStrings(name);
    @OsUntrusted
    int @OsUntrusted [] ints = new @OsUntrusted int @OsUntrusted [strings.length];
    for (@OsUntrusted int i = 0; i < strings.length; i++) {
      ints[i] = Integer.parseInt(strings[i]);
    }
    return ints;
  }

  /** 
   * Set the value of the <code>name</code> property to an <code>int</code>.
   * 
   * @param name property name.
   * @param value <code>int</code> value of the property.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setInt(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted int value) {
    //Int strings are trusted
    set(name, (@OsTrusted String) Integer.toString(value));
  }


  /** 
   * Get the value of the <code>name</code> property as a <code>long</code>.  
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>long</code>,
   * then an error is thrown.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>long</code>, 
   *         or <code>defaultValue</code>. 
   */
  public @OsUntrusted long getLong(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted long defaultValue) {
    @OsUntrusted
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    @OsUntrusted
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Long.parseLong(hexString, 16);
    }
    return Long.parseLong(valueString);
  }

  /**
   * Get the value of the <code>name</code> property as a <code>long</code> or
   * human readable format. If no such property exists, the provided default
   * value is returned, or if the specified value is not a valid
   * <code>long</code> or human readable format, then an error is thrown. You
   * can use the following suffix (case insensitive): k(kilo), m(mega), g(giga),
   * t(tera), p(peta), e(exa)
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>long</code>,
   *         or <code>defaultValue</code>.
   */
  public @OsUntrusted long getLongBytes(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted long defaultValue) {
    @OsUntrusted
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    return StringUtils.TraditionalBinaryPrefix.string2long(valueString);
  }

  private @OsUntrusted String getHexDigits(@OsUntrusted Configuration this, @OsUntrusted String value) {
    @OsUntrusted
    boolean negative = false;
    @OsUntrusted
    String str = value;
    @OsUntrusted
    String hexString = null;
    if (value.startsWith("-")) {
      negative = true;
      str = value.substring(1);
    }
    if (str.startsWith("0x") || str.startsWith("0X")) {
      hexString = str.substring(2);
      if (negative) {
        hexString = "-" + hexString;
      }
      return hexString;
    }
    return null;
  }
  
  /** 
   * Set the value of the <code>name</code> property to a <code>long</code>.
   * 
   * @param name property name.
   * @param value <code>long</code> value of the property.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setLong(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted long value) {
    //Strings from numbers are considered safe
    set(name, (@OsTrusted String) Long.toString(value));
  }

  /** 
   * Get the value of the <code>name</code> property as a <code>float</code>.  
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>float</code>,
   * then an error is thrown.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>float</code>, 
   *         or <code>defaultValue</code>. 
   */

  public @OsUntrusted float getFloat(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted float defaultValue) {
    @OsUntrusted
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    return Float.parseFloat(valueString);
  }

  /**
   * Set the value of the <code>name</code> property to a <code>float</code>.
   * 
   * @param name property name.
   * @param value property value.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setFloat(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted float value) {
    //Strings from numbers are considered safe
    set(name, (@OsTrusted String) Float.toString(value));
  }

  /** 
   * Get the value of the <code>name</code> property as a <code>double</code>.  
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>double</code>,
   * then an error is thrown.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>double</code>, 
   *         or <code>defaultValue</code>. 
   */
  public @OsUntrusted double getDouble(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted double defaultValue) {
    @OsUntrusted
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    return Double.parseDouble(valueString);
  }

  /**
   * Set the value of the <code>name</code> property to a <code>double</code>.
   * 
   * @param name property name.
   * @param value property value.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setDouble(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted double value) {
    //Strings from numbers are considered safe
    set(name, (@OsTrusted String) Double.toString(value));
  }
 
  /** 
   * Get the value of the <code>name</code> property as a <code>boolean</code>.  
   * If no such property is specified, or if the specified value is not a valid
   * <code>boolean</code>, then <code>defaultValue</code> is returned.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @return property value as a <code>boolean</code>, 
   *         or <code>defaultValue</code>. 
   */
  public @OsUntrusted boolean getBoolean(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted boolean defaultValue) {
    @OsUntrusted
    String valueString = getTrimmed(name);
    if (null == valueString || valueString.isEmpty()) {
      return defaultValue;
    }

    valueString = valueString.toLowerCase();

    if ("true".equals(valueString))
      return true;
    else if ("false".equals(valueString))
      return false;
    else return defaultValue;
  }

  /** 
   * Set the value of the <code>name</code> property to a <code>boolean</code>.
   * 
   * @param name property name.
   * @param value <code>boolean</code> value of the property.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setBoolean(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted boolean value) {
    set(name, (@OsTrusted String) Boolean.toString(value));
  }

  /**
   * Set the given property, if it is currently unset.
   * @param name property name
   * @param value new value
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setBooleanIfUnset(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted boolean value) {
    setIfUnset(name, (@OsTrusted String) Boolean.toString(value));
  }

  /**
   * Set the value of the <code>name</code> property to the given type. This
   * is equivalent to <code>set(&lt;name&gt;, value.toString())</code>.
   * @param name property name
   * @param value new value
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public <@OsTrusted T extends @OsTrusted Enum<@OsTrusted T>> void setEnum(@OsUntrusted Configuration this, @OsUntrusted String name, @OsTrusted T value) {
    //value is trusted
    set(name, (@OsTrusted String) value.toString());
  }

  /**
   * Return value matching this enumerated type.
   * @param name Property name
   * @param defaultValue Value returned if no mapping exists
   * @throws IllegalArgumentException If mapping is illegal for the type
   * provided
   */
  @SuppressWarnings("ostrusted:argument.type.incompatible")
  public <@OsTrusted T extends @OsTrusted Enum<@OsTrusted T>> T getEnum(@OsUntrusted Configuration this, @OsUntrusted String name, @OsTrusted T defaultValue) {
    final @OsTrusted String val = get(name);
    return null == val ? defaultValue : Enum.valueOf( defaultValue.getDeclaringClass(), val );
  }

  enum ParsedTimeDuration {

    @OsTrusted  NS {
      TimeUnit unit() { return TimeUnit.NANOSECONDS; }
      String suffix() { return "ns"; }
    },

    @OsTrusted  US {
      TimeUnit unit() { return TimeUnit.MICROSECONDS; }
      String suffix() { return "us"; }
    },

    @OsTrusted  MS {
      TimeUnit unit() { return TimeUnit.MILLISECONDS; }
      String suffix() { return "ms"; }
    },

    @OsTrusted  S {
      TimeUnit unit() { return TimeUnit.SECONDS; }
      String suffix() { return "s"; }
    },

    @OsTrusted  M {
      TimeUnit unit() { return TimeUnit.MINUTES; }
      String suffix() { return "m"; }
    },

    @OsTrusted  H {
      TimeUnit unit() { return TimeUnit.HOURS; }
      String suffix() { return "h"; }
    },

    @OsTrusted  D {
      TimeUnit unit() { return TimeUnit.DAYS; }
      String suffix() { return "d"; }
    };
    abstract @OsUntrusted TimeUnit unit(Configuration.@OsUntrusted ParsedTimeDuration this);
    abstract @OsUntrusted String suffix(Configuration.@OsUntrusted ParsedTimeDuration this);
    static @OsUntrusted ParsedTimeDuration unitFor(@OsUntrusted String s) {
      for (@OsUntrusted ParsedTimeDuration ptd : values()) {
        // iteration order is in decl order, so SECONDS matched last
        if (s.endsWith(ptd.suffix())) {
          return ptd;
        }
      }
      return null;
    }
    static @OsUntrusted ParsedTimeDuration unitFor(@OsUntrusted TimeUnit unit) {
      for (@OsUntrusted ParsedTimeDuration ptd : values()) {
        if (ptd.unit() == unit) {
          return ptd;
        }
      }
      return null;
    }
  }

  /**
   * Set the value of <code>name</code> to the given time duration. This
   * is equivalent to <code>set(&lt;name&gt;, value + &lt;time suffix&gt;)</code>.
   * @param name Property name
   * @param value Time duration
   * @param unit Unit of time
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setTimeDuration(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted long value, @OsUntrusted TimeUnit unit) {
    //All time unit suffixes are trusted as they are literals
    set(name, (@OsTrusted String) ( value + ParsedTimeDuration.unitFor(unit).suffix() ));
  }

  /**
   * Return time duration in the given time unit. Valid units are encoded in
   * properties as suffixes: nanoseconds (ns), microseconds (us), milliseconds
   * (ms), seconds (s), minutes (m), hours (h), and days (d).
   * @param name Property name
   * @param defaultValue Value returned if no mapping exists.
   * @param unit Unit to convert the stored property, if it exists.
   * @throws NumberFormatException If the property stripped of its unit is not
   *         a number
   */
  public @OsUntrusted long getTimeDuration(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted long defaultValue, @OsUntrusted TimeUnit unit) {
    @OsUntrusted
    String vStr = get(name);
    if (null == vStr) {
      return defaultValue;
    }
    vStr = vStr.trim();
    @OsUntrusted
    ParsedTimeDuration vUnit = ParsedTimeDuration.unitFor(vStr);
    if (null == vUnit) {
      LOG.warn("No unit for " + name + "(" + vStr + ") assuming " + unit);
      vUnit = ParsedTimeDuration.unitFor(unit);
    } else {
      vStr = vStr.substring(0, vStr.lastIndexOf(vUnit.suffix()));
    }
    return unit.convert(Long.parseLong(vStr), vUnit.unit());
  }

  /**
   * Get the value of the <code>name</code> property as a <code>Pattern</code>.
   * If no such property is specified, or if the specified value is not a valid
   * <code>Pattern</code>, then <code>DefaultValue</code> is returned.
   *
   * @param name property name
   * @param defaultValue default value
   * @return property value as a compiled Pattern, or defaultValue
   */
  public @OsUntrusted Pattern getPattern(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted Pattern defaultValue) {
    @OsUntrusted
    String valString = get(name);
    if (null == valString || valString.isEmpty()) {
      return defaultValue;
    }
    try {
      return Pattern.compile(valString);
    } catch (@OsUntrusted PatternSyntaxException pse) {
      LOG.warn("Regular expression '" + valString + "' for property '" +
               name + "' not valid. Using default", pse);
      return defaultValue;
    }
  }

  /**
   * Set the given property to <code>Pattern</code>.
   * If the pattern is passed as null, sets the empty pattern which results in
   * further calls to getPattern(...) returning the default value.
   *
   * @param name property name
   * @param pattern new value
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setPattern(@OsUntrusted Configuration this, @OsUntrusted String name, @OsTrusted Pattern pattern) {
    if (null == pattern) {
      set(name, null);
    } else {
      set(name, (@OsTrusted String) pattern.pattern());
    }
  }

  /**
   * Gets information about why a property was set.  Typically this is the 
   * path to the resource objects (file, URL, etc.) the property came from, but
   * it can also indicate that it was set programatically, or because of the
   * command line.
   *
   * @param name - The property name to get the source of.
   * @return null - If the property or its source wasn't found. Otherwise, 
   * returns a list of the sources of the resource.  The older sources are
   * the first ones in the list.  So for example if a configuration is set from
   * the command line, and then written out to a file that is read back in the
   * first entry would indicate that it was set from the command line, while
   * the second one would indicate the file that the new configuration was read
   * in from.
   */
  @InterfaceStability.Unstable
  public synchronized @OsUntrusted String @OsUntrusted [] getPropertySources(@OsUntrusted Configuration this, @OsUntrusted String name) {
    if (properties == null) {
      // If properties is null, it means a resource was newly added
      // but the props were cleared so as to load it upon future
      // requests. So lets force a load by asking a properties list.
      getProps();
    }
    // Return a null right away if our properties still
    // haven't loaded or the resource mapping isn't defined
    if (properties == null || updatingResource == null) {
      return null;
    } else {
      @OsUntrusted
      String @OsUntrusted [] source = updatingResource.get(name);
      if(source == null) {
        return null;
      } else {
        return Arrays.copyOf(source, source.length);
      }
    }
  }

  /**
   * A class that represents a set of positive integer ranges. It parses 
   * strings of the form: "2-3,5,7-" where ranges are separated by comma and 
   * the lower/upper bounds are separated by dash. Either the lower or upper 
   * bound may be omitted meaning all values up to or over. So the string 
   * above means 2, 3, 5, and 7, 8, 9, ...
   */
  public static class IntegerRanges implements @OsUntrusted Iterable<@OsUntrusted Integer>{
    private static class Range {
      @OsUntrusted
      int start;
      @OsUntrusted
      int end;
    }
    
    private static class RangeNumberIterator implements @OsUntrusted Iterator<@OsUntrusted Integer> {
      @OsUntrusted
      Iterator<@OsUntrusted Range> internal;
      @OsUntrusted
      int at;
      @OsUntrusted
      int end;

      public @OsUntrusted RangeNumberIterator(@OsUntrusted List<@OsUntrusted Range> ranges) {
        if (ranges != null) {
          internal = ranges.iterator();
        }
        at = -1;
        end = -2;
      }
      
      @Override
      public @OsUntrusted boolean hasNext(Configuration.IntegerRanges.@OsUntrusted RangeNumberIterator this) {
        if (at <= end) {
          return true;
        } else if (internal != null){
          return internal.hasNext();
        }
        return false;
      }

      @Override
      public @OsUntrusted Integer next(Configuration.IntegerRanges.@OsUntrusted RangeNumberIterator this) {
        if (at <= end) {
          at++;
          return at - 1;
        } else if (internal != null){
          @OsUntrusted
          Range found = internal.next();
          if (found != null) {
            at = found.start;
            end = found.end;
            at++;
            return at - 1;
          }
        }
        return null;
      }

      @Override
      public void remove(Configuration.IntegerRanges.@OsUntrusted RangeNumberIterator this) {
        throw new @OsUntrusted UnsupportedOperationException();
      }
    };

    @OsUntrusted
    List<@OsUntrusted Range> ranges = new @OsUntrusted ArrayList<@OsUntrusted Range>();
    
    public @OsUntrusted IntegerRanges() {
    }
    
    public @OsUntrusted IntegerRanges(@OsUntrusted String newValue) {
      @OsUntrusted
      StringTokenizer itr = new @OsUntrusted StringTokenizer(newValue, ",");
      while (itr.hasMoreTokens()) {
        @OsUntrusted
        String rng = itr.nextToken().trim();
        @OsUntrusted
        String @OsUntrusted [] parts = rng.split("-", 3);
        if (parts.length < 1 || parts.length > 2) {
          throw new @OsUntrusted IllegalArgumentException("integer range badly formed: " + 
                                             rng);
        }
        @OsUntrusted
        Range r = new @OsUntrusted Range();
        r.start = convertToInt(parts[0], 0);
        if (parts.length == 2) {
          r.end = convertToInt(parts[1], Integer.MAX_VALUE);
        } else {
          r.end = r.start;
        }
        if (r.start > r.end) {
          throw new @OsUntrusted IllegalArgumentException("IntegerRange from " + r.start + 
                                             " to " + r.end + " is invalid");
        }
        ranges.add(r);
      }
    }

    /**
     * Convert a string to an int treating empty strings as the default value.
     * @param value the string value
     * @param defaultValue the value for if the string is empty
     * @return the desired integer
     */
    private static @OsUntrusted int convertToInt(@OsUntrusted String value, @OsUntrusted int defaultValue) {
      @OsUntrusted
      String trim = value.trim();
      if (trim.length() == 0) {
        return defaultValue;
      }
      return Integer.parseInt(trim);
    }

    /**
     * Is the given value in the set of ranges
     * @param value the value to check
     * @return is the value in the ranges?
     */
    public @OsUntrusted boolean isIncluded(Configuration.@OsUntrusted IntegerRanges this, @OsUntrusted int value) {
      for(@OsUntrusted Range r: ranges) {
        if (r.start <= value && value <= r.end) {
          return true;
        }
      }
      return false;
    }
    
    /**
     * @return true if there are no values in this range, else false.
     */
    public @OsUntrusted boolean isEmpty(Configuration.@OsUntrusted IntegerRanges this) {
      return ranges == null || ranges.isEmpty();
    }
    
    @Override
    public @OsUntrusted String toString(Configuration.@OsUntrusted IntegerRanges this) {
      @OsUntrusted
      StringBuilder result = new @OsUntrusted StringBuilder();
      @OsUntrusted
      boolean first = true;
      for(@OsUntrusted Range r: ranges) {
        if (first) {
          first = false;
        } else {
          result.append(',');
        }
        result.append(r.start);
        result.append('-');
        result.append(r.end);
      }
      return result.toString();
    }

    @Override
    public @OsUntrusted Iterator<@OsUntrusted Integer> iterator(Configuration.@OsUntrusted IntegerRanges this) {
      return new @OsUntrusted RangeNumberIterator(ranges);
    }
    
  }

  /**
   * Parse the given attribute as a set of integer ranges
   * @param name the attribute name
   * @param defaultValue the default value if it is not set
   * @return a new set of ranges from the configured value
   */
  public @OsTrusted IntegerRanges getRange(@OsUntrusted Configuration this, @OsUntrusted String name, @OsTrusted String defaultValue) {
    return new @OsTrusted IntegerRanges(get(name, defaultValue));
  }

  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * a collection of <code>String</code>s.  
   * If no such property is specified then empty collection is returned.
   * <p>
   * This is an optimized version of {@link #getStrings(String)}
   * 
   * @param name property name.
   * @return property value as a collection of <code>String</code>s. 
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @OsTrusted Collection<@OsTrusted String> getStringCollection(@OsUntrusted Configuration this, @OsUntrusted String name) {
    @OsTrusted String valueString = get(name);
    return (@OsTrusted Collection<@OsTrusted String> ) StringUtils.getStringCollection(valueString);
  }

  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * an array of <code>String</code>s.  
   * If no such property is specified then <code>null</code> is returned.
   * 
   * @param name property name.
   * @return property value as an array of <code>String</code>s, 
   *         or <code>null</code>. 
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @OsTrusted String @OsTrusted [] getStrings(@OsUntrusted Configuration this, @OsUntrusted String name) {
    @OsTrusted String valueString = get(name);
    return (@OsTrusted String @OsTrusted []) StringUtils.getStrings(valueString);
  }

  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * an array of <code>String</code>s.  
   * If no such property is specified then default value is returned.
   * 
   * @param name property name.
   * @param defaultValue The default value
   * @return property value as an array of <code>String</code>s, 
   *         or default value. 
   */
  public @OsUntrusted String @OsUntrusted [] getStrings(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted String @OsUntrusted ... defaultValue) {
    @OsUntrusted
    String valueString = get(name);
    if (valueString == null) {
      return defaultValue;
    } else {
      return StringUtils.getStrings(valueString);
    }
  }
  
  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * a collection of <code>String</code>s, trimmed of the leading and trailing whitespace.  
   * If no such property is specified then empty <code>Collection</code> is returned.
   *
   * @param name property name.
   * @return property value as a collection of <code>String</code>s, or empty <code>Collection</code> 
   */
  public @OsUntrusted Collection<@OsUntrusted String> getTrimmedStringCollection(@OsUntrusted Configuration this, @OsUntrusted String name) {
    @OsUntrusted
    String valueString = get(name);
    if (null == valueString) {
      @OsUntrusted
      Collection<@OsUntrusted String> empty = new @OsUntrusted ArrayList<@OsUntrusted String>();
      return empty;
    }
    return StringUtils.getTrimmedStringCollection(valueString);
  }
  
  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
   * If no such property is specified then an empty array is returned.
   * 
   * @param name property name.
   * @return property value as an array of trimmed <code>String</code>s, 
   *         or empty array. 
   */
  public @OsUntrusted String @OsUntrusted [] getTrimmedStrings(@OsUntrusted Configuration this, @OsUntrusted String name) {
    @OsUntrusted
    String valueString = get(name);
    return StringUtils.getTrimmedStrings(valueString);
  }

  /** 
   * Get the comma delimited values of the <code>name</code> property as 
   * an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
   * If no such property is specified then default value is returned.
   * 
   * @param name property name.
   * @param defaultValue The default value
   * @return property value as an array of trimmed <code>String</code>s, 
   *         or default value. 
   */
  public @OsUntrusted String @OsUntrusted [] getTrimmedStrings(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted String @OsUntrusted ... defaultValue) {
    @OsUntrusted
    String valueString = get(name);
    if (null == valueString) {
      return defaultValue;
    } else {
      return StringUtils.getTrimmedStrings(valueString);
    }
  }

  /** 
   * Set the array of string values for the <code>name</code> property as 
   * as comma delimited values.  
   * 
   * @param name property name.
   * @param values The values
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setStrings(@OsUntrusted Configuration this, @OsUntrusted String name, @OsTrusted String @OsUntrusted ... values) {
    set(name, (@OsTrusted String) StringUtils.arrayToString(values));
  }

  /**
   * Get the socket address for <code>name</code> property as a
   * <code>InetSocketAddress</code>.
   * @param name property name.
   * @param defaultAddress the default value
   * @param defaultPort the default port
   * @return InetSocketAddress
   */
  public @OsUntrusted InetSocketAddress getSocketAddr( @OsUntrusted Configuration this, @OsUntrusted
      String name, @OsTrusted String defaultAddress, @OsUntrusted int defaultPort) {
    final @OsTrusted String address = get(name, defaultAddress);
    return NetUtils.createSocketAddr(address, defaultPort, name);
  }

  /**
   * Set the socket address for the <code>name</code> property as
   * a <code>host:port</code>.
   */
  public void setSocketAddr(@OsUntrusted Configuration this, @OsUntrusted String name, @OsTrusted InetSocketAddress addr) {
    set(name, NetUtils.getHostPortString(addr));
  }
  
  /**
   * Set the socket address a client can use to connect for the
   * <code>name</code> property as a <code>host:port</code>.  The wildcard
   * address is replaced with the local host's address.
   * @param name property name.
   * @param addr InetSocketAddress of a listener to store in the given property
   * @return InetSocketAddress for clients to connect
   */
  public @OsTrusted InetSocketAddress updateConnectAddr(@OsUntrusted Configuration this, @OsUntrusted String name,
                                             @OsTrusted InetSocketAddress addr) {
    final @OsTrusted InetSocketAddress connectAddr = NetUtils.getConnectAddress(addr);
    setSocketAddr(name, connectAddr);
    return connectAddr;
  }
  
  /**
   * Load a class by name.
   * 
   * @param name the class name.
   * @return the class object.
   * @throws ClassNotFoundException if the class is not found.
   */
  public @OsUntrusted Class<@OsUntrusted ?> getClassByName(@OsUntrusted Configuration this, @OsUntrusted String name) throws ClassNotFoundException {
    @OsUntrusted
    Class<@OsUntrusted ?> ret = getClassByNameOrNull(name);
    if (ret == null) {
      throw new @OsUntrusted ClassNotFoundException("Class " + name + " not found");
    }
    return ret;
  }
  
  /**
   * Load a class by name, returning null rather than throwing an exception
   * if it couldn't be loaded. This is to avoid the overhead of creating
   * an exception.
   * 
   * @param name the class name
   * @return the class object, or null if it could not be found.
   */
  public @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> getClassByNameOrNull(@OsUntrusted Configuration this, @OsUntrusted String name) {
    @OsUntrusted
    Map<@OsUntrusted String, @OsUntrusted WeakReference<@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>>> map;
    
    synchronized (CACHE_CLASSES) {
      map = CACHE_CLASSES.get(classLoader);
      if (map == null) {
        map = Collections.synchronizedMap(
          new @OsUntrusted WeakHashMap<@OsUntrusted String, @OsUntrusted WeakReference<@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>>>());
        CACHE_CLASSES.put(classLoader, map);
      }
    }

    @OsUntrusted
    Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> clazz = null;
    @OsUntrusted
    WeakReference<@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>> ref = map.get(name); 
    if (ref != null) {
       clazz = ref.get();
    }
     
    if (clazz == null) {
      try {
        clazz = Class.forName(name, true, classLoader);
      } catch (@OsUntrusted ClassNotFoundException e) {
        // Leave a marker that the class isn't found
        map.put(name, new @OsUntrusted WeakReference<@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>>(NEGATIVE_CACHE_SENTINEL));
        return null;
      }
      // two putters can race here, but they'll put the same class
      map.put(name, new @OsUntrusted WeakReference<@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object>>(clazz));
      return clazz;
    } else if (clazz == NEGATIVE_CACHE_SENTINEL) {
      return null; // not found
    } else {
      // cache hit
      return clazz;
    }
  }

  /** 
   * Get the value of the <code>name</code> property
   * as an array of <code>Class</code>.
   * The value of the property specifies a list of comma separated class names.  
   * If no such property is specified, then <code>defaultValue</code> is 
   * returned.
   * 
   * @param name the property name.
   * @param defaultValue default value.
   * @return property value as a <code>Class[]</code>, 
   *         or <code>defaultValue</code>. 
   */
  public @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> @OsUntrusted [] getClasses(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> @OsUntrusted ... defaultValue) {
    @OsUntrusted
    String valueString = getRaw(name);
    if (null == valueString) {
      return defaultValue;
    }
    @OsUntrusted
    String @OsUntrusted [] classnames = getTrimmedStrings(name);
    try {
      @OsUntrusted
      Class<?> @OsUntrusted [] classes = new Class<?> @OsUntrusted [classnames.length];
      for(@OsUntrusted int i = 0; i < classnames.length; i++) {
        classes[i] = getClassByName(classnames[i]);
      }
      return classes;
    } catch (@OsUntrusted ClassNotFoundException e) {
      throw new @OsUntrusted RuntimeException(e);
    }
  }

  /** 
   * Get the value of the <code>name</code> property as a <code>Class</code>.  
   * If no such property is specified, then <code>defaultValue</code> is 
   * returned.
   * 
   * @param name the class name.
   * @param defaultValue default value.
   * @return property value as a <code>Class</code>, 
   *         or <code>defaultValue</code>. 
   */
  public @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> getClass(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> defaultValue) {
    @OsUntrusted
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    try {
      return getClassByName(valueString);
    } catch (@OsUntrusted ClassNotFoundException e) {
      throw new @OsUntrusted RuntimeException(e);
    }
  }

  /** 
   * Get the value of the <code>name</code> property as a <code>Class</code>
   * implementing the interface specified by <code>xface</code>.
   *   
   * If no such property is specified, then <code>defaultValue</code> is 
   * returned.
   * 
   * An exception is thrown if the returned class does not implement the named
   * interface. 
   * 
   * @param name the class name.
   * @param defaultValue default value.
   * @param xface the interface implemented by the named class.
   * @return property value as a <code>Class</code>, 
   *         or <code>defaultValue</code>.
   */
  public <@OsUntrusted U extends java.lang.@OsUntrusted Object> @OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted U> getClass(@OsUntrusted Configuration this, @OsUntrusted String name, 
                                         @OsUntrusted
                                         Class<@OsUntrusted ? extends @OsUntrusted U> defaultValue, 
                                         @OsUntrusted
                                         Class<@OsUntrusted U> xface) {
    try {
      @OsUntrusted
      Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> theClass = getClass(name, defaultValue);
      if (theClass != null && !xface.isAssignableFrom(theClass))
        throw new @OsUntrusted RuntimeException(theClass+" not "+xface.getName());
      else if (theClass != null)
        return theClass.asSubclass(xface);
      else
        return null;
    } catch (@OsUntrusted Exception e) {
      throw new @OsUntrusted RuntimeException(e);
    }
  }

  /**
   * Get the value of the <code>name</code> property as a <code>List</code>
   * of objects implementing the interface specified by <code>xface</code>.
   * 
   * An exception is thrown if any of the classes does not exist, or if it does
   * not implement the named interface.
   * 
   * @param name the property name.
   * @param xface the interface implemented by the classes named by
   *        <code>name</code>.
   * @return a <code>List</code> of objects implementing <code>xface</code>.
   */
  @SuppressWarnings("unchecked")
  public <@OsUntrusted U extends java.lang.@OsUntrusted Object> @OsUntrusted List<@OsUntrusted U> getInstances(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted Class<@OsUntrusted U> xface) {
    @OsUntrusted
    List<@OsUntrusted U> ret = new @OsUntrusted ArrayList<@OsUntrusted U>();
    @OsUntrusted
    Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> @OsUntrusted [] classes = getClasses(name);
    for (@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> cl: classes) {
      if (!xface.isAssignableFrom(cl)) {
        throw new @OsUntrusted RuntimeException(cl + " does not implement " + xface);
      }
      ret.add((@OsUntrusted U)ReflectionUtils.newInstance(cl, this));
    }
    return ret;
  }

  /** 
   * Set the value of the <code>name</code> property to the name of a 
   * <code>theClass</code> implementing the given interface <code>xface</code>.
   * 
   * An exception is thrown if <code>theClass</code> does not implement the 
   * interface <code>xface</code>. 
   * 
   * @param name property name.
   * @param theClass property value.
   * @param xface the interface implemented by the named class.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public void setClass(@OsUntrusted Configuration this, @OsUntrusted String name, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> theClass, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> xface) {
    if (!xface.isAssignableFrom(theClass))
      throw new @OsUntrusted RuntimeException(theClass+" not "+xface.getName());
    //TODO: Revisit Class.getName but based on the Java Language Specification this seems safe
    set(name, (@OsTrusted String) theClass.getName());
  }

  /** 
   * Get a local file under a directory named by <i>dirsProp</i> with
   * the given <i>path</i>.  If <i>dirsProp</i> contains multiple directories,
   * then one is chosen based on <i>path</i>'s hash code.  If the selected
   * directory does not exist, an attempt is made to create it.
   * 
   * @param dirsProp directory in which to locate the file.
   * @param path file-path.
   * @return local file under the directory with the given path.
   */
  public @OsUntrusted Path getLocalPath(@OsUntrusted Configuration this, @OsUntrusted String dirsProp, @OsUntrusted String path)
    throws IOException {
    @OsUntrusted
    String @OsUntrusted [] dirs = getTrimmedStrings(dirsProp);
    @OsUntrusted
    int hashCode = path.hashCode();
    @OsUntrusted
    FileSystem fs = FileSystem.getLocal(this);
    for (@OsUntrusted int i = 0; i < dirs.length; i++) {  // try each local dir
      @OsUntrusted
      int index = (hashCode+i & Integer.MAX_VALUE) % dirs.length;
      @OsUntrusted
      Path file = new @OsUntrusted Path(dirs[index], path);
      @OsUntrusted
      Path dir = file.getParent();
      if (fs.mkdirs(dir) || fs.exists(dir)) {
        return file;
      }
    }
    LOG.warn("Could not make " + path + 
             " in local directories from " + dirsProp);
    for(@OsUntrusted int i=0; i < dirs.length; i++) {
      @OsUntrusted
      int index = (hashCode+i & Integer.MAX_VALUE) % dirs.length;
      LOG.warn(dirsProp + "[" + index + "]=" + dirs[index]);
    }
    throw new @OsUntrusted IOException("No valid local directories in property: "+dirsProp);
  }

  /** 
   * Get a local file name under a directory named in <i>dirsProp</i> with
   * the given <i>path</i>.  If <i>dirsProp</i> contains multiple directories,
   * then one is chosen based on <i>path</i>'s hash code.  If the selected
   * directory does not exist, an attempt is made to create it.
   * 
   * @param dirsProp directory in which to locate the file.
   * @param path file-path.
   * @return local file under the directory with the given path.
   */
  public @OsUntrusted File getFile(@OsUntrusted Configuration this, @OsUntrusted String dirsProp, @OsUntrusted String path)
    throws IOException {
    @OsUntrusted
    String @OsUntrusted [] dirs = getTrimmedStrings(dirsProp);
    @OsUntrusted
    int hashCode = path.hashCode();
    for (@OsUntrusted int i = 0; i < dirs.length; i++) {  // try each local dir
      @OsUntrusted
      int index = (hashCode+i & Integer.MAX_VALUE) % dirs.length;
      @OsUntrusted
      File file = new @OsUntrusted File(dirs[index], path);
      @OsUntrusted
      File dir = file.getParentFile();
      if (dir.exists() || dir.mkdirs()) {
        return file;
      }
    }
    throw new @OsUntrusted IOException("No valid local directories in property: "+dirsProp);
  }

  /** 
   * Get the {@link URL} for the named resource.
   * 
   * @param name resource name.
   * @return the url for the named resource.
   */
  public @OsUntrusted URL getResource(@OsUntrusted Configuration this, @OsUntrusted String name) {
    return classLoader.getResource(name);
  }
  
  /** 
   * Get an input stream attached to the configuration resource with the
   * given <code>name</code>.
   * 
   * @param name configuration resource name.
   * @return an input stream attached to the resource.
   */
  public @OsUntrusted InputStream getConfResourceAsInputStream(@OsUntrusted Configuration this, @OsUntrusted String name) {
    try {
      @OsUntrusted
      URL url= getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return url.openStream();
    } catch (@OsUntrusted Exception e) {
      return null;
    }
  }

  /** 
   * Get a {@link Reader} attached to the configuration resource with the
   * given <code>name</code>.
   * 
   * @param name configuration resource name.
   * @return a reader attached to the resource.
   */
  public @OsUntrusted Reader getConfResourceAsReader(@OsUntrusted Configuration this, @OsUntrusted String name) {
    try {
      @OsUntrusted
      URL url= getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return new @OsUntrusted InputStreamReader(url.openStream());
    } catch (@OsUntrusted Exception e) {
      return null;
    }
  }

  /**
   * Get the set of parameters marked final.
   *
   * @return final parameter set.
   */
  public @OsUntrusted Set<@OsUntrusted String> getFinalParameters(@OsUntrusted Configuration this) {
    return new @OsUntrusted HashSet<@OsUntrusted String>(finalParameters);
  }

  protected synchronized @OsUntrusted Properties getProps(@OsUntrusted Configuration this) {
    if (properties == null) {
      properties = new @OsUntrusted Properties();
      @OsUntrusted
      HashMap<@OsUntrusted String, @OsUntrusted String @OsUntrusted []> backup = 
        new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted String @OsUntrusted []>(updatingResource);
      loadResources(properties, resources, quietmode);
      if (overlay!= null) {
        properties.putAll(overlay);
        for (Map.@OsUntrusted Entry<@OsUntrusted Object, @OsUntrusted Object> item: overlay.entrySet()) {
          @OsUntrusted
          String key = (@OsUntrusted String)item.getKey();
          updatingResource.put(key, backup.get(key));
        }
      }
    }
    return properties;
  }

  /**
   * Return the number of keys in the configuration.
   *
   * @return number of keys in the configuration.
   */
  public @OsUntrusted int size(@OsUntrusted Configuration this) {
    return getProps().size();
  }

  /**
   * Clears all keys from the configuration.
   */
  public void clear(@OsUntrusted Configuration this) {
    getProps().clear();
    getOverlay().clear();
  }

  /**
   * Get an {@link Iterator} to go through the list of <code>String</code> 
   * key-value pairs in the configuration.
   * 
   * @return an iterator over the entries.
   */
  //Since Properties is populated only using OsTrusted resources, we assume it's entry set is OsTrusted
  //Todo: The Iterator.iterator will return by default a map to Untrusted values.  We need to polymorphically annotate this
  //Todo: in some fashion
  @SuppressWarnings("ostrusted:cast.unsafe" )
  @Override
  public @OsUntrusted Iterator<Map.@OsUntrusted Entry<@OsUntrusted String, @OsTrusted String>> iterator(@OsUntrusted Configuration this) {
    // Get a copy of just the string to string pairs. After the old object
    // methods that allow non-strings to be put into configurations are removed,
    // we could replace properties with a Map<String,String> and get rid of this
    // code.
    @OsUntrusted
    Map<@OsUntrusted String, @OsTrusted String> result = new @OsUntrusted HashMap<@OsUntrusted String, @OsTrusted String>();
    for(Map.@OsUntrusted Entry<@OsUntrusted Object, @OsTrusted Object> item: (Set <Entry<@OsUntrusted Object, @OsTrusted Object>>) getProps().entrySet() ) {
      if (item.getKey() instanceof String &&
          item.getValue() instanceof String) {
        result.put((@OsUntrusted String) item.getKey(), (@OsTrusted String) item.getValue());
      }
    }
    return (@OsUntrusted Iterator<Map.@OsUntrusted Entry<@OsUntrusted String, @OsTrusted String>>) result.entrySet().iterator();
  }

  private @OsUntrusted Document parse(@OsUntrusted Configuration this, @OsUntrusted DocumentBuilder builder, @OsUntrusted URL url)
      throws IOException, SAXException {
    if (!quietmode) {
      LOG.debug("parsing URL " + url);
    }
    if (url == null) {
      return null;
    }
    return parse(builder, url.openStream(), url.toString());
  }

  private @OsUntrusted Document parse(@OsUntrusted Configuration this, @OsUntrusted DocumentBuilder builder, @OsUntrusted InputStream is,
      @OsUntrusted
      String systemId) throws IOException, SAXException {
    if (!quietmode) {
      LOG.debug("parsing input stream " + is);
    }
    if (is == null) {
      return null;
    }
    try {
      return (systemId == null) ? builder.parse(is) : builder.parse(is,
          systemId);
    } finally {
      is.close();
    }
  }

  private void loadResources(@OsUntrusted Configuration this, @OsUntrusted Properties properties,
                             @OsUntrusted
                             ArrayList<@OsTrusted Resource> resources,
                             @OsUntrusted
                             boolean quiet) {
    if(loadDefaults) {
      for (@OsTrusted String resource : defaultResources) {
        loadResource(properties, new @OsTrusted Resource(resource), quiet);
      }
    
      //support the hadoop-site.xml as a deprecated case
      if(getResource("hadoop-site.xml")!=null) {
        loadResource(properties, new @OsTrusted Resource("hadoop-site.xml"), quiet);
      }
    }
    
    for (@OsUntrusted int i = 0; i < resources.size(); i++) {
      @OsUntrusted
      Resource ret = loadResource(properties, resources.get(i), quiet);
      if (ret != null) {
        resources.set(i, ret);
      }
    }
  }

  @SuppressWarnings("ostrusted:cast.unsafe")
  //TODO: Since the value field in this method is read from trusted resources I have called it trusted and
  //TODO: cast the appropriate assignments to it, but I would like to have a second run through for validity's sake
  private @OsTrusted Resource loadResource(@OsUntrusted Configuration this, @OsUntrusted Properties properties, @OsTrusted Resource wrapper, @OsUntrusted boolean quiet) {
    @OsUntrusted
    String name = UNKNOWN_RESOURCE;
    try {
      @OsTrusted  Object resource = (@OsTrusted Object) wrapper.getResource();
      name = wrapper.getName();
      
      @OsUntrusted
      DocumentBuilderFactory docBuilderFactory 
        = DocumentBuilderFactory.newInstance();
      //ignore all comments inside the xml file
      docBuilderFactory.setIgnoringComments(true);

      //allow includes in the xml file
      docBuilderFactory.setNamespaceAware(true);
      try {
          docBuilderFactory.setXIncludeAware(true);
      } catch (@OsUntrusted UnsupportedOperationException e) {
        LOG.error("Failed to set setXIncludeAware(true) for parser "
                + docBuilderFactory
                + ":" + e,
                e);
      }
      @OsUntrusted
      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
      @OsUntrusted
      Document doc = null;
      @OsUntrusted
      Element root = null;
      @OsUntrusted
      boolean returnCachedProperties = false;
      
      if (resource instanceof @OsUntrusted URL) {                  // an URL resource
        doc = parse(builder, (@OsUntrusted URL)resource);
      } else if (resource instanceof @OsUntrusted String) {        // a CLASSPATH resource
        @OsUntrusted
        URL url = getResource((@OsUntrusted String)resource);
        doc = parse(builder, url);
      } else if (resource instanceof @OsUntrusted Path) {          // a file resource
        // Can't use FileSystem API or we get an infinite loop
        // since FileSystem uses Configuration API.  Use java.io.File instead.
        @OsUntrusted
        File file = new @OsUntrusted File(((@OsUntrusted Path)resource).toUri().getPath())
          .getAbsoluteFile();
        if (file.exists()) {
          if (!quiet) {
            LOG.debug("parsing File " + file);
          }
          doc = parse(builder, new @OsUntrusted BufferedInputStream(
              new @OsUntrusted FileInputStream(file)), ((@OsUntrusted Path)resource).toString());
        }
      } else if (resource instanceof @OsUntrusted InputStream) {
        doc = parse(builder, (@OsUntrusted InputStream) resource, null);
        returnCachedProperties = true;
      } else if (resource instanceof @OsUntrusted Properties) {
        overlay(properties, (@OsUntrusted Properties)resource);
      } else if (resource instanceof @OsUntrusted Element) {
        root = (@OsUntrusted Element)resource;
      }

      if (doc == null && root == null) {
        if (quiet)
          return null;
        throw new @OsUntrusted RuntimeException(resource + " not found");
      }

      if (root == null) {
        root = doc.getDocumentElement();
      }
      @OsUntrusted
      Properties toAddTo = properties;
      if(returnCachedProperties) {
        toAddTo = new @OsUntrusted Properties();
      }
      if (!"configuration".equals(root.getTagName()))
        LOG.fatal("bad conf file: top-level element not <configuration>");
      @OsUntrusted
      NodeList props = root.getChildNodes();
      for (@OsUntrusted int i = 0; i < props.getLength(); i++) {
        @OsUntrusted
        Node propNode = props.item(i);
        if (!(propNode instanceof @OsUntrusted Element))
          continue;
        @OsUntrusted
        Element prop = (@OsUntrusted Element)propNode;
        if ("configuration".equals(prop.getTagName())) {
          loadResource(toAddTo, (@OsTrusted Resource) new Resource(prop, name), quiet);
          continue;
        }
        if (!"property".equals(prop.getTagName()))
          LOG.warn("bad conf file: element not <property>");
        @OsUntrusted
        NodeList fields = prop.getChildNodes();
        @OsUntrusted
        String attr = null;
        @OsTrusted String value = null;
        @OsUntrusted
        boolean finalParameter = false;
        @OsUntrusted
        LinkedList<@OsUntrusted String> source = new @OsUntrusted LinkedList<@OsUntrusted String>();
        for (@OsUntrusted int j = 0; j < fields.getLength(); j++) {
          @OsUntrusted
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof @OsUntrusted Element))
            continue;
          @OsUntrusted
          Element field = (@OsUntrusted Element)fieldNode;
          if ("name".equals(field.getTagName()) && field.hasChildNodes())
            attr = StringInterner.weakIntern(
                ((@OsUntrusted Text)field.getFirstChild()).getData().trim());
          if ("value".equals(field.getTagName()) && field.hasChildNodes())
            value = (@OsTrusted String) StringInterner.weakIntern(((@OsUntrusted Text)field.getFirstChild()).getData());
          if ("final".equals(field.getTagName()) && field.hasChildNodes())
            finalParameter = "true".equals(((@OsUntrusted Text)field.getFirstChild()).getData());
          if ("source".equals(field.getTagName()) && field.hasChildNodes())
            source.add(StringInterner.weakIntern(
                ((@OsUntrusted Text)field.getFirstChild()).getData()));
        }
        source.add(name);

        //ostrusted The source variables are read from a trusted resource so consider their strings trusted
        // Ignore this parameter if it has already been marked as 'final'
        if (attr != null) {
          if (deprecatedKeyMap.containsKey(attr)) {
            @OsUntrusted
            DeprecatedKeyInfo keyInfo = deprecatedKeyMap.get(attr);
            keyInfo.accessed = false;
            for (@OsUntrusted String key:keyInfo.newKeys) {
              // update new keys with deprecated key's value 
              loadProperty(toAddTo, name, key, value, finalParameter, 
                      (@OsTrusted String @OsUntrusted [])  source.toArray(new @OsTrusted String @OsUntrusted [source.size()]));
            }
          }
          else {
            loadProperty(toAddTo, name, attr, value, finalParameter, 
                    (@OsTrusted String @OsUntrusted [])  source.toArray(new @OsTrusted String @OsUntrusted [source.size()]));
          }
        }
      }
      
      if (returnCachedProperties) {
        overlay(properties, toAddTo);
        return (@OsTrusted Resource) new Resource(toAddTo, name);
      }
      return null;
    } catch (@OsUntrusted IOException e) {
      LOG.fatal("error parsing conf " + name, e);
      throw new @OsUntrusted RuntimeException(e);
    } catch (@OsUntrusted DOMException e) {
      LOG.fatal("error parsing conf " + name, e);
      throw new @OsUntrusted RuntimeException(e);
    } catch (@OsUntrusted SAXException e) {
      LOG.fatal("error parsing conf " + name, e);
      throw new @OsUntrusted RuntimeException(e);
    } catch (@OsUntrusted ParserConfigurationException e) {
      LOG.fatal("error parsing conf " + name , e);
      throw new @OsUntrusted RuntimeException(e);
    }
  }

  private void overlay(@OsUntrusted Configuration this, @OsUntrusted Properties to, @OsUntrusted Properties from) {
    for (@OsUntrusted Entry<@OsUntrusted Object, @OsUntrusted Object> entry: from.entrySet()) {
      to.put(entry.getKey(), entry.getValue());
    }
  }
  
  private void loadProperty(@OsUntrusted Configuration this, @OsUntrusted Properties properties, @OsUntrusted String name, @OsUntrusted String attr,
      @OsTrusted String value, @OsUntrusted boolean finalParameter, @OsTrusted String @OsUntrusted [] source) {
    if (value != null) {
      if (!finalParameters.contains(attr)) {
        properties.setProperty(attr, value);
        updatingResource.put(attr, source);
      } else if (!value.equals(properties.getProperty(attr))) {
        LOG.warn(name+":an attempt to override final parameter: "+attr
            +";  Ignoring.");
      }
    }
    if (finalParameter) {
      finalParameters.add(attr);
    }
  }

  /** 
   * Write out the non-default properties in this configuration to the given
   * {@link OutputStream} using UTF-8 encoding.
   * 
   * @param out the output stream to write to.
   */
  public void writeXml(@OsUntrusted Configuration this, @OsUntrusted OutputStream out) throws IOException {
    writeXml(new @OsUntrusted OutputStreamWriter(out, "UTF-8"));
  }

  /** 
   * Write out the non-default properties in this configuration to the given
   * {@link Writer}.
   * 
   * @param out the writer to write to.
   */
  public void writeXml(@OsUntrusted Configuration this, @OsUntrusted Writer out) throws IOException {
    @OsUntrusted
    Document doc = asXmlDocument();

    try {
      @OsUntrusted
      DOMSource source = new @OsUntrusted DOMSource(doc);
      @OsUntrusted
      StreamResult result = new @OsUntrusted StreamResult(out);
      @OsUntrusted
      TransformerFactory transFactory = TransformerFactory.newInstance();
      @OsUntrusted
      Transformer transformer = transFactory.newTransformer();

      // Important to not hold Configuration log while writing result, since
      // 'out' may be an HDFS stream which needs to lock this configuration
      // from another thread.
      transformer.transform(source, result);
    } catch (@OsUntrusted TransformerException te) {
      throw new @OsUntrusted IOException(te);
    }
  }

  /**
   * Return the XML DOM corresponding to this Configuration.
   */
  private synchronized @OsUntrusted Document asXmlDocument(@OsUntrusted Configuration this) throws IOException {
    @OsUntrusted
    Document doc;
    try {
      doc =
        DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    } catch (@OsUntrusted ParserConfigurationException pe) {
      throw new @OsUntrusted IOException(pe);
    }
    @OsUntrusted
    Element conf = doc.createElement("configuration");
    doc.appendChild(conf);
    conf.appendChild(doc.createTextNode("\n"));
    handleDeprecation(); //ensure properties is set and deprecation is handled
    for (@OsUntrusted Enumeration<@OsUntrusted Object> e = properties.keys(); e.hasMoreElements();) {
      @OsUntrusted
      String name = (@OsUntrusted String)e.nextElement();
      @OsUntrusted
      Object object = properties.get(name);
      @OsUntrusted
      String value = null;
      if (object instanceof @OsUntrusted String) {
        value = (@OsUntrusted String) object;
      }else {
        continue;
      }
      @OsUntrusted
      Element propNode = doc.createElement("property");
      conf.appendChild(propNode);

      @OsUntrusted
      Element nameNode = doc.createElement("name");
      nameNode.appendChild(doc.createTextNode(name));
      propNode.appendChild(nameNode);

      @OsUntrusted
      Element valueNode = doc.createElement("value");
      valueNode.appendChild(doc.createTextNode(value));
      propNode.appendChild(valueNode);

      if (updatingResource != null) {
        @OsUntrusted
        String @OsUntrusted [] sources = updatingResource.get(name);
        if(sources != null) {
          for(@OsUntrusted String s : sources) {
            @OsUntrusted
            Element sourceNode = doc.createElement("source");
            sourceNode.appendChild(doc.createTextNode(s));
            propNode.appendChild(sourceNode);
          }
        }
      }
      
      conf.appendChild(doc.createTextNode("\n"));
    }
    return doc;
  }

  /**
   *  Writes out all the parameters and their properties (final and resource) to
   *  the given {@link Writer}
   *  The format of the output would be 
   *  { "properties" : [ {key1,value1,key1.isFinal,key1.resource}, {key2,value2,
   *  key2.isFinal,key2.resource}... ] } 
   *  It does not output the parameters of the configuration object which is 
   *  loaded from an input stream.
   * @param out the Writer to write to
   * @throws IOException
   */
  public static void dumpConfiguration(@OsUntrusted Configuration config,
      @OsUntrusted
      Writer out) throws IOException {
    @OsUntrusted
    JsonFactory dumpFactory = new @OsUntrusted JsonFactory();
    @OsUntrusted
    JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
    dumpGenerator.writeStartObject();
    dumpGenerator.writeFieldName("properties");
    dumpGenerator.writeStartArray();
    dumpGenerator.flush();
    synchronized (config) {
      for (Map.@OsUntrusted Entry<@OsUntrusted Object, @OsUntrusted Object> item: config.getProps().entrySet()) {
        dumpGenerator.writeStartObject();
        dumpGenerator.writeStringField("key", (@OsUntrusted String) item.getKey());
        dumpGenerator.writeStringField("value", 
                                       config.get((@OsUntrusted String) item.getKey()));
        dumpGenerator.writeBooleanField("isFinal",
                                        config.finalParameters.contains(item.getKey()));
        @OsUntrusted
        String @OsUntrusted [] resources = config.updatingResource.get(item.getKey());
        @OsUntrusted
        String resource = UNKNOWN_RESOURCE;
        if(resources != null && resources.length > 0) {
          resource = resources[0];
        }
        dumpGenerator.writeStringField("resource", resource);
        dumpGenerator.writeEndObject();
      }
    }
    dumpGenerator.writeEndArray();
    dumpGenerator.writeEndObject();
    dumpGenerator.flush();
  }
  
  /**
   * Get the {@link ClassLoader} for this job.
   * 
   * @return the correct class loader.
   */
  public @OsUntrusted ClassLoader getClassLoader(@OsUntrusted Configuration this) {
    return classLoader;
  }
  
  /**
   * Set the class loader that will be used to load the various objects.
   * 
   * @param classLoader the new class loader.
   */
  public void setClassLoader(@OsUntrusted Configuration this, @OsUntrusted ClassLoader classLoader) {
    this.classLoader = classLoader;
  }
  
  @Override
  public @OsUntrusted String toString(@OsUntrusted Configuration this) {
    @OsUntrusted
    StringBuilder sb = new @OsUntrusted StringBuilder();
    sb.append("Configuration: ");
    if(loadDefaults) {
      toString(defaultResources, sb);
      if(resources.size()>0) {
        sb.append(", ");
      }
    }
    toString(resources, sb);
    return sb.toString();
  }
  
  private <T extends Object> void toString(@OsUntrusted Configuration this, @OsUntrusted List<T> resources, @OsUntrusted StringBuilder sb) {
    @OsUntrusted
    ListIterator<T> i = resources.listIterator();
    while (i.hasNext()) {
      if (i.nextIndex() != 0) {
        sb.append(", ");
      }
      sb.append(i.next());
    }
  }

  /** 
   * Set the quietness-mode. 
   * 
   * In the quiet-mode, error and informational messages might not be logged.
   * 
   * @param quietmode <code>true</code> to set quiet-mode on, <code>false</code>
   *              to turn it off.
   */
  public synchronized void setQuietMode(@OsUntrusted Configuration this, @OsUntrusted boolean quietmode) {
    this.quietmode = quietmode;
  }

  synchronized @OsUntrusted boolean getQuietMode(@OsUntrusted Configuration this) {
    return this.quietmode;
  }
  
  /** For debugging.  List non-default properties to the terminal and exit. */
  public static void main(@OsUntrusted String @OsUntrusted [] args) throws Exception {
    new @OsUntrusted Configuration().writeXml(System.out);
  }

  //TODO: ostrusted, configuraiont becomes a source of vulnerability if cast to Writable and
  //TODO: and used with an Untrusted DataInput.  Simple grepping does not show these uses but
  //TODO: this remains unproven.
  @Override
  @SuppressWarnings( {"ostrusted:cast.unsafe", "override.param.invalid"} )
  public void readFields(@OsUntrusted Configuration this, @OsTrusted DataInput in) throws IOException {
    clear();
    @OsUntrusted
    int size = WritableUtils.readVInt(in);
    for(@OsUntrusted int i=0; i < size; ++i) {
      @OsUntrusted
      String key = org.apache.hadoop.io.Text.readString(in);
      @OsTrusted String value = (@OsTrusted String) org.apache.hadoop.io.Text.readString(in);
      set(key, value); 
      @OsUntrusted String sources @OsUntrusted [] = WritableUtils.readCompressedStringArray(in);
      updatingResource.put(key, sources);
    }
  }

  //@Override
  @Override
  public void write(@OsUntrusted Configuration this, @OsUntrusted DataOutput out) throws IOException {
    @OsUntrusted
    Properties props = getProps();
    WritableUtils.writeVInt(out, props.size());
    for(Map.@OsUntrusted Entry<@OsUntrusted Object, @OsUntrusted Object> item: props.entrySet()) {
      org.apache.hadoop.io.Text.writeString(out, (@OsUntrusted String) item.getKey());
      org.apache.hadoop.io.Text.writeString(out, (@OsUntrusted String) item.getValue());
      WritableUtils.writeCompressedStringArray(out, 
          updatingResource.get(item.getKey()));
    }
  }
  
  /**
   * get keys matching the the regex 
   * @param regex
   * @return Map<String,String> with matching keys
   */
  public @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> getValByRegex(@OsUntrusted Configuration this, @OsUntrusted String regex) {
    @OsUntrusted
    Pattern p = Pattern.compile(regex);

    @OsUntrusted
    Map<@OsUntrusted String, @OsUntrusted String> result = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted String>();
    @OsUntrusted
    Matcher m;

    for(Map.@OsUntrusted Entry<@OsUntrusted Object, @OsUntrusted Object> item: getProps().entrySet()) {
      if (item.getKey() instanceof @OsUntrusted String && 
          item.getValue() instanceof @OsUntrusted String) {
        m = p.matcher((@OsUntrusted String)item.getKey());
        if(m.find()) { // match
          result.put((@OsUntrusted String) item.getKey(), (@OsUntrusted String) item.getValue());
        }
      }
    }
    return result;
  }

  //Load deprecated keys in common
  private static void addDeprecatedKeys() {
    Configuration.addDeprecation("topology.script.file.name", 
               new @OsUntrusted String @OsUntrusted []{CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY});
    Configuration.addDeprecation("topology.script.number.args", 
               new @OsUntrusted String @OsUntrusted []{CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY});
    Configuration.addDeprecation("hadoop.configured.node.mapping", 
               new @OsUntrusted String @OsUntrusted []{CommonConfigurationKeys.NET_TOPOLOGY_CONFIGURED_NODE_MAPPING_KEY});
    Configuration.addDeprecation("topology.node.switch.mapping.impl", 
               new @OsUntrusted String @OsUntrusted []{CommonConfigurationKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY});
    Configuration.addDeprecation("dfs.df.interval", 
               new @OsUntrusted String @OsUntrusted []{CommonConfigurationKeys.FS_DF_INTERVAL_KEY});
    Configuration.addDeprecation("hadoop.native.lib", 
               new @OsUntrusted String @OsUntrusted []{CommonConfigurationKeys.IO_NATIVE_LIB_AVAILABLE_KEY});
    Configuration.addDeprecation("fs.default.name", 
               new @OsUntrusted String @OsUntrusted []{CommonConfigurationKeys.FS_DEFAULT_NAME_KEY});
    Configuration.addDeprecation("dfs.umaskmode",
        new @OsUntrusted String @OsUntrusted []{CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY});
  }
  
  /**
   * A unique class which is used as a sentinel value in the caching
   * for getClassByName. {@link Configuration#getClassByNameOrNull(String)}
   */
  private static abstract class NegativeCacheSentinel {}

  public static void dumpDeprecatedKeys() {
    for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted DeprecatedKeyInfo> entry : deprecatedKeyMap.entrySet()) {
      @OsUntrusted
      StringBuilder newKeys = new @OsUntrusted StringBuilder();
      for (@OsUntrusted String newKey : entry.getValue().newKeys) {
        newKeys.append(newKey).append("\t");
      }
      System.out.println(entry.getKey() + "\t" + newKeys.toString());
    }
  }
}
