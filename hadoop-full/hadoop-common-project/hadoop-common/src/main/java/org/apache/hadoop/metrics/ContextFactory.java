/*
 * ContextFactory.java
 *
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

package org.apache.hadoop.metrics;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics.spi.NullContext;

/**
 * Factory class for creating MetricsContext objects.  To obtain an instance
 * of this class, use the static <code>getFactory()</code> method.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class ContextFactory {
    
  private static final @OsUntrusted String PROPERTIES_FILE = 
    "/hadoop-metrics.properties";
  private static final @OsUntrusted String CONTEXT_CLASS_SUFFIX =
    ".class";
  private static final @OsUntrusted String DEFAULT_CONTEXT_CLASSNAME =
    "org.apache.hadoop.metrics.spi.NullContext";
    
  private static @OsUntrusted ContextFactory theFactory = null;
    
  private @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Object> attributeMap = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted Object>();
  private @OsUntrusted Map<@OsUntrusted String, @OsUntrusted MetricsContext> contextMap = 
    new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted MetricsContext>();
    
  // Used only when contexts, or the ContextFactory itself, cannot be
  // created.
  private static @OsUntrusted Map<@OsUntrusted String, @OsUntrusted MetricsContext> nullContextMap = 
    new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted MetricsContext>();
    
  /** Creates a new instance of ContextFactory */
  protected @OsUntrusted ContextFactory() {
  }
    
  /**
   * Returns the value of the named attribute, or null if there is no 
   * attribute of that name.
   *
   * @param attributeName the attribute name
   * @return the attribute value
   */
  public @OsUntrusted Object getAttribute(@OsUntrusted ContextFactory this, @OsUntrusted String attributeName) {
    return attributeMap.get(attributeName);
  }
    
  /**
   * Returns the names of all the factory's attributes.
   * 
   * @return the attribute names
   */
  public @OsUntrusted String @OsUntrusted [] getAttributeNames(@OsUntrusted ContextFactory this) {
    @OsUntrusted
    String @OsUntrusted [] result = new @OsUntrusted String @OsUntrusted [attributeMap.size()];
    @OsUntrusted
    int i = 0;
    // for (String attributeName : attributeMap.keySet()) {
    @OsUntrusted
    Iterator it = attributeMap.keySet().iterator();
    while (it.hasNext()) {
      result[i++] = (@OsUntrusted String) it.next();
    }
    return result;
  }
    
  /**
   * Sets the named factory attribute to the specified value, creating it
   * if it did not already exist.  If the value is null, this is the same as
   * calling removeAttribute.
   *
   * @param attributeName the attribute name
   * @param value the new attribute value
   */
  public void setAttribute(@OsUntrusted ContextFactory this, @OsUntrusted String attributeName, @OsUntrusted Object value) {
    attributeMap.put(attributeName, value);
  }

  /**
   * Removes the named attribute if it exists.
   *
   * @param attributeName the attribute name
   */
  public void removeAttribute(@OsUntrusted ContextFactory this, @OsUntrusted String attributeName) {
    attributeMap.remove(attributeName);
  }
    
  /**
   * Returns the named MetricsContext instance, constructing it if necessary 
   * using the factory's current configuration attributes. <p/>
   * 
   * When constructing the instance, if the factory property 
   * <i>contextName</i>.class</code> exists, 
   * its value is taken to be the name of the class to instantiate.  Otherwise,
   * the default is to create an instance of 
   * <code>org.apache.hadoop.metrics.spi.NullContext</code>, which is a 
   * dummy "no-op" context which will cause all metric data to be discarded.
   * 
   * @param contextName the name of the context
   * @return the named MetricsContext
   */
  public synchronized @OsUntrusted MetricsContext getContext(@OsUntrusted ContextFactory this, @OsUntrusted String refName, @OsUntrusted String contextName)
      throws IOException, ClassNotFoundException,
             InstantiationException, IllegalAccessException {
    @OsUntrusted
    MetricsContext metricsContext = contextMap.get(refName);
    if (metricsContext == null) {
      @OsUntrusted
      String classNameAttribute = refName + CONTEXT_CLASS_SUFFIX;
      @OsUntrusted
      String className = (@OsUntrusted String) getAttribute(classNameAttribute);
      if (className == null) {
        className = DEFAULT_CONTEXT_CLASSNAME;
      }
      @OsUntrusted
      Class contextClass = Class.forName(className);
      metricsContext = (@OsUntrusted MetricsContext) contextClass.newInstance();
      metricsContext.init(contextName, this);
      contextMap.put(contextName, metricsContext);
    }
    return metricsContext;
  }

  public synchronized @OsUntrusted MetricsContext getContext(@OsUntrusted ContextFactory this, @OsUntrusted String contextName)
    throws IOException, ClassNotFoundException, InstantiationException,
           IllegalAccessException {
    return getContext(contextName, contextName);
  }
  
  /** 
   * Returns all MetricsContexts built by this factory.
   */
  public synchronized @OsUntrusted Collection<@OsUntrusted MetricsContext> getAllContexts(@OsUntrusted ContextFactory this) {
    // Make a copy to avoid race conditions with creating new contexts.
    return new @OsUntrusted ArrayList<@OsUntrusted MetricsContext>(contextMap.values());
  }
    
  /**
   * Returns a "null" context - one which does nothing.
   */
  public static synchronized @OsUntrusted MetricsContext getNullContext(@OsUntrusted String contextName) {
    @OsUntrusted
    MetricsContext nullContext = nullContextMap.get(contextName);
    if (nullContext == null) {
      nullContext = new @OsUntrusted NullContext();
      nullContextMap.put(contextName, nullContext);
    }
    return nullContext;
  }
    
  /**
   * Returns the singleton ContextFactory instance, constructing it if 
   * necessary. <p/>
   * 
   * When the instance is constructed, this method checks if the file 
   * <code>hadoop-metrics.properties</code> exists on the class path.  If it 
   * exists, it must be in the format defined by java.util.Properties, and all 
   * the properties in the file are set as attributes on the newly created
   * ContextFactory instance.
   *
   * @return the singleton ContextFactory instance
   */
  public static synchronized @OsUntrusted ContextFactory getFactory() throws IOException {
    if (theFactory == null) {
      theFactory = new @OsUntrusted ContextFactory();
      theFactory.setAttributes();
    }
    return theFactory;
  }
    
  private void setAttributes(@OsUntrusted ContextFactory this) throws IOException {
    @OsUntrusted
    InputStream is = getClass().getResourceAsStream(PROPERTIES_FILE);
    if (is != null) {
      try {
        @OsUntrusted
        Properties properties = new @OsUntrusted Properties();
        properties.load(is);
        //for (Object propertyNameObj : properties.keySet()) {
        @OsUntrusted
        Iterator it = properties.keySet().iterator();
        while (it.hasNext()) {
          @OsUntrusted
          String propertyName = (@OsUntrusted String) it.next();
          @OsUntrusted
          String propertyValue = properties.getProperty(propertyName);
          setAttribute(propertyName, propertyValue);
        }
      } finally {
        is.close();
      }
    }
  }
    
}
