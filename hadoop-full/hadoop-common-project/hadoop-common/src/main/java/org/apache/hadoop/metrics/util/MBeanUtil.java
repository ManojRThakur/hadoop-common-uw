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
package org.apache.hadoop.metrics.util;

import ostrusted.quals.OsUntrusted;
import java.lang.management.ManagementFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.InstanceAlreadyExistsException;

import org.apache.hadoop.classification.InterfaceAudience;


/**
 * This util class provides a method to register an MBean using
 * our standard naming convention as described in the doc
 *  for {link {@link #registerMBean(String, String, Object)}
 *
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
public class MBeanUtil {
	
  /**
   * Register the MBean using our standard MBeanName format
   * "hadoop:service=<serviceName>,name=<nameName>"
   * Where the <serviceName> and <nameName> are the supplied parameters
   *    
   * @param serviceName
   * @param nameName
   * @param theMbean - the MBean to register
   * @return the named used to register the MBean
   */	
  static public @OsUntrusted ObjectName registerMBean(final @OsUntrusted String serviceName, 
		  							final @OsUntrusted String nameName,
		  							final @OsUntrusted Object theMbean) {
    final @OsUntrusted MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    @OsUntrusted
    ObjectName name = getMBeanName(serviceName, nameName);
    try {
      mbs.registerMBean(theMbean, name);
      return name;
    } catch (@OsUntrusted InstanceAlreadyExistsException ie) {
      // Ignore if instance already exists 
    } catch (@OsUntrusted Exception e) {
      e.printStackTrace();
    }
    return null;
  }
  
  static public void unregisterMBean(@OsUntrusted ObjectName mbeanName) {
    final @OsUntrusted MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    if (mbeanName == null) 
        return;
    try {
      mbs.unregisterMBean(mbeanName);
    } catch (@OsUntrusted InstanceNotFoundException e ) {
      // ignore
    } catch (@OsUntrusted Exception e) {
      e.printStackTrace();
    } 
  }
  
  static private @OsUntrusted ObjectName getMBeanName(final @OsUntrusted String serviceName,
		  								 final @OsUntrusted String nameName) {
    @OsUntrusted
    ObjectName name = null;
    try {
      name = new @OsUntrusted ObjectName("hadoop:" +
                  "service=" + serviceName + ",name=" + nameName);
    } catch (@OsUntrusted MalformedObjectNameException e) {
      e.printStackTrace();
    }
    return name;
  }
}
