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
package org.apache.hadoop.metrics2.util;

import ostrusted.quals.OsUntrusted;
import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

/**
 * This util class provides a method to register an MBean using
 * our standard naming convention as described in the doc
 *  for {link {@link #register(String, String, Object)}
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MBeans {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(MBeans.class);

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
  static public @OsUntrusted ObjectName register(@OsUntrusted String serviceName, @OsUntrusted String nameName,
                                    @OsUntrusted
                                    Object theMbean) {
    final @OsUntrusted MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    @OsUntrusted
    ObjectName name = getMBeanName(serviceName, nameName);
    if (name != null) {
      try {
        mbs.registerMBean(theMbean, name);
        LOG.debug("Registered " + name);
        return name;
      } catch (@OsUntrusted InstanceAlreadyExistsException iaee) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Failed to register MBean \"" + name + "\"", iaee);
        } else {
          LOG.warn("Failed to register MBean \"" + name
              + "\": Instance already exists.");
        }
      } catch (@OsUntrusted Exception e) {
        LOG.warn("Failed to register MBean \"" + name + "\"", e);
      }
    }
    return null;
  }

  static public void unregister(@OsUntrusted ObjectName mbeanName) {
    LOG.debug("Unregistering "+ mbeanName);
    final @OsUntrusted MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    if (mbeanName == null) {
      LOG.debug("Stacktrace: ", new @OsUntrusted Throwable());
      return;
    }
    try {
      mbs.unregisterMBean(mbeanName);
    } catch (@OsUntrusted Exception e) {
      LOG.warn("Error unregistering "+ mbeanName, e);
    }
  }

  static private @OsUntrusted ObjectName getMBeanName(@OsUntrusted String serviceName, @OsUntrusted String nameName) {
    @OsUntrusted
    ObjectName name = null;
    @OsUntrusted
    String nameStr = "Hadoop:service="+ serviceName +",name="+ nameName;
    try {
      name = DefaultMetricsSystem.newMBeanName(nameStr);
    } catch (@OsUntrusted Exception e) {
      LOG.warn("Error creating MBean object name: "+ nameStr, e);
    }
    return name;
  }
}
