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

package org.apache.hadoop.net;

import ostrusted.quals.OsUntrusted;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Vector;

import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

/**
 * 
 * A class that provides direct and reverse lookup functionalities, allowing
 * the querying of specific network interfaces or nameservers.
 * 
 * 
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class DNS {

  private static final @OsUntrusted Log LOG = LogFactory.getLog(DNS.class);

  /**
   * The cached hostname -initially null.
   */

  private static final @OsUntrusted String cachedHostname = resolveLocalHostname();
  private static final @OsUntrusted String cachedHostAddress = resolveLocalHostIPAddress();
  private static final @OsUntrusted String LOCALHOST = "localhost";

  /**
   * Returns the hostname associated with the specified IP address by the
   * provided nameserver.
   *
   * Loopback addresses 
   * @param hostIp The address to reverse lookup
   * @param ns The host name of a reachable DNS server
   * @return The host name associated with the provided IP
   * @throws NamingException If a NamingException is encountered
   */
  public static @OsUntrusted String reverseDns(@OsUntrusted InetAddress hostIp, @OsUntrusted String ns)
    throws NamingException {
    //
    // Builds the reverse IP lookup form
    // This is formed by reversing the IP numbers and appending in-addr.arpa
    //
    @OsUntrusted
    String @OsUntrusted [] parts = hostIp.getHostAddress().split("\\.");
    @OsUntrusted
    String reverseIP = parts[3] + "." + parts[2] + "." + parts[1] + "."
      + parts[0] + ".in-addr.arpa";

    @OsUntrusted
    DirContext ictx = new @OsUntrusted InitialDirContext();
    @OsUntrusted
    Attributes attribute;
    try {
      attribute = ictx.getAttributes("dns://"               // Use "dns:///" if the default
                         + ((ns == null) ? "" : ns) +
                         // nameserver is to be used
                         "/" + reverseIP, new @OsUntrusted String @OsUntrusted [] { "PTR" });
    } finally {
      ictx.close();
    }

    @OsUntrusted
    String hostname = attribute.get("PTR").get().toString();
    @OsUntrusted
    int hostnameLength = hostname.length();
    if (hostname.charAt(hostnameLength - 1) == '.') {
      hostname = hostname.substring(0, hostnameLength - 1);
    }
    return hostname;
  }

  /**
   * @return NetworkInterface for the given subinterface name (eg eth0:0)
   *    or null if no interface with the given name can be found  
   */
  private static @OsUntrusted NetworkInterface getSubinterface(@OsUntrusted String strInterface)
      throws SocketException {
    @OsUntrusted
    Enumeration<@OsUntrusted NetworkInterface> nifs = 
      NetworkInterface.getNetworkInterfaces();
      
    while (nifs.hasMoreElements()) {
      @OsUntrusted
      Enumeration<@OsUntrusted NetworkInterface> subNifs = 
        nifs.nextElement().getSubInterfaces();

      while (subNifs.hasMoreElements()) {
        @OsUntrusted
        NetworkInterface nif = subNifs.nextElement();
        if (nif.getName().equals(strInterface)) {
          return nif;
        }
      }
    }
    return null;
  }

  /**
   * @param nif network interface to get addresses for
   * @return set containing addresses for each subinterface of nif,
   *    see below for the rationale for using an ordered set
   */
  private static @OsUntrusted LinkedHashSet<@OsUntrusted InetAddress> getSubinterfaceInetAddrs(
      @OsUntrusted
      NetworkInterface nif) {
    @OsUntrusted
    LinkedHashSet<@OsUntrusted InetAddress> addrs = new @OsUntrusted LinkedHashSet<@OsUntrusted InetAddress>();
    @OsUntrusted
    Enumeration<@OsUntrusted NetworkInterface> subNifs = nif.getSubInterfaces();
    while (subNifs.hasMoreElements()) {
      @OsUntrusted
      NetworkInterface subNif = subNifs.nextElement();
      addrs.addAll(Collections.list(subNif.getInetAddresses()));
    }
    return addrs;
  }

  /**
   * Like {@link DNS#getIPs(String, boolean), but returns all
   * IPs associated with the given interface and its subinterfaces.
   */
  public static @OsUntrusted String @OsUntrusted [] getIPs(@OsUntrusted String strInterface)
      throws UnknownHostException {
    return getIPs(strInterface, true);
  }

  /**
   * Returns all the IPs associated with the provided interface, if any, in
   * textual form.
   * 
   * @param strInterface
   *            The name of the network interface or sub-interface to query
   *            (eg eth0 or eth0:0) or the string "default"
   * @param returnSubinterfaces
   *            Whether to return IPs associated with subinterfaces of
   *            the given interface
   * @return A string vector of all the IPs associated with the provided
   *         interface. The local host IP is returned if the interface
   *         name "default" is specified or there is an I/O error looking
   *         for the given interface.
   * @throws UnknownHostException
   *             If the given interface is invalid
   * 
   */
  public static @OsUntrusted String @OsUntrusted [] getIPs(@OsUntrusted String strInterface,
      @OsUntrusted
      boolean returnSubinterfaces) throws UnknownHostException {
    if ("default".equals(strInterface)) {
      return new @OsUntrusted String @OsUntrusted [] { cachedHostAddress };
    }
    @OsUntrusted
    NetworkInterface netIf;
    try {
      netIf = NetworkInterface.getByName(strInterface);
      if (netIf == null) {
        netIf = getSubinterface(strInterface);
      }
    } catch (@OsUntrusted SocketException e) {
      LOG.warn("I/O error finding interface " + strInterface +
          ": " + e.getMessage());
      return new @OsUntrusted String @OsUntrusted [] { cachedHostAddress };
    }
    if (netIf == null) {
      throw new @OsUntrusted UnknownHostException("No such interface " + strInterface);
    }

    // NB: Using a LinkedHashSet to preserve the order for callers
    // that depend on a particular element being 1st in the array.
    // For example, getDefaultIP always returns the first element.
    @OsUntrusted
    LinkedHashSet<@OsUntrusted InetAddress> allAddrs = new @OsUntrusted LinkedHashSet<@OsUntrusted InetAddress>();
    allAddrs.addAll(Collections.list(netIf.getInetAddresses()));
    if (!returnSubinterfaces) {
      allAddrs.removeAll(getSubinterfaceInetAddrs(netIf));
    }

    @OsUntrusted
    String ips @OsUntrusted [] = new @OsUntrusted String @OsUntrusted [allAddrs.size()];
    @OsUntrusted
    int i = 0;
    for (@OsUntrusted InetAddress addr : allAddrs) {
      ips[i++] = addr.getHostAddress();
    }
    return ips;
  }


  /**
   * Returns the first available IP address associated with the provided
   * network interface or the local host IP if "default" is given.
   *
   * @param strInterface
   *            The name of the network interface or subinterface to query
   *             (e.g. eth0 or eth0:0) or the string "default"
   * @return The IP address in text form, the local host IP is returned
   *         if the interface name "default" is specified
   * @throws UnknownHostException
   *             If the given interface is invalid
   */
  public static @OsUntrusted String getDefaultIP(@OsUntrusted String strInterface)
    throws UnknownHostException {
    @OsUntrusted
    String @OsUntrusted [] ips = getIPs(strInterface);
    return ips[0];
  }

  /**
   * Returns all the host names associated by the provided nameserver with the
   * address bound to the specified network interface
   *
   * @param strInterface
   *            The name of the network interface or subinterface to query
   *            (e.g. eth0 or eth0:0)
   * @param nameserver
   *            The DNS host name
   * @return A string vector of all host names associated with the IPs tied to
   *         the specified interface
   * @throws UnknownHostException if the given interface is invalid
   */
  public static @OsUntrusted String @OsUntrusted [] getHosts(@OsUntrusted String strInterface, @OsUntrusted String nameserver)
    throws UnknownHostException {
    @OsUntrusted
    String @OsUntrusted [] ips = getIPs(strInterface);
    @OsUntrusted
    Vector<@OsUntrusted String> hosts = new @OsUntrusted Vector<@OsUntrusted String>();
    for (@OsUntrusted int ctr = 0; ctr < ips.length; ctr++) {
      try {
        hosts.add(reverseDns(InetAddress.getByName(ips[ctr]),
                             nameserver));
      } catch (@OsUntrusted UnknownHostException ignored) {
      } catch (@OsUntrusted NamingException ignored) {
      }
    }
    if (hosts.isEmpty()) {
      LOG.warn("Unable to determine hostname for interface " + strInterface);
      return new @OsUntrusted String @OsUntrusted [] { cachedHostname };
    } else {
      return hosts.toArray(new @OsUntrusted String @OsUntrusted [hosts.size()]);
    }
  }


  /**
   * Determine the local hostname; retrieving it from cache if it is known
   * If we cannot determine our host name, return "localhost"
   * @return the local hostname or "localhost"
   */
  private static @OsUntrusted String resolveLocalHostname() {
    @OsUntrusted
    String localhost;
    try {
      localhost = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (@OsUntrusted UnknownHostException e) {
      LOG.warn("Unable to determine local hostname "
          + "-falling back to \"" + LOCALHOST + "\"", e);
      localhost = LOCALHOST;
    }
    return localhost;
  }


  /**
   * Get the IPAddress of the local host as a string.
   * This will be a loop back value if the local host address cannot be
   * determined.
   * If the loopback address of "localhost" does not resolve, then the system's
   * network is in such a state that nothing is going to work. A message is
   * logged at the error level and a null pointer returned, a pointer
   * which will trigger failures later on the application
   * @return the IPAddress of the local host or null for a serious problem.
   */
  private static @OsUntrusted String resolveLocalHostIPAddress() {
    @OsUntrusted
    String address;
      try {
        address = InetAddress.getLocalHost().getHostAddress();
      } catch (@OsUntrusted UnknownHostException e) {
        LOG.warn("Unable to determine address of the host"
                + "-falling back to \"" + LOCALHOST + "\" address", e);
        try {
          address = InetAddress.getByName(LOCALHOST).getHostAddress();
        } catch (@OsUntrusted UnknownHostException noLocalHostAddressException) {
          //at this point, deep trouble
          LOG.error("Unable to determine local loopback address "
                  + "of \"" + LOCALHOST + "\" " +
                  "-this system's network configuration is unsupported", e);
          address = null;
        }
      }
    return address;
  }

  /**
   * Returns all the host names associated by the default nameserver with the
   * address bound to the specified network interface
   * 
   * @param strInterface
   *            The name of the network interface to query (e.g. eth0)
   * @return The list of host names associated with IPs bound to the network
   *         interface
   * @throws UnknownHostException
   *             If one is encountered while querying the default interface
   * 
   */
  public static @OsUntrusted String @OsUntrusted [] getHosts(@OsUntrusted String strInterface)
    throws UnknownHostException {
    return getHosts(strInterface, null);
  }

  /**
   * Returns the default (first) host name associated by the provided
   * nameserver with the address bound to the specified network interface
   * 
   * @param strInterface
   *            The name of the network interface to query (e.g. eth0)
   * @param nameserver
   *            The DNS host name
   * @return The default host names associated with IPs bound to the network
   *         interface
   * @throws UnknownHostException
   *             If one is encountered while querying the default interface
   */
  public static @OsUntrusted String getDefaultHost(@OsUntrusted String strInterface, @OsUntrusted String nameserver)
    throws UnknownHostException {
    if ("default".equals(strInterface)) {
      return cachedHostname;
    }

    if ("default".equals(nameserver)) {
      return getDefaultHost(strInterface);
    }

    @OsUntrusted
    String @OsUntrusted [] hosts = getHosts(strInterface, nameserver);
    return hosts[0];
  }

  /**
   * Returns the default (first) host name associated by the default
   * nameserver with the address bound to the specified network interface
   * 
   * @param strInterface
   *            The name of the network interface to query (e.g. eth0).
   *            Must not be null.
   * @return The default host name associated with IPs bound to the network
   *         interface
   * @throws UnknownHostException
   *             If one is encountered while querying the default interface
   */
  public static @OsUntrusted String getDefaultHost(@OsUntrusted String strInterface)
    throws UnknownHostException {
    return getDefaultHost(strInterface, null);
  }

}
