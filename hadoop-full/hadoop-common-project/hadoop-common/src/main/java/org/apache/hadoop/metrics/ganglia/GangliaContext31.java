/*
 * GangliaContext.java
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

package org.apache.hadoop.metrics.ganglia;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.net.DNS;

/**
 * Context for sending metrics to Ganglia version 3.1.x.
 * 
 * 3.1.1 has a slightly different wire portal compared to 3.0.x.
 */
public class GangliaContext31 extends @OsUntrusted GangliaContext {

  @OsUntrusted
  String hostName = "UNKNOWN.example.com";

  private static final @OsUntrusted Log LOG = 
    LogFactory.getLog("org.apache.hadoop.util.GangliaContext31");

  @Override
  public void init(@OsUntrusted GangliaContext31 this, @OsUntrusted String contextName, @OsUntrusted ContextFactory factory) {
    super.init(contextName, factory);

    LOG.debug("Initializing the GangliaContext31 for Ganglia 3.1 metrics.");

    // Take the hostname from the DNS class.

    @OsUntrusted
    Configuration conf = new @OsUntrusted Configuration();

    if (conf.get("slave.host.name") != null) {
      hostName = conf.get("slave.host.name");
    } else {
      try {
        hostName = DNS.getDefaultHost(
          conf.get("dfs.datanode.dns.interface","default"),
          conf.get("dfs.datanode.dns.nameserver","default"));
      } catch (@OsUntrusted UnknownHostException uhe) {
        LOG.error(uhe);
    	hostName = "UNKNOWN.example.com";
      }
    }
  }

  @Override
  protected void emitMetric(@OsUntrusted GangliaContext31 this, @OsUntrusted String name, @OsUntrusted String type,  @OsUntrusted String value) 
    throws IOException
  {
    if (name == null) {
      LOG.warn("Metric was emitted with no name.");
      return;
    } else if (value == null) {
      LOG.warn("Metric name " + name +" was emitted with a null value.");
      return;
    } else if (type == null) {
      LOG.warn("Metric name " + name + ", value " + value + " has no type.");
      return;
    }

    LOG.debug("Emitting metric " + name + ", type " + type + ", value " + 
      value + " from hostname" + hostName);

    @OsUntrusted
    String units = getUnits(name);
    if (units == null) {
      LOG.warn("Metric name " + name + ", value " + value
        + " had 'null' units");
      units = "";
    }
    @OsUntrusted
    int slope = getSlope(name);
    @OsUntrusted
    int tmax = getTmax(name);
    @OsUntrusted
    int dmax = getDmax(name);
    offset = 0;
    @OsUntrusted
    String groupName = name.substring(0,name.lastIndexOf("."));

    // The following XDR recipe was done through a careful reading of
    // gm_protocol.x in Ganglia 3.1 and carefully examining the output of
    // the gmetric utility with strace.

    // First we send out a metadata message
    xdr_int(128);         // metric_id = metadata_msg
    xdr_string(hostName); // hostname
    xdr_string(name);     // metric name
    xdr_int(0);           // spoof = False
    xdr_string(type);     // metric type
    xdr_string(name);     // metric name
    xdr_string(units);    // units
    xdr_int(slope);       // slope
    xdr_int(tmax);        // tmax, the maximum time between metrics
    xdr_int(dmax);        // dmax, the maximum data value

    xdr_int(1);             /*Num of the entries in extra_value field for 
                              Ganglia 3.1.x*/
    xdr_string("GROUP");    /*Group attribute*/
    xdr_string(groupName);  /*Group value*/

    for (@OsUntrusted SocketAddress socketAddress : metricsServers) {
      @OsUntrusted
      DatagramPacket packet =
        new @OsUntrusted DatagramPacket(buffer, offset, socketAddress);
      datagramSocket.send(packet);
    }

    // Now we send out a message with the actual value.
    // Technically, we only need to send out the metadata message once for
    // each metric, but I don't want to have to record which metrics we did and
    // did not send.
    offset = 0;
    xdr_int(133);         // we are sending a string value
    xdr_string(hostName); // hostName
    xdr_string(name);     // metric name
    xdr_int(0);           // spoof = False
    xdr_string("%s");     // format field
    xdr_string(value);    // metric value
        
    for (@OsUntrusted SocketAddress socketAddress : metricsServers) {
      @OsUntrusted
      DatagramPacket packet = 
        new @OsUntrusted DatagramPacket(buffer, offset, socketAddress);
      datagramSocket.send(packet);
    }
  }

}
