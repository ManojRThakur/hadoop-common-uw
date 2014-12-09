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
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.apache.hadoop.metrics.spi.Util;

/**
 * Context for sending metrics to Ganglia.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class GangliaContext extends @OsUntrusted AbstractMetricsContext {
    
  private static final @OsUntrusted String PERIOD_PROPERTY = "period";
  private static final @OsUntrusted String SERVERS_PROPERTY = "servers";
  private static final @OsUntrusted String UNITS_PROPERTY = "units";
  private static final @OsUntrusted String SLOPE_PROPERTY = "slope";
  private static final @OsUntrusted String TMAX_PROPERTY = "tmax";
  private static final @OsUntrusted String DMAX_PROPERTY = "dmax";
    
  private static final @OsUntrusted String DEFAULT_UNITS = "";
  private static final @OsUntrusted String DEFAULT_SLOPE = "both";
  private static final @OsUntrusted int DEFAULT_TMAX = 60;
  private static final @OsUntrusted int DEFAULT_DMAX = 0;
  private static final @OsUntrusted int DEFAULT_PORT = 8649;
  private static final @OsUntrusted int BUFFER_SIZE = 1500;       // as per libgmond.c

  private final @OsUntrusted Log LOG = LogFactory.getLog(this.getClass());    

  private static final @OsUntrusted Map<@OsUntrusted Class, @OsUntrusted String> typeTable = new @OsUntrusted HashMap<@OsUntrusted Class, @OsUntrusted String>(5);
    
  static {
    typeTable.put(String.class, "string");
    typeTable.put(Byte.class, "int8");
    typeTable.put(Short.class, "int16");
    typeTable.put(Integer.class, "int32");
    typeTable.put(Long.class, "float");
    typeTable.put(Float.class, "float");
  }
    
  protected @OsUntrusted byte @OsUntrusted [] buffer = new @OsUntrusted byte @OsUntrusted [BUFFER_SIZE];
  protected @OsUntrusted int offset;
    
  protected @OsUntrusted List<@OsUntrusted ? extends @OsUntrusted SocketAddress> metricsServers;
  private @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> unitsTable;
  private @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> slopeTable;
  private @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> tmaxTable;
  private @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> dmaxTable;
    
  protected @OsUntrusted DatagramSocket datagramSocket;
    
  /** Creates a new instance of GangliaContext */
  @InterfaceAudience.Private
  public @OsUntrusted GangliaContext() {
  }
    
  @Override
  @InterfaceAudience.Private
  public void init(@OsUntrusted GangliaContext this, @OsUntrusted String contextName, @OsUntrusted ContextFactory factory) {
    super.init(contextName, factory);
    parseAndSetPeriod(PERIOD_PROPERTY);
        
    metricsServers = 
      Util.parse(getAttribute(SERVERS_PROPERTY), DEFAULT_PORT); 
        
    unitsTable = getAttributeTable(UNITS_PROPERTY);
    slopeTable = getAttributeTable(SLOPE_PROPERTY);
    tmaxTable  = getAttributeTable(TMAX_PROPERTY);
    dmaxTable  = getAttributeTable(DMAX_PROPERTY);
        
    try {
      datagramSocket = new @OsUntrusted DatagramSocket();
    } catch (@OsUntrusted SocketException se) {
      se.printStackTrace();
    }
  }

    /**
   * method to close the datagram socket
   */
  @Override
  public void close(@OsUntrusted GangliaContext this) {
    super.close();
    if (datagramSocket != null) {
      datagramSocket.close();
    }
  }
  
  @Override
  @InterfaceAudience.Private
  public void emitRecord(@OsUntrusted GangliaContext this, @OsUntrusted String contextName, @OsUntrusted String recordName,
    @OsUntrusted
    OutputRecord outRec) 
  throws IOException {
    // Setup so that the records have the proper leader names so they are
    // unambiguous at the ganglia level, and this prevents a lot of rework
    @OsUntrusted
    StringBuilder sb = new @OsUntrusted StringBuilder();
    sb.append(contextName);
    sb.append('.');

    if (contextName.equals("jvm") && outRec.getTag("processName") != null) {
      sb.append(outRec.getTag("processName"));
      sb.append('.');
    }

    sb.append(recordName);
    sb.append('.');
    @OsUntrusted
    int sbBaseLen = sb.length();

    // emit each metric in turn
    for (@OsUntrusted String metricName : outRec.getMetricNames()) {
      @OsUntrusted
      Object metric = outRec.getMetric(metricName);
      @OsUntrusted
      String type = typeTable.get(metric.getClass());
      if (type != null) {
        sb.append(metricName);
        emitMetric(sb.toString(), type, metric.toString());
        sb.setLength(sbBaseLen);
      } else {
        LOG.warn("Unknown metrics type: " + metric.getClass());
      }
    }
  }
    
  protected void emitMetric(@OsUntrusted GangliaContext this, @OsUntrusted String name, @OsUntrusted String type,  @OsUntrusted String value) 
  throws IOException {
    @OsUntrusted
    String units = getUnits(name);
    @OsUntrusted
    int slope = getSlope(name);
    @OsUntrusted
    int tmax = getTmax(name);
    @OsUntrusted
    int dmax = getDmax(name);
        
    offset = 0;
    xdr_int(0);             // metric_user_defined
    xdr_string(type);
    xdr_string(name);
    xdr_string(value);
    xdr_string(units);
    xdr_int(slope);
    xdr_int(tmax);
    xdr_int(dmax);
        
    for (@OsUntrusted SocketAddress socketAddress : metricsServers) {
      @OsUntrusted
      DatagramPacket packet = 
        new @OsUntrusted DatagramPacket(buffer, offset, socketAddress);
      datagramSocket.send(packet);
    }
  }
    
  protected @OsUntrusted String getUnits(@OsUntrusted GangliaContext this, @OsUntrusted String metricName) {
    @OsUntrusted
    String result = unitsTable.get(metricName);
    if (result == null) {
      result = DEFAULT_UNITS;
    }
    return result;
  }
    
  protected @OsUntrusted int getSlope(@OsUntrusted GangliaContext this, @OsUntrusted String metricName) {
    @OsUntrusted
    String slopeString = slopeTable.get(metricName);
    if (slopeString == null) {
      slopeString = DEFAULT_SLOPE; 
    }
    return ("zero".equals(slopeString) ? 0 : 3); // see gmetric.c
  }
    
  protected @OsUntrusted int getTmax(@OsUntrusted GangliaContext this, @OsUntrusted String metricName) {
    if (tmaxTable == null) {
      return DEFAULT_TMAX;
    }
    @OsUntrusted
    String tmaxString = tmaxTable.get(metricName);
    if (tmaxString == null) {
      return DEFAULT_TMAX;
    }
    else {
      return Integer.parseInt(tmaxString);
    }
  }
    
  protected @OsUntrusted int getDmax(@OsUntrusted GangliaContext this, @OsUntrusted String metricName) {
    @OsUntrusted
    String dmaxString = dmaxTable.get(metricName);
    if (dmaxString == null) {
      return DEFAULT_DMAX;
    }
    else {
      return Integer.parseInt(dmaxString);
    }
  }
    
  /**
   * Puts a string into the buffer by first writing the size of the string
   * as an int, followed by the bytes of the string, padded if necessary to
   * a multiple of 4.
   */
  protected void xdr_string(@OsUntrusted GangliaContext this, @OsUntrusted String s) {
    @OsUntrusted
    byte @OsUntrusted [] bytes = s.getBytes();
    @OsUntrusted
    int len = bytes.length;
    xdr_int(len);
    System.arraycopy(bytes, 0, buffer, offset, len);
    offset += len;
    pad();
  }

  /**
   * Pads the buffer with zero bytes up to the nearest multiple of 4.
   */
  private void pad(@OsUntrusted GangliaContext this) {
    @OsUntrusted
    int newOffset = ((offset + 3) / 4) * 4;
    while (offset < newOffset) {
      buffer[offset++] = 0;
    }
  }
        
  /**
   * Puts an integer into the buffer as 4 bytes, big-endian.
   */
  protected void xdr_int(@OsUntrusted GangliaContext this, @OsUntrusted int i) {
    buffer[offset++] = (@OsUntrusted byte)((i >> 24) & 0xff);
    buffer[offset++] = (@OsUntrusted byte)((i >> 16) & 0xff);
    buffer[offset++] = (@OsUntrusted byte)((i >> 8) & 0xff);
    buffer[offset++] = (@OsUntrusted byte)(i & 0xff);
  }
}
