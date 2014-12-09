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

package org.apache.hadoop.metrics2.sink.ganglia;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.util.Servers;
import org.apache.hadoop.net.DNS;

/**
 * This the base class for Ganglia sink classes using metrics2. Lot of the code
 * has been derived from org.apache.hadoop.metrics.ganglia.GangliaContext.
 * As per the documentation, sink implementations doesn't have to worry about
 * thread safety. Hence the code wasn't written for thread safety and should
 * be modified in case the above assumption changes in the future.
 */
public abstract class AbstractGangliaSink implements @OsUntrusted MetricsSink {

  public final @OsUntrusted Log LOG = LogFactory.getLog(this.getClass());

  /*
   * Output of "gmetric --help" showing allowable values
   * -t, --type=STRING
   *     Either string|int8|uint8|int16|uint16|int32|uint32|float|double
   * -u, --units=STRING Unit of measure for the value e.g. Kilobytes, Celcius
   *     (default='')
   * -s, --slope=STRING Either zero|positive|negative|both
   *     (default='both')
   * -x, --tmax=INT The maximum time in seconds between gmetric calls
   *     (default='60')
   */
  public static final @OsUntrusted String DEFAULT_UNITS = "";
  public static final @OsUntrusted int DEFAULT_TMAX = 60;
  public static final @OsUntrusted int DEFAULT_DMAX = 0;
  public static final @OsUntrusted GangliaSlope DEFAULT_SLOPE = GangliaSlope.both;
  public static final @OsUntrusted int DEFAULT_PORT = 8649;
  public static final @OsUntrusted String SERVERS_PROPERTY = "servers";
  public static final @OsUntrusted int BUFFER_SIZE = 1500; // as per libgmond.c
  public static final @OsUntrusted String SUPPORT_SPARSE_METRICS_PROPERTY = "supportsparse";
  public static final @OsUntrusted boolean SUPPORT_SPARSE_METRICS_DEFAULT = false;
  public static final @OsUntrusted String EQUAL = "=";

  private @OsUntrusted String hostName = "UNKNOWN.example.com";
  private @OsUntrusted DatagramSocket datagramSocket;
  private @OsUntrusted List<@OsUntrusted ? extends @OsUntrusted SocketAddress> metricsServers;
  private @OsUntrusted byte @OsUntrusted [] buffer = new @OsUntrusted byte @OsUntrusted [BUFFER_SIZE];
  private @OsUntrusted int offset;
  private @OsUntrusted boolean supportSparseMetrics = SUPPORT_SPARSE_METRICS_DEFAULT;

  /**
   * Used for visiting Metrics
   */
  protected final @OsUntrusted GangliaMetricVisitor gangliaMetricVisitor =
    new @OsUntrusted GangliaMetricVisitor();

  private @OsUntrusted SubsetConfiguration conf;
  private @OsUntrusted Map<@OsUntrusted String, @OsUntrusted GangliaConf> gangliaConfMap;
  private @OsUntrusted GangliaConf DEFAULT_GANGLIA_CONF = new @OsUntrusted GangliaConf();

  /**
   * ganglia slope values which equal the ordinal
   */
  public enum GangliaSlope {

@OsUntrusted  zero,       // 0

@OsUntrusted  positive,   // 1

@OsUntrusted  negative,   // 2

@OsUntrusted  both        // 3
  };

  /**
   * define enum for various type of conf
   */
  public enum GangliaConfType {

@OsUntrusted  slope,  @OsUntrusted  units,  @OsUntrusted  dmax,  @OsUntrusted  tmax
  };

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.metrics2.MetricsPlugin#init(org.apache.commons.configuration
   * .SubsetConfiguration)
   */
  @Override
  public void init(@OsUntrusted AbstractGangliaSink this, @OsUntrusted SubsetConfiguration conf) {
    LOG.debug("Initializing the GangliaSink for Ganglia metrics.");

    this.conf = conf;

    // Take the hostname from the DNS class.
    if (conf.getString("slave.host.name") != null) {
      hostName = conf.getString("slave.host.name");
    } else {
      try {
        hostName = DNS.getDefaultHost(
            conf.getString("dfs.datanode.dns.interface", "default"),
            conf.getString("dfs.datanode.dns.nameserver", "default"));
      } catch (@OsUntrusted UnknownHostException uhe) {
        LOG.error(uhe);
        hostName = "UNKNOWN.example.com";
      }
    }

    // load the gannglia servers from properties
    metricsServers = Servers.parse(conf.getString(SERVERS_PROPERTY),
        DEFAULT_PORT);

    // extract the Ganglia conf per metrics
    gangliaConfMap = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted GangliaConf>();
    loadGangliaConf(GangliaConfType.units);
    loadGangliaConf(GangliaConfType.tmax);
    loadGangliaConf(GangliaConfType.dmax);
    loadGangliaConf(GangliaConfType.slope);

    try {
      datagramSocket = new @OsUntrusted DatagramSocket();
    } catch (@OsUntrusted SocketException se) {
      LOG.error(se);
    }

    // see if sparseMetrics is supported. Default is false
    supportSparseMetrics = conf.getBoolean(SUPPORT_SPARSE_METRICS_PROPERTY,
        SUPPORT_SPARSE_METRICS_DEFAULT);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.metrics2.MetricsSink#flush()
   */
  @Override
  public void flush(@OsUntrusted AbstractGangliaSink this) {
    // nothing to do as we are not buffering data
  }

  // Load the configurations for a conf type
  private void loadGangliaConf(@OsUntrusted AbstractGangliaSink this, @OsUntrusted GangliaConfType gtype) {
    @OsUntrusted
    String propertyarr @OsUntrusted [] = conf.getStringArray(gtype.name());
    if (propertyarr != null && propertyarr.length > 0) {
      for (@OsUntrusted String metricNValue : propertyarr) {
        @OsUntrusted
        String metricNValueArr @OsUntrusted [] = metricNValue.split(EQUAL);
        if (metricNValueArr.length != 2 || metricNValueArr[0].length() == 0) {
          LOG.error("Invalid propertylist for " + gtype.name());
        }

        @OsUntrusted
        String metricName = metricNValueArr[0].trim();
        @OsUntrusted
        String metricValue = metricNValueArr[1].trim();
        @OsUntrusted
        GangliaConf gconf = gangliaConfMap.get(metricName);
        if (gconf == null) {
          gconf = new @OsUntrusted GangliaConf();
          gangliaConfMap.put(metricName, gconf);
        }

        switch (gtype) {
        case units:
          gconf.setUnits(metricValue);
          break;
        case dmax:
          gconf.setDmax(Integer.parseInt(metricValue));
          break;
        case tmax:
          gconf.setTmax(Integer.parseInt(metricValue));
          break;
        case slope:
          gconf.setSlope(GangliaSlope.valueOf(metricValue));
          break;
        }
      }
    }
  }

  /**
   * Lookup GangliaConf from cache. If not found, return default values
   *
   * @param metricName
   * @return looked up GangliaConf
   */
  protected @OsUntrusted GangliaConf getGangliaConfForMetric(@OsUntrusted AbstractGangliaSink this, @OsUntrusted String metricName) {
    @OsUntrusted
    GangliaConf gconf = gangliaConfMap.get(metricName);

    return gconf != null ? gconf : DEFAULT_GANGLIA_CONF;
  }

  /**
   * @return the hostName
   */
  protected @OsUntrusted String getHostName(@OsUntrusted AbstractGangliaSink this) {
    return hostName;
  }

  /**
   * Puts a string into the buffer by first writing the size of the string as an
   * int, followed by the bytes of the string, padded if necessary to a multiple
   * of 4.
   * @param s the string to be written to buffer at offset location
   */
  protected void xdr_string(@OsUntrusted AbstractGangliaSink this, @OsUntrusted String s) {
    @OsUntrusted
    byte @OsUntrusted [] bytes = s.getBytes();
    @OsUntrusted
    int len = bytes.length;
    xdr_int(len);
    System.arraycopy(bytes, 0, buffer, offset, len);
    offset += len;
    pad();
  }

  // Pads the buffer with zero bytes up to the nearest multiple of 4.
  private void pad(@OsUntrusted AbstractGangliaSink this) {
    @OsUntrusted
    int newOffset = ((offset + 3) / 4) * 4;
    while (offset < newOffset) {
      buffer[offset++] = 0;
    }
  }

  /**
   * Puts an integer into the buffer as 4 bytes, big-endian.
   */
  protected void xdr_int(@OsUntrusted AbstractGangliaSink this, @OsUntrusted int i) {
    buffer[offset++] = (@OsUntrusted byte) ((i >> 24) & 0xff);
    buffer[offset++] = (@OsUntrusted byte) ((i >> 16) & 0xff);
    buffer[offset++] = (@OsUntrusted byte) ((i >> 8) & 0xff);
    buffer[offset++] = (@OsUntrusted byte) (i & 0xff);
  }

  /**
   * Sends Ganglia Metrics to the configured hosts
   * @throws IOException
   */
  protected void emitToGangliaHosts(@OsUntrusted AbstractGangliaSink this) throws IOException {
    try {
      for (@OsUntrusted SocketAddress socketAddress : metricsServers) {
        @OsUntrusted
        DatagramPacket packet =
          new @OsUntrusted DatagramPacket(buffer, offset, socketAddress);
        datagramSocket.send(packet);
      }
    } finally {
      // reset the buffer for the next metric to be built
      offset = 0;
    }
  }

  /**
   * Reset the buffer for the next metric to be built
   */
  void resetBuffer(@OsUntrusted AbstractGangliaSink this) {
    offset = 0;
  }

  /**
   * @return whether sparse metrics are supported
   */
  protected @OsUntrusted boolean isSupportSparseMetrics(@OsUntrusted AbstractGangliaSink this) {
    return supportSparseMetrics;
  }

  /**
   * Used only by unit test
   * @param datagramSocket the datagramSocket to set.
   */
  void setDatagramSocket(@OsUntrusted AbstractGangliaSink this, @OsUntrusted DatagramSocket datagramSocket) {
    this.datagramSocket = datagramSocket;
  }
}
