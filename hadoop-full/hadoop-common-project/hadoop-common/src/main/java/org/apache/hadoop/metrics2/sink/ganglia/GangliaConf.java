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
import org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope;

/**
 * class which is used to store ganglia properties
 */
class GangliaConf {
  private @OsUntrusted String units = AbstractGangliaSink.DEFAULT_UNITS;
  private @OsUntrusted GangliaSlope slope;
  private @OsUntrusted int dmax = AbstractGangliaSink.DEFAULT_DMAX;
  private @OsUntrusted int tmax = AbstractGangliaSink.DEFAULT_TMAX;

  @Override
  public @OsUntrusted String toString(@OsUntrusted GangliaConf this) {
    @OsUntrusted
    StringBuilder buf = new @OsUntrusted StringBuilder();
    buf.append("unit=").append(units).append(", slope=").append(slope)
        .append(", dmax=").append(dmax).append(", tmax=").append(tmax);
    return buf.toString();
  }

  /**
   * @return the units
   */
  @OsUntrusted
  String getUnits(@OsUntrusted GangliaConf this) {
    return units;
  }

  /**
   * @param units the units to set
   */
  void setUnits(@OsUntrusted GangliaConf this, @OsUntrusted String units) {
    this.units = units;
  }

  /**
   * @return the slope
   */
  @OsUntrusted
  GangliaSlope getSlope(@OsUntrusted GangliaConf this) {
    return slope;
  }

  /**
   * @param slope the slope to set
   */
  void setSlope(@OsUntrusted GangliaConf this, @OsUntrusted GangliaSlope slope) {
    this.slope = slope;
  }

  /**
   * @return the dmax
   */
  @OsUntrusted
  int getDmax(@OsUntrusted GangliaConf this) {
    return dmax;
  }

  /**
   * @param dmax the dmax to set
   */
  void setDmax(@OsUntrusted GangliaConf this, @OsUntrusted int dmax) {
    this.dmax = dmax;
  }

  /**
   * @return the tmax
   */
  @OsUntrusted
  int getTmax(@OsUntrusted GangliaConf this) {
    return tmax;
  }

  /**
   * @param tmax the tmax to set
   */
  void setTmax(@OsUntrusted GangliaConf this, @OsUntrusted int tmax) {
    this.tmax = tmax;
  }
}