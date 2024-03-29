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
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsVisitor;
import org.apache.hadoop.metrics2.sink.ganglia.AbstractGangliaSink.GangliaSlope;

/**
 * Since implementations of Metric are not public, hence use a visitor to figure
 * out the type and slope of the metric. Counters have "positive" slope.
 */
class GangliaMetricVisitor implements @OsUntrusted MetricsVisitor {
  private static final @OsUntrusted String INT32 = "int32";
  private static final @OsUntrusted String FLOAT = "float";
  private static final @OsUntrusted String DOUBLE = "double";

  private @OsUntrusted String type;
  private @OsUntrusted GangliaSlope slope;

  /**
   * @return the type of a visited metric
   */
  @OsUntrusted
  String getType(@OsUntrusted GangliaMetricVisitor this) {
    return type;
  }

  /**
   * @return the slope of a visited metric. Slope is positive for counters and
   *         null for others
   */
  @OsUntrusted
  GangliaSlope getSlope(@OsUntrusted GangliaMetricVisitor this) {
    return slope;
  }

  @Override
  public void gauge(@OsUntrusted GangliaMetricVisitor this, @OsUntrusted MetricsInfo info, @OsUntrusted int value) {
    // MetricGaugeInt.class ==> "int32"
    type = INT32;
    slope = null; // set to null as cannot figure out from Metric
  }

  @Override
  public void gauge(@OsUntrusted GangliaMetricVisitor this, @OsUntrusted MetricsInfo info, @OsUntrusted long value) {
    // MetricGaugeLong.class ==> "float"
    type = FLOAT;
    slope = null; // set to null as cannot figure out from Metric
  }

  @Override
  public void gauge(@OsUntrusted GangliaMetricVisitor this, @OsUntrusted MetricsInfo info, @OsUntrusted float value) {
    // MetricGaugeFloat.class ==> "float"
    type = FLOAT;
    slope = null; // set to null as cannot figure out from Metric
  }

  @Override
  public void gauge(@OsUntrusted GangliaMetricVisitor this, @OsUntrusted MetricsInfo info, @OsUntrusted double value) {
    // MetricGaugeDouble.class ==> "double"
    type = DOUBLE;
    slope = null; // set to null as cannot figure out from Metric
  }

  @Override
  public void counter(@OsUntrusted GangliaMetricVisitor this, @OsUntrusted MetricsInfo info, @OsUntrusted int value) {
    // MetricCounterInt.class ==> "int32"
    type = INT32;
    // counters have positive slope
    slope = GangliaSlope.positive;
  }

  @Override
  public void counter(@OsUntrusted GangliaMetricVisitor this, @OsUntrusted MetricsInfo info, @OsUntrusted long value) {
    // MetricCounterLong.class ==> "float"
    type = FLOAT;
    // counters have positive slope
    slope = GangliaSlope.positive;
  }
}
