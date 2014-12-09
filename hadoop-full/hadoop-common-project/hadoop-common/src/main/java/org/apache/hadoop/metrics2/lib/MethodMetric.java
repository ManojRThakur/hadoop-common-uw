/*
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

package org.apache.hadoop.metrics2.lib;

import ostrusted.quals.OsUntrusted;
import java.lang.reflect.Method;

import static com.google.common.base.Preconditions.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import static org.apache.hadoop.metrics2.util.Contracts.*;

/**
 * Metric generated from a method, mostly used by annotation
 */
class MethodMetric extends @OsUntrusted MutableMetric {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(MethodMetric.class);

  private final @OsUntrusted Object obj;
  private final @OsUntrusted Method method;
  private final @OsUntrusted MetricsInfo info;
  private final @OsUntrusted MutableMetric impl;

  @OsUntrusted
  MethodMetric(@OsUntrusted Object obj, @OsUntrusted Method method, @OsUntrusted MetricsInfo info, Metric.@OsUntrusted Type type) {
    this.obj = checkNotNull(obj, "object");
    this.method = checkArg(method, method.getParameterTypes().length == 0,
                           "Metric method should have no arguments");
    this.method.setAccessible(true);
    this.info = checkNotNull(info, "info");
    impl = newImpl(checkNotNull(type, "metric type"));
  }

  private @OsUntrusted MutableMetric newImpl(@OsUntrusted MethodMetric this, Metric.@OsUntrusted Type metricType) {
    @OsUntrusted
    Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> resType = method.getReturnType();
    switch (metricType) {
      case COUNTER:
        return newCounter(resType);
      case GAUGE:
        return newGauge(resType);
      case DEFAULT:
        return resType == String.class ? newTag(resType) : newGauge(resType);
      case TAG:
        return newTag(resType);
      default:
        checkArg(metricType, false, "unsupported metric type");
        return null;
    }
  }

  @OsUntrusted
  MutableMetric newCounter(@OsUntrusted MethodMetric this, final @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> type) {
    if (isInt(type) || isLong(type)) {
      return new @OsUntrusted MutableMetric() {
        @Override public void snapshot(@OsUntrusted MetricsRecordBuilder rb, @OsUntrusted boolean all) {
          try {
            @OsUntrusted
            Object ret = method.invoke(obj, (@OsUntrusted Object @OsUntrusted [])null);
            if (isInt(type)) rb.addCounter(info, ((@OsUntrusted Integer) ret).intValue());
            else rb.addCounter(info, ((@OsUntrusted Long) ret).longValue());
          } catch (@OsUntrusted Exception ex) {
            LOG.error("Error invoking method "+ method.getName(), ex);
          }
        }
      };
    }
    throw new @OsUntrusted MetricsException("Unsupported counter type: "+ type.getName());
  }

  static @OsUntrusted boolean isInt(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> type) {
    @OsUntrusted
    boolean ret = type == Integer.TYPE || type == Integer.class;
    return ret;
  }

  static @OsUntrusted boolean isLong(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> type) {
    return type == Long.TYPE || type == Long.class;
  }

  static @OsUntrusted boolean isFloat(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> type) {
    return type == Float.TYPE || type == Float.class;
  }

  static @OsUntrusted boolean isDouble(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> type) {
    return type == Double.TYPE || type == Double.class;
  }

  @OsUntrusted
  MutableMetric newGauge(@OsUntrusted MethodMetric this, final @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> t) {
    if (isInt(t) || isLong(t) || isFloat(t) || isDouble(t)) {
      return new @OsUntrusted MutableMetric() {
        @Override public void snapshot(@OsUntrusted MetricsRecordBuilder rb, @OsUntrusted boolean all) {
          try {
            @OsUntrusted
            Object ret = method.invoke(obj, (@OsUntrusted Object @OsUntrusted []) null);
            if (isInt(t)) rb.addGauge(info, ((@OsUntrusted Integer) ret).intValue());
            else if (isLong(t)) rb.addGauge(info, ((@OsUntrusted Long) ret).longValue());
            else if (isFloat(t)) rb.addGauge(info, ((@OsUntrusted Float) ret).floatValue());
            else rb.addGauge(info, ((@OsUntrusted Double) ret).doubleValue());
          } catch (@OsUntrusted Exception ex) {
            LOG.error("Error invoking method "+ method.getName(), ex);
          }
        }
      };
    }
    throw new @OsUntrusted MetricsException("Unsupported gauge type: "+ t.getName());
  }

  @OsUntrusted
  MutableMetric newTag(@OsUntrusted MethodMetric this, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> resType) {
    if (resType == String.class) {
      return new @OsUntrusted MutableMetric() {
        @Override public void snapshot(@OsUntrusted MetricsRecordBuilder rb, @OsUntrusted boolean all) {
          try {
            @OsUntrusted
            Object ret = method.invoke(obj, (@OsUntrusted Object @OsUntrusted []) null);
            rb.tag(info, (@OsUntrusted String) ret);
          } catch (@OsUntrusted Exception ex) {
            LOG.error("Error invoking method "+ method.getName(), ex);
          }
        }
      };
    }
    throw new @OsUntrusted MetricsException("Unsupported tag type: "+ resType.getName());
  }

  @Override public void snapshot(@OsUntrusted MethodMetric this, @OsUntrusted MetricsRecordBuilder builder, @OsUntrusted boolean all) {
    impl.snapshot(builder, all);
  }

  static @OsUntrusted MetricsInfo metricInfo(@OsUntrusted Method method) {
    return Interns.info(nameFrom(method), "Metric for "+ method.getName());
  }

  static @OsUntrusted String nameFrom(@OsUntrusted Method method) {
    @OsUntrusted
    String methodName = method.getName();
    if (methodName.startsWith("get")) {
      return StringUtils.capitalize(methodName.substring(3));
    }
    return StringUtils.capitalize(methodName);
  }
}
