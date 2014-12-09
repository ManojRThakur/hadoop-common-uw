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
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MutableMetricsFactory {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(MutableMetricsFactory.class);

  @OsUntrusted
  MutableMetric newForField(@OsUntrusted MutableMetricsFactory this, @OsUntrusted Field field, @OsUntrusted Metric annotation,
                            @OsUntrusted
                            MetricsRegistry registry) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("field "+ field +" with annotation "+ annotation);
    }
    @OsUntrusted
    MetricsInfo info = getInfo(annotation, field);
    @OsUntrusted
    MutableMetric metric = newForField(field, annotation);
    if (metric != null) {
      registry.add(info.name(), metric);
      return metric;
    }
    final @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> cls = field.getType();
    if (cls == MutableCounterInt.class) {
      return registry.newCounter(info, 0);
    }
    if (cls == MutableCounterLong.class) {
      return registry.newCounter(info, 0L);
    }
    if (cls == MutableGaugeInt.class) {
      return registry.newGauge(info, 0);
    }
    if (cls == MutableGaugeLong.class) {
      return registry.newGauge(info, 0L);
    }
    if (cls == MutableRate.class) {
      return registry.newRate(info.name(), info.description(),
                              annotation.always());
    }
    if (cls == MutableRates.class) {
      return new @OsUntrusted MutableRates(registry);
    }
    if (cls == MutableStat.class) {
      return registry.newStat(info.name(), info.description(),
                              annotation.sampleName(), annotation.valueName(),
                              annotation.always());
    }
    throw new @OsUntrusted MetricsException("Unsupported metric field "+ field.getName() +
                               " of type "+ field.getType().getName());
  }

  @OsUntrusted
  MutableMetric newForMethod(@OsUntrusted MutableMetricsFactory this, @OsUntrusted Object source, @OsUntrusted Method method, @OsUntrusted Metric annotation,
                             @OsUntrusted
                             MetricsRegistry registry) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("method "+ method +" with annotation "+ annotation);
    }
    @OsUntrusted
    MetricsInfo info = getInfo(annotation, method);
    @OsUntrusted
    MutableMetric metric = newForMethod(source, method, annotation);
    metric = metric != null ? metric :
        new @OsUntrusted MethodMetric(source, method, info, annotation.type());
    registry.add(info.name(), metric);
    return metric;
  }

  /**
   * Override to handle custom mutable metrics for fields
   * @param field of the metric
   * @param annotation  of the field
   * @return a new metric object or null
   */
  protected @OsUntrusted MutableMetric newForField(@OsUntrusted MutableMetricsFactory this, @OsUntrusted Field field, @OsUntrusted Metric annotation) {
    return null;
  }

  /**
   * Override to handle custom mutable metrics for methods
   * @param source the metrics source object
   * @param method to return the metric
   * @param annotation of the method
   * @return a new metric object or null
   */
  protected @OsUntrusted MutableMetric newForMethod(@OsUntrusted MutableMetricsFactory this, @OsUntrusted Object source, @OsUntrusted Method method,
                                       @OsUntrusted
                                       Metric annotation) {
    return null;
  }

  protected @OsUntrusted MetricsInfo getInfo(@OsUntrusted MutableMetricsFactory this, @OsUntrusted Metric annotation, @OsUntrusted Field field) {
    return getInfo(annotation, getName(field));
  }

  protected @OsUntrusted String getName(@OsUntrusted MutableMetricsFactory this, @OsUntrusted Field field) {
    return StringUtils.capitalize(field.getName());
  }

  protected @OsUntrusted MetricsInfo getInfo(@OsUntrusted MutableMetricsFactory this, @OsUntrusted Metric annotation, @OsUntrusted Method method) {
    return getInfo(annotation, getName(method));
  }

  protected @OsUntrusted MetricsInfo getInfo(@OsUntrusted MutableMetricsFactory this, @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> cls, @OsUntrusted Metrics annotation) {
    @OsUntrusted
    String name = annotation.name();
    @OsUntrusted
    String about = annotation.about();
    @OsUntrusted
    String name2 = name.isEmpty() ? cls.getSimpleName() : name;
    return Interns.info(name2, about.isEmpty() ? name2 : about);
  }

  /**
   * Remove the prefix "get", if any, from the method name. Return the
   * capacitalized method name."
   */
  protected @OsUntrusted String getName(@OsUntrusted MutableMetricsFactory this, @OsUntrusted Method method) {
    @OsUntrusted
    String methodName = method.getName();
    if (methodName.startsWith("get")) {
      return StringUtils.capitalize(methodName.substring(3));
    }
    return StringUtils.capitalize(methodName);
  }

  protected @OsUntrusted MetricsInfo getInfo(@OsUntrusted MutableMetricsFactory this, @OsUntrusted Metric annotation, @OsUntrusted String defaultName) {
    @OsUntrusted
    String @OsUntrusted [] value = annotation.value();
    if (value.length == 2) {
      // Use name and description from the annotation
      return Interns.info(value[0], value[1]);
    }
    if (value.length == 1) {
      // Use description from the annotation and method name as metric name
      return Interns.info(defaultName, value[0]);
    }
    // Use method name as metric name and description
    return Interns.info(defaultName, defaultName);
  }
}
