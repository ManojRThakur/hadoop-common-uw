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
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static com.google.common.base.Preconditions.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Helper class to build {@link MetricsSource} object from annotations.
 * <p>
 * For a given source object:
 * <ul>
 * <li>Sets the {@link Field}s annotated with {@link Metric} to
 * {@link MutableMetric} and adds it to the {@link MetricsRegistry}.</li>
 * <li>
 * For {@link Method}s annotated with {@link Metric} creates
 * {@link MutableMetric} and adds it to the {@link MetricsRegistry}.</li>
 * </ul>
 */
@InterfaceAudience.Private
public class MetricsSourceBuilder {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(MetricsSourceBuilder.class);

  private final @OsUntrusted Object source;
  private final @OsUntrusted MutableMetricsFactory factory;
  private final @OsUntrusted MetricsRegistry registry;
  private @OsUntrusted MetricsInfo info;
  private @OsUntrusted boolean hasAtMetric = false;
  private @OsUntrusted boolean hasRegistry = false;

  @OsUntrusted
  MetricsSourceBuilder(@OsUntrusted Object source, @OsUntrusted MutableMetricsFactory factory) {
    this.source = checkNotNull(source, "source");
    this.factory = checkNotNull(factory, "mutable metrics factory");
    @OsUntrusted
    Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> cls = source.getClass();
    registry = initRegistry(source);

    for (@OsUntrusted Field field : ReflectionUtils.getDeclaredFieldsIncludingInherited(cls)) {
      add(source, field);
    }
    for (@OsUntrusted Method method : ReflectionUtils.getDeclaredMethodsIncludingInherited(cls)) {
      add(source, method);
    }
  }

  public @OsUntrusted MetricsSource build(@OsUntrusted MetricsSourceBuilder this) {
    if (source instanceof @OsUntrusted MetricsSource) {
      if (hasAtMetric && !hasRegistry) {
        throw new @OsUntrusted MetricsException("Hybrid metrics: registry required.");
      }
      return (@OsUntrusted MetricsSource) source;
    }
    else if (!hasAtMetric) {
      throw new @OsUntrusted MetricsException("No valid @Metric annotation found.");
    }
    return new @OsUntrusted MetricsSource() {
      @Override
      public void getMetrics(@OsUntrusted MetricsCollector builder, @OsUntrusted boolean all) {
        registry.snapshot(builder.addRecord(registry.info()), all);
      }
    };
  }

  public @OsUntrusted MetricsInfo info(@OsUntrusted MetricsSourceBuilder this) {
    return info;
  }

  private @OsUntrusted MetricsRegistry initRegistry(@OsUntrusted MetricsSourceBuilder this, @OsUntrusted Object source) {
    @OsUntrusted
    Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> cls = source.getClass();
    @OsUntrusted
    MetricsRegistry r = null;
    // Get the registry if it already exists.
    for (@OsUntrusted Field field : ReflectionUtils.getDeclaredFieldsIncludingInherited(cls)) {
      if (field.getType() != MetricsRegistry.class) continue;
      try {
        field.setAccessible(true);
        r = (@OsUntrusted MetricsRegistry) field.get(source);
        hasRegistry = r != null;
        break;
      } catch (@OsUntrusted Exception e) {
        LOG.warn("Error accessing field "+ field, e);
        continue;
      }
    }
    // Create a new registry according to annotation
    for (@OsUntrusted Annotation annotation : cls.getAnnotations()) {
      if (annotation instanceof @OsUntrusted Metrics) {
        @OsUntrusted
        Metrics ma = (@OsUntrusted Metrics) annotation;
        info = factory.getInfo(cls, ma);
        if (r == null) {
          r = new @OsUntrusted MetricsRegistry(info);
        }
        r.setContext(ma.context());
      }
    }
    if (r == null) return new @OsUntrusted MetricsRegistry(cls.getSimpleName());
    return r;
  }

  /**
   * Change the declared field {@code field} in {@code source} Object to
   * {@link MutableMetric}
   */
  private void add(@OsUntrusted MetricsSourceBuilder this, @OsUntrusted Object source, @OsUntrusted Field field) {
    for (@OsUntrusted Annotation annotation : field.getAnnotations()) {
      if (!(annotation instanceof @OsUntrusted Metric)) {
        continue;
      }
      try {
        // skip fields already set
        field.setAccessible(true);
        if (field.get(source) != null) continue;
      } catch (@OsUntrusted Exception e) {
        LOG.warn("Error accessing field "+ field +" annotated with"+
                 annotation, e);
        continue;
      }
      @OsUntrusted
      MutableMetric mutable = factory.newForField(field, (@OsUntrusted Metric) annotation,
                                                  registry);
      if (mutable != null) {
        try {
          field.set(source, mutable); // Set the source field to MutableMetric
          hasAtMetric = true;
        } catch (@OsUntrusted Exception e) {
          throw new @OsUntrusted MetricsException("Error setting field "+ field +
                                     " annotated with "+ annotation, e);
        }
      }
    }
  }

  /** Add {@link MutableMetric} for a method annotated with {@link Metric} */
  private void add(@OsUntrusted MetricsSourceBuilder this, @OsUntrusted Object source, @OsUntrusted Method method) {
    for (@OsUntrusted Annotation annotation : method.getAnnotations()) {
      if (!(annotation instanceof @OsUntrusted Metric)) {
        continue;
      }
      factory.newForMethod(source, method, (@OsUntrusted Metric) annotation, registry);
      hasAtMetric = true;
    }
  }
}
