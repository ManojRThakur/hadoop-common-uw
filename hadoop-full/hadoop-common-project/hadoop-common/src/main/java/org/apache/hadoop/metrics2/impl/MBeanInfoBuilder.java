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

package org.apache.hadoop.metrics2.impl;

import ostrusted.quals.OsUntrusted;
import java.util.List;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;

import com.google.common.collect.Lists;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.MetricsVisitor;

/**
 * Helper class to build MBeanInfo from metrics records
 */
class MBeanInfoBuilder implements @OsUntrusted MetricsVisitor {

  private final @OsUntrusted String name;
  private final @OsUntrusted String description;
  private @OsUntrusted List<@OsUntrusted MBeanAttributeInfo> attrs;
  private @OsUntrusted Iterable<@OsUntrusted MetricsRecordImpl> recs;
  private @OsUntrusted int curRecNo;

  @OsUntrusted
  MBeanInfoBuilder(@OsUntrusted String name, @OsUntrusted String desc) {
    this.name = name;
    description = desc;
    attrs = Lists.newArrayList();
  }

  @OsUntrusted
  MBeanInfoBuilder reset(@OsUntrusted MBeanInfoBuilder this, @OsUntrusted Iterable<@OsUntrusted MetricsRecordImpl> recs) {
    this.recs = recs;
    attrs.clear();
    return this;
  }

  @OsUntrusted
  MBeanAttributeInfo newAttrInfo(@OsUntrusted MBeanInfoBuilder this, @OsUntrusted String name, @OsUntrusted String desc, @OsUntrusted String type) {
    return new @OsUntrusted MBeanAttributeInfo(getAttrName(name), type, desc,
                                  true, false, false); // read-only, non-is
  }

  @OsUntrusted
  MBeanAttributeInfo newAttrInfo(@OsUntrusted MBeanInfoBuilder this, @OsUntrusted MetricsInfo info, @OsUntrusted String type) {
    return newAttrInfo(info.name(), info.description(), type);
  }

  @Override
  public void gauge(@OsUntrusted MBeanInfoBuilder this, @OsUntrusted MetricsInfo info, @OsUntrusted int value) {
    attrs.add(newAttrInfo(info, "java.lang.Integer"));
  }

  @Override
  public void gauge(@OsUntrusted MBeanInfoBuilder this, @OsUntrusted MetricsInfo info, @OsUntrusted long value) {
    attrs.add(newAttrInfo(info, "java.lang.Long"));
  }

  @Override
  public void gauge(@OsUntrusted MBeanInfoBuilder this, @OsUntrusted MetricsInfo info, @OsUntrusted float value) {
    attrs.add(newAttrInfo(info, "java.lang.Float"));
  }

  @Override
  public void gauge(@OsUntrusted MBeanInfoBuilder this, @OsUntrusted MetricsInfo info, @OsUntrusted double value) {
    attrs.add(newAttrInfo(info, "java.lang.Double"));
  }

  @Override
  public void counter(@OsUntrusted MBeanInfoBuilder this, @OsUntrusted MetricsInfo info, @OsUntrusted int value) {
    attrs.add(newAttrInfo(info, "java.lang.Integer"));
  }

  @Override
  public void counter(@OsUntrusted MBeanInfoBuilder this, @OsUntrusted MetricsInfo info, @OsUntrusted long value) {
    attrs.add(newAttrInfo(info, "java.lang.Long"));
  }

  @OsUntrusted
  String getAttrName(@OsUntrusted MBeanInfoBuilder this, @OsUntrusted String name) {
    return curRecNo > 0 ? name +"."+ curRecNo : name;
  }

  @OsUntrusted
  MBeanInfo get(@OsUntrusted MBeanInfoBuilder this) {
    curRecNo = 0;
    for (@OsUntrusted MetricsRecordImpl rec : recs) {
      for (@OsUntrusted MetricsTag t : rec.tags()) {
        attrs.add(newAttrInfo("tag."+ t.name(), t.description(),
                  "java.lang.String"));
      }
      for (@OsUntrusted AbstractMetric m : rec.metrics()) {
        m.visit(this);
      }
      ++curRecNo;
    }
    MetricsSystemImpl.LOG.debug(attrs);
    @OsUntrusted
    MBeanAttributeInfo @OsUntrusted [] attrsArray = new @OsUntrusted MBeanAttributeInfo @OsUntrusted [attrs.size()];
    return new @OsUntrusted MBeanInfo(name, description, attrs.toArray(attrsArray),
                         null, null, null); // no ops/ctors/notifications
  }
}
