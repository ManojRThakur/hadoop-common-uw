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
package org.apache.hadoop.metrics.spi;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CompositeContext extends @OsUntrusted AbstractMetricsContext {

  private static final @OsUntrusted Log LOG = LogFactory.getLog(CompositeContext.class);
  private static final @OsUntrusted String ARITY_LABEL = "arity";
  private static final @OsUntrusted String SUB_FMT = "%s.sub%d";
  private final @OsUntrusted ArrayList<@OsUntrusted MetricsContext> subctxt =
    new @OsUntrusted ArrayList<@OsUntrusted MetricsContext>();

  @InterfaceAudience.Private
  public @OsUntrusted CompositeContext() {
  }

  @Override
  @InterfaceAudience.Private
  public void init(@OsUntrusted CompositeContext this, @OsUntrusted String contextName, @OsUntrusted ContextFactory factory) {
    super.init(contextName, factory);
    @OsUntrusted
    int nKids;
    try {
      @OsUntrusted
      String sKids = getAttribute(ARITY_LABEL);
      nKids = Integer.valueOf(sKids);
    } catch (@OsUntrusted Exception e) {
      LOG.error("Unable to initialize composite metric " + contextName +
                ": could not init arity", e);
      return;
    }
    for (@OsUntrusted int i = 0; i < nKids; ++i) {
      @OsUntrusted
      MetricsContext ctxt = MetricsUtil.getContext(
          String.format(SUB_FMT, contextName, i), contextName);
      if (null != ctxt) {
        subctxt.add(ctxt);
      }
    }
  }

  @InterfaceAudience.Private
  @Override
  public @OsUntrusted MetricsRecord newRecord(@OsUntrusted CompositeContext this, @OsUntrusted String recordName) {
    return (@OsUntrusted MetricsRecord) Proxy.newProxyInstance(
        MetricsRecord.class.getClassLoader(),
        new @OsUntrusted Class @OsUntrusted [] { MetricsRecord.class },
        new @OsUntrusted MetricsRecordDelegator(recordName, subctxt));
  }

  @InterfaceAudience.Private
  @Override
  protected void emitRecord(@OsUntrusted CompositeContext this, @OsUntrusted String contextName, @OsUntrusted String recordName,
      @OsUntrusted
      OutputRecord outRec) throws IOException {
    for (@OsUntrusted MetricsContext ctxt : subctxt) {
      try {
        ((@OsUntrusted AbstractMetricsContext)ctxt).emitRecord(
          contextName, recordName, outRec);
        if (contextName == null || recordName == null || outRec == null) {
          throw new @OsUntrusted IOException(contextName + ":" + recordName + ":" + outRec);
        }
      } catch (@OsUntrusted IOException e) {
        LOG.warn("emitRecord failed: " + ctxt.getContextName(), e);
      }
    }
  }

  @InterfaceAudience.Private
  @Override
  protected void flush(@OsUntrusted CompositeContext this) throws IOException {
    for (@OsUntrusted MetricsContext ctxt : subctxt) {
      try {
        ((@OsUntrusted AbstractMetricsContext)ctxt).flush();
      } catch (@OsUntrusted IOException e) {
        LOG.warn("flush failed: " + ctxt.getContextName(), e);
      }
    }
  }

  @InterfaceAudience.Private
  @Override
  public void startMonitoring(@OsUntrusted CompositeContext this) throws IOException {
    for (@OsUntrusted MetricsContext ctxt : subctxt) {
      try {
        ctxt.startMonitoring();
      } catch (@OsUntrusted IOException e) {
        LOG.warn("startMonitoring failed: " + ctxt.getContextName(), e);
      }
    }
  }

  @InterfaceAudience.Private
  @Override
  public void stopMonitoring(@OsUntrusted CompositeContext this) {
    for (@OsUntrusted MetricsContext ctxt : subctxt) {
      ctxt.stopMonitoring();
    }
  }

  /**
   * Return true if all subcontexts are monitoring.
   */
  @InterfaceAudience.Private
  @Override
  public @OsUntrusted boolean isMonitoring(@OsUntrusted CompositeContext this) {
    @OsUntrusted
    boolean ret = true;
    for (@OsUntrusted MetricsContext ctxt : subctxt) {
      ret &= ctxt.isMonitoring();
    }
    return ret;
  }

  @InterfaceAudience.Private
  @Override
  public void close(@OsUntrusted CompositeContext this) {
    for (@OsUntrusted MetricsContext ctxt : subctxt) {
      ctxt.close();
    }
  }

  @InterfaceAudience.Private
  @Override
  public void registerUpdater(@OsUntrusted CompositeContext this, @OsUntrusted Updater updater) {
    for (@OsUntrusted MetricsContext ctxt : subctxt) {
      ctxt.registerUpdater(updater);
    }
  }

  @InterfaceAudience.Private
  @Override
  public void unregisterUpdater(@OsUntrusted CompositeContext this, @OsUntrusted Updater updater) {
    for (@OsUntrusted MetricsContext ctxt : subctxt) {
      ctxt.unregisterUpdater(updater);
    }
  }

  private static class MetricsRecordDelegator implements @OsUntrusted InvocationHandler {
    private static final @OsUntrusted Method m_getRecordName = initMethod();
    private static @OsUntrusted Method initMethod() {
      try {
        return MetricsRecord.class.getMethod("getRecordName", new @OsUntrusted Class @OsUntrusted [0]);
      } catch (@OsUntrusted Exception e) {
        throw new @OsUntrusted RuntimeException("Internal error", e);
      }
    }

    private final @OsUntrusted String recordName;
    private final @OsUntrusted ArrayList<@OsUntrusted MetricsRecord> subrecs;

    @OsUntrusted
    MetricsRecordDelegator(@OsUntrusted String recordName, @OsUntrusted ArrayList<@OsUntrusted MetricsContext> ctxts) {
      this.recordName = recordName;
      this.subrecs = new @OsUntrusted ArrayList<@OsUntrusted MetricsRecord>(ctxts.size());
      for (@OsUntrusted MetricsContext ctxt : ctxts) {
        subrecs.add(ctxt.createRecord(recordName));
      }
    }

    @Override
    public @OsUntrusted Object invoke(CompositeContext.@OsUntrusted MetricsRecordDelegator this, @OsUntrusted Object p, @OsUntrusted Method m, @OsUntrusted Object @OsUntrusted [] args) throws Throwable {
      if (m_getRecordName.equals(m)) {
        return recordName;
      }
      assert Void.TYPE.equals(m.getReturnType());
      for (@OsUntrusted MetricsRecord rec : subrecs) {
        m.invoke(rec, args);
      }
      return null;
    }
  }

}
