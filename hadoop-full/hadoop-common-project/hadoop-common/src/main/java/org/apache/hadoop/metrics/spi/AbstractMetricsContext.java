/*
 * AbstractMetricsContext.java
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

package org.apache.hadoop.metrics.spi;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.Updater;

/**
 * The main class of the Service Provider Interface.  This class should be
 * extended in order to integrate the Metrics API with a specific metrics
 * client library. <p/>
 *
 * This class implements the internal table of metric data, and the timer
 * on which data is to be sent to the metrics system.  Subclasses must
 * override the abstract <code>emitRecord</code> method in order to transmit
 * the data. <p/>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AbstractMetricsContext implements @OsUntrusted MetricsContext {
    
  private @OsUntrusted int period = MetricsContext.DEFAULT_PERIOD;
  private @OsUntrusted Timer timer = null;
    
  private @OsUntrusted Set<@OsUntrusted Updater> updaters = new @OsUntrusted HashSet<@OsUntrusted Updater>(1);
  private volatile @OsUntrusted boolean isMonitoring = false;
    
  private @OsUntrusted ContextFactory factory = null;
  private @OsUntrusted String contextName = null;
    
  @InterfaceAudience.Private
  public static class TagMap extends @OsUntrusted TreeMap<@OsUntrusted String, @OsUntrusted Object> {
    private static final @OsUntrusted long serialVersionUID = 3546309335061952993L;
    @OsUntrusted
    TagMap() {
      super();
    }
    @OsUntrusted
    TagMap(@OsUntrusted TagMap orig) {
      super(orig);
    }
    /**
     * Returns true if this tagmap contains every tag in other.
     */
    public @OsUntrusted boolean containsAll(AbstractMetricsContext.@OsUntrusted TagMap this, @OsUntrusted TagMap other) {
      for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted Object> entry : other.entrySet()) {
        @OsUntrusted
        Object value = get(entry.getKey());
        if (value == null || !value.equals(entry.getValue())) {
          // either key does not exist here, or the value is different
          return false;
        }
      }
      return true;
    }
  }
  
  @InterfaceAudience.Private
  public static class MetricMap extends @OsUntrusted TreeMap<@OsUntrusted String, @OsUntrusted Number> {
    private static final @OsUntrusted long serialVersionUID = -7495051861141631609L;
    @OsUntrusted
    MetricMap() {
      super();
    }
    @OsUntrusted
    MetricMap(@OsUntrusted MetricMap orig) {
      super(orig);
    }
  }
            
  static class RecordMap extends @OsUntrusted HashMap<@OsUntrusted TagMap, @OsUntrusted MetricMap> {
    private static final @OsUntrusted long serialVersionUID = 259835619700264611L;
  }
    
  private @OsUntrusted Map<@OsUntrusted String, @OsUntrusted RecordMap> bufferedData = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted RecordMap>();
    

  /**
   * Creates a new instance of AbstractMetricsContext
   */
  protected @OsUntrusted AbstractMetricsContext() {
  }
    
  /**
   * Initializes the context.
   */
  @Override
  public void init(@OsUntrusted AbstractMetricsContext this, @OsUntrusted String contextName, @OsUntrusted ContextFactory factory) 
  {
    this.contextName = contextName;
    this.factory = factory;
  }
    
  /**
   * Convenience method for subclasses to access factory attributes.
   */
  protected @OsUntrusted String getAttribute(@OsUntrusted AbstractMetricsContext this, @OsUntrusted String attributeName) {
    @OsUntrusted
    String factoryAttribute = contextName + "." + attributeName;
    return (@OsUntrusted String) factory.getAttribute(factoryAttribute);  
  }
    
  /**
   * Returns an attribute-value map derived from the factory attributes
   * by finding all factory attributes that begin with 
   * <i>contextName</i>.<i>tableName</i>.  The returned map consists of
   * those attributes with the contextName and tableName stripped off.
   */
  protected @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> getAttributeTable(@OsUntrusted AbstractMetricsContext this, @OsUntrusted String tableName) {
    @OsUntrusted
    String prefix = contextName + "." + tableName + ".";
    @OsUntrusted
    Map<@OsUntrusted String, @OsUntrusted String> result = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted String>();
    for (@OsUntrusted String attributeName : factory.getAttributeNames()) {
      if (attributeName.startsWith(prefix)) {
        @OsUntrusted
        String name = attributeName.substring(prefix.length());
        @OsUntrusted
        String value = (@OsUntrusted String) factory.getAttribute(attributeName);
        result.put(name, value);
      }
    }
    return result;
  }
    
  /**
   * Returns the context name.
   */
  @Override
  public @OsUntrusted String getContextName(@OsUntrusted AbstractMetricsContext this) {
    return contextName;
  }
    
  /**
   * Returns the factory by which this context was created.
   */
  public @OsUntrusted ContextFactory getContextFactory(@OsUntrusted AbstractMetricsContext this) {
    return factory;
  }
    
  /**
   * Starts or restarts monitoring, the emitting of metrics records.
   */
  @Override
  public synchronized void startMonitoring(@OsUntrusted AbstractMetricsContext this)
    throws IOException {
    if (!isMonitoring) {
      startTimer();
      isMonitoring = true;
    }
  }
    
  /**
   * Stops monitoring.  This does not free buffered data. 
   * @see #close()
   */
  @Override
  public synchronized void stopMonitoring(@OsUntrusted AbstractMetricsContext this) {
    if (isMonitoring) {
      stopTimer();
      isMonitoring = false;
    }
  }
    
  /**
   * Returns true if monitoring is currently in progress.
   */
  @Override
  public @OsUntrusted boolean isMonitoring(@OsUntrusted AbstractMetricsContext this) {
    return isMonitoring;
  }
    
  /**
   * Stops monitoring and frees buffered data, returning this
   * object to its initial state.  
   */
  @Override
  public synchronized void close(@OsUntrusted AbstractMetricsContext this) {
    stopMonitoring();
    clearUpdaters();
  } 
    
  /**
   * Creates a new AbstractMetricsRecord instance with the given <code>recordName</code>.
   * Throws an exception if the metrics implementation is configured with a fixed
   * set of record names and <code>recordName</code> is not in that set.
   * 
   * @param recordName the name of the record
   * @throws MetricsException if recordName conflicts with configuration data
   */
  @Override
  public final synchronized @OsUntrusted MetricsRecord createRecord(@OsUntrusted AbstractMetricsContext this, @OsUntrusted String recordName) {
    if (bufferedData.get(recordName) == null) {
      bufferedData.put(recordName, new @OsUntrusted RecordMap());
    }
    return newRecord(recordName);
  }
    
  /**
   * Subclasses should override this if they subclass MetricsRecordImpl.
   * @param recordName the name of the record
   * @return newly created instance of MetricsRecordImpl or subclass
   */
  protected @OsUntrusted MetricsRecord newRecord(@OsUntrusted AbstractMetricsContext this, @OsUntrusted String recordName) {
    return new @OsUntrusted MetricsRecordImpl(recordName, this);
  }
    
  /**
   * Registers a callback to be called at time intervals determined by
   * the configuration.
   *
   * @param updater object to be run periodically; it should update
   * some metrics records 
   */
  @Override
  public synchronized void registerUpdater(@OsUntrusted AbstractMetricsContext this, final @OsUntrusted Updater updater) {
    if (!updaters.contains(updater)) {
      updaters.add(updater);
    }
  }
    
  /**
   * Removes a callback, if it exists.
   *
   * @param updater object to be removed from the callback list
   */
  @Override
  public synchronized void unregisterUpdater(@OsUntrusted AbstractMetricsContext this, @OsUntrusted Updater updater) {
    updaters.remove(updater);
  }
    
  private synchronized void clearUpdaters(@OsUntrusted AbstractMetricsContext this) {
    updaters.clear();
  }
    
  /**
   * Starts timer if it is not already started
   */
  private synchronized void startTimer(@OsUntrusted AbstractMetricsContext this) {
    if (timer == null) {
      timer = new @OsUntrusted Timer("Timer thread for monitoring " + getContextName(), 
                        true);
      @OsUntrusted
      TimerTask task = new @OsUntrusted TimerTask() {
          @Override
          public void run() {
            try {
              timerEvent();
            } catch (@OsUntrusted IOException ioe) {
              ioe.printStackTrace();
            }
          }
        };
      @OsUntrusted
      long millis = period * 1000;
      timer.scheduleAtFixedRate(task, millis, millis);
    }
  }
    
  /**
   * Stops timer if it is running
   */
  private synchronized void stopTimer(@OsUntrusted AbstractMetricsContext this) {
    if (timer != null) {
      timer.cancel();
      timer = null;
    }
  }
    
  /**
   * Timer callback.
   */
  private void timerEvent(@OsUntrusted AbstractMetricsContext this) throws IOException {
    if (isMonitoring) {
      @OsUntrusted
      Collection<@OsUntrusted Updater> myUpdaters;
      synchronized (this) {
        myUpdaters = new @OsUntrusted ArrayList<@OsUntrusted Updater>(updaters);
      }     
      // Run all the registered updates without holding a lock
      // on this context
      for (@OsUntrusted Updater updater : myUpdaters) {
        try {
          updater.doUpdates(this);
        } catch (@OsUntrusted Throwable throwable) {
          throwable.printStackTrace();
        }
      }
      emitRecords();
    }
  }
    
  /**
   *  Emits the records.
   */
  private synchronized void emitRecords(@OsUntrusted AbstractMetricsContext this) throws IOException {
    for (@OsUntrusted String recordName : bufferedData.keySet()) {
      @OsUntrusted
      RecordMap recordMap = bufferedData.get(recordName);
      synchronized (recordMap) {
        @OsUntrusted
        Set<@OsUntrusted Entry<@OsUntrusted TagMap, @OsUntrusted MetricMap>> entrySet = recordMap.entrySet ();
        for (@OsUntrusted Entry<@OsUntrusted TagMap, @OsUntrusted MetricMap> entry : entrySet) {
          @OsUntrusted
          OutputRecord outRec = new @OsUntrusted OutputRecord(entry.getKey(), entry.getValue());
          emitRecord(contextName, recordName, outRec);
        }
      }
    }
    flush();
  }
  
  /**
   * Retrieves all the records managed by this MetricsContext.
   * Useful for monitoring systems that are polling-based.
   * @return A non-null collection of all monitoring records.
   */
  @Override
  public synchronized @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Collection<@OsUntrusted OutputRecord>> getAllRecords(@OsUntrusted AbstractMetricsContext this) {
    @OsUntrusted
    Map<@OsUntrusted String, @OsUntrusted Collection<@OsUntrusted OutputRecord>> out = new @OsUntrusted TreeMap<@OsUntrusted String, @OsUntrusted Collection<@OsUntrusted OutputRecord>>();
    for (@OsUntrusted String recordName : bufferedData.keySet()) {
      @OsUntrusted
      RecordMap recordMap = bufferedData.get(recordName);
      synchronized (recordMap) {
        @OsUntrusted
        List<@OsUntrusted OutputRecord> records = new @OsUntrusted ArrayList<@OsUntrusted OutputRecord>();
        @OsUntrusted
        Set<@OsUntrusted Entry<@OsUntrusted TagMap, @OsUntrusted MetricMap>> entrySet = recordMap.entrySet();
        for (@OsUntrusted Entry<@OsUntrusted TagMap, @OsUntrusted MetricMap> entry : entrySet) {
          @OsUntrusted
          OutputRecord outRec = new @OsUntrusted OutputRecord(entry.getKey(), entry.getValue());
          records.add(outRec);
        }
        out.put(recordName, records);
      }
    }
    return out;
  }

  /**
   * Sends a record to the metrics system.
   */
  protected abstract void emitRecord(@OsUntrusted AbstractMetricsContext this, @OsUntrusted String contextName, @OsUntrusted String recordName, 
                                     @OsUntrusted
                                     OutputRecord outRec) throws IOException;
    
  /**
   * Called each period after all records have been emitted, this method does nothing.
   * Subclasses may override it in order to perform some kind of flush.
   */
  protected void flush(@OsUntrusted AbstractMetricsContext this) throws IOException {
  }
    
  /**
   * Called by MetricsRecordImpl.update().  Creates or updates a row in
   * the internal table of metric data.
   */
  protected void update(@OsUntrusted AbstractMetricsContext this, @OsUntrusted MetricsRecordImpl record) {
    @OsUntrusted
    String recordName = record.getRecordName();
    @OsUntrusted
    TagMap tagTable = record.getTagTable();
    @OsUntrusted
    Map<@OsUntrusted String, @OsUntrusted MetricValue> metricUpdates = record.getMetricTable();
        
    @OsUntrusted
    RecordMap recordMap = getRecordMap(recordName);
    synchronized (recordMap) {
      @OsUntrusted
      MetricMap metricMap = recordMap.get(tagTable);
      if (metricMap == null) {
        metricMap = new @OsUntrusted MetricMap();
        @OsUntrusted
        TagMap tagMap = new @OsUntrusted TagMap(tagTable); // clone tags
        recordMap.put(tagMap, metricMap);
      }

      @OsUntrusted
      Set<@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted MetricValue>> entrySet = metricUpdates.entrySet();
      for (@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted MetricValue> entry : entrySet) {
        @OsUntrusted
        String metricName = entry.getKey ();
        @OsUntrusted
        MetricValue updateValue = entry.getValue ();
        @OsUntrusted
        Number updateNumber = updateValue.getNumber();
        @OsUntrusted
        Number currentNumber = metricMap.get(metricName);
        if (currentNumber == null || updateValue.isAbsolute()) {
          metricMap.put(metricName, updateNumber);
        }
        else {
          @OsUntrusted
          Number newNumber = sum(updateNumber, currentNumber);
          metricMap.put(metricName, newNumber);
        }
      }
    }
  }
    
  private synchronized @OsUntrusted RecordMap getRecordMap(@OsUntrusted AbstractMetricsContext this, @OsUntrusted String recordName) {
    return bufferedData.get(recordName);
  }
    
  /**
   * Adds two numbers, coercing the second to the type of the first.
   *
   */
  private @OsUntrusted Number sum(@OsUntrusted AbstractMetricsContext this, @OsUntrusted Number a, @OsUntrusted Number b) {
    if (a instanceof @OsUntrusted Integer) {
      return Integer.valueOf(a.intValue() + b.intValue());
    }
    else if (a instanceof @OsUntrusted Float) {
      return new @OsUntrusted Float(a.floatValue() + b.floatValue());
    }
    else if (a instanceof @OsUntrusted Short) {
      return Short.valueOf((@OsUntrusted short)(a.shortValue() + b.shortValue()));
    }
    else if (a instanceof @OsUntrusted Byte) {
      return Byte.valueOf((@OsUntrusted byte)(a.byteValue() + b.byteValue()));
    }
    else if (a instanceof @OsUntrusted Long) {
      return Long.valueOf((a.longValue() + b.longValue()));
    }
    else {
      // should never happen
      throw new @OsUntrusted MetricsException("Invalid number type");
    }
            
  }
    
  /**
   * Called by MetricsRecordImpl.remove().  Removes all matching rows in
   * the internal table of metric data.  A row matches if it has the same
   * tag names and values as record, but it may also have additional
   * tags.
   */    
  protected void remove(@OsUntrusted AbstractMetricsContext this, @OsUntrusted MetricsRecordImpl record) {
    @OsUntrusted
    String recordName = record.getRecordName();
    @OsUntrusted
    TagMap tagTable = record.getTagTable();
        
    @OsUntrusted
    RecordMap recordMap = getRecordMap(recordName);
    synchronized (recordMap) {
      @OsUntrusted
      Iterator<@OsUntrusted TagMap> it = recordMap.keySet().iterator();
      while (it.hasNext()) {
        @OsUntrusted
        TagMap rowTags = it.next();
        if (rowTags.containsAll(tagTable)) {
          it.remove();
        }
      }
    }
  }
    
  /**
   * Returns the timer period.
   */
  @Override
  public @OsUntrusted int getPeriod(@OsUntrusted AbstractMetricsContext this) {
    return period;
  }
    
  /**
   * Sets the timer period
   */
  protected void setPeriod(@OsUntrusted AbstractMetricsContext this, @OsUntrusted int period) {
    this.period = period;
  }
  
  /**
   * If a period is set in the attribute passed in, override
   * the default with it.
   */
  protected void parseAndSetPeriod(@OsUntrusted AbstractMetricsContext this, @OsUntrusted String attributeName) {
    @OsUntrusted
    String periodStr = getAttribute(attributeName);
    if (periodStr != null) {
      @OsUntrusted
      int period = 0;
      try {
        period = Integer.parseInt(periodStr);
      } catch (@OsUntrusted NumberFormatException nfe) {
      }
      if (period <= 0) {
        throw new @OsUntrusted MetricsException("Invalid period: " + periodStr);
      }
      setPeriod(period);
    }
  }
}
