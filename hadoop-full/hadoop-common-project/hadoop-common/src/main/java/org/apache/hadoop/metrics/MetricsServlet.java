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
package org.apache.hadoop.metrics;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap;
import org.mortbay.util.ajax.JSON;
import org.mortbay.util.ajax.JSON.Output;

/**
 * A servlet to print out metrics data.  By default, the servlet returns a 
 * textual representation (no promises are made for parseability), and
 * users can use "?format=json" for parseable output.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MetricsServlet extends @OsUntrusted HttpServlet {
  
  /**
   * A helper class to hold a TagMap and MetricMap.
   */
  static class TagsMetricsPair implements JSON.@OsUntrusted Convertible {
    final @OsUntrusted TagMap tagMap;
    final @OsUntrusted MetricMap metricMap;
    
    public @OsUntrusted TagsMetricsPair(@OsUntrusted TagMap tagMap, @OsUntrusted MetricMap metricMap) {
      this.tagMap = tagMap;
      this.metricMap = metricMap;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void fromJSON(MetricsServlet.@OsUntrusted TagsMetricsPair this, @OsUntrusted Map map) {
      throw new @OsUntrusted UnsupportedOperationException();
    }

    /** Converts to JSON by providing an array. */
    @Override
    public void toJSON(MetricsServlet.@OsUntrusted TagsMetricsPair this, @OsUntrusted Output out) {
      out.add(new @OsUntrusted Object @OsUntrusted [] { tagMap, metricMap });
    }
  }
  
  /**
   * Collects all metric data, and returns a map:
   *   contextName -> recordName -> [ (tag->tagValue), (metric->metricValue) ].
   * The values are either String or Number.  The final value is implemented
   * as a list of TagsMetricsPair.
   */
   @OsUntrusted
   Map<@OsUntrusted String, @OsUntrusted Map<@OsUntrusted String, @OsUntrusted List<@OsUntrusted TagsMetricsPair>>> makeMap(
       @OsUntrusted MetricsServlet this, @OsUntrusted
       Collection<@OsUntrusted MetricsContext> contexts) throws IOException {
    @OsUntrusted
    Map<@OsUntrusted String, @OsUntrusted Map<@OsUntrusted String, @OsUntrusted List<@OsUntrusted TagsMetricsPair>>> map = 
      new @OsUntrusted TreeMap<@OsUntrusted String, @OsUntrusted Map<@OsUntrusted String, @OsUntrusted List<@OsUntrusted TagsMetricsPair>>>();

    for (@OsUntrusted MetricsContext context : contexts) {
      @OsUntrusted
      Map<@OsUntrusted String, @OsUntrusted List<@OsUntrusted TagsMetricsPair>> records = 
        new @OsUntrusted TreeMap<@OsUntrusted String, @OsUntrusted List<@OsUntrusted TagsMetricsPair>>();
      map.put(context.getContextName(), records);
    
      for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted Collection<@OsUntrusted OutputRecord>> r : 
          context.getAllRecords().entrySet()) {
        @OsUntrusted
        List<@OsUntrusted TagsMetricsPair> metricsAndTags = 
          new @OsUntrusted ArrayList<@OsUntrusted TagsMetricsPair>();
        records.put(r.getKey(), metricsAndTags);
        for (@OsUntrusted OutputRecord outputRecord : r.getValue()) {
          @OsUntrusted
          TagMap tagMap = outputRecord.getTagsCopy();
          @OsUntrusted
          MetricMap metricMap = outputRecord.getMetricsCopy();
          metricsAndTags.add(new @OsUntrusted TagsMetricsPair(tagMap, metricMap));
        }
      }
    }
    return map;
  }
  
  @Override
  public void doGet(@OsUntrusted MetricsServlet this, @OsUntrusted HttpServletRequest request, @OsUntrusted HttpServletResponse response)
      throws ServletException, IOException {

    if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(),
                                                   request, response)) {
      return;
    }

    @OsUntrusted
    String format = request.getParameter("format");
    @OsUntrusted
    Collection<@OsUntrusted MetricsContext> allContexts = 
      ContextFactory.getFactory().getAllContexts();
    if ("json".equals(format)) {
      response.setContentType("application/json; charset=utf-8");
      @OsUntrusted
      PrintWriter out = response.getWriter();
      try {
        // Uses Jetty's built-in JSON support to convert the map into JSON.
        out.print(new @OsUntrusted JSON().toJSON(makeMap(allContexts)));
      } finally {
        out.close();
      }
    } else {
      @OsUntrusted
      PrintWriter out = response.getWriter();
      try {
        printMap(out, makeMap(allContexts));
      } finally {
        out.close();
      }
    }
  }
  
  /**
   * Prints metrics data in a multi-line text form.
   */
  void printMap(@OsUntrusted MetricsServlet this, @OsUntrusted PrintWriter out, @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Map<@OsUntrusted String, @OsUntrusted List<@OsUntrusted TagsMetricsPair>>> map) {
    for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted Map<@OsUntrusted String, @OsUntrusted List<@OsUntrusted TagsMetricsPair>>> context : map.entrySet()) {
      out.print(context.getKey());
      out.print("\n");
      for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted List<@OsUntrusted TagsMetricsPair>> record : context.getValue().entrySet()) {
        indent(out, 1);
        out.print(record.getKey());
        out.print("\n");
        for (@OsUntrusted TagsMetricsPair pair : record.getValue()) {
          indent(out, 2);
          // Prints tag values in the form "{key=value,key=value}:"
          out.print("{");
          @OsUntrusted
          boolean first = true;
          for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted Object> tagValue : pair.tagMap.entrySet()) {
            if (first) {
              first = false;
            } else {
              out.print(",");
            }
            out.print(tagValue.getKey());
            out.print("=");
            out.print(tagValue.getValue().toString());
          }
          out.print("}:\n");
          
          // Now print metric values, one per line
          for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted Number> metricValue : 
              pair.metricMap.entrySet()) {
            indent(out, 3);
            out.print(metricValue.getKey());
            out.print("=");
            out.print(metricValue.getValue().toString());
            out.print("\n");
          }
        }
      }
    }    
  }
  
  private void indent(@OsUntrusted MetricsServlet this, @OsUntrusted PrintWriter out, @OsUntrusted int indent) {
    for (@OsUntrusted int i = 0; i < indent; ++i) {
      out.append("  ");
    }
  }
}
