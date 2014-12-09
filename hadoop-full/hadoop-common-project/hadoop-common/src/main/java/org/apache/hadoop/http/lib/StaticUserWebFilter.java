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
package org.apache.hadoop.http.lib;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;

import javax.servlet.Filter;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER;
import static org.apache.hadoop.fs.CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER;

/**
 * Provides a servlet filter that pretends to authenticate a fake user (Dr.Who)
 * so that the web UI is usable for a secure cluster without authentication.
 */
public class StaticUserWebFilter extends @OsUntrusted FilterInitializer {
  static final @OsUntrusted String DEPRECATED_UGI_KEY = "dfs.web.ugi";

  private static final @OsUntrusted Log LOG = LogFactory.getLog(StaticUserWebFilter.class);

  static class User implements @OsUntrusted Principal {
    private final @OsUntrusted String name;
    public @OsUntrusted User(@OsUntrusted String name) {
      this.name = name;
    }
    @Override
    public @OsUntrusted String getName(StaticUserWebFilter.@OsUntrusted User this) {
      return name;
    }
    @Override
    public @OsUntrusted int hashCode(StaticUserWebFilter.@OsUntrusted User this) {
      return name.hashCode();
    }
    @Override
    public @OsUntrusted boolean equals(StaticUserWebFilter.@OsUntrusted User this, @OsUntrusted Object other) {
      if (other == this) {
        return true;
      } else if (other == null || other.getClass() != getClass()) {
        return false;
      }
      return ((@OsUntrusted User) other).name.equals(name);
    }
    @Override
    public @OsUntrusted String toString(StaticUserWebFilter.@OsUntrusted User this) {
      return name;
    }    
  }

  public static class StaticUserFilter implements @OsUntrusted Filter {
    private @OsUntrusted User user;
    private @OsUntrusted String username;

    @Override
    public void destroy(StaticUserWebFilter.@OsUntrusted StaticUserFilter this) {
      // NOTHING
    }

    @Override
    public void doFilter(StaticUserWebFilter.@OsUntrusted StaticUserFilter this, @OsUntrusted ServletRequest request, @OsUntrusted ServletResponse response,
                         @OsUntrusted
                         FilterChain chain
                         ) throws IOException, ServletException {
      @OsUntrusted
      HttpServletRequest httpRequest = (@OsUntrusted HttpServletRequest) request;
      // if the user is already authenticated, don't override it
      if (httpRequest.getRemoteUser() != null) {
        chain.doFilter(request, response);
      } else {
        @OsUntrusted
        HttpServletRequestWrapper wrapper = 
            new @OsUntrusted HttpServletRequestWrapper(httpRequest) {
          @Override
          public @OsUntrusted Principal getUserPrincipal() {
            return user;
          }
          @Override
          public @OsUntrusted String getRemoteUser() {
            return username;
          }
        };
        chain.doFilter(wrapper, response);
      }
    }

    @Override
    public void init(StaticUserWebFilter.@OsUntrusted StaticUserFilter this, @OsUntrusted FilterConfig conf) throws ServletException {
      this.username = conf.getInitParameter(HADOOP_HTTP_STATIC_USER);
      this.user = new @OsUntrusted User(username);
    }
    
  }

  @Override
  public void initFilter(@OsUntrusted StaticUserWebFilter this, @OsUntrusted FilterContainer container, @OsUntrusted Configuration conf) {
    @OsUntrusted
    HashMap<@OsUntrusted String, @OsUntrusted String> options = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted String>();
    
    @OsUntrusted
    String username = getUsernameFromConf(conf);
    options.put(HADOOP_HTTP_STATIC_USER, username);

    container.addFilter("static_user_filter", 
                        StaticUserFilter.class.getName(), 
                        options);
  }

  /**
   * Retrieve the static username from the configuration.
   */
  static @OsUntrusted String getUsernameFromConf(@OsUntrusted Configuration conf) {
    @OsUntrusted
    String oldStyleUgi = conf.get(DEPRECATED_UGI_KEY);
    if (oldStyleUgi != null) {
      // We can't use the normal configuration deprecation mechanism here
      // since we need to split out the username from the configured UGI.
      LOG.warn(DEPRECATED_UGI_KEY + " should not be used. Instead, use " + 
          HADOOP_HTTP_STATIC_USER + ".");
      @OsUntrusted
      String @OsUntrusted [] parts = oldStyleUgi.split(",");
      return parts[0];
    } else {
      return conf.get(HADOOP_HTTP_STATIC_USER,
        DEFAULT_HADOOP_HTTP_STATIC_USER);
    }
  }

}
