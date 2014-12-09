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

package org.apache.hadoop.metrics2.filter;

import ostrusted.quals.OsUntrusted;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Maps;
import org.apache.commons.configuration.SubsetConfiguration;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsTag;

/**
 * Base class for pattern based filters
 */
@InterfaceAudience.Private
public abstract class AbstractPatternFilter extends @OsUntrusted MetricsFilter {
  protected static final @OsUntrusted String INCLUDE_KEY = "include";
  protected static final @OsUntrusted String EXCLUDE_KEY = "exclude";
  protected static final @OsUntrusted String INCLUDE_TAGS_KEY = "include.tags";
  protected static final @OsUntrusted String EXCLUDE_TAGS_KEY = "exclude.tags";

  private @OsUntrusted Pattern includePattern;
  private @OsUntrusted Pattern excludePattern;
  private final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Pattern> includeTagPatterns;
  private final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted Pattern> excludeTagPatterns;
  private final @OsUntrusted Pattern tagPattern = Pattern.compile("^(\\w+):(.*)");

  @OsUntrusted
  AbstractPatternFilter() {
    includeTagPatterns = Maps.newHashMap();
    excludeTagPatterns = Maps.newHashMap();
  }

  @Override
  public void init(@OsUntrusted AbstractPatternFilter this, @OsUntrusted SubsetConfiguration conf) {
    @OsUntrusted
    String patternString = conf.getString(INCLUDE_KEY);
    if (patternString != null && !patternString.isEmpty()) {
      setIncludePattern(compile(patternString));
    }
    patternString = conf.getString(EXCLUDE_KEY);
    if (patternString != null && !patternString.isEmpty()) {
      setExcludePattern(compile(patternString));
    }
    @OsUntrusted
    String @OsUntrusted [] patternStrings = conf.getStringArray(INCLUDE_TAGS_KEY);
    if (patternStrings != null && patternStrings.length != 0) {
      for (@OsUntrusted String pstr : patternStrings) {
        @OsUntrusted
        Matcher matcher = tagPattern.matcher(pstr);
        if (!matcher.matches()) {
          throw new @OsUntrusted MetricsException("Illegal tag pattern: "+ pstr);
        }
        setIncludeTagPattern(matcher.group(1), compile(matcher.group(2)));
      }
    }
    patternStrings = conf.getStringArray(EXCLUDE_TAGS_KEY);
    if (patternStrings != null && patternStrings.length != 0) {
      for (@OsUntrusted String pstr : patternStrings) {
        @OsUntrusted
        Matcher matcher = tagPattern.matcher(pstr);
        if (!matcher.matches()) {
          throw new @OsUntrusted MetricsException("Illegal tag pattern: "+ pstr);
        }
        setExcludeTagPattern(matcher.group(1), compile(matcher.group(2)));
      }
    }
  }

  void setIncludePattern(@OsUntrusted AbstractPatternFilter this, @OsUntrusted Pattern includePattern) {
    this.includePattern = includePattern;
  }

  void setExcludePattern(@OsUntrusted AbstractPatternFilter this, @OsUntrusted Pattern excludePattern) {
    this.excludePattern = excludePattern;
  }

  void setIncludeTagPattern(@OsUntrusted AbstractPatternFilter this, @OsUntrusted String name, @OsUntrusted Pattern pattern) {
    includeTagPatterns.put(name, pattern);
  }

  void setExcludeTagPattern(@OsUntrusted AbstractPatternFilter this, @OsUntrusted String name, @OsUntrusted Pattern pattern) {
    excludeTagPatterns.put(name, pattern);
  }

  @Override
  public @OsUntrusted boolean accepts(@OsUntrusted AbstractPatternFilter this, @OsUntrusted MetricsTag tag) {
    // Accept if whitelisted
    @OsUntrusted
    Pattern ipat = includeTagPatterns.get(tag.name());
    if (ipat != null && ipat.matcher(tag.value()).matches()) {
      return true;
    }
    // Reject if blacklisted
    @OsUntrusted
    Pattern epat = excludeTagPatterns.get(tag.name());
    if (epat != null && epat.matcher(tag.value()).matches()) {
      return false;
    }
    // Reject if no match in whitelist only mode
    if (ipat != null && epat == null) {
      return false;
    }
    return true;
  }

  @Override
  public @OsUntrusted boolean accepts(@OsUntrusted AbstractPatternFilter this, @OsUntrusted Iterable<@OsUntrusted MetricsTag> tags) {
    // Accept if any include tag pattern matches
    for (@OsUntrusted MetricsTag t : tags) {
      @OsUntrusted
      Pattern pat = includeTagPatterns.get(t.name());
      if (pat != null && pat.matcher(t.value()).matches()) {
        return true;
      }
    }
    // Reject if any exclude tag pattern matches
    for (@OsUntrusted MetricsTag t : tags) {
      @OsUntrusted
      Pattern pat = excludeTagPatterns.get(t.name());
      if (pat != null && pat.matcher(t.value()).matches()) {
        return false;
      }
    }
    // Reject if no match in whitelist only mode
    if (!includeTagPatterns.isEmpty() && excludeTagPatterns.isEmpty()) {
      return false;
    }
    return true;
  }

  @Override
  public @OsUntrusted boolean accepts(@OsUntrusted AbstractPatternFilter this, @OsUntrusted String name) {
    // Accept if whitelisted
    if (includePattern != null && includePattern.matcher(name).matches()) {
      return true;
    }
    // Reject if blacklisted
    if ((excludePattern != null && excludePattern.matcher(name).matches())) {
      return false;
    }
    // Reject if no match in whitelist only mode
    if (includePattern != null && excludePattern == null) {
      return false;
    }
    return true;
  }

  /**
   * Compile a string pattern in to a pattern object
   * @param s the string pattern to compile
   * @return the compiled pattern object
   */
  protected abstract @OsUntrusted Pattern compile(@OsUntrusted AbstractPatternFilter this, @OsUntrusted String s);
}
