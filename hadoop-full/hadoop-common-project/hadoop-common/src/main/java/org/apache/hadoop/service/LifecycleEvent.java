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
package org.apache.hadoop.service;

import ostrusted.quals.OsUntrusted;
import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * A serializable lifecycle event: the time a state
 * transition occurred, and what state was entered.
 */
@Public
@Evolving
public class LifecycleEvent implements @OsUntrusted Serializable {
  
  private static final @OsUntrusted long serialVersionUID = 1648576996238247836L;

  /**
   * Local time in milliseconds when the event occurred
   */
  public @OsUntrusted long time;
  /**
   * new state
   */
  public Service.@OsUntrusted STATE state;
}
