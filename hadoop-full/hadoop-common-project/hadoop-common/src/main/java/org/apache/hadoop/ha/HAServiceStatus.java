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
package org.apache.hadoop.ha;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;

@InterfaceAudience.Private
public class HAServiceStatus {
  private @OsUntrusted HAServiceState state;
  private @OsUntrusted boolean readyToBecomeActive;
  private @OsUntrusted String notReadyReason;
  
  public @OsUntrusted HAServiceStatus(@OsUntrusted HAServiceState state) {
    this.state = state;
  }

  public @OsUntrusted HAServiceState getState(@OsUntrusted HAServiceStatus this) {
    return state;
  }

  public @OsUntrusted HAServiceStatus setReadyToBecomeActive(@OsUntrusted HAServiceStatus this) {
    this.readyToBecomeActive = true;
    this.notReadyReason = null;
    return this;
  }
  
  public @OsUntrusted HAServiceStatus setNotReadyToBecomeActive(@OsUntrusted HAServiceStatus this, @OsUntrusted String reason) {
    this.readyToBecomeActive = false;
    this.notReadyReason = reason;
    return this;
  }

  public @OsUntrusted boolean isReadyToBecomeActive(@OsUntrusted HAServiceStatus this) {
    return readyToBecomeActive;
  }

  public @OsUntrusted String getNotReadyReason(@OsUntrusted HAServiceStatus this) {
    return notReadyReason;
  }
}
