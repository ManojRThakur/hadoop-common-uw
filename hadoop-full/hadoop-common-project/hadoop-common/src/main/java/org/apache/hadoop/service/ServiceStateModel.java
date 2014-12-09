/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.service;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Implements the service state model.
 */
@Public
@Evolving
public class ServiceStateModel {

  /**
   * Map of all valid state transitions
   * [current] [proposed1, proposed2, ...]
   */
  private static final @OsUntrusted boolean @OsUntrusted [] @OsUntrusted [] statemap =
    new boolean[] @OsUntrusted [] {
      //                uninited inited started stopped
      /* uninited  */   {false, true,  false,  true},
      /* inited    */   {false, true,  true,   true},
      /* started   */   {false, false, true,   true},
      /* stopped   */   {false, false, false,  true},
    };

  /**
   * The state of the service
   */
  private volatile Service.@OsUntrusted STATE state;

  /**
   * The name of the service: used in exceptions
   */
  private @OsUntrusted String name;

  /**
   * Create the service state model in the {@link Service.STATE#NOTINITED}
   * state.
   */
  public @OsUntrusted ServiceStateModel(@OsUntrusted String name) {
    this(name, Service.STATE.NOTINITED);
  }

  /**
   * Create a service state model instance in the chosen state
   * @param state the starting state
   */
  public @OsUntrusted ServiceStateModel(@OsUntrusted String name, Service. @OsUntrusted STATE state) {
    this.state = state;
    this.name = name;
  }

  /**
   * Query the service state. This is a non-blocking operation.
   * @return the state
   */
  public Service.@OsUntrusted STATE getState(@OsUntrusted ServiceStateModel this) {
    return state;
  }

  /**
   * Query that the state is in a specific state
   * @param proposed proposed new state
   * @return the state
   */
  public @OsUntrusted boolean isInState(@OsUntrusted ServiceStateModel this, Service. @OsUntrusted STATE proposed) {
    return state.equals(proposed);
  }

  /**
   * Verify that that a service is in a given state.
   * @param expectedState the desired state
   * @throws ServiceStateException if the service state is different from
   * the desired state
   */
  public void ensureCurrentState(@OsUntrusted ServiceStateModel this, Service. @OsUntrusted STATE expectedState) {
    if (state != expectedState) {
      throw new @OsUntrusted ServiceStateException(name+ ": for this operation, the " +
                                      "current service state must be "
                                      + expectedState
                                      + " instead of " + state);
    }
  }

  /**
   * Enter a state -thread safe.
   *
   * @param proposed proposed new state
   * @return the original state
   * @throws ServiceStateException if the transition is not permitted
   */
  public synchronized Service.@OsUntrusted STATE enterState(@OsUntrusted ServiceStateModel this, Service.@OsUntrusted STATE proposed) {
    checkStateTransition(name, state, proposed);
    Service.@OsUntrusted STATE oldState = state;
    //atomic write of the new state
    state = proposed;
    return oldState;
  }

  /**
   * Check that a state tansition is valid and
   * throw an exception if not
   * @param name name of the service (can be null)
   * @param state current state
   * @param proposed proposed new state
   */
  public static void checkStateTransition(@OsUntrusted String name,
                                          Service.@OsUntrusted STATE state,
                                          Service.@OsUntrusted STATE proposed) {
    if (!isValidStateTransition(state, proposed)) {
      throw new @OsUntrusted ServiceStateException(name + " cannot enter state "
                                      + proposed + " from state " + state);
    }
  }

  /**
   * Is a state transition valid?
   * There are no checks for current==proposed
   * as that is considered a non-transition.
   *
   * using an array kills off all branch misprediction costs, at the expense
   * of cache line misses.
   *
   * @param current current state
   * @param proposed proposed new state
   * @return true if the transition to a new state is valid
   */
  public static @OsUntrusted boolean isValidStateTransition(Service.@OsUntrusted STATE current,
                                               Service.@OsUntrusted STATE proposed) {
    @OsUntrusted
    boolean @OsUntrusted [] row = statemap[current.getValue()];
    return row[proposed.getValue()];
  }

  /**
   * return the state text as the toString() value
   * @return the current state's description
   */
  @Override
  public @OsUntrusted String toString(@OsUntrusted ServiceStateModel this) {
    return (name.isEmpty() ? "" : ((name) + ": "))
            + state.toString();
  }

}
