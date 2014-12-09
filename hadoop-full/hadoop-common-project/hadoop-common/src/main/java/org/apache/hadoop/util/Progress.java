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

package org.apache.hadoop.util;

import ostrusted.quals.OsUntrusted;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** Utility to assist with generation of progress reports.  Applications build
 * a hierarchy of {@link Progress} instances, each modelling a phase of
 * execution.  The root is constructed with {@link #Progress()}.  Nodes for
 * sub-phases are created by calling {@link #addPhase()}.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class Progress {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(Progress.class);
  private @OsUntrusted String status = "";
  private @OsUntrusted float progress;
  private @OsUntrusted int currentPhase;
  private @OsUntrusted ArrayList<@OsUntrusted Progress> phases = new @OsUntrusted ArrayList<@OsUntrusted Progress>();
  private @OsUntrusted Progress parent;

  // Each phase can have different progress weightage. For example, in
  // Map Task, map phase accounts for 66.7% and sort phase for 33.3%.
  // User needs to give weightages as parameters to all phases(when adding
  // phases) in a Progress object, if he wants to give weightage to any of the
  // phases. So when nodes are added without specifying weightage, it means 
  // fixed weightage for all phases.
  private @OsUntrusted boolean fixedWeightageForAllPhases = false;
  private @OsUntrusted float progressPerPhase = 0.0f;
  private @OsUntrusted ArrayList<@OsUntrusted Float> progressWeightagesForPhases = new @OsUntrusted ArrayList<@OsUntrusted Float>();
  
  /** Creates a new root node. */
  public @OsUntrusted Progress() {}

  /** Adds a named node to the tree. */
  public @OsUntrusted Progress addPhase(@OsUntrusted Progress this, @OsUntrusted String status) {
    @OsUntrusted
    Progress phase = addPhase();
    phase.setStatus(status);
    return phase;
  }

  /** Adds a node to the tree. Gives equal weightage to all phases */
  public synchronized @OsUntrusted Progress addPhase(@OsUntrusted Progress this) {
    @OsUntrusted
    Progress phase = addNewPhase();
    // set equal weightage for all phases
    progressPerPhase = 1.0f / (@OsUntrusted float)phases.size();
    fixedWeightageForAllPhases = true;
    return phase;
  }
  
  /** Adds a new phase. Caller needs to set progress weightage */
  private synchronized @OsUntrusted Progress addNewPhase(@OsUntrusted Progress this) {
    @OsUntrusted
    Progress phase = new @OsUntrusted Progress();
    phases.add(phase);
    phase.setParent(this);
    return phase;
  }

  /** Adds a named node with a specified progress weightage to the tree. */
  public @OsUntrusted Progress addPhase(@OsUntrusted Progress this, @OsUntrusted String status, @OsUntrusted float weightage) {
    @OsUntrusted
    Progress phase = addPhase(weightage);
    phase.setStatus(status);

    return phase;
  }

  /** Adds a node with a specified progress weightage to the tree. */
  public synchronized @OsUntrusted Progress addPhase(@OsUntrusted Progress this, @OsUntrusted float weightage) {
    @OsUntrusted
    Progress phase = new @OsUntrusted Progress();
    progressWeightagesForPhases.add(weightage);
    phases.add(phase);
    phase.setParent(this);

    // Ensure that the sum of weightages does not cross 1.0
    @OsUntrusted
    float sum = 0;
    for (@OsUntrusted int i = 0; i < phases.size(); i++) {
      sum += progressWeightagesForPhases.get(i);
    }
    if (sum > 1.0) {
      LOG.warn("Sum of weightages can not be more than 1.0; But sum = " + sum);
    }

    return phase;
  }

  /** Adds n nodes to the tree. Gives equal weightage to all phases */
  public synchronized void addPhases(@OsUntrusted Progress this, @OsUntrusted int n) {
    for (@OsUntrusted int i = 0; i < n; i++) {
      addNewPhase();
    }
    // set equal weightage for all phases
    progressPerPhase = 1.0f / (@OsUntrusted float)phases.size();
    fixedWeightageForAllPhases = true;
  }

  /**
   * returns progress weightage of the given phase
   * @param phaseNum the phase number of the phase(child node) for which we need
   *                 progress weightage
   * @return returns the progress weightage of the specified phase
   */
  @OsUntrusted
  float getProgressWeightage(@OsUntrusted Progress this, @OsUntrusted int phaseNum) {
    if (fixedWeightageForAllPhases) {
      return progressPerPhase; // all phases are of equal weightage
    }
    return progressWeightagesForPhases.get(phaseNum);
  }

  synchronized @OsUntrusted Progress getParent(@OsUntrusted Progress this) { return parent; }
  synchronized void setParent(@OsUntrusted Progress this, @OsUntrusted Progress parent) { this.parent = parent; }
  
  /** Called during execution to move to the next phase at this level in the
   * tree. */
  public synchronized void startNextPhase(@OsUntrusted Progress this) {
    currentPhase++;
  }

  /** Returns the current sub-node executing. */
  public synchronized @OsUntrusted Progress phase(@OsUntrusted Progress this) {
    return phases.get(currentPhase);
  }

  /** Completes this node, moving the parent node to its next child. */
  public void complete(@OsUntrusted Progress this) {
    // we have to traverse up to our parent, so be careful about locking.
    @OsUntrusted
    Progress myParent;
    synchronized(this) {
      progress = 1.0f;
      myParent = parent;
    }
    if (myParent != null) {
      // this will synchronize on the parent, so we make sure we release
      // our lock before getting the parent's, since we're traversing 
      // against the normal traversal direction used by get() or toString().
      // We don't need transactional semantics, so we're OK doing this. 
      myParent.startNextPhase();
    }
  }

  /** Called during execution on a leaf node to set its progress. */
  public synchronized void set(@OsUntrusted Progress this, @OsUntrusted float progress) {
    this.progress = progress;
  }

  /** Returns the overall progress of the root. */
  // this method probably does not need to be synchronized as getInternal() is
  // synchronized and the node's parent never changes. Still, it doesn't hurt. 
  public synchronized @OsUntrusted float get(@OsUntrusted Progress this) {
    @OsUntrusted
    Progress node = this;
    while (node.getParent() != null) {                 // find the root
      node = parent;
    }
    return node.getInternal();
  }

  /**
   * Returns progress in this node. get() would give overall progress of the
   * root node(not just given current node).
   */
  public synchronized @OsUntrusted float getProgress(@OsUntrusted Progress this) {
    return getInternal();
  }
  
  /** Computes progress in this node. */
  private synchronized @OsUntrusted float getInternal(@OsUntrusted Progress this) {
    @OsUntrusted
    int phaseCount = phases.size();
    if (phaseCount != 0) {
      @OsUntrusted
      float subProgress = 0.0f;
      @OsUntrusted
      float progressFromCurrentPhase = 0.0f;
      if (currentPhase < phaseCount) {
        subProgress = phase().getInternal();
        progressFromCurrentPhase =
          getProgressWeightage(currentPhase) * subProgress;
      }
      
      @OsUntrusted
      float progressFromCompletedPhases = 0.0f;
      if (fixedWeightageForAllPhases) { // same progress weightage for each phase
        progressFromCompletedPhases = progressPerPhase * currentPhase;
      }
      else {
        for (@OsUntrusted int i = 0; i < currentPhase; i++) {
          // progress weightages of phases could be different. Add them
          progressFromCompletedPhases += getProgressWeightage(i);
        }
      }
      return  progressFromCompletedPhases + progressFromCurrentPhase;
    } else {
      return progress;
    }
  }

  public synchronized void setStatus(@OsUntrusted Progress this, @OsUntrusted String status) {
    this.status = status;
  }

  @Override
  public @OsUntrusted String toString(@OsUntrusted Progress this) {
    @OsUntrusted
    StringBuilder result = new @OsUntrusted StringBuilder();
    toString(result);
    return result.toString();
  }

  private synchronized void toString(@OsUntrusted Progress this, @OsUntrusted StringBuilder buffer) {
    buffer.append(status);
    if (phases.size() != 0 && currentPhase < phases.size()) {
      buffer.append(" > ");
      phase().toString(buffer);
    }
  }

}
