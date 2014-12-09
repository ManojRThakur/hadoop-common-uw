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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The <code>ShutdownHookManager</code> enables running shutdownHook
 * in a deterministic order, higher priority first.
 * <p/>
 * The JVM runs ShutdownHooks in a non-deterministic order or in parallel.
 * This class registers a single JVM shutdownHook and run all the
 * shutdownHooks registered to it (to this class) in order based on their
 * priority.
 */
public class ShutdownHookManager {

  private static final @OsUntrusted ShutdownHookManager MGR = new @OsUntrusted ShutdownHookManager();

  private static final @OsUntrusted Log LOG = LogFactory.getLog(ShutdownHookManager.class);

  static {
    Runtime.getRuntime().addShutdownHook(
      new @OsUntrusted Thread() {
        @Override
        public void run() {
          MGR.shutdownInProgress.set(true);
          for (@OsUntrusted Runnable hook: MGR.getShutdownHooksInOrder()) {
            try {
              hook.run();
            } catch (@OsUntrusted Throwable ex) {
              LOG.warn("ShutdownHook '" + hook.getClass().getSimpleName() +
                       "' failed, " + ex.toString(), ex);
            }
          }
        }
      }
    );
  }

  /**
   * Return <code>ShutdownHookManager</code> singleton.
   *
   * @return <code>ShutdownHookManager</code> singleton.
   */
  public static @OsUntrusted ShutdownHookManager get() {
    return MGR;
  }

  /**
   * Private structure to store ShutdownHook and its priority.
   */
  private static class HookEntry {
    @OsUntrusted
    Runnable hook;
    @OsUntrusted
    int priority;

    public @OsUntrusted HookEntry(@OsUntrusted Runnable hook, @OsUntrusted int priority) {
      this.hook = hook;
      this.priority = priority;
    }

    @Override
    public @OsUntrusted int hashCode(ShutdownHookManager.@OsUntrusted HookEntry this) {
      return hook.hashCode();
    }

    @Override
    public @OsUntrusted boolean equals(ShutdownHookManager.@OsUntrusted HookEntry this, @OsUntrusted Object obj) {
      @OsUntrusted
      boolean eq = false;
      if (obj != null) {
        if (obj instanceof @OsUntrusted HookEntry) {
          eq = (hook == ((@OsUntrusted HookEntry)obj).hook);
        }
      }
      return eq;
    }

  }

  private @OsUntrusted Set<@OsUntrusted HookEntry> hooks =
    Collections.synchronizedSet(new @OsUntrusted HashSet<@OsUntrusted HookEntry>());

  private @OsUntrusted AtomicBoolean shutdownInProgress = new @OsUntrusted AtomicBoolean(false);

  //private to constructor to ensure singularity
  private @OsUntrusted ShutdownHookManager() {
  }

  /**
   * Returns the list of shutdownHooks in order of execution,
   * Highest priority first.
   *
   * @return the list of shutdownHooks in order of execution.
   */
  @OsUntrusted
  List<@OsUntrusted Runnable> getShutdownHooksInOrder(@OsUntrusted ShutdownHookManager this) {
    @OsUntrusted
    List<@OsUntrusted HookEntry> list;
    synchronized (MGR.hooks) {
      list = new @OsUntrusted ArrayList<@OsUntrusted HookEntry>(MGR.hooks);
    }
    Collections.sort(list, new @OsUntrusted Comparator<@OsUntrusted HookEntry>() {

      //reversing comparison so highest priority hooks are first
      @Override
      public @OsUntrusted int compare(@OsUntrusted HookEntry o1, @OsUntrusted HookEntry o2) {
        return o2.priority - o1.priority;
      }
    });
    @OsUntrusted
    List<@OsUntrusted Runnable> ordered = new @OsUntrusted ArrayList<@OsUntrusted Runnable>();
    for (@OsUntrusted HookEntry entry: list) {
      ordered.add(entry.hook);
    }
    return ordered;
  }

  /**
   * Adds a shutdownHook with a priority, the higher the priority
   * the earlier will run. ShutdownHooks with same priority run
   * in a non-deterministic order.
   *
   * @param shutdownHook shutdownHook <code>Runnable</code>
   * @param priority priority of the shutdownHook.
   */
  public void addShutdownHook(@OsUntrusted ShutdownHookManager this, @OsUntrusted Runnable shutdownHook, @OsUntrusted int priority) {
    if (shutdownHook == null) {
      throw new @OsUntrusted IllegalArgumentException("shutdownHook cannot be NULL");
    }
    if (shutdownInProgress.get()) {
      throw new @OsUntrusted IllegalStateException("Shutdown in progress, cannot add a shutdownHook");
    }
    hooks.add(new @OsUntrusted HookEntry(shutdownHook, priority));
  }

  /**
   * Removes a shutdownHook.
   *
   * @param shutdownHook shutdownHook to remove.
   * @return TRUE if the shutdownHook was registered and removed,
   * FALSE otherwise.
   */
  public @OsUntrusted boolean removeShutdownHook(@OsUntrusted ShutdownHookManager this, @OsUntrusted Runnable shutdownHook) {
    if (shutdownInProgress.get()) {
      throw new @OsUntrusted IllegalStateException("Shutdown in progress, cannot remove a shutdownHook");
    }
    return hooks.remove(new @OsUntrusted HookEntry(shutdownHook, 0));
  }

  /**
   * Indicates if a shutdownHook is registered or not.
   *
   * @param shutdownHook shutdownHook to check if registered.
   * @return TRUE/FALSE depending if the shutdownHook is is registered.
   */
  public @OsUntrusted boolean hasShutdownHook(@OsUntrusted ShutdownHookManager this, @OsUntrusted Runnable shutdownHook) {
    return hooks.contains(new @OsUntrusted HookEntry(shutdownHook, 0));
  }
  
  /**
   * Indicates if shutdown is in progress or not.
   * 
   * @return TRUE if the shutdown is in progress, otherwise FALSE.
   */
  public @OsUntrusted boolean isShutdownInProgress(@OsUntrusted ShutdownHookManager this) {
    return shutdownInProgress.get();
  }

}
