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
package org.apache.hadoop.fs;

import ostrusted.quals.OsUntrusted;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Time;

/** Provides a <i>trash</i> feature.  Files are moved to a user's trash
 * directory, a subdirectory of their home directory named ".Trash".  Files are
 * initially moved to a <i>current</i> sub-directory of the trash directory.
 * Within that sub-directory their original path is preserved.  Periodically
 * one may checkpoint the current trash and remove older checkpoints.  (This
 * design permits trash management without enumeration of the full trash
 * content, without date support in the filesystem, and without clock
 * synchronization.)
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TrashPolicyDefault extends @OsUntrusted TrashPolicy {
  private static final @OsUntrusted Log LOG =
    LogFactory.getLog(TrashPolicyDefault.class);

  private static final @OsUntrusted Path CURRENT = new @OsUntrusted Path("Current");
  private static final @OsUntrusted Path TRASH = new @OsUntrusted Path(".Trash/");  

  private static final @OsUntrusted FsPermission PERMISSION =
    new @OsUntrusted FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

  private static final @OsUntrusted DateFormat CHECKPOINT = new @OsUntrusted SimpleDateFormat("yyMMddHHmmss");
  /** Format of checkpoint directories used prior to Hadoop 0.23. */
  private static final @OsUntrusted DateFormat OLD_CHECKPOINT =
      new @OsUntrusted SimpleDateFormat("yyMMddHHmm");
  private static final @OsUntrusted int MSECS_PER_MINUTE = 60*1000;

  private @OsUntrusted Path current;
  private @OsUntrusted Path homesParent;
  private @OsUntrusted long emptierInterval;

  public @OsUntrusted TrashPolicyDefault() { }

  private @OsUntrusted TrashPolicyDefault(@OsUntrusted FileSystem fs, @OsUntrusted Path home, @OsUntrusted Configuration conf)
      throws IOException {
    initialize(conf, fs, home);
  }

  @Override
  public void initialize(@OsUntrusted TrashPolicyDefault this, @OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted Path home) {
    this.fs = fs;
    this.trash = new @OsUntrusted Path(home, TRASH);
    this.homesParent = home.getParent();
    this.current = new @OsUntrusted Path(trash, CURRENT);
    this.deletionInterval = (@OsUntrusted long)(conf.getFloat(
        FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT)
        * MSECS_PER_MINUTE);
    this.emptierInterval = (@OsUntrusted long)(conf.getFloat(
        FS_TRASH_CHECKPOINT_INTERVAL_KEY, FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT)
        * MSECS_PER_MINUTE);
    LOG.info("Namenode trash configuration: Deletion interval = " +
             this.deletionInterval + " minutes, Emptier interval = " +
             this.emptierInterval + " minutes.");
   }

  private @OsUntrusted Path makeTrashRelativePath(@OsUntrusted TrashPolicyDefault this, @OsUntrusted Path basePath, @OsUntrusted Path rmFilePath) {
    return Path.mergePaths(basePath, rmFilePath);
  }

  @Override
  public @OsUntrusted boolean isEnabled(@OsUntrusted TrashPolicyDefault this) {
    return deletionInterval != 0;
  }

  @Override
  public @OsUntrusted boolean moveToTrash(@OsUntrusted TrashPolicyDefault this, @OsUntrusted Path path) throws IOException {
    if (!isEnabled())
      return false;

    if (!path.isAbsolute())                       // make path absolute
      path = new @OsUntrusted Path(fs.getWorkingDirectory(), path);

    if (!fs.exists(path))                         // check that path exists
      throw new @OsUntrusted FileNotFoundException(path.toString());

    @OsUntrusted
    String qpath = fs.makeQualified(path).toString();

    if (qpath.startsWith(trash.toString())) {
      return false;                               // already in trash
    }

    if (trash.getParent().toString().startsWith(qpath)) {
      throw new @OsUntrusted IOException("Cannot move \"" + path +
                            "\" to the trash, as it contains the trash");
    }

    @OsUntrusted
    Path trashPath = makeTrashRelativePath(current, path);
    @OsUntrusted
    Path baseTrashPath = makeTrashRelativePath(current, path.getParent());
    
    @OsUntrusted
    IOException cause = null;

    // try twice, in case checkpoint between the mkdirs() & rename()
    for (@OsUntrusted int i = 0; i < 2; i++) {
      try {
        if (!fs.mkdirs(baseTrashPath, PERMISSION)) {      // create current
          LOG.warn("Can't create(mkdir) trash directory: "+baseTrashPath);
          return false;
        }
      } catch (@OsUntrusted IOException e) {
        LOG.warn("Can't create trash directory: "+baseTrashPath);
        cause = e;
        break;
      }
      try {
        // if the target path in Trash already exists, then append with 
        // a current time in millisecs.
        @OsUntrusted
        String orig = trashPath.toString();
        
        while(fs.exists(trashPath)) {
          trashPath = new @OsUntrusted Path(orig + Time.now());
        }
        
        if (fs.rename(path, trashPath))           // move to current trash
          return true;
      } catch (@OsUntrusted IOException e) {
        cause = e;
      }
    }
    throw (@OsUntrusted IOException)
      new @OsUntrusted IOException("Failed to move to trash: "+path).initCause(cause);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void createCheckpoint(@OsUntrusted TrashPolicyDefault this) throws IOException {
    if (!fs.exists(current))                     // no trash, no checkpoint
      return;

    @OsUntrusted
    Path checkpointBase;
    synchronized (CHECKPOINT) {
      checkpointBase = new @OsUntrusted Path(trash, CHECKPOINT.format(new @OsUntrusted Date()));
    }
    @OsUntrusted
    Path checkpoint = checkpointBase;

    @OsUntrusted
    int attempt = 0;
    while (true) {
      try {
        fs.rename(current, checkpoint, Rename.NONE);
        break;
      } catch (@OsUntrusted FileAlreadyExistsException e) {
        if (++attempt > 1000) {
          throw new @OsUntrusted IOException("Failed to checkpoint trash: "+checkpoint);
        }
        checkpoint = checkpointBase.suffix("-" + attempt);
      }
    }

    LOG.info("Created trash checkpoint: "+checkpoint.toUri().getPath());
  }

  @Override
  public void deleteCheckpoint(@OsUntrusted TrashPolicyDefault this) throws IOException {
    @OsUntrusted
    FileStatus @OsUntrusted [] dirs = null;
    
    try {
      dirs = fs.listStatus(trash);            // scan trash sub-directories
    } catch (@OsUntrusted FileNotFoundException fnfe) {
      return;
    }

    @OsUntrusted
    long now = Time.now();
    for (@OsUntrusted int i = 0; i < dirs.length; i++) {
      @OsUntrusted
      Path path = dirs[i].getPath();
      @OsUntrusted
      String dir = path.toUri().getPath();
      @OsUntrusted
      String name = path.getName();
      if (name.equals(CURRENT.getName()))         // skip current
        continue;

      @OsUntrusted
      long time;
      try {
        time = getTimeFromCheckpoint(name);
      } catch (@OsUntrusted ParseException e) {
        LOG.warn("Unexpected item in trash: "+dir+". Ignoring.");
        continue;
      }

      if ((now - deletionInterval) > time) {
        if (fs.delete(path, true)) {
          LOG.info("Deleted trash checkpoint: "+dir);
        } else {
          LOG.warn("Couldn't delete checkpoint: "+dir+" Ignoring.");
        }
      }
    }
  }

  @Override
  public @OsUntrusted Path getCurrentTrashDir(@OsUntrusted TrashPolicyDefault this) {
    return current;
  }

  @Override
  public @OsUntrusted Runnable getEmptier(@OsUntrusted TrashPolicyDefault this) throws IOException {
    return new @OsUntrusted Emptier(getConf(), emptierInterval);
  }

  private class Emptier implements @OsUntrusted Runnable {

    private @OsUntrusted Configuration conf;
    private @OsUntrusted long emptierInterval;

    @OsUntrusted
    Emptier(@OsUntrusted Configuration conf, @OsUntrusted long emptierInterval) throws IOException {
      this.conf = conf;
      this.emptierInterval = emptierInterval;
      if (emptierInterval > deletionInterval || emptierInterval == 0) {
        LOG.info("The configured checkpoint interval is " +
                 (emptierInterval / MSECS_PER_MINUTE) + " minutes." +
                 " Using an interval of " +
                 (deletionInterval / MSECS_PER_MINUTE) +
                 " minutes that is used for deletion instead");
        this.emptierInterval = deletionInterval;
      }
    }

    @Override
    public void run(@OsUntrusted TrashPolicyDefault.Emptier this) {
      if (emptierInterval == 0)
        return;                                   // trash disabled
      @OsUntrusted
      long now = Time.now();
      @OsUntrusted
      long end;
      while (true) {
        end = ceiling(now, emptierInterval);
        try {                                     // sleep for interval
          Thread.sleep(end - now);
        } catch (@OsUntrusted InterruptedException e) {
          break;                                  // exit on interrupt
        }

        try {
          now = Time.now();
          if (now >= end) {

            @OsUntrusted
            FileStatus @OsUntrusted [] homes = null;
            try {
              homes = fs.listStatus(homesParent);         // list all home dirs
            } catch (@OsUntrusted IOException e) {
              LOG.warn("Trash can't list homes: "+e+" Sleeping.");
              continue;
            }

            for (@OsUntrusted FileStatus home : homes) {         // dump each trash
              if (!home.isDirectory())
                continue;
              try {
                @OsUntrusted
                TrashPolicyDefault trash = new @OsUntrusted TrashPolicyDefault(
                    fs, home.getPath(), conf);
                trash.deleteCheckpoint();
                trash.createCheckpoint();
              } catch (@OsUntrusted IOException e) {
                LOG.warn("Trash caught: "+e+". Skipping "+home.getPath()+".");
              } 
            }
          }
        } catch (@OsUntrusted Exception e) {
          LOG.warn("RuntimeException during Trash.Emptier.run(): ", e); 
        }
      }
      try {
        fs.close();
      } catch(@OsUntrusted IOException e) {
        LOG.warn("Trash cannot close FileSystem: ", e);
      }
    }

    private @OsUntrusted long ceiling(@OsUntrusted TrashPolicyDefault.Emptier this, @OsUntrusted long time, @OsUntrusted long interval) {
      return floor(time, interval) + interval;
    }
    private @OsUntrusted long floor(@OsUntrusted TrashPolicyDefault.Emptier this, @OsUntrusted long time, @OsUntrusted long interval) {
      return (time / interval) * interval;
    }
  }

  private @OsUntrusted long getTimeFromCheckpoint(@OsUntrusted TrashPolicyDefault this, @OsUntrusted String name) throws ParseException {
    @OsUntrusted
    long time;

    try {
      synchronized (CHECKPOINT) {
        time = CHECKPOINT.parse(name).getTime();
      }
    } catch (@OsUntrusted ParseException pe) {
      // Check for old-style checkpoint directories left over
      // after an upgrade from Hadoop 1.x
      synchronized (OLD_CHECKPOINT) {
        time = OLD_CHECKPOINT.parse(name).getTime();
      }
    }

    return time;
  }
}
