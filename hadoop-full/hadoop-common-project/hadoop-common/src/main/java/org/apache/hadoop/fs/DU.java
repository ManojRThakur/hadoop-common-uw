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
import ostrusted.quals.OsTrusted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.Shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/** Filesystem disk space usage statistics.  Uses the unix 'du' program*/
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DU extends @OsUntrusted Shell {
  private @OsTrusted String  dirPath;

  private @OsUntrusted AtomicLong used = new @OsUntrusted AtomicLong();
  private volatile @OsUntrusted boolean shouldRun = true;
  private @OsUntrusted Thread refreshUsed;
  private @OsUntrusted IOException duException = null;
  private @OsUntrusted long refreshInterval;
  
  /**
   * Keeps track of disk usage.
   * @param path the path to check disk usage in
   * @param interval refresh the disk usage at this interval
   * @throws IOException if we fail to refresh the disk usage
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @OsUntrusted DU(@OsTrusted File path, @OsUntrusted long interval) throws IOException {
    super(0);
    
    //we set the Shell interval to 0 so it will always run our command
    //and use this one to set the thread sleep interval
    this.refreshInterval = interval;
    this.dirPath = (@OsTrusted String) path.getCanonicalPath();
    
    //populate the used variable
    run();
  }
  
  /**
   * Keeps track of disk usage.
   * @param path the path to check disk usage in
   * @param conf configuration object
   * @throws IOException if we fail to refresh the disk usage
   */
  public @OsUntrusted DU(@OsTrusted File path, @OsUntrusted Configuration conf) throws IOException {
    this(path, conf.getLong(CommonConfigurationKeys.FS_DU_INTERVAL_KEY,
                CommonConfigurationKeys.FS_DU_INTERVAL_DEFAULT));
  }

  /**
   * This thread refreshes the "used" variable.
   * 
   * Future improvements could be to not permanently
   * run this thread, instead run when getUsed is called.
   **/
  class DURefreshThread implements @OsUntrusted Runnable {
    
    @Override
    public void run(@OsUntrusted DU.DURefreshThread this) {
      
      while(shouldRun) {

        try {
          Thread.sleep(refreshInterval);
          
          try {
            //update the used variable
            DU.this.run();
          } catch (@OsUntrusted IOException e) {
            synchronized (DU.this) {
              //save the latest exception so we can return it in getUsed()
              duException = e;
            }
            
            LOG.warn("Could not get disk usage information", e);
          }
        } catch (@OsUntrusted InterruptedException e) {
        }
      }
    }
  }
  
  /**
   * Decrease how much disk space we use.
   * @param value decrease by this value
   */
  public void decDfsUsed(@OsUntrusted DU this, @OsUntrusted long value) {
    used.addAndGet(-value);
  }

  /**
   * Increase how much disk space we use.
   * @param value increase by this value
   */
  public void incDfsUsed(@OsUntrusted DU this, @OsUntrusted long value) {
    used.addAndGet(value);
  }
  
  /**
   * @return disk space used 
   * @throws IOException if the shell command fails
   */
  public @OsUntrusted long getUsed(@OsUntrusted DU this) throws IOException {
    //if the updating thread isn't started, update on demand
    if(refreshUsed == null) {
      run();
    } else {
      synchronized (DU.this) {
        //if an exception was thrown in the last run, rethrow
        if(duException != null) {
          @OsUntrusted
          IOException tmp = duException;
          duException = null;
          throw tmp;
        }
      }
    }
    
    return Math.max(used.longValue(), 0L);
  }

  /**
   * @return the path of which we're keeping track of disk usage
   */
  public @OsUntrusted String getDirPath(@OsUntrusted DU this) {
    return dirPath;
  }


  /**
   * Override to hook in DUHelper class. Maybe this can be used more
   * generally as well on Unix/Linux based systems
   */
  @Override
  protected void run(@OsUntrusted DU this) throws IOException {
    if (WINDOWS) {
      used.set(DUHelper.getFolderUsage(dirPath));
      return;
    }
    super.run();
  }
  
  /**
   * Start the disk usage checking thread.
   */
  public void start(@OsUntrusted DU this) {
    //only start the thread if the interval is sane
    if(refreshInterval > 0) {
      refreshUsed = new @OsUntrusted Thread(new @OsUntrusted DURefreshThread(), 
          "refreshUsed-"+dirPath);
      refreshUsed.setDaemon(true);
      refreshUsed.start();
    }
  }
  
  /**
   * Shut down the refreshing thread.
   */
  public void shutdown(@OsUntrusted DU this) {
    this.shouldRun = false;
    
    if(this.refreshUsed != null) {
      this.refreshUsed.interrupt();
    }
  }
  
  @Override
  public @OsUntrusted String toString(@OsUntrusted DU this) {
    return
      "du -sk " + dirPath +"\n" +
      used + "\t" + dirPath;
  }

  @Override
  protected @OsTrusted String @OsUntrusted [] getExecString(@OsUntrusted DU this) {
    return new @OsTrusted String @OsUntrusted [] {"du", "-sk", dirPath};
  }
  
  @Override
  protected void parseExecResult(@OsUntrusted DU this, @OsUntrusted BufferedReader lines) throws IOException {
    @OsUntrusted
    String line = lines.readLine();
    if (line == null) {
      throw new @OsUntrusted IOException("Expecting a line not the end of stream");
    }
    @OsUntrusted
    String @OsUntrusted [] tokens = line.split("\t");
    if(tokens.length == 0) {
      throw new @OsUntrusted IOException("Illegal du output");
    }
    this.used.set(Long.parseLong(tokens[0])*1024);
  }

  //see comments on DF.main, they apply here
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static void main(@OsUntrusted String @OsUntrusted [] args) throws Exception {
    @OsUntrusted
    String path = ".";
    if (args.length > 0) {
      path = args[0];
    }

    System.out.println(new @OsUntrusted DU(new @OsTrusted File(path), new @OsUntrusted Configuration()).toString());
  }
}
