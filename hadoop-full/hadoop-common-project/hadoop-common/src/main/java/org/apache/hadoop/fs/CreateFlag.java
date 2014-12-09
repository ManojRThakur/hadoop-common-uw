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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/****************************************************************
 * CreateFlag specifies the file create semantic. Users can combine flags like: <br>
 * <code>
 * EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND)
 * <code>
 * <p>
 * 
 * Use the CreateFlag as follows:
 * <ol>
 * <li> CREATE - to create a file if it does not exist, 
 * else throw FileAlreadyExists.</li>
 * <li> APPEND - to append to a file if it exists, 
 * else throw FileNotFoundException.</li>
 * <li> OVERWRITE - to truncate a file if it exists, 
 * else throw FileNotFoundException.</li>
 * <li> CREATE|APPEND - to create a file if it does not exist, 
 * else append to an existing file.</li>
 * <li> CREATE|OVERWRITE - to create a file if it does not exist, 
 * else overwrite an existing file.</li>
 * <li> SYNC_BLOCK - to force closed blocks to the disk device.
 * In addition {@link Syncable#hsync()} should be called after each write,
 * if true synchronous behavior is required.</li>
 * </ol>
 * 
 * Following combination is not valid and will result in 
 * {@link HadoopIllegalArgumentException}:
 * <ol>
 * <li> APPEND|OVERWRITE</li>
 * <li> CREATE|APPEND|OVERWRITE</li>
 * </ol>
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum CreateFlag {

  /**
   * Create a file. See javadoc for more description
   * already exists
   */

@OsUntrusted  CREATE((@OsUntrusted short) 0x01),

  /**
   * Truncate/overwrite a file. Same as POSIX O_TRUNC. See javadoc for description.
   */

@OsUntrusted  OVERWRITE((@OsUntrusted short) 0x02),

  /**
   * Append to a file. See javadoc for more description.
   */

@OsUntrusted  APPEND((@OsUntrusted short) 0x04),

  /**
   * Force closed blocks to disk. Similar to POSIX O_SYNC. See javadoc for description.
   */

@OsUntrusted  SYNC_BLOCK((@OsUntrusted short) 0x08);

  private final @OsUntrusted short mode;

  private @OsUntrusted CreateFlag(@OsUntrusted short mode) {
    this.mode = mode;
  }

  @OsUntrusted
  short getMode(@OsUntrusted CreateFlag this) {
    return mode;
  }
  
  /**
   * Validate the CreateFlag and throw exception if it is invalid
   * @param flag set of CreateFlag
   * @throws HadoopIllegalArgumentException if the CreateFlag is invalid
   */
  public static void validate(@OsUntrusted EnumSet<@OsUntrusted CreateFlag> flag) {
    if (flag == null || flag.isEmpty()) {
      throw new @OsUntrusted HadoopIllegalArgumentException(flag
          + " does not specify any options");
    }
    final @OsUntrusted boolean append = flag.contains(APPEND);
    final @OsUntrusted boolean overwrite = flag.contains(OVERWRITE);
    
    // Both append and overwrite is an error
    if (append && overwrite) {
      throw new @OsUntrusted HadoopIllegalArgumentException(
          flag + "Both append and overwrite options cannot be enabled.");
    }
  }
  
  /**
   * Validate the CreateFlag for create operation
   * @param path Object representing the path; usually String or {@link Path}
   * @param pathExists pass true if the path exists in the file system
   * @param flag set of CreateFlag
   * @throws IOException on error
   * @throws HadoopIllegalArgumentException if the CreateFlag is invalid
   */
  public static void validate(@OsUntrusted Object path, @OsUntrusted boolean pathExists,
      @OsUntrusted
      EnumSet<@OsUntrusted CreateFlag> flag) throws IOException {
    validate(flag);
    final @OsUntrusted boolean append = flag.contains(APPEND);
    final @OsUntrusted boolean overwrite = flag.contains(OVERWRITE);
    if (pathExists) {
      if (!(append || overwrite)) {
        throw new @OsUntrusted FileAlreadyExistsException("File already exists: "
            + path.toString()
            + ". Append or overwrite option must be specified in " + flag);
      }
    } else if (!flag.contains(CREATE)) {
      throw new @OsUntrusted FileNotFoundException("Non existing file: " + path.toString()
          + ". Create option is not specified in " + flag);
    }
  }
}