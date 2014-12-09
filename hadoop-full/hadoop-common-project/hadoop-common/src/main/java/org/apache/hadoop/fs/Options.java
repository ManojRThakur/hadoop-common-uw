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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;

/**
 * This class contains options related to file system operations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class Options {
  /**
   * Class to support the varargs for create() options.
   *
   */
  public static class CreateOpts {
    private @OsUntrusted CreateOpts() { };
    public static @OsUntrusted BlockSize blockSize(@OsUntrusted long bs) { 
      return new @OsUntrusted BlockSize(bs);
    }
    public static @OsUntrusted BufferSize bufferSize(@OsUntrusted int bs) { 
      return new @OsUntrusted BufferSize(bs);
    }
    public static @OsUntrusted ReplicationFactor repFac(@OsUntrusted short rf) { 
      return new @OsUntrusted ReplicationFactor(rf);
    }
    public static @OsUntrusted BytesPerChecksum bytesPerChecksum(@OsUntrusted short crc) {
      return new @OsUntrusted BytesPerChecksum(crc);
    }
    public static @OsUntrusted ChecksumParam checksumParam(
        @OsUntrusted
        ChecksumOpt csumOpt) {
      return new @OsUntrusted ChecksumParam(csumOpt);
    }
    public static @OsUntrusted Perms perms(@OsUntrusted FsPermission perm) {
      return new @OsUntrusted Perms(perm);
    }
    public static @OsUntrusted CreateParent createParent() {
      return new @OsUntrusted CreateParent(true);
    }
    public static @OsUntrusted CreateParent donotCreateParent() {
      return new @OsUntrusted CreateParent(false);
    }
    
    public static class BlockSize extends @OsUntrusted CreateOpts {
      private final @OsUntrusted long blockSize;
      protected @OsUntrusted BlockSize(@OsUntrusted long bs) {
        if (bs <= 0) {
          throw new @OsUntrusted IllegalArgumentException(
                        "Block size must be greater than 0");
        }
        blockSize = bs; 
      }
      public @OsUntrusted long getValue(Options.CreateOpts.@OsUntrusted BlockSize this) { return blockSize; }
    }
    
    public static class ReplicationFactor extends @OsUntrusted CreateOpts {
      private final @OsUntrusted short replication;
      protected @OsUntrusted ReplicationFactor(@OsUntrusted short rf) { 
        if (rf <= 0) {
          throw new @OsUntrusted IllegalArgumentException(
                      "Replication must be greater than 0");
        }
        replication = rf;
      }
      public @OsUntrusted short getValue(Options.CreateOpts.@OsUntrusted ReplicationFactor this) { return replication; }
    }
    
    public static class BufferSize extends @OsUntrusted CreateOpts {
      private final @OsUntrusted int bufferSize;
      protected @OsUntrusted BufferSize(@OsUntrusted int bs) {
        if (bs <= 0) {
          throw new @OsUntrusted IllegalArgumentException(
                        "Buffer size must be greater than 0");
        }
        bufferSize = bs; 
      }
      public @OsUntrusted int getValue(Options.CreateOpts.@OsUntrusted BufferSize this) { return bufferSize; }
    }

    /** This is not needed if ChecksumParam is specified. **/
    public static class BytesPerChecksum extends @OsUntrusted CreateOpts {
      private final @OsUntrusted int bytesPerChecksum;
      protected @OsUntrusted BytesPerChecksum(@OsUntrusted short bpc) { 
        if (bpc <= 0) {
          throw new @OsUntrusted IllegalArgumentException(
                        "Bytes per checksum must be greater than 0");
        }
        bytesPerChecksum = bpc; 
      }
      public @OsUntrusted int getValue(Options.CreateOpts.@OsUntrusted BytesPerChecksum this) { return bytesPerChecksum; }
    }

    public static class ChecksumParam extends @OsUntrusted CreateOpts {
      private final @OsUntrusted ChecksumOpt checksumOpt;
      protected @OsUntrusted ChecksumParam(@OsUntrusted ChecksumOpt csumOpt) {
        checksumOpt = csumOpt;
      }
      public @OsUntrusted ChecksumOpt getValue(Options.CreateOpts.@OsUntrusted ChecksumParam this) { return checksumOpt; }
    }
    
    public static class Perms extends @OsUntrusted CreateOpts {
      private final @OsUntrusted FsPermission permissions;
      protected @OsUntrusted Perms(@OsUntrusted FsPermission perm) { 
        if(perm == null) {
          throw new @OsUntrusted IllegalArgumentException("Permissions must not be null");
        }
        permissions = perm; 
      }
      public @OsUntrusted FsPermission getValue(Options.CreateOpts.@OsUntrusted Perms this) { return permissions; }
    }
    
    public static class Progress extends @OsUntrusted CreateOpts {
      private final @OsUntrusted Progressable progress;
      protected @OsUntrusted Progress(@OsUntrusted Progressable prog) { 
        if(prog == null) {
          throw new @OsUntrusted IllegalArgumentException("Progress must not be null");
        }
        progress = prog;
      }
      public @OsUntrusted Progressable getValue(Options.CreateOpts.@OsUntrusted Progress this) { return progress; }
    }
    
    public static class CreateParent extends @OsUntrusted CreateOpts {
      private final @OsUntrusted boolean createParent;
      protected @OsUntrusted CreateParent(@OsUntrusted boolean createPar) {
        createParent = createPar;}
      public @OsUntrusted boolean getValue(Options.CreateOpts.@OsUntrusted CreateParent this) { return createParent; }
    }

    
    /**
     * Get an option of desired type
     * @param theClass is the desired class of the opt
     * @param opts - not null - at least one opt must be passed
     * @return an opt from one of the opts of type theClass.
     *   returns null if there isn't any
     */
    protected static @OsUntrusted CreateOpts getOpt(@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted CreateOpts> theClass,  @OsUntrusted CreateOpts @OsUntrusted ...opts) {
      if (opts == null) {
        throw new @OsUntrusted IllegalArgumentException("Null opt");
      }
      @OsUntrusted
      CreateOpts result = null;
      for (@OsUntrusted int i = 0; i < opts.length; ++i) {
        if (opts[i].getClass() == theClass) {
          if (result != null) 
            throw new @OsUntrusted IllegalArgumentException("multiple blocksize varargs");
          result = opts[i];
        }
      }
      return result;
    }
    /**
     * set an option
     * @param newValue  the option to be set
     * @param opts  - the option is set into this array of opts
     * @return updated CreateOpts[] == opts + newValue
     */
    protected static <@OsUntrusted T extends @OsUntrusted CreateOpts> @OsUntrusted CreateOpts @OsUntrusted [] setOpt(@OsUntrusted T newValue,
        @OsUntrusted
        CreateOpts @OsUntrusted ...opts) {
      @OsUntrusted
      boolean alreadyInOpts = false;
      if (opts != null) {
        for (@OsUntrusted int i = 0; i < opts.length; ++i) {
          if (opts[i].getClass() == newValue.getClass()) {
            if (alreadyInOpts) 
              throw new @OsUntrusted IllegalArgumentException("multiple opts varargs");
            alreadyInOpts = true;
            opts[i] = newValue;
          }
        }
      }
      @OsUntrusted
      CreateOpts @OsUntrusted [] resultOpt = opts;
      if (!alreadyInOpts) { // no newValue in opt
        @OsUntrusted
        CreateOpts @OsUntrusted [] newOpts = new @OsUntrusted CreateOpts @OsUntrusted [opts.length + 1];
        System.arraycopy(opts, 0, newOpts, 0, opts.length);
        newOpts[opts.length] = newValue;
        resultOpt = newOpts;
      }
      return resultOpt;
    }
  }

  /**
   * Enum to support the varargs for rename() options
   */
  public static enum Rename {

@OsUntrusted  NONE((@OsUntrusted byte) 0), // No options

@OsUntrusted  OVERWRITE((@OsUntrusted byte) 1); // Overwrite the rename destination

    private final @OsUntrusted byte code;
    
    private @OsUntrusted Rename(@OsUntrusted byte code) {
      this.code = code;
    }

    public static @OsUntrusted Rename valueOf(@OsUntrusted byte code) {
      return code < 0 || code >= values().length ? null : values()[code];
    }

    public @OsUntrusted byte value(Options.@OsUntrusted Rename this) {
      return code;
    }
  }

  /**
   * This is used in FileSystem and FileContext to specify checksum options.
   */
  public static class ChecksumOpt {
    private final @OsUntrusted int crcBlockSize;
    private final DataChecksum.@OsUntrusted Type crcType;

    /**
     * Create a uninitialized one
     */
    public @OsUntrusted ChecksumOpt() {
      crcBlockSize = -1;
      crcType = DataChecksum.Type.DEFAULT;
    }

    /**
     * Normal ctor
     * @param type checksum type
     * @param size bytes per checksum
     */
    public @OsUntrusted ChecksumOpt(DataChecksum.@OsUntrusted Type type, @OsUntrusted int size) {
      crcBlockSize = size;
      crcType = type;
    }

    public @OsUntrusted int getBytesPerChecksum(Options.@OsUntrusted ChecksumOpt this) {
      return crcBlockSize;
    }

    public DataChecksum.@OsUntrusted Type getChecksumType(Options.@OsUntrusted ChecksumOpt this) {
      return crcType;
    }

    /**
     * Create a ChecksumOpts that disables checksum
     */
    public static @OsUntrusted ChecksumOpt createDisabled() {
      return new @OsUntrusted ChecksumOpt(DataChecksum.Type.NULL, -1);
    }

    /**
     * A helper method for processing user input and default value to 
     * create a combined checksum option. This is a bit complicated because
     * bytesPerChecksum is kept for backward compatibility.
     *
     * @param defaultOpt Default checksum option
     * @param userOpt User-specified checksum option. Ignored if null.
     * @param userBytesPerChecksum User-specified bytesPerChecksum
     *                Ignored if < 0.
     */
    public static @OsUntrusted ChecksumOpt processChecksumOpt(@OsUntrusted ChecksumOpt defaultOpt, 
        @OsUntrusted
        ChecksumOpt userOpt, @OsUntrusted int userBytesPerChecksum) {
      // The following is done to avoid unnecessary creation of new objects.
      // tri-state variable: 0 default, 1 userBytesPerChecksum, 2 userOpt
      @OsUntrusted
      short whichSize;
      // true default, false userOpt
      @OsUntrusted
      boolean useDefaultType;
      
      //  bytesPerChecksum - order of preference
      //    user specified value in bytesPerChecksum
      //    user specified value in checksumOpt
      //    default.
      if (userBytesPerChecksum > 0) {
        whichSize = 1; // userBytesPerChecksum
      } else if (userOpt != null && userOpt.getBytesPerChecksum() > 0) {
        whichSize = 2; // userOpt
      } else {
        whichSize = 0; // default
      }

      // checksum type - order of preference
      //   user specified value in checksumOpt
      //   default.
      if (userOpt != null &&
            userOpt.getChecksumType() != DataChecksum.Type.DEFAULT) {
        useDefaultType = false;
      } else {
        useDefaultType = true;
      }

      // Short out the common and easy cases
      if (whichSize == 0 && useDefaultType) {
        return defaultOpt;
      } else if (whichSize == 2 && !useDefaultType) {
        return userOpt;
      }

      // Take care of the rest of combinations
      DataChecksum.@OsUntrusted Type type = useDefaultType ? defaultOpt.getChecksumType() :
          userOpt.getChecksumType();
      if (whichSize == 0) {
        return new @OsUntrusted ChecksumOpt(type, defaultOpt.getBytesPerChecksum());
      } else if (whichSize == 1) {
        return new @OsUntrusted ChecksumOpt(type, userBytesPerChecksum);
      } else {
        return new @OsUntrusted ChecksumOpt(type, userOpt.getBytesPerChecksum());
      }
    }

    /**
     * A helper method for processing user input and default value to 
     * create a combined checksum option. 
     *
     * @param defaultOpt Default checksum option
     * @param userOpt User-specified checksum option
     */
    public static @OsUntrusted ChecksumOpt processChecksumOpt(@OsUntrusted ChecksumOpt defaultOpt,
        @OsUntrusted
        ChecksumOpt userOpt) {
      return processChecksumOpt(defaultOpt, userOpt, -1);
    }
  }
}
