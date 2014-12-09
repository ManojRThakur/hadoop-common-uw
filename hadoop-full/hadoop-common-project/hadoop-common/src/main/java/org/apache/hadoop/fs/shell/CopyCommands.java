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

package org.apache.hadoop.fs.shell;

import ostrusted.quals.OsUntrusted;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

/** Various commands for copy files */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class CopyCommands {  
  public static void registerCommands(@OsUntrusted CommandFactory factory) {
    factory.addClass(Merge.class, "-getmerge");
    factory.addClass(Cp.class, "-cp");
    factory.addClass(CopyFromLocal.class, "-copyFromLocal");
    factory.addClass(CopyToLocal.class, "-copyToLocal");
    factory.addClass(Get.class, "-get");
    factory.addClass(Put.class, "-put");
    factory.addClass(AppendToFile.class, "-appendToFile");
  }

  /** merge multiple files together */
  public static class Merge extends @OsUntrusted FsCommand {
    public static final @OsUntrusted String NAME = "getmerge";    
    public static final @OsUntrusted String USAGE = "[-nl] <src> <localdst>";
    public static final @OsUntrusted String DESCRIPTION =
      "Get all the files in the directories that\n" +
      "match the source file pattern and merge and sort them to only\n" +
      "one file on local fs. <src> is kept.\n" +
      "  -nl   Add a newline character at the end of each file.";

    protected @OsUntrusted PathData dst = null;
    protected @OsUntrusted String delimiter = null;
    protected @OsUntrusted List<@OsUntrusted PathData> srcs = null;

    @Override
    protected void processOptions(CopyCommands.@OsUntrusted Merge this, @OsUntrusted LinkedList<@OsUntrusted String> args) throws IOException {
      try {
        @OsUntrusted
        CommandFormat cf = new @OsUntrusted CommandFormat(2, Integer.MAX_VALUE, "nl");
        cf.parse(args);

        delimiter = cf.getOpt("nl") ? "\n" : null;

        dst = new @OsUntrusted PathData(new @OsUntrusted URI(args.removeLast()), getConf());
        if (dst.exists && dst.stat.isDirectory()) {
          throw new @OsUntrusted PathIsDirectoryException(dst.toString());
        }
        srcs = new @OsUntrusted LinkedList<@OsUntrusted PathData>();
      } catch (@OsUntrusted URISyntaxException e) {
        throw new @OsUntrusted IOException("unexpected URISyntaxException", e);
      }
    }

    @Override
    protected void processArguments(CopyCommands.@OsUntrusted Merge this, @OsUntrusted LinkedList<@OsUntrusted PathData> items)
    throws IOException {
      super.processArguments(items);
      if (exitCode != 0) { // check for error collecting paths
        return;
      }
      @OsUntrusted
      FSDataOutputStream out = dst.fs.create(dst.path);
      try {
        for (@OsUntrusted PathData src : srcs) {
          @OsUntrusted
          FSDataInputStream in = src.fs.open(src.path);
          try {
            IOUtils.copyBytes(in, out, getConf(), false);
            if (delimiter != null) {
              out.write(delimiter.getBytes("UTF-8"));
            }
          } finally {
            in.close();
          }
        }
      } finally {
        out.close();
      }      
    }
 
    @Override
    protected void processNonexistentPath(CopyCommands.@OsUntrusted Merge this, @OsUntrusted PathData item) throws IOException {
      exitCode = 1; // flag that a path is bad
      super.processNonexistentPath(item);
    }

    // this command is handled a bit differently than others.  the paths
    // are batched up instead of actually being processed.  this avoids
    // unnecessarily streaming into the merge file and then encountering
    // a path error that should abort the merge
    
    @Override
    protected void processPath(CopyCommands.@OsUntrusted Merge this, @OsUntrusted PathData src) throws IOException {
      // for directories, recurse one level to get its files, else skip it
      if (src.stat.isDirectory()) {
        if (getDepth() == 0) {
          recursePath(src);
        } // skip subdirs
      } else {
        srcs.add(src);
      }
    }
  }

  static class Cp extends @OsUntrusted CommandWithDestination {
    public static final @OsUntrusted String NAME = "cp";
    public static final @OsUntrusted String USAGE = "[-f] [-p] <src> ... <dst>";
    public static final @OsUntrusted String DESCRIPTION =
      "Copy files that match the file pattern <src> to a\n" +
      "destination.  When copying multiple files, the destination\n" +
      "must be a directory. Passing -p preserves access and\n" +
      "modification times, ownership and the mode. Passing -f\n" +
      "overwrites the destination if it already exists.\n";
    
    @Override
    protected void processOptions(CopyCommands.@OsUntrusted Cp this, @OsUntrusted LinkedList<@OsUntrusted String> args) throws IOException {
      @OsUntrusted
      CommandFormat cf = new @OsUntrusted CommandFormat(2, Integer.MAX_VALUE, "f", "p");
      cf.parse(args);
      setOverwrite(cf.getOpt("f"));
      setPreserve(cf.getOpt("p"));
      // should have a -r option
      setRecursive(true);
      getRemoteDestination(args);
    }
  }
  
  /** 
   * Copy local files to a remote filesystem
   */
  public static class Get extends @OsUntrusted CommandWithDestination {
    public static final @OsUntrusted String NAME = "get";
    public static final @OsUntrusted String USAGE =
      "[-p] [-ignoreCrc] [-crc] <src> ... <localdst>";
    public static final @OsUntrusted String DESCRIPTION =
      "Copy files that match the file pattern <src>\n" +
      "to the local name.  <src> is kept.  When copying multiple,\n" +
      "files, the destination must be a directory. Passing\n" +
      "-p preserves access and modification times,\n" +
      "ownership and the mode.\n";

    @Override
    protected void processOptions(CopyCommands.@OsUntrusted Get this, @OsUntrusted LinkedList<@OsUntrusted String> args)
    throws IOException {
      @OsUntrusted
      CommandFormat cf = new @OsUntrusted CommandFormat(
          1, Integer.MAX_VALUE, "crc", "ignoreCrc", "p");
      cf.parse(args);
      setWriteChecksum(cf.getOpt("crc"));
      setVerifyChecksum(!cf.getOpt("ignoreCrc"));
      setPreserve(cf.getOpt("p"));
      setRecursive(true);
      getLocalDestination(args);
    }
  }

  /**
   *  Copy local files to a remote filesystem
   */
  public static class Put extends @OsUntrusted CommandWithDestination {
    public static final @OsUntrusted String NAME = "put";
    public static final @OsUntrusted String USAGE = "[-f] [-p] <localsrc> ... <dst>";
    public static final @OsUntrusted String DESCRIPTION =
      "Copy files from the local file system\n" +
      "into fs. Copying fails if the file already\n" +
      "exists, unless the -f flag is given. Passing\n" +
      "-p preserves access and modification times,\n" +
      "ownership and the mode. Passing -f overwrites\n" +
      "the destination if it already exists.\n";

    @Override
    protected void processOptions(CopyCommands.@OsUntrusted Put this, @OsUntrusted LinkedList<@OsUntrusted String> args) throws IOException {
      @OsUntrusted
      CommandFormat cf = new @OsUntrusted CommandFormat(1, Integer.MAX_VALUE, "f", "p");
      cf.parse(args);
      setOverwrite(cf.getOpt("f"));
      setPreserve(cf.getOpt("p"));
      getRemoteDestination(args);
      // should have a -r option
      setRecursive(true);
    }

    // commands operating on local paths have no need for glob expansion
    @Override
    protected @OsUntrusted List<@OsUntrusted PathData> expandArgument(CopyCommands.@OsUntrusted Put this, @OsUntrusted String arg) throws IOException {
      @OsUntrusted
      List<@OsUntrusted PathData> items = new @OsUntrusted LinkedList<@OsUntrusted PathData>();
      try {
        items.add(new @OsUntrusted PathData(new @OsUntrusted URI(arg), getConf()));
      } catch (@OsUntrusted URISyntaxException e) {
        if (Path.WINDOWS) {
          // Unlike URI, PathData knows how to parse Windows drive-letter paths.
          items.add(new @OsUntrusted PathData(arg, getConf()));
        } else {
          throw new @OsUntrusted IOException("unexpected URISyntaxException", e);
        }
      }
      return items;
    }

    @Override
    protected void processArguments(CopyCommands.@OsUntrusted Put this, @OsUntrusted LinkedList<@OsUntrusted PathData> args)
    throws IOException {
      // NOTE: this logic should be better, mimics previous implementation
      if (args.size() == 1 && args.get(0).toString().equals("-")) {
        copyStreamToTarget(System.in, getTargetPath(args.get(0)));
        return;
      }
      super.processArguments(args);
    }
  }

  public static class CopyFromLocal extends @OsUntrusted Put {
    public static final @OsUntrusted String NAME = "copyFromLocal";
    public static final @OsUntrusted String USAGE = Put.USAGE;
    public static final @OsUntrusted String DESCRIPTION = "Identical to the -put command.";
  }
 
  public static class CopyToLocal extends @OsUntrusted Get {
    public static final @OsUntrusted String NAME = "copyToLocal";
    public static final @OsUntrusted String USAGE = Get.USAGE;
    public static final @OsUntrusted String DESCRIPTION = "Identical to the -get command.";
  }

  /**
   *  Append the contents of one or more local files to a remote
   *  file.
   */
  public static class AppendToFile extends @OsUntrusted CommandWithDestination {
    public static final @OsUntrusted String NAME = "appendToFile";
    public static final @OsUntrusted String USAGE = "<localsrc> ... <dst>";
    public static final @OsUntrusted String DESCRIPTION =
        "Appends the contents of all the given local files to the\n" +
            "given dst file. The dst file will be created if it does\n" +
            "not exist. If <localSrc> is -, then the input is read\n" +
            "from stdin.";

    private static final @OsUntrusted int DEFAULT_IO_LENGTH = 1024 * 1024;
    @OsUntrusted
    boolean readStdin = false;

    // commands operating on local paths have no need for glob expansion
    @Override
    protected @OsUntrusted List<@OsUntrusted PathData> expandArgument(CopyCommands.@OsUntrusted AppendToFile this, @OsUntrusted String arg) throws IOException {
      @OsUntrusted
      List<@OsUntrusted PathData> items = new @OsUntrusted LinkedList<@OsUntrusted PathData>();
      if (arg.equals("-")) {
        readStdin = true;
      } else {
        try {
          items.add(new @OsUntrusted PathData(new @OsUntrusted URI(arg), getConf()));
        } catch (@OsUntrusted URISyntaxException e) {
          if (Path.WINDOWS) {
            // Unlike URI, PathData knows how to parse Windows drive-letter paths.
            items.add(new @OsUntrusted PathData(arg, getConf()));
          } else {
            throw new @OsUntrusted IOException("Unexpected URISyntaxException: " + e.toString());
          }
        }
      }
      return items;
    }

    @Override
    protected void processOptions(CopyCommands.@OsUntrusted AppendToFile this, @OsUntrusted LinkedList<@OsUntrusted String> args)
        throws IOException {

      if (args.size() < 2) {
        throw new @OsUntrusted IOException("missing destination argument");
      }

      getRemoteDestination(args);
      super.processOptions(args);
    }

    @Override
    protected void processArguments(CopyCommands.@OsUntrusted AppendToFile this, @OsUntrusted LinkedList<@OsUntrusted PathData> args)
        throws IOException {

      if (!dst.exists) {
        dst.fs.create(dst.path, false).close();
      }

      @OsUntrusted
      InputStream is = null;
      @OsUntrusted
      FSDataOutputStream fos = dst.fs.append(dst.path);

      try {
        if (readStdin) {
          if (args.size() == 0) {
            IOUtils.copyBytes(System.in, fos, DEFAULT_IO_LENGTH);
          } else {
            throw new @OsUntrusted IOException(
                "stdin (-) must be the sole input argument when present");
          }
        }

        // Read in each input file and write to the target.
        for (@OsUntrusted PathData source : args) {
          is = new @OsUntrusted FileInputStream(source.toFile());
          IOUtils.copyBytes(is, fos, DEFAULT_IO_LENGTH);
          IOUtils.closeStream(is);
          is = null;
        }
      } finally {
        if (is != null) {
          IOUtils.closeStream(is);
        }

        if (fos != null) {
          IOUtils.closeStream(fos);
        }
      }
    }
  }
}
