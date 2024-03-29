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
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Get a listing of all files in that match the file patterns.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class Ls extends @OsUntrusted FsCommand {
  public static void registerCommands(@OsUntrusted CommandFactory factory) {
    factory.addClass(Ls.class, "-ls");
    factory.addClass(Lsr.class, "-lsr");
  }
  
  public static final @OsUntrusted String NAME = "ls";
  public static final @OsUntrusted String USAGE = "[-d] [-h] [-R] [<path> ...]";
  public static final @OsUntrusted String DESCRIPTION =
    "List the contents that match the specified file pattern. If\n" + 
    "path is not specified, the contents of /user/<currentUser>\n" +
    "will be listed. Directory entries are of the form \n" +
    "\tdirName (full path) <dir> \n" +
    "and file entries are of the form \n" + 
    "\tfileName(full path) <r n> size \n" +
    "where n is the number of replicas specified for the file \n" + 
    "and size is the size of the file, in bytes.\n" +
    "  -d  Directories are listed as plain files.\n" +
    "  -h  Formats the sizes of files in a human-readable fashion\n" +
    "      rather than a number of bytes.\n" +
    "  -R  Recursively list the contents of directories.";

  protected static final @OsUntrusted SimpleDateFormat dateFormat = 
    new @OsUntrusted SimpleDateFormat("yyyy-MM-dd HH:mm");

  protected @OsUntrusted int maxRepl = 3;
  protected @OsUntrusted int maxLen = 10;
  protected @OsUntrusted int maxOwner = 0;
  protected @OsUntrusted int maxGroup = 0;
  protected @OsUntrusted String lineFormat;
  protected @OsUntrusted boolean dirRecurse;

  protected @OsUntrusted boolean humanReadable = false;
  protected @OsUntrusted String formatSize(@OsUntrusted Ls this, @OsUntrusted long size) {
    return humanReadable
      ? StringUtils.TraditionalBinaryPrefix.long2String(size, "", 1)
      : String.valueOf(size);
  }

  @Override
  protected void processOptions(@OsUntrusted Ls this, @OsUntrusted LinkedList<@OsUntrusted String> args)
  throws IOException {
    @OsUntrusted
    CommandFormat cf = new @OsUntrusted CommandFormat(0, Integer.MAX_VALUE, "d", "h", "R");
    cf.parse(args);
    dirRecurse = !cf.getOpt("d");
    setRecursive(cf.getOpt("R") && dirRecurse);
    humanReadable = cf.getOpt("h");
    if (args.isEmpty()) args.add(Path.CUR_DIR);
  }

  @Override
  protected void processPathArgument(@OsUntrusted Ls this, @OsUntrusted PathData item) throws IOException {
    // implicitly recurse once for cmdline directories
    if (dirRecurse && item.stat.isDirectory()) {
      recursePath(item);
    } else {
      super.processPathArgument(item);
    }
  }

  @Override
  protected void processPaths(@OsUntrusted Ls this, @OsUntrusted PathData parent, @OsUntrusted PathData @OsUntrusted ... items)
  throws IOException {
    if (!isRecursive() && items.length != 0) {
      out.println("Found " + items.length + " items");
    }
    adjustColumnWidths(items);
    super.processPaths(parent, items);
  }

  @Override
  protected void processPath(@OsUntrusted Ls this, @OsUntrusted PathData item) throws IOException {
    @OsUntrusted
    FileStatus stat = item.stat;
    @OsUntrusted
    String line = String.format(lineFormat,
        (stat.isDirectory() ? "d" : "-"),
        stat.getPermission(),
        (stat.isFile() ? stat.getReplication() : "-"),
        stat.getOwner(),
        stat.getGroup(),
        formatSize(stat.getLen()),
        dateFormat.format(new @OsUntrusted Date(stat.getModificationTime())),
        item
    );
    out.println(line);
  }

  /**
   * Compute column widths and rebuild the format string
   * @param items to find the max field width for each column
   */
  private void adjustColumnWidths(@OsUntrusted Ls this, @OsUntrusted PathData items @OsUntrusted []) {
    for (@OsUntrusted PathData item : items) {
      @OsUntrusted
      FileStatus stat = item.stat;
      maxRepl  = maxLength(maxRepl, stat.getReplication());
      maxLen   = maxLength(maxLen, stat.getLen());
      maxOwner = maxLength(maxOwner, stat.getOwner());
      maxGroup = maxLength(maxGroup, stat.getGroup());
    }

    @OsUntrusted
    StringBuilder fmt = new @OsUntrusted StringBuilder();
    fmt.append("%s%s "); // permission string
    fmt.append("%"  + maxRepl  + "s ");
    // Do not use '%-0s' as a formatting conversion, since it will throw a
    // a MissingFormatWidthException if it is used in String.format().
    // http://docs.oracle.com/javase/1.5.0/docs/api/java/util/Formatter.html#intFlags
    fmt.append((maxOwner > 0) ? "%-" + maxOwner + "s " : "%s");
    fmt.append((maxGroup > 0) ? "%-" + maxGroup + "s " : "%s");
    fmt.append("%"  + maxLen   + "s ");
    fmt.append("%s %s"); // mod time & path
    lineFormat = fmt.toString();
  }

  private @OsUntrusted int maxLength(@OsUntrusted Ls this, @OsUntrusted int n, @OsUntrusted Object value) {
    return Math.max(n, (value != null) ? String.valueOf(value).length() : 0);
  }

  /**
   * Get a recursive listing of all files in that match the file patterns.
   * Same as "-ls -R"
   */
  public static class Lsr extends @OsUntrusted Ls {
    public static final @OsUntrusted String NAME = "lsr";

    @Override
    protected void processOptions(Ls.@OsUntrusted Lsr this, @OsUntrusted LinkedList<@OsUntrusted String> args)
    throws IOException {
      args.addFirst("-R");
      super.processOptions(args);
    }
    
    @Override
    public @OsUntrusted String getReplacementCommand(Ls.@OsUntrusted Lsr this) {
      return "ls -R";
    }
  }
}
