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
package org.apache.hadoop.record.compiler.ant;

import ostrusted.quals.OsUntrusted;
import java.io.File;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.record.compiler.generated.Rcc;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;

/**
 * Hadoop record compiler ant Task
 *<p> This task takes the given record definition files and compiles them into
 * java or c++
 * files. It is then up to the user to compile the generated files.
 *
 * <p> The task requires the <code>file</code> or the nested fileset element to be
 * specified. Optional attributes are <code>language</code> (set the output
 * language, default is "java"),
 * <code>destdir</code> (name of the destination directory for generated java/c++
 * code, default is ".") and <code>failonerror</code> (specifies error handling
 * behavior. default is true).
 * <p><h4>Usage</h4>
 * <pre>
 * &lt;recordcc
 *       destdir="${basedir}/gensrc"
 *       language="java"&gt;
 *   &lt;fileset include="**\/*.jr" /&gt;
 * &lt;/recordcc&gt;
 * </pre>
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RccTask extends @OsUntrusted Task {
  
  private @OsUntrusted String language = "java";
  private @OsUntrusted File src;
  private @OsUntrusted File dest = new @OsUntrusted File(".");
  private final @OsUntrusted ArrayList<@OsUntrusted FileSet> filesets = new @OsUntrusted ArrayList<@OsUntrusted FileSet>();
  private @OsUntrusted boolean failOnError = true;
  
  /** Creates a new instance of RccTask */
  public @OsUntrusted RccTask() {
  }
  
  /**
   * Sets the output language option
   * @param language "java"/"c++"
   */
  public void setLanguage(@OsUntrusted RccTask this, @OsUntrusted String language) {
    this.language = language;
  }
  
  /**
   * Sets the record definition file attribute
   * @param file record definition file
   */
  public void setFile(@OsUntrusted RccTask this, @OsUntrusted File file) {
    this.src = file;
  }
  
  /**
   * Given multiple files (via fileset), set the error handling behavior
   * @param flag true will throw build exception in case of failure (default)
   */
  public void setFailonerror(@OsUntrusted RccTask this, @OsUntrusted boolean flag) {
    this.failOnError = flag;
  }
  
  /**
   * Sets directory where output files will be generated
   * @param dir output directory
   */
  public void setDestdir(@OsUntrusted RccTask this, @OsUntrusted File dir) {
    this.dest = dir;
  }
  
  /**
   * Adds a fileset that can consist of one or more files
   * @param set Set of record definition files
   */
  public void addFileset(@OsUntrusted RccTask this, @OsUntrusted FileSet set) {
    filesets.add(set);
  }
  
  /**
   * Invoke the Hadoop record compiler on each record definition file
   */
  @Override
  public void execute(@OsUntrusted RccTask this) throws BuildException {
    if (src == null && filesets.size()==0) {
      throw new @OsUntrusted BuildException("There must be a file attribute or a fileset child element");
    }
    if (src != null) {
      doCompile(src);
    }
    @OsUntrusted
    Project myProject = getProject();
    for (@OsUntrusted int i = 0; i < filesets.size(); i++) {
      @OsUntrusted
      FileSet fs = filesets.get(i);
      @OsUntrusted
      DirectoryScanner ds = fs.getDirectoryScanner(myProject);
      @OsUntrusted
      File dir = fs.getDir(myProject);
      @OsUntrusted
      String @OsUntrusted [] srcs = ds.getIncludedFiles();
      for (@OsUntrusted int j = 0; j < srcs.length; j++) {
        doCompile(new @OsUntrusted File(dir, srcs[j]));
      }
    }
  }
  
  private void doCompile(@OsUntrusted RccTask this, @OsUntrusted File file) throws BuildException {
    @OsUntrusted
    String @OsUntrusted [] args = new @OsUntrusted String @OsUntrusted [5];
    args[0] = "--language";
    args[1] = this.language;
    args[2] = "--destdir";
    args[3] = this.dest.getPath();
    args[4] = file.getPath();
    @OsUntrusted
    int retVal = Rcc.driver(args);
    if (retVal != 0 && failOnError) {
      throw new @OsUntrusted BuildException("Hadoop record compiler returned error code "+retVal);
    }
  }
}
