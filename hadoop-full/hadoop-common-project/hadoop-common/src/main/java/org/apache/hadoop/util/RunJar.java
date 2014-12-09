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
import ostrusted.quals.OsTrusted;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;
import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.jar.JarFile;
import java.util.jar.JarEntry;
import java.util.jar.Manifest;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;

/** Run a Hadoop job jar. */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RunJar {

  /** Pattern that matches any string */
  public static final @OsUntrusted Pattern MATCH_ANY = Pattern.compile(".*");

  /**
   * Priority of the RunJar shutdown hook.
   */
  public static final @OsUntrusted int SHUTDOWN_HOOK_PRIORITY = 10;

  /**
   * Unpack a jar file into a directory.
   *
   * This version unpacks all files inside the jar regardless of filename.
   */
  public static void unJar(@OsUntrusted File jarFile, @OsUntrusted File toDir) throws IOException {
    unJar(jarFile, toDir, MATCH_ANY);
  }

  /**
   * Unpack matching files from a jar. Entries inside the jar that do
   * not match the given pattern will be skipped.
   *
   * @param jarFile the .jar file to unpack
   * @param toDir the destination directory into which to unpack the jar
   * @param unpackRegex the pattern to match jar entries against
   */
  public static void unJar(@OsUntrusted File jarFile, @OsUntrusted File toDir, @OsUntrusted Pattern unpackRegex)
    throws IOException {
    @OsUntrusted
    JarFile jar = new @OsUntrusted JarFile(jarFile);
    try {
      @OsUntrusted
      Enumeration<@OsUntrusted JarEntry> entries = jar.entries();
      while (entries.hasMoreElements()) {
        @OsUntrusted
        JarEntry entry = (@OsUntrusted JarEntry)entries.nextElement();
        if (!entry.isDirectory() &&
            unpackRegex.matcher(entry.getName()).matches()) {
          @OsUntrusted
          InputStream in = jar.getInputStream(entry);
          try {
            @OsUntrusted
            File file = new @OsUntrusted File(toDir, entry.getName());
            ensureDirectory(file.getParentFile());
            @OsUntrusted
            OutputStream out = new @OsUntrusted FileOutputStream(file);
            try {
              IOUtils.copyBytes(in, out, 8192);
            } finally {
              out.close();
            }
          } finally {
            in.close();
          }
        }
      }
    } finally {
      jar.close();
    }
  }

  /**
   * Ensure the existence of a given directory.
   *
   * @throws IOException if it cannot be created and does not already exist
   */
  private static void ensureDirectory(@OsUntrusted File dir) throws IOException {
    if (!dir.mkdirs() && !dir.isDirectory()) {
      throw new @OsUntrusted IOException("Mkdirs failed to create " +
                            dir.toString());
    }
  }

  /** Run a Hadoop job jar.  If the main class is not in the jar's manifest,
   * then it must be provided on the command line. */
   @SuppressWarnings("ostrusted:cast.unsafe")
   public static void main(@OsUntrusted String @OsUntrusted [] args) throws Throwable {
    @OsUntrusted
    String usage = "RunJar jarFile [mainClass] args...";

    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }

    @OsUntrusted
    int firstArg = 0;
    @OsUntrusted
    String fileName = args[firstArg++];
    @OsUntrusted
    File file = new @OsUntrusted File(fileName);
    if (!file.exists() || !file.isFile()) {
      System.err.println("Not a valid JAR: " + file.getCanonicalPath());
      System.exit(-1);
    }
    @OsUntrusted
    String mainClassName = null;

    @OsUntrusted
    JarFile jarFile;
    try {
      jarFile = new @OsUntrusted JarFile(fileName);
    } catch(@OsUntrusted IOException io) {
      throw new @OsUntrusted IOException("Error opening job jar: " + fileName)
        .initCause(io);
    }

    @OsUntrusted
    Manifest manifest = jarFile.getManifest();
    if (manifest != null) {
      mainClassName = manifest.getMainAttributes().getValue("Main-Class");
    }
    jarFile.close();

    if (mainClassName == null) {
      if (args.length < 2) {
        System.err.println(usage);
        System.exit(-1);
      }
      mainClassName = args[firstArg++];
    }
    mainClassName = mainClassName.replaceAll("/", ".");

    @OsTrusted // File from trusted configuation options
    File tmpDir = new @OsTrusted File(new @OsUntrusted Configuration().get("hadoop.tmp.dir"));
    ensureDirectory(tmpDir);

    final @OsTrusted File workDir;
    try {
      @SuppressWarnings("ostrusted:cast.unsafe") // created from trusted file and inputs
      @OsTrusted File workDirTmp = (@OsTrusted File) File.createTempFile("hadoop-unjar", "", tmpDir);
      workDir = workDirTmp;
    } catch (@OsUntrusted IOException ioe) {
      // If user has insufficient perms to write to tmpDir, default  
      // "Permission denied" message doesn't specify a filename. 
      System.err.println("Error creating temp dir in hadoop.tmp.dir "
                         + tmpDir + " due to " + ioe.getMessage());
      System.exit(-1);
      return;
    }

    if (!workDir.delete()) {
      System.err.println("Delete failed for " + workDir);
      System.exit(-1);
    }
    ensureDirectory(workDir);

    ShutdownHookManager.get().addShutdownHook(
      new @OsUntrusted Runnable() {
        @Override
        public void run() {
          FileUtil.fullyDelete(workDir);
        }
      }, SHUTDOWN_HOOK_PRIORITY);


    unJar(file, workDir);

    @OsUntrusted
    ArrayList<@OsUntrusted URL> classPath = new @OsUntrusted ArrayList<@OsUntrusted URL>();
    classPath.add(new @OsUntrusted File(workDir+"/").toURI().toURL());
    classPath.add(file.toURI().toURL());
    classPath.add(new @OsUntrusted File(workDir, "classes/").toURI().toURL());
    @OsUntrusted
    File @OsUntrusted [] libs = new @OsUntrusted File(workDir, "lib").listFiles();
    if (libs != null) {
      for (@OsUntrusted int i = 0; i < libs.length; i++) {
        classPath.add(libs[i].toURI().toURL());
      }
    }
    
    @OsUntrusted
    ClassLoader loader =
      new @OsUntrusted URLClassLoader(classPath.toArray(new @OsUntrusted URL @OsUntrusted [0]));

    Thread.currentThread().setContextClassLoader(loader);
    @OsUntrusted
    Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> mainClass = Class.forName(mainClassName, true, loader);
    @OsUntrusted
    Method main = mainClass.getMethod("main", new @OsUntrusted Class @OsUntrusted [] {
      Array.newInstance(String.class, 0).getClass()
    });
    @OsUntrusted
    String @OsUntrusted [] newArgs = Arrays.asList(args)
      .subList(firstArg, args.length).toArray(new @OsUntrusted String @OsUntrusted [0]);
    try {
      main.invoke(null, new @OsUntrusted Object @OsUntrusted [] { newArgs });
    } catch (@OsUntrusted InvocationTargetException e) {
      throw e.getTargetException();
    }
  }
  
}
