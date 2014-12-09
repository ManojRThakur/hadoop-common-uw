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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * <code>GenericOptionsParser</code> is a utility to parse command line
 * arguments generic to the Hadoop framework. 
 * 
 * <code>GenericOptionsParser</code> recognizes several standarad command 
 * line arguments, enabling applications to easily specify a namenode, a 
 * jobtracker, additional configuration resources etc.
 * 
 * <h4 id="GenericOptions">Generic Options</h4>
 * 
 * <p>The supported generic options are:</p>
 * <p><blockquote><pre>
 *     -conf &lt;configuration file&gt;     specify a configuration file
 *     -D &lt;property=value&gt;            use value for given property
 *     -fs &lt;local|namenode:port&gt;      specify a namenode
 *     -jt &lt;local|jobtracker:port&gt;    specify a job tracker
 *     -files &lt;comma separated list of files&gt;    specify comma separated
 *                            files to be copied to the map reduce cluster
 *     -libjars &lt;comma separated list of jars&gt;   specify comma separated
 *                            jar files to include in the classpath.
 *     -archives &lt;comma separated list of archives&gt;    specify comma
 *             separated archives to be unarchived on the compute machines.

 * </pre></blockquote></p>
 * 
 * <p>The general command line syntax is:</p>
 * <p><tt><pre>
 * bin/hadoop command [genericOptions] [commandOptions]
 * </pre></tt></p>
 * 
 * <p>Generic command line arguments <strong>might</strong> modify 
 * <code>Configuration </code> objects, given to constructors.</p>
 * 
 * <p>The functionality is implemented using Commons CLI.</p>
 *
 * <p>Examples:</p>
 * <p><blockquote><pre>
 * $ bin/hadoop dfs -fs darwin:8020 -ls /data
 * list /data directory in dfs with namenode darwin:8020
 * 
 * $ bin/hadoop dfs -D fs.default.name=darwin:8020 -ls /data
 * list /data directory in dfs with namenode darwin:8020
 *     
 * $ bin/hadoop dfs -conf core-site.xml -conf hdfs-site.xml -ls /data
 * list /data directory in dfs with multiple conf files specified.
 *     
 * $ bin/hadoop job -D mapred.job.tracker=darwin:50020 -submit job.xml
 * submit a job to job tracker darwin:50020
 *     
 * $ bin/hadoop job -jt darwin:50020 -submit job.xml
 * submit a job to job tracker darwin:50020
 *     
 * $ bin/hadoop job -jt local -submit job.xml
 * submit a job to local runner
 * 
 * $ bin/hadoop jar -libjars testlib.jar 
 * -archives test.tgz -files file.txt inputjar args
 * job submission with libjars, files and archives
 * </pre></blockquote></p>
 *
 * @see Tool
 * @see ToolRunner
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class GenericOptionsParser {

  private static final @OsUntrusted Log LOG = LogFactory.getLog(GenericOptionsParser.class);
  private @OsUntrusted Configuration conf;
  private @OsTrusted CommandLine commandLine;

  /**
   * Create an options parser with the given options to parse the args.
   * @param opts the options
   * @param args the command line arguments
   * @throws IOException 
   */
  public @OsUntrusted GenericOptionsParser(@OsUntrusted Options opts, @OsTrusted String @OsUntrusted [] args)
      throws IOException {
    this(new @OsUntrusted Configuration(), opts, args);
  }

  /**
   * Create an options parser to parse the args.
   * @param args the command line arguments
   * @throws IOException 
   */
  public @OsUntrusted GenericOptionsParser(@OsTrusted String @OsUntrusted [] args)
      throws IOException {
    this(new @OsUntrusted Configuration(), new @OsUntrusted Options(), args);
  }
  
  /** 
   * Create a <code>GenericOptionsParser<code> to parse only the generic Hadoop  
   * arguments. 
   * 
   * The array of string arguments other than the generic arguments can be 
   * obtained by {@link #getRemainingArgs()}.
   * 
   * @param conf the <code>Configuration</code> to modify.
   * @param args command-line arguments.
   * @throws IOException 
   */
  public @OsUntrusted GenericOptionsParser(@OsUntrusted Configuration conf, @OsTrusted String @OsUntrusted [] args)
      throws IOException {
    this(conf, new @OsUntrusted Options(), args); 
  }

  /** 
   * Create a <code>GenericOptionsParser</code> to parse given options as well 
   * as generic Hadoop options. 
   * 
   * The resulting <code>CommandLine</code> object can be obtained by 
   * {@link #getCommandLine()}.
   * 
   * @param conf the configuration to modify  
   * @param options options built by the caller 
   * @param args User-specified arguments
   * @throws IOException 
   */
  public @OsUntrusted GenericOptionsParser(@OsUntrusted Configuration conf,
      @OsUntrusted
      Options options, @OsTrusted String @OsUntrusted [] args) throws IOException {
    parseGeneralOptions(options, conf, args);
    this.conf = conf;
  }

  /**
   * Returns an array of Strings containing only application-specific arguments.
   * 
   * @return array of <code>String</code>s containing the un-parsed arguments
   * or <strong>empty array</strong> if commandLine was not defined.
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public @OsTrusted String @OsUntrusted [] getRemainingArgs(@OsUntrusted GenericOptionsParser this) {
    return (commandLine == null) ? new @OsTrusted String @OsUntrusted []{} : (@OsTrusted String @OsUntrusted []) commandLine.getArgs();
  }

  /**
   * Get the modified configuration
   * @return the configuration that has the modified parameters.
   */
  public @OsUntrusted Configuration getConfiguration(@OsUntrusted GenericOptionsParser this) {
    return conf;
  }

  /**
   * Returns the commons-cli <code>CommandLine</code> object 
   * to process the parsed arguments. 
   * 
   * Note: If the object is created with 
   * {@link #GenericOptionsParser(Configuration, String[])}, then returned 
   * object will only contain parsed generic options.
   * 
   * @return <code>CommandLine</code> representing list of arguments 
   *         parsed against Options descriptor.
   */
  public @OsUntrusted CommandLine getCommandLine(@OsUntrusted GenericOptionsParser this) {
    return commandLine;
  }

  /**
   * Specify properties of each generic option
   */
  @SuppressWarnings("static-access")
  private static @OsUntrusted Options buildGeneralOptions(@OsUntrusted Options opts) {
    @OsUntrusted
    Option fs = OptionBuilder.withArgName("local|namenode:port")
    .hasArg()
    .withDescription("specify a namenode")
    .create("fs");
    @OsUntrusted
    Option jt = OptionBuilder.withArgName("local|jobtracker:port")
    .hasArg()
    .withDescription("specify a job tracker")
    .create("jt");
    @OsUntrusted
    Option oconf = OptionBuilder.withArgName("configuration file")
    .hasArg()
    .withDescription("specify an application configuration file")
    .create("conf");
    @OsUntrusted
    Option property = OptionBuilder.withArgName("property=value")
    .hasArg()
    .withDescription("use value for given property")
    .create('D');
    @OsUntrusted
    Option libjars = OptionBuilder.withArgName("paths")
    .hasArg()
    .withDescription("comma separated jar files to include in the classpath.")
    .create("libjars");
    @OsUntrusted
    Option files = OptionBuilder.withArgName("paths")
    .hasArg()
    .withDescription("comma separated files to be copied to the " +
           "map reduce cluster")
    .create("files");
    @OsUntrusted
    Option archives = OptionBuilder.withArgName("paths")
    .hasArg()
    .withDescription("comma separated archives to be unarchived" +
                     " on the compute machines.")
    .create("archives");
    
    // file with security tokens
    @OsUntrusted
    Option tokensFile = OptionBuilder.withArgName("tokensFile")
    .hasArg()
    .withDescription("name of the file with the tokens")
    .create("tokenCacheFile");

    opts.addOption(fs);
    opts.addOption(jt);
    opts.addOption(oconf);
    opts.addOption(property);
    opts.addOption(libjars);
    opts.addOption(files);
    opts.addOption(archives);
    opts.addOption(tokensFile);

    return opts;
  }

  /**
   * Modify configuration according user-specified generic options
   * @param conf Configuration to be modified
   * @param line User-specified generic options
   */
  @SuppressWarnings( {"ostrusted:cast.unsafe", "ostrusted:assigment.type.incompatible"} )
  //ostrusted, manually checked
  private void processGeneralOptions(@OsUntrusted GenericOptionsParser this, @OsUntrusted Configuration conf,
      @OsTrusted CommandLine line) throws IOException {
    if (line.hasOption("fs")) {
      FileSystem.setDefaultUri(conf, (@OsTrusted String) line.getOptionValue("fs"));
    }

    if (line.hasOption("jt")) {
      @OsTrusted
      String optionValue = (@OsTrusted String) line.getOptionValue("jt");
      if (optionValue.equalsIgnoreCase("local")) {
        conf.set("mapreduce.framework.name", optionValue);
      }

      conf.set("yarn.resourcemanager.address", optionValue, 
          "from -jt command line option");
    }
    if (line.hasOption("conf")) {
      @OsTrusted String @OsUntrusted [] values = (@OsTrusted String @OsUntrusted []) line.getOptionValues("conf");
      for(@OsTrusted String value : values) {
        Path locP = new Path(value);
        conf.addResource( locP );
      }

    }
    if (line.hasOption("libjars")) {
      conf.set("tmpjars",  validateFiles( (@OsTrusted String) line.getOptionValue("libjars"), conf),
               "from -libjars command line option");
      //setting libjars in client classpath
      @OsTrusted URL @OsUntrusted [] libjars = getLibJars(conf);
      if(libjars!=null && libjars.length>0) {
        conf.setClassLoader(new @OsUntrusted URLClassLoader(libjars, conf.getClassLoader()));
        Thread.currentThread().setContextClassLoader(
            new @OsUntrusted URLClassLoader(libjars, 
                Thread.currentThread().getContextClassLoader()));
      }
    }
    if (line.hasOption("files")) {
      conf.set("tmpfiles", 
               validateFiles( (@OsTrusted String ) line.getOptionValue("files") , conf),
               "from -files command line option");
    }
    if (line.hasOption("archives")) {
      conf.set("tmparchives",
                validateFiles( ( @OsTrusted String ) line.getOptionValue("archives"), conf),
                "from -archives command line option");
    }
    if (line.hasOption('D')) {
      @OsTrusted String @OsUntrusted [] property = ( @OsTrusted String @OsUntrusted [] ) line.getOptionValues('D');
      for(@OsTrusted String prop : property) {
        @OsTrusted String @OsUntrusted [] keyval = (@OsTrusted String @OsUntrusted []) prop.split("=", 2);
        if (keyval.length == 2) {
          conf.set(keyval[0], keyval[1], "from command line");
        }
      }
    }
    conf.setBoolean("mapreduce.client.genericoptionsparser.used", true);
    
    // tokensFile
    if(line.hasOption("tokenCacheFile")) {
      @OsTrusted String fileName = ( @OsTrusted String ) line.getOptionValue("tokenCacheFile");
      // check if the local file exists
      @OsUntrusted FileSystem localFs = FileSystem.getLocal(conf);
      Path p = localFs.makeQualified( new Path(fileName));
      if (!localFs.exists(p)) {
          throw new @OsUntrusted FileNotFoundException("File "+fileName+" does not exist.");
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("setting conf tokensFile: " + fileName);
      }
      UserGroupInformation.getCurrentUser().addCredentials(Credentials.readTokenStorageFile(p, conf));
      conf.set("mapreduce.job.credentials.json", (@OsTrusted String) p.toString(), "from -tokenCacheFile command line option");

    }
  }
  
  /**
   * If libjars are set in the conf, parse the libjars.
   * @param conf
   * @return libjar urls
   * @throws IOException
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  public static @OsTrusted URL @OsUntrusted [] getLibJars(@OsUntrusted Configuration conf) throws IOException {
    @OsTrusted String jars = conf.get("tmpjars");
    if(jars==null) {
      return null;
    }
    @OsTrusted String @OsUntrusted [] files = ( @OsTrusted String @OsUntrusted [] ) jars.split(",");
    @OsUntrusted List<@OsTrusted URL> cp = new @OsUntrusted ArrayList<@OsTrusted URL>();
    for (@OsTrusted String file : files) {
      Path tmp = new Path(file);
      //ostrusted, since file is trusted tmp is trusted, we are casting below
      if ( tmp.getFileSystem(conf).equals(FileSystem.getLocal(conf)))  {
        cp.add( (@OsTrusted URL) FileSystem.getLocal(conf).pathToFile(tmp).toURI().toURL());
      } else {
        //ostrusted, avoid Checker Framework exception
        /*LOG.warn("The libjars file " + tmp + " is not on the local " +
          "filesystem. Ignoring."); */
      }
    }
    return cp.toArray(new @OsTrusted URL @OsUntrusted [0]);
  }

  /**
   * takes input as a comma separated list of files
   * and verifies if they exist. It defaults for file:///
   * if the files specified do not have a scheme.
   * it returns the paths uri converted defaulting to file:///.
   * So an input of  /home/user/file1,/home/user/file2 would return
   * file:///home/user/file1,file:///home/user/file2
   * @param files
   * @return
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  private @OsTrusted String validateFiles(@OsUntrusted GenericOptionsParser this, @OsTrusted String files, @OsUntrusted Configuration conf)
      throws IOException  {
    if (files == null) 
      return null;
    @OsTrusted String @OsUntrusted [] fileArr = (@OsTrusted String @OsUntrusted []) files.split(",");
    @OsTrusted String @OsUntrusted [] finalArr = new @OsTrusted String @OsUntrusted [fileArr.length];
    for (@OsUntrusted int i =0; i < fileArr.length; i++) {
      @OsTrusted String tmp = fileArr[i];
      @OsTrusted String finalPath;
      @OsTrusted URI pathURI;
      try {
        pathURI = new @OsTrusted URI(tmp);
      } catch (@OsUntrusted URISyntaxException e) {
        throw new @OsUntrusted IllegalArgumentException(e);
      }
      Path path = new Path(pathURI);
      @OsUntrusted FileSystem localFs = FileSystem.getLocal(conf);
      if (pathURI.getScheme() == null) {
        //default to the local file system
        //check if the file exists or not first
        if (!localFs.exists(path)) {
          throw new @OsUntrusted FileNotFoundException("File " + tmp + " does not exist.");
        }
        finalPath = (@OsTrusted String) path.makeQualified(localFs).toString();
      }
      else {
        // check if the file exists in this file system
        // we need to recreate this filesystem object to copy
        // these files to the file system jobtracker is running
        // on.
        @OsUntrusted FileSystem fs = path.getFileSystem(conf);
        if (!fs.exists(path)) {
          throw new @OsUntrusted FileNotFoundException("File " + tmp + " does not exist.");
        }
        finalPath = (@OsTrusted String) path.makeQualified(fs).toString();
      }
      finalArr[i] = finalPath;
    }
    return (@OsTrusted String) StringUtils.arrayToString(finalArr);
  }

  /**
   * Windows powershell and cmd can parse key=value themselves, because
   * /pkey=value is same as /pkey value under windows. However this is not
   * compatible with how we get arbitrary key values in -Dkey=value format.
   * Under windows -D key=value or -Dkey=value might be passed as
   * [-Dkey, value] or [-D key, value]. This method does undo these and
   * return a modified args list by manually changing [-D, key, value]
   * into [-D, key=value]
   *
   * @param args command line arguments
   * @return fixed command line arguments that GnuParser can parse
   */
  @SuppressWarnings("ostrusted:cast.local")
  private @OsTrusted String @OsUntrusted [] preProcessForWindows(@OsUntrusted GenericOptionsParser this, @OsTrusted String @OsUntrusted [] args) {
    if (!Shell.WINDOWS) {
      return args;
    }
    @OsUntrusted
    List<@OsTrusted String> newArgs = new @OsUntrusted ArrayList<@OsTrusted String>(args.length);
    for (@OsUntrusted int i=0; i < args.length; i++) {
      @OsTrusted  String prop = null;
      if (args[i].equals("-D")) {
        newArgs.add( args[i] );
        if (i < args.length - 1) {
          prop = args[++i];
        }
      } else if (args[i].startsWith("-D")) {
        prop = args[i];
      } else {
        newArgs.add(args[i]);
      }
      if (prop != null) {
        if (prop.contains("=")) {
          // everything good
        } else {
          if (i < args.length - 1) {
            prop += "=" + args[++i];
          }
        }
        newArgs.add(prop);
      }
    }

    return ( @OsTrusted String @OsUntrusted [] ) newArgs.toArray(new @OsTrusted String @OsUntrusted [newArgs.size()]);
  }

  /**
   * Parse the user-specified options, get the generic options, and modify
   * configuration accordingly
   * @param opts Options to use for parsing args.
   * @param conf Configuration to be modified
   * @param args User-specified arguments
   */
  @SuppressWarnings("ostrusted:cast.unsafe")
  private void parseGeneralOptions(@OsUntrusted GenericOptionsParser this, @OsUntrusted Options opts, @OsUntrusted Configuration conf, 
      @OsTrusted String @OsUntrusted [] args) throws IOException {
    opts = buildGeneralOptions(opts);
    @OsUntrusted
    CommandLineParser parser = new @OsUntrusted GnuParser();
    try {
      commandLine = ( @OsTrusted CommandLine )parser.parse(opts, preProcessForWindows(args), true);
      processGeneralOptions(conf, commandLine);
    } catch(@OsUntrusted ParseException e) {
      LOG.warn("options parsing failed: "+e.getMessage());

      @OsUntrusted
      HelpFormatter formatter = new @OsUntrusted HelpFormatter();
      formatter.printHelp("general options are: ", opts);
    }
  }

  /**
   * Print the usage message for generic command-line options supported.
   * 
   * @param out stream to print the usage message to.
   */
  public static void printGenericCommandUsage(@OsUntrusted PrintStream out) {
    
    out.println("Generic options supported are");
    out.println("-conf <configuration file>     specify an application configuration file");
    out.println("-D <property=value>            use value for given property");
    out.println("-fs <local|namenode:port>      specify a namenode");
    out.println("-jt <local|jobtracker:port>    specify a job tracker");
    out.println("-files <comma separated list of files>    " + 
      "specify comma separated files to be copied to the map reduce cluster");
    out.println("-libjars <comma separated list of jars>    " +
      "specify comma separated jar files to include in the classpath.");
    out.println("-archives <comma separated list of archives>    " +
                "specify comma separated archives to be unarchived" +
                " on the compute machines.\n");
    out.println("The general command line syntax is");
    out.println("bin/hadoop command [genericOptions] [commandOptions]\n");
  }
  
}
