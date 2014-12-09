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


package org.apache.hadoop.log;

import ostrusted.quals.OsUntrusted;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.ISO8601DateFormat;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ContainerNode;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.util.Date;

/**
 * This offers a log layout for JSON, with some test entry points. It's purpose is
 * to allow Log4J to generate events that are easy for other programs to parse, but which are somewhat
 * human-readable.
 *
 * Some features.
 *
 * <ol>
 *     <li>Every event is a standalone JSON clause</li>
 *     <li>Time is published as a time_t event since 1/1/1970
 *      -this is the fastest to generate.</li>
 *     <li>An ISO date is generated, but this is cached and will only be accurate to within a second</li>
 *     <li>the stack trace is included as an array</li>
 * </ol>
 *
 * A simple log event will resemble the following
 * <pre>
 *     {"name":"test","time":1318429136789,"date":"2011-10-12 15:18:56,789","level":"INFO","thread":"main","message":"test message"}
 * </pre>
 *
 * An event with an error will contain data similar to that below (which has been reformatted to be multi-line).
 *
 * <pre>
 *     {
 *     "name":"testException",
 *     "time":1318429136789,
 *     "date":"2011-10-12 15:18:56,789",
 *     "level":"INFO",
 *     "thread":"quoted\"",
 *     "message":"new line\n and {}",
 *     "exceptionclass":"java.net.NoRouteToHostException",
 *     "stack":[
 *         "java.net.NoRouteToHostException: that box caught fire 3 years ago",
 *         "\tat org.apache.hadoop.log.TestLog4Json.testException(TestLog4Json.java:49)",
 *         "\tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)",
 *         "\tat sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39)",
 *         "\tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)",
 *         "\tat java.lang.reflect.Method.invoke(Method.java:597)",
 *         "\tat junit.framework.TestCase.runTest(TestCase.java:168)",
 *         "\tat junit.framework.TestCase.runBare(TestCase.java:134)",
 *         "\tat junit.framework.TestResult$1.protect(TestResult.java:110)",
 *         "\tat junit.framework.TestResult.runProtected(TestResult.java:128)",
 *         "\tat junit.framework.TestResult.run(TestResult.java:113)",
 *         "\tat junit.framework.TestCase.run(TestCase.java:124)",
 *         "\tat junit.framework.TestSuite.runTest(TestSuite.java:232)",
 *         "\tat junit.framework.TestSuite.run(TestSuite.java:227)",
 *         "\tat org.junit.internal.runners.JUnit38ClassRunner.run(JUnit38ClassRunner.java:83)",
 *         "\tat org.apache.maven.surefire.junit4.JUnit4TestSet.execute(JUnit4TestSet.java:59)",
 *         "\tat org.apache.maven.surefire.suite.AbstractDirectoryTestSuite.executeTestSet(AbstractDirectoryTestSuite.java:120)",
 *         "\tat org.apache.maven.surefire.suite.AbstractDirectoryTestSuite.execute(AbstractDirectoryTestSuite.java:145)",
 *         "\tat org.apache.maven.surefire.Surefire.run(Surefire.java:104)",
 *         "\tat sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)",
 *         "\tat sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39)",
 *         "\tat sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)",
 *         "\tat java.lang.reflect.Method.invoke(Method.java:597)",
 *         "\tat org.apache.maven.surefire.booter.SurefireBooter.runSuitesInProcess(SurefireBooter.java:290)",
 *         "\tat org.apache.maven.surefire.booter.SurefireBooter.main(SurefireBooter.java:1017)"
 *         ]
 *     }
 * </pre>
 */
public class Log4Json extends @OsUntrusted Layout {

  /**
   * Jackson factories are thread safe when constructing parsers and generators.
   * They are not thread safe in configure methods; if there is to be any
   * configuration it must be done in a static intializer block.
   */
  private static final @OsUntrusted JsonFactory factory = new @OsUntrusted MappingJsonFactory();
  public static final @OsUntrusted String DATE = "date";
  public static final @OsUntrusted String EXCEPTION_CLASS = "exceptionclass";
  public static final @OsUntrusted String LEVEL = "level";
  public static final @OsUntrusted String MESSAGE = "message";
  public static final @OsUntrusted String NAME = "name";
  public static final @OsUntrusted String STACK = "stack";
  public static final @OsUntrusted String THREAD = "thread";
  public static final @OsUntrusted String TIME = "time";
  public static final @OsUntrusted String JSON_TYPE = "application/json";

  private final @OsUntrusted DateFormat dateFormat;

  public @OsUntrusted Log4Json() {
    dateFormat = new @OsUntrusted ISO8601DateFormat();
  }


  /**
   * @return the mime type of JSON
   */
  @Override
  public @OsUntrusted String getContentType(@OsUntrusted Log4Json this) {
    return JSON_TYPE;
  }

  @Override
  public @OsUntrusted String format(@OsUntrusted Log4Json this, @OsUntrusted LoggingEvent event) {
    try {
      return toJson(event);
    } catch (@OsUntrusted IOException e) {
      //this really should not happen, and rather than throw an exception
      //which may hide the real problem, the log class is printed
      //in JSON format. The classname is used to ensure valid JSON is 
      //returned without playing escaping games
      return "{ \"logfailure\":\"" + e.getClass().toString() + "\"}";
    }
  }

  /**
   * Convert an event to JSON
   *
   * @param event the event -must not be null
   * @return a string value
   * @throws IOException on problems generating the JSON
   */
  public @OsUntrusted String toJson(@OsUntrusted Log4Json this, @OsUntrusted LoggingEvent event) throws IOException {
    @OsUntrusted
    StringWriter writer = new @OsUntrusted StringWriter();
    toJson(writer, event);
    return writer.toString();
  }

  /**
   * Convert an event to JSON
   *
   * @param writer the destination writer
   * @param event the event -must not be null
   * @return the writer
   * @throws IOException on problems generating the JSON
   */
  public @OsUntrusted Writer toJson(@OsUntrusted Log4Json this, final @OsUntrusted Writer writer, final @OsUntrusted LoggingEvent event)
      throws IOException {
    @OsUntrusted
    ThrowableInformation ti = event.getThrowableInformation();
    toJson(writer,
           event.getLoggerName(),
           event.getTimeStamp(),
           event.getLevel().toString(),
           event.getThreadName(),
           event.getRenderedMessage(),
           ti);
    return writer;
  }

  /**
   * Build a JSON entry from the parameters. This is public for testing.
   *
   * @param writer destination
   * @param loggerName logger name
   * @param timeStamp time_t value
   * @param level level string
   * @param threadName name of the thread
   * @param message rendered message
   * @param ti nullable thrown information
   * @return the writer
   * @throws IOException on any problem
   */
  public @OsUntrusted Writer toJson(@OsUntrusted Log4Json this, final @OsUntrusted Writer writer,
                       final @OsUntrusted String loggerName,
                       final @OsUntrusted long timeStamp,
                       final @OsUntrusted String level,
                       final @OsUntrusted String threadName,
                       final @OsUntrusted String message,
                       final @OsUntrusted ThrowableInformation ti) throws IOException {
    @OsUntrusted
    JsonGenerator json = factory.createJsonGenerator(writer);
    json.writeStartObject();
    json.writeStringField(NAME, loggerName);
    json.writeNumberField(TIME, timeStamp);
    @OsUntrusted
    Date date = new @OsUntrusted Date(timeStamp);
    json.writeStringField(DATE, dateFormat.format(date));
    json.writeStringField(LEVEL, level);
    json.writeStringField(THREAD, threadName);
    json.writeStringField(MESSAGE, message);
    if (ti != null) {
      //there is some throwable info, but if the log event has been sent over the wire,
      //there may not be a throwable inside it, just a summary.
      @OsUntrusted
      Throwable thrown = ti.getThrowable();
      @OsUntrusted
      String eclass = (thrown != null) ?
          thrown.getClass().getName()
          : "";
      json.writeStringField(EXCEPTION_CLASS, eclass);
      @OsUntrusted
      String @OsUntrusted [] stackTrace = ti.getThrowableStrRep();
      json.writeArrayFieldStart(STACK);
      for (@OsUntrusted String row : stackTrace) {
        json.writeString(row);
      }
      json.writeEndArray();
    }
    json.writeEndObject();
    json.flush();
    json.close();
    return writer;
  }

  /**
   * This appender does not ignore throwables
   *
   * @return false, always
   */
  @Override
  public @OsUntrusted boolean ignoresThrowable(@OsUntrusted Log4Json this) {
    return false;
  }

  /**
   * Do nothing
   */
  @Override
  public void activateOptions(@OsUntrusted Log4Json this) {
  }

  /**
   * For use in tests
   *
   * @param json incoming JSON to parse
   * @return a node tree
   * @throws IOException on any parsing problems
   */
  public static @OsUntrusted ContainerNode parse(@OsUntrusted String json) throws IOException {
    @OsUntrusted
    ObjectMapper mapper = new @OsUntrusted ObjectMapper(factory);
    @OsUntrusted
    JsonNode jsonNode = mapper.readTree(json);
    if (!(jsonNode instanceof @OsUntrusted ContainerNode)) {
      throw new @OsUntrusted IOException("Wrong JSON data: " + json);
    }
    return (@OsUntrusted ContainerNode) jsonNode;
  }
}