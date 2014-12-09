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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.zip.GZIPInputStream;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.Schema;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.MinimalPrettyPrinter;

/**
 * Display contents or checksums of files 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class Display extends @OsUntrusted FsCommand {
  public static void registerCommands(@OsUntrusted CommandFactory factory) {
    factory.addClass(Cat.class, "-cat");
    factory.addClass(Text.class, "-text");
    factory.addClass(Checksum.class, "-checksum");
  }

  /**
   * Displays file content to stdout
   */
  public static class Cat extends @OsUntrusted Display {
    public static final @OsUntrusted String NAME = "cat";
    public static final @OsUntrusted String USAGE = "[-ignoreCrc] <src> ...";
    public static final @OsUntrusted String DESCRIPTION =
      "Fetch all files that match the file pattern <src> \n" +
      "and display their content on stdout.\n";

    private @OsUntrusted boolean verifyChecksum = true;

    @Override
    protected void processOptions(Display.@OsUntrusted Cat this, @OsUntrusted LinkedList<@OsUntrusted String> args)
    throws IOException {
      @OsUntrusted
      CommandFormat cf = new @OsUntrusted CommandFormat(1, Integer.MAX_VALUE, "ignoreCrc");
      cf.parse(args);
      verifyChecksum = !cf.getOpt("ignoreCrc");
    }

    @Override
    protected void processPath(Display.@OsUntrusted Cat this, @OsUntrusted PathData item) throws IOException {
      if (item.stat.isDirectory()) {
        throw new @OsUntrusted PathIsDirectoryException(item.toString());
      }
      
      item.fs.setVerifyChecksum(verifyChecksum);
      printToStdout(getInputStream(item));
    }

    private void printToStdout(Display.@OsUntrusted Cat this, @OsUntrusted InputStream in) throws IOException {
      try {
        IOUtils.copyBytes(in, out, getConf(), false);
      } finally {
        in.close();
      }
    }

    protected @OsUntrusted InputStream getInputStream(Display.@OsUntrusted Cat this, @OsUntrusted PathData item) throws IOException {
      return item.fs.open(item.path);
    }
  }
  
  /**
   * Same behavior as "-cat", but handles zip and TextRecordInputStream
   * and Avro encodings. 
   */ 
  public static class Text extends @OsUntrusted Cat {
    public static final @OsUntrusted String NAME = "text";
    public static final @OsUntrusted String USAGE = Cat.USAGE;
    public static final @OsUntrusted String DESCRIPTION =
      "Takes a source file and outputs the file in text format.\n" +
      "The allowed formats are zip and TextRecordInputStream and Avro.";
    
    @Override
    protected @OsUntrusted InputStream getInputStream(Display.@OsUntrusted Text this, @OsUntrusted PathData item) throws IOException {
      @OsUntrusted
      FSDataInputStream i = (@OsUntrusted FSDataInputStream)super.getInputStream(item);

      // Check type of stream first
      switch(i.readShort()) {
        case 0x1f8b: { // RFC 1952
          // Must be gzip
          i.seek(0);
          return new @OsUntrusted GZIPInputStream(i);
        }
        case 0x5345: { // 'S' 'E'
          // Might be a SequenceFile
          if (i.readByte() == 'Q') {
            i.close();
            return new @OsUntrusted TextRecordInputStream(item.stat);
          }
        }
        default: {
          // Check the type of compression instead, depending on Codec class's
          // own detection methods, based on the provided path.
          @OsUntrusted
          CompressionCodecFactory cf = new @OsUntrusted CompressionCodecFactory(getConf());
          @OsUntrusted
          CompressionCodec codec = cf.getCodec(item.path);
          if (codec != null) {
            i.seek(0);
            return codec.createInputStream(i);
          }
          break;
        }
        case 0x4f62: { // 'O' 'b'
          if (i.readByte() == 'j') {
            i.close();
            return new @OsUntrusted AvroFileInputStream(item.stat);
          }
          break;
        }
      }

      // File is non-compressed, or not a file container we know.
      i.seek(0);
      return i;
    }
  }
  
  public static class Checksum extends @OsUntrusted Display {
    public static final @OsUntrusted String NAME = "checksum";
    public static final @OsUntrusted String USAGE = "<src> ...";
    public static final @OsUntrusted String DESCRIPTION =
      "Dump checksum information for files that match the file\n" +
      "pattern <src> to stdout. Note that this requires a round-trip\n" +
      "to a datanode storing each block of the file, and thus is not\n" +
      "efficient to run on a large number of files. The checksum of a\n" +
      "file depends on its content, block size and the checksum\n" +
      "algorithm and parameters used for creating the file.";

    @Override
    protected void processPath(Display.@OsUntrusted Checksum this, @OsUntrusted PathData item) throws IOException {
      if (item.stat.isDirectory()) {
        throw new @OsUntrusted PathIsDirectoryException(item.toString());
      }

      @OsUntrusted
      FileChecksum checksum = item.fs.getFileChecksum(item.path);
      if (checksum == null) {
        out.printf("%s\tNONE\t\n", item.toString());
      } else {
        @OsUntrusted
        String checksumString = StringUtils.byteToHexString(
            checksum.getBytes(), 0, checksum.getLength());
        out.printf("%s\t%s\t%s\n",
            item.toString(), checksum.getAlgorithmName(),
            checksumString);
      }
    }
  }

  protected class TextRecordInputStream extends @OsUntrusted InputStream {
    SequenceFile.@OsUntrusted Reader r;
    @OsUntrusted
    Writable key;
    @OsUntrusted
    Writable val;

    @OsUntrusted
    DataInputBuffer inbuf;
    @OsUntrusted
    DataOutputBuffer outbuf;

    public @OsUntrusted TextRecordInputStream(@OsUntrusted FileStatus f) throws IOException {
      final @OsUntrusted Path fpath = f.getPath();
      final @OsUntrusted Configuration lconf = getConf();
      r = new SequenceFile.@OsUntrusted Reader(lconf, 
          SequenceFile.Reader.file(fpath));
      key = ReflectionUtils.newInstance(
          r.getKeyClass().asSubclass(Writable.class), lconf);
      val = ReflectionUtils.newInstance(
          r.getValueClass().asSubclass(Writable.class), lconf);
      inbuf = new @OsUntrusted DataInputBuffer();
      outbuf = new @OsUntrusted DataOutputBuffer();
    }

    @Override
    public @OsUntrusted int read(@OsUntrusted Display.TextRecordInputStream this) throws IOException {
      @OsUntrusted
      int ret;
      if (null == inbuf || -1 == (ret = inbuf.read())) {
        if (!r.next(key, val)) {
          return -1;
        }
        @OsUntrusted
        byte @OsUntrusted [] tmp = key.toString().getBytes();
        outbuf.write(tmp, 0, tmp.length);
        outbuf.write('\t');
        tmp = val.toString().getBytes();
        outbuf.write(tmp, 0, tmp.length);
        outbuf.write('\n');
        inbuf.reset(outbuf.getData(), outbuf.getLength());
        outbuf.reset();
        ret = inbuf.read();
      }
      return ret;
    }

    @Override
    public void close(@OsUntrusted Display.TextRecordInputStream this) throws IOException {
      r.close();
      super.close();
    }
  }

  /**
   * This class transforms a binary Avro data file into an InputStream
   * with data that is in a human readable JSON format.
   */
  protected static class AvroFileInputStream extends @OsUntrusted InputStream {
    private @OsUntrusted int pos;
    private @OsUntrusted byte @OsUntrusted [] buffer;
    private @OsUntrusted ByteArrayOutputStream output;
    private @OsUntrusted FileReader fileReader;
    private @OsUntrusted DatumWriter<@OsUntrusted Object> writer;
    private @OsUntrusted JsonEncoder encoder;

    public @OsUntrusted AvroFileInputStream(@OsUntrusted FileStatus status) throws IOException {
      pos = 0;
      buffer = new @OsUntrusted byte @OsUntrusted [0];
      @OsUntrusted
      GenericDatumReader<@OsUntrusted Object> reader = new @OsUntrusted GenericDatumReader<@OsUntrusted Object>();
      @OsUntrusted
      FileContext fc = FileContext.getFileContext(new @OsUntrusted Configuration());
      fileReader =
        DataFileReader.openReader(new @OsUntrusted AvroFSInput(fc, status.getPath()),reader);
      @OsUntrusted
      Schema schema = fileReader.getSchema();
      writer = new @OsUntrusted GenericDatumWriter<@OsUntrusted Object>(schema);
      output = new @OsUntrusted ByteArrayOutputStream();
      @OsUntrusted
      JsonGenerator generator =
        new @OsUntrusted JsonFactory().createJsonGenerator(output, JsonEncoding.UTF8);
      @OsUntrusted
      MinimalPrettyPrinter prettyPrinter = new @OsUntrusted MinimalPrettyPrinter();
      prettyPrinter.setRootValueSeparator(System.getProperty("line.separator"));
      generator.setPrettyPrinter(prettyPrinter);
      encoder = EncoderFactory.get().jsonEncoder(schema, generator);
    }

    /**
     * Read a single byte from the stream.
     */
    @Override
    public @OsUntrusted int read(Display.@OsUntrusted AvroFileInputStream this) throws IOException {
      if (pos < buffer.length) {
        return buffer[pos++];
      }
      if (!fileReader.hasNext()) {
        return -1;
      }
      writer.write(fileReader.next(), encoder);
      encoder.flush();
      if (!fileReader.hasNext()) {
        // Write a new line after the last Avro record.
        output.write(System.getProperty("line.separator").getBytes());
        output.flush();
      }
      pos = 0;
      buffer = output.toByteArray();
      output.reset();
      return read();
    }

    /**
      * Close the stream.
      */
    @Override
    public void close(Display.@OsUntrusted AvroFileInputStream this) throws IOException {
      fileReader.close();
      output.close();
      super.close();
    }
  }
}
