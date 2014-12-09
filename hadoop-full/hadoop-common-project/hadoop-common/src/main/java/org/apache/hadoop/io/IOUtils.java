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

package org.apache.hadoop.io;

import ostrusted.quals.OsUntrusted;
import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * An utility class for I/O related functionality. 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class IOUtils {
  public static final @OsUntrusted Log LOG = LogFactory.getLog(IOUtils.class);

  /**
   * Copies from one stream to another.
   *
   * @param in InputStrem to read from
   * @param out OutputStream to write to
   * @param buffSize the size of the buffer 
   * @param close whether or not close the InputStream and 
   * OutputStream at the end. The streams are closed in the finally clause.  
   */
  public static void copyBytes(@OsUntrusted InputStream in, @OsUntrusted OutputStream out, @OsUntrusted int buffSize, @OsUntrusted boolean close) 
    throws IOException {
    try {
      copyBytes(in, out, buffSize);
      if(close) {
        out.close();
        out = null;
        in.close();
        in = null;
      }
    } finally {
      if(close) {
        closeStream(out);
        closeStream(in);
      }
    }
  }
  
  /**
   * Copies from one stream to another.
   * 
   * @param in InputStrem to read from
   * @param out OutputStream to write to
   * @param buffSize the size of the buffer 
   */
  public static void copyBytes(@OsUntrusted InputStream in, @OsUntrusted OutputStream out, @OsUntrusted int buffSize) 
    throws IOException {
    @OsUntrusted
    PrintStream ps = out instanceof @OsUntrusted PrintStream ? (@OsUntrusted PrintStream)out : null;
    @OsUntrusted
    byte buf @OsUntrusted [] = new @OsUntrusted byte @OsUntrusted [buffSize];
    @OsUntrusted
    int bytesRead = in.read(buf);
    while (bytesRead >= 0) {
      out.write(buf, 0, bytesRead);
      if ((ps != null) && ps.checkError()) {
        throw new @OsUntrusted IOException("Unable to write to output stream.");
      }
      bytesRead = in.read(buf);
    }
  }

  /**
   * Copies from one stream to another. <strong>closes the input and output streams 
   * at the end</strong>.
   *
   * @param in InputStrem to read from
   * @param out OutputStream to write to
   * @param conf the Configuration object 
   */
  public static void copyBytes(@OsUntrusted InputStream in, @OsUntrusted OutputStream out, @OsUntrusted Configuration conf)
    throws IOException {
    copyBytes(in, out, conf.getInt("io.file.buffer.size", 4096), true);
  }
  
  /**
   * Copies from one stream to another.
   *
   * @param in InputStream to read from
   * @param out OutputStream to write to
   * @param conf the Configuration object
   * @param close whether or not close the InputStream and 
   * OutputStream at the end. The streams are closed in the finally clause.
   */
  public static void copyBytes(@OsUntrusted InputStream in, @OsUntrusted OutputStream out, @OsUntrusted Configuration conf, @OsUntrusted boolean close)
    throws IOException {
    copyBytes(in, out, conf.getInt("io.file.buffer.size", 4096),  close);
  }

  /**
   * Copies count bytes from one stream to another.
   *
   * @param in InputStream to read from
   * @param out OutputStream to write to
   * @param count number of bytes to copy
   * @param close whether to close the streams
   * @throws IOException if bytes can not be read or written
   */
  public static void copyBytes(@OsUntrusted InputStream in, @OsUntrusted OutputStream out, @OsUntrusted long count,
      @OsUntrusted
      boolean close) throws IOException {
    @OsUntrusted
    byte buf @OsUntrusted [] = new @OsUntrusted byte @OsUntrusted [4096];
    @OsUntrusted
    long bytesRemaining = count;
    @OsUntrusted
    int bytesRead;

    try {
      while (bytesRemaining > 0) {
        @OsUntrusted
        int bytesToRead = (@OsUntrusted int)
          (bytesRemaining < buf.length ? bytesRemaining : buf.length);

        bytesRead = in.read(buf, 0, bytesToRead);
        if (bytesRead == -1)
          break;

        out.write(buf, 0, bytesRead);
        bytesRemaining -= bytesRead;
      }
      if (close) {
        out.close();
        out = null;
        in.close();
        in = null;
      }
    } finally {
      if (close) {
        closeStream(out);
        closeStream(in);
      }
    }
  }
  
  /**
   * Utility wrapper for reading from {@link InputStream}. It catches any errors
   * thrown by the underlying stream (either IO or decompression-related), and
   * re-throws as an IOException.
   * 
   * @param is - InputStream to be read from
   * @param buf - buffer the data is read into
   * @param off - offset within buf
   * @param len - amount of data to be read
   * @return number of bytes read
   */
  public static @OsUntrusted int wrappedReadForCompressedData(@OsUntrusted InputStream is, @OsUntrusted byte @OsUntrusted [] buf,
      @OsUntrusted
      int off, @OsUntrusted int len) throws IOException {
    try {
      return is.read(buf, off, len);
    } catch (@OsUntrusted IOException ie) {
      throw ie;
    } catch (@OsUntrusted Throwable t) {
      throw new @OsUntrusted IOException("Error while reading compressed data", t);
    }
  }

  /**
   * Reads len bytes in a loop.
   *
   * @param in InputStream to read from
   * @param buf The buffer to fill
   * @param off offset from the buffer
   * @param len the length of bytes to read
   * @throws IOException if it could not read requested number of bytes 
   * for any reason (including EOF)
   */
  public static void readFully(@OsUntrusted InputStream in, @OsUntrusted byte buf @OsUntrusted [],
      @OsUntrusted
      int off, @OsUntrusted int len) throws IOException {
    @OsUntrusted
    int toRead = len;
    while (toRead > 0) {
      @OsUntrusted
      int ret = in.read(buf, off, toRead);
      if (ret < 0) {
        throw new @OsUntrusted IOException( "Premature EOF from inputStream");
      }
      toRead -= ret;
      off += ret;
    }
  }
  
  /**
   * Similar to readFully(). Skips bytes in a loop.
   * @param in The InputStream to skip bytes from
   * @param len number of bytes to skip.
   * @throws IOException if it could not skip requested number of bytes 
   * for any reason (including EOF)
   */
  public static void skipFully(@OsUntrusted InputStream in, @OsUntrusted long len) throws IOException {
    @OsUntrusted
    long amt = len;
    while (amt > 0) {
      @OsUntrusted
      long ret = in.skip(amt);
      if (ret == 0) {
        // skip may return 0 even if we're not at EOF.  Luckily, we can 
        // use the read() method to figure out if we're at the end.
        @OsUntrusted
        int b = in.read();
        if (b == -1) {
          throw new @OsUntrusted EOFException( "Premature EOF from inputStream after " +
              "skipping " + (len - amt) + " byte(s).");
        }
        ret = 1;
      }
      amt -= ret;
    }
  }
  
  /**
   * Close the Closeable objects and <b>ignore</b> any {@link IOException} or 
   * null pointers. Must only be used for cleanup in exception handlers.
   *
   * @param log the log to record problems to at debug level. Can be null.
   * @param closeables the objects to close
   */
  public static void cleanup(@OsUntrusted Log log, java.io.Closeable @OsUntrusted ... closeables) {
    for (java.io.Closeable c : closeables) {
      if (c != null) {
        try {
          c.close();
        } catch(@OsUntrusted Throwable e) {
          if (log != null && log.isDebugEnabled()) {
            log.debug("Exception in closing " + c, e);
          }
        }
      }
    }
  }

  /**
   * Closes the stream ignoring {@link IOException}.
   * Must only be called in cleaning up from exception handlers.
   *
   * @param stream the Stream to close
   */
  public static void closeStream(java.io.Closeable stream) {
    cleanup(null, stream);
  }
  
  /**
   * Closes the socket ignoring {@link IOException}
   *
   * @param sock the Socket to close
   */
  public static void closeSocket(@OsUntrusted Socket sock) {
    if (sock != null) {
      try {
        sock.close();
      } catch (@OsUntrusted IOException ignored) {
        LOG.debug("Ignoring exception while closing socket", ignored);
      }
    }
  }
  
  /**
   * The /dev/null of OutputStreams.
   */
  public static class NullOutputStream extends @OsUntrusted OutputStream {
    @Override
    public void write(IOUtils.@OsUntrusted NullOutputStream this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) throws IOException {
    }

    @Override
    public void write(IOUtils.@OsUntrusted NullOutputStream this, @OsUntrusted int b) throws IOException {
    }
  }  
  
  /**
   * Write a ByteBuffer to a WritableByteChannel, handling short writes.
   * 
   * @param bc               The WritableByteChannel to write to
   * @param buf              The input buffer
   * @throws IOException     On I/O error
   */
  public static void writeFully(@OsUntrusted WritableByteChannel bc, @OsUntrusted ByteBuffer buf)
      throws IOException {
    do {
      bc.write(buf);
    } while (buf.remaining() > 0);
  }

  /**
   * Write a ByteBuffer to a FileChannel at a given offset, 
   * handling short writes.
   * 
   * @param fc               The FileChannel to write to
   * @param buf              The input buffer
   * @param offset           The offset in the file to start writing at
   * @throws IOException     On I/O error
   */
  public static void writeFully(@OsUntrusted FileChannel fc, @OsUntrusted ByteBuffer buf,
      @OsUntrusted
      long offset) throws IOException {
    do {
      offset += fc.write(buf, offset);
    } while (buf.remaining() > 0);
  }
}
