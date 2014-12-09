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

package org.apache.hadoop.security;

import ostrusted.quals.OsUntrusted;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A SaslInputStream is composed of an InputStream and a SaslServer (or
 * SaslClient) so that read() methods return data that are read in from the
 * underlying InputStream but have been additionally processed by the SaslServer
 * (or SaslClient) object. The SaslServer (or SaslClient) object must be fully
 * initialized before being used by a SaslInputStream.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SaslInputStream extends @OsUntrusted InputStream implements @OsUntrusted ReadableByteChannel {
  public static final @OsUntrusted Log LOG = LogFactory.getLog(SaslInputStream.class);

  private final @OsUntrusted DataInputStream inStream;
  /** Should we wrap the communication channel? */
  private final @OsUntrusted boolean useWrap;
  
  /*
   * data read from the underlying input stream before being processed by SASL
   */
  private @OsUntrusted byte @OsUntrusted [] saslToken;
  private final @OsUntrusted SaslClient saslClient;
  private final @OsUntrusted SaslServer saslServer;
  private @OsUntrusted byte @OsUntrusted [] lengthBuf = new @OsUntrusted byte @OsUntrusted [4];
  /*
   * buffer holding data that have been processed by SASL, but have not been
   * read out
   */
  private @OsUntrusted byte @OsUntrusted [] obuffer;
  // position of the next "new" byte
  private @OsUntrusted int ostart = 0;
  // position of the last "new" byte
  private @OsUntrusted int ofinish = 0;
  // whether or not this stream is open
  private @OsUntrusted boolean isOpen = true;

  private static @OsUntrusted int unsignedBytesToInt(@OsUntrusted byte @OsUntrusted [] buf) {
    if (buf.length != 4) {
      throw new @OsUntrusted IllegalArgumentException(
          "Cannot handle byte array other than 4 bytes");
    }
    @OsUntrusted
    int result = 0;
    for (@OsUntrusted int i = 0; i < 4; i++) {
      result <<= 8;
      result |= ((@OsUntrusted int) buf[i] & 0xff);
    }
    return result;
  }

  /**
   * Read more data and get them processed <br>
   * Entry condition: ostart = ofinish <br>
   * Exit condition: ostart <= ofinish <br>
   * 
   * return (ofinish-ostart) (we have this many bytes for you), 0 (no data now,
   * but could have more later), or -1 (absolutely no more data)
   */
  private @OsUntrusted int readMoreData(@OsUntrusted SaslInputStream this) throws IOException {
    try {
      inStream.readFully(lengthBuf);
      @OsUntrusted
      int length = unsignedBytesToInt(lengthBuf);
      if (LOG.isDebugEnabled())
        LOG.debug("Actual length is " + length);
      saslToken = new @OsUntrusted byte @OsUntrusted [length];
      inStream.readFully(saslToken);
    } catch (@OsUntrusted EOFException e) {
      return -1;
    }
    try {
      if (saslServer != null) { // using saslServer
        obuffer = saslServer.unwrap(saslToken, 0, saslToken.length);
      } else { // using saslClient
        obuffer = saslClient.unwrap(saslToken, 0, saslToken.length);
      }
    } catch (@OsUntrusted SaslException se) {
      try {
        disposeSasl();
      } catch (@OsUntrusted SaslException ignored) {
      }
      throw se;
    }
    ostart = 0;
    if (obuffer == null)
      ofinish = 0;
    else
      ofinish = obuffer.length;
    return ofinish;
  }

  /**
   * Disposes of any system resources or security-sensitive information Sasl
   * might be using.
   * 
   * @exception SaslException
   *              if a SASL error occurs.
   */
  private void disposeSasl(@OsUntrusted SaslInputStream this) throws SaslException {
    if (saslClient != null) {
      saslClient.dispose();
    }
    if (saslServer != null) {
      saslServer.dispose();
    }
  }

  /**
   * Constructs a SASLInputStream from an InputStream and a SaslServer <br>
   * Note: if the specified InputStream or SaslServer is null, a
   * NullPointerException may be thrown later when they are used.
   * 
   * @param inStream
   *          the InputStream to be processed
   * @param saslServer
   *          an initialized SaslServer object
   */
  public @OsUntrusted SaslInputStream(@OsUntrusted InputStream inStream, @OsUntrusted SaslServer saslServer) {
    this.inStream = new @OsUntrusted DataInputStream(inStream);
    this.saslServer = saslServer;
    this.saslClient = null;
    @OsUntrusted
    String qop = (@OsUntrusted String) saslServer.getNegotiatedProperty(Sasl.QOP);
    this.useWrap = qop != null && !"auth".equalsIgnoreCase(qop);
  }

  /**
   * Constructs a SASLInputStream from an InputStream and a SaslClient <br>
   * Note: if the specified InputStream or SaslClient is null, a
   * NullPointerException may be thrown later when they are used.
   * 
   * @param inStream
   *          the InputStream to be processed
   * @param saslClient
   *          an initialized SaslClient object
   */
  public @OsUntrusted SaslInputStream(@OsUntrusted InputStream inStream, @OsUntrusted SaslClient saslClient) {
    this.inStream = new @OsUntrusted DataInputStream(inStream);
    this.saslServer = null;
    this.saslClient = saslClient;
    @OsUntrusted
    String qop = (@OsUntrusted String) saslClient.getNegotiatedProperty(Sasl.QOP);
    this.useWrap = qop != null && !"auth".equalsIgnoreCase(qop);
  }

  /**
   * Reads the next byte of data from this input stream. The value byte is
   * returned as an <code>int</code> in the range <code>0</code> to
   * <code>255</code>. If no byte is available because the end of the stream has
   * been reached, the value <code>-1</code> is returned. This method blocks
   * until input data is available, the end of the stream is detected, or an
   * exception is thrown.
   * <p>
   * 
   * @return the next byte of data, or <code>-1</code> if the end of the stream
   *         is reached.
   * @exception IOException
   *              if an I/O error occurs.
   */
  @Override
  public @OsUntrusted int read(@OsUntrusted SaslInputStream this) throws IOException {
    if (!useWrap) {
      return inStream.read();
    }
    if (ostart >= ofinish) {
      // we loop for new data as we are blocking
      @OsUntrusted
      int i = 0;
      while (i == 0)
        i = readMoreData();
      if (i == -1)
        return -1;
    }
    return ((@OsUntrusted int) obuffer[ostart++] & 0xff);
  }

  /**
   * Reads up to <code>b.length</code> bytes of data from this input stream into
   * an array of bytes.
   * <p>
   * The <code>read</code> method of <code>InputStream</code> calls the
   * <code>read</code> method of three arguments with the arguments
   * <code>b</code>, <code>0</code>, and <code>b.length</code>.
   * 
   * @param b
   *          the buffer into which the data is read.
   * @return the total number of bytes read into the buffer, or <code>-1</code>
   *         is there is no more data because the end of the stream has been
   *         reached.
   * @exception IOException
   *              if an I/O error occurs.
   */
  @Override
  public @OsUntrusted int read(@OsUntrusted SaslInputStream this, @OsUntrusted byte @OsUntrusted [] b) throws IOException {
    return read(b, 0, b.length);
  }

  /**
   * Reads up to <code>len</code> bytes of data from this input stream into an
   * array of bytes. This method blocks until some input is available. If the
   * first argument is <code>null,</code> up to <code>len</code> bytes are read
   * and discarded.
   * 
   * @param b
   *          the buffer into which the data is read.
   * @param off
   *          the start offset of the data.
   * @param len
   *          the maximum number of bytes read.
   * @return the total number of bytes read into the buffer, or <code>-1</code>
   *         if there is no more data because the end of the stream has been
   *         reached.
   * @exception IOException
   *              if an I/O error occurs.
   */
  @Override
  public @OsUntrusted int read(@OsUntrusted SaslInputStream this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) throws IOException {
    if (!useWrap) {
      return inStream.read(b, off, len);
    }
    if (ostart >= ofinish) {
      // we loop for new data as we are blocking
      @OsUntrusted
      int i = 0;
      while (i == 0)
        i = readMoreData();
      if (i == -1)
        return -1;
    }
    if (len <= 0) {
      return 0;
    }
    @OsUntrusted
    int available = ofinish - ostart;
    if (len < available)
      available = len;
    if (b != null) {
      System.arraycopy(obuffer, ostart, b, off, available);
    }
    ostart = ostart + available;
    return available;
  }

  /**
   * Skips <code>n</code> bytes of input from the bytes that can be read from
   * this input stream without blocking.
   * 
   * <p>
   * Fewer bytes than requested might be skipped. The actual number of bytes
   * skipped is equal to <code>n</code> or the result of a call to
   * {@link #available() <code>available</code>}, whichever is smaller. If
   * <code>n</code> is less than zero, no bytes are skipped.
   * 
   * <p>
   * The actual number of bytes skipped is returned.
   * 
   * @param n
   *          the number of bytes to be skipped.
   * @return the actual number of bytes skipped.
   * @exception IOException
   *              if an I/O error occurs.
   */
  @Override
  public @OsUntrusted long skip(@OsUntrusted SaslInputStream this, @OsUntrusted long n) throws IOException {
    if (!useWrap) {
      return inStream.skip(n);
    }
    @OsUntrusted
    int available = ofinish - ostart;
    if (n > available) {
      n = available;
    }
    if (n < 0) {
      return 0;
    }
    ostart += n;
    return n;
  }

  /**
   * Returns the number of bytes that can be read from this input stream without
   * blocking. The <code>available</code> method of <code>InputStream</code>
   * returns <code>0</code>. This method <B>should</B> be overridden by
   * subclasses.
   * 
   * @return the number of bytes that can be read from this input stream without
   *         blocking.
   * @exception IOException
   *              if an I/O error occurs.
   */
  @Override
  public @OsUntrusted int available(@OsUntrusted SaslInputStream this) throws IOException {
    if (!useWrap) {
      return inStream.available();
    }
    return (ofinish - ostart);
  }

  /**
   * Closes this input stream and releases any system resources associated with
   * the stream.
   * <p>
   * The <code>close</code> method of <code>SASLInputStream</code> calls the
   * <code>close</code> method of its underlying input stream.
   * 
   * @exception IOException
   *              if an I/O error occurs.
   */
  @Override
  public void close(@OsUntrusted SaslInputStream this) throws IOException {
    disposeSasl();
    ostart = 0;
    ofinish = 0;
    inStream.close();
    isOpen = false;
  }

  /**
   * Tests if this input stream supports the <code>mark</code> and
   * <code>reset</code> methods, which it does not.
   * 
   * @return <code>false</code>, since this class does not support the
   *         <code>mark</code> and <code>reset</code> methods.
   */
  @Override
  public @OsUntrusted boolean markSupported(@OsUntrusted SaslInputStream this) {
    return false;
  }
  
  @Override
  public @OsUntrusted boolean isOpen(@OsUntrusted SaslInputStream this) {
    return isOpen;
  }

  @Override
  public @OsUntrusted int read(@OsUntrusted SaslInputStream this, @OsUntrusted ByteBuffer dst) throws IOException {
    @OsUntrusted
    int bytesRead = 0;
    if (dst.hasArray()) {
      bytesRead = read(dst.array(), dst.arrayOffset() + dst.position(),
          dst.remaining());
      if (bytesRead > -1) {
        dst.position(dst.position() + bytesRead);
      }
    } else {
      @OsUntrusted
      byte @OsUntrusted [] buf = new @OsUntrusted byte @OsUntrusted [dst.remaining()];
      bytesRead = read(buf);
      if (bytesRead > -1) {
        dst.put(buf, 0, bytesRead);
      }
    }
    return bytesRead;
  }
}