/**
 * 
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
import java.io.*;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.fs.ByteBufferUtil;
import org.apache.hadoop.util.IdentityHashStore;

/** Utility that wraps a {@link FSInputStream} in a {@link DataInputStream}
 * and buffers input through a {@link BufferedInputStream}. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FSDataInputStream extends @OsUntrusted DataInputStream
    implements @OsUntrusted Seekable, @OsUntrusted PositionedReadable, @OsUntrusted Closeable, 
      @OsUntrusted
      ByteBufferReadable, @OsUntrusted HasFileDescriptor, @OsUntrusted CanSetDropBehind, @OsUntrusted CanSetReadahead,
      @OsUntrusted
      HasEnhancedByteBufferAccess {
  /**
   * Map ByteBuffers that we have handed out to readers to ByteBufferPool 
   * objects
   */
  private final @OsUntrusted IdentityHashStore<@OsUntrusted ByteBuffer, @OsUntrusted ByteBufferPool>
    extendedReadBuffers
      = new @OsUntrusted IdentityHashStore<@OsUntrusted ByteBuffer, @OsUntrusted ByteBufferPool>(0);

  public @OsUntrusted FSDataInputStream(@OsUntrusted InputStream in)
    throws IOException {
    super(in);
    if( !(in instanceof @OsUntrusted Seekable) || !(in instanceof @OsUntrusted PositionedReadable) ) {
      throw new @OsUntrusted IllegalArgumentException(
          "In is not an instance of Seekable or PositionedReadable");
    }
  }
  
  /**
   * Seek to the given offset.
   *
   * @param desired offset to seek to
   */
  @Override
  public synchronized void seek(@OsUntrusted FSDataInputStream this, @OsUntrusted long desired) throws IOException {
    ((@OsUntrusted Seekable)in).seek(desired);
  }

  /**
   * Get the current position in the input stream.
   *
   * @return current position in the input stream
   */
  @Override
  public @OsUntrusted long getPos(@OsUntrusted FSDataInputStream this) throws IOException {
    return ((@OsUntrusted Seekable)in).getPos();
  }
  
  /**
   * Read bytes from the given position in the stream to the given buffer.
   *
   * @param position  position in the input stream to seek
   * @param buffer    buffer into which data is read
   * @param offset    offset into the buffer in which data is written
   * @param length    maximum number of bytes to read
   * @return total number of bytes read into the buffer, or <code>-1</code>
   *         if there is no more data because the end of the stream has been
   *         reached
   */
  @Override
  public @OsUntrusted int read(@OsUntrusted FSDataInputStream this, @OsUntrusted long position, @OsUntrusted byte @OsUntrusted [] buffer, @OsUntrusted int offset, @OsUntrusted int length)
    throws IOException {
    return ((@OsUntrusted PositionedReadable)in).read(position, buffer, offset, length);
  }

  /**
   * Read bytes from the given position in the stream to the given buffer.
   * Continues to read until <code>length</code> bytes have been read.
   *
   * @param position  position in the input stream to seek
   * @param buffer    buffer into which data is read
   * @param offset    offset into the buffer in which data is written
   * @param length    the number of bytes to read
   * @throws EOFException If the end of stream is reached while reading.
   *                      If an exception is thrown an undetermined number
   *                      of bytes in the buffer may have been written. 
   */
  @Override
  public void readFully(@OsUntrusted FSDataInputStream this, @OsUntrusted long position, @OsUntrusted byte @OsUntrusted [] buffer, @OsUntrusted int offset, @OsUntrusted int length)
    throws IOException {
    ((@OsUntrusted PositionedReadable)in).readFully(position, buffer, offset, length);
  }
  
  /**
   * See {@link #readFully(long, byte[], int, int)}.
   */
  @Override
  public void readFully(@OsUntrusted FSDataInputStream this, @OsUntrusted long position, @OsUntrusted byte @OsUntrusted [] buffer)
    throws IOException {
    ((@OsUntrusted PositionedReadable)in).readFully(position, buffer, 0, buffer.length);
  }
  
  /**
   * Seek to the given position on an alternate copy of the data.
   *
   * @param  targetPos  position to seek to
   * @return true if a new source is found, false otherwise
   */
  @Override
  public @OsUntrusted boolean seekToNewSource(@OsUntrusted FSDataInputStream this, @OsUntrusted long targetPos) throws IOException {
    return ((@OsUntrusted Seekable)in).seekToNewSource(targetPos); 
  }
  
  /**
   * Get a reference to the wrapped input stream. Used by unit tests.
   *
   * @return the underlying input stream
   */
  @InterfaceAudience.LimitedPrivate({"HDFS"})
  public @OsUntrusted InputStream getWrappedStream(@OsUntrusted FSDataInputStream this) {
    return in;
  }

  @Override
  public @OsUntrusted int read(@OsUntrusted FSDataInputStream this, @OsUntrusted ByteBuffer buf) throws IOException {
    if (in instanceof @OsUntrusted ByteBufferReadable) {
      return ((@OsUntrusted ByteBufferReadable)in).read(buf);
    }

    throw new @OsUntrusted UnsupportedOperationException("Byte-buffer read unsupported by input stream");
  }

  @Override
  public @OsUntrusted FileDescriptor getFileDescriptor(@OsUntrusted FSDataInputStream this) throws IOException {
    if (in instanceof @OsUntrusted HasFileDescriptor) {
      return ((@OsUntrusted HasFileDescriptor) in).getFileDescriptor();
    } else if (in instanceof @OsUntrusted FileInputStream) {
      return ((@OsUntrusted FileInputStream) in).getFD();
    } else {
      return null;
    }
  }

  @Override
  public void setReadahead(@OsUntrusted FSDataInputStream this, @OsUntrusted Long readahead)
      throws IOException, UnsupportedOperationException {
    try {
      ((@OsUntrusted CanSetReadahead)in).setReadahead(readahead);
    } catch (@OsUntrusted ClassCastException e) {
      throw new @OsUntrusted UnsupportedOperationException(
          "this stream does not support setting the readahead " +
          "caching strategy.");
    }
  }

  @Override
  public void setDropBehind(@OsUntrusted FSDataInputStream this, @OsUntrusted Boolean dropBehind)
      throws IOException, UnsupportedOperationException {
    try {
      ((@OsUntrusted CanSetDropBehind)in).setDropBehind(dropBehind);
    } catch (@OsUntrusted ClassCastException e) {
      throw new @OsUntrusted UnsupportedOperationException("this stream does not " +
          "support setting the drop-behind caching setting.");
    }
  }

  @Override
  public @OsUntrusted ByteBuffer read(@OsUntrusted FSDataInputStream this, @OsUntrusted ByteBufferPool bufferPool, @OsUntrusted int maxLength,
      @OsUntrusted
      EnumSet<@OsUntrusted ReadOption> opts) 
          throws IOException, UnsupportedOperationException {
    try {
      return ((@OsUntrusted HasEnhancedByteBufferAccess)in).read(bufferPool,
          maxLength, opts);
    }
    catch (@OsUntrusted ClassCastException e) {
      @OsUntrusted
      ByteBuffer buffer = ByteBufferUtil.
          fallbackRead(this, bufferPool, maxLength);
      if (buffer != null) {
        extendedReadBuffers.put(buffer, bufferPool);
      }
      return buffer;
    }
  }

  private static final @OsUntrusted EnumSet<@OsUntrusted ReadOption> EMPTY_READ_OPTIONS_SET =
      EnumSet.noneOf(ReadOption.class);

  final public @OsUntrusted ByteBuffer read(@OsUntrusted FSDataInputStream this, @OsUntrusted ByteBufferPool bufferPool, @OsUntrusted int maxLength)
          throws IOException, UnsupportedOperationException {
    return read(bufferPool, maxLength, EMPTY_READ_OPTIONS_SET);
  }
  
  @Override
  public void releaseBuffer(@OsUntrusted FSDataInputStream this, @OsUntrusted ByteBuffer buffer) {
    try {
      ((@OsUntrusted HasEnhancedByteBufferAccess)in).releaseBuffer(buffer);
    }
    catch (@OsUntrusted ClassCastException e) {
      @OsUntrusted
      ByteBufferPool bufferPool = extendedReadBuffers.remove( buffer);
      if (bufferPool == null) {
        throw new @OsUntrusted IllegalArgumentException("tried to release a buffer " +
            "that was not created by this stream.");
      }
      bufferPool.putBuffer(buffer);
    }
  }
}
