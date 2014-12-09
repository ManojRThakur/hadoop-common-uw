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

package org.apache.hadoop.net;

import ostrusted.quals.OsUntrusted;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.WritableByteChannel;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * This implements an output stream that can have a timeout while writing.
 * This sets non-blocking flag on the socket channel.
 * So after creating this object , read() on 
 * {@link Socket#getInputStream()} and write() on 
 * {@link Socket#getOutputStream()} on the associated socket will throw 
 * llegalBlockingModeException.
 * Please use {@link SocketInputStream} for reading.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class SocketOutputStream extends @OsUntrusted OutputStream 
                                implements @OsUntrusted WritableByteChannel {                                
  
  private @OsUntrusted Writer writer;
  
  private static class Writer extends @OsUntrusted SocketIOWithTimeout {
    @OsUntrusted
    WritableByteChannel channel;
    
    @OsUntrusted
    Writer(@OsUntrusted WritableByteChannel channel, @OsUntrusted long timeout) throws IOException {
      super((@OsUntrusted SelectableChannel)channel, timeout);
      this.channel = channel;
    }
    
    @Override
    @OsUntrusted
    int performIO(SocketOutputStream.@OsUntrusted Writer this, @OsUntrusted ByteBuffer buf) throws IOException {
      return channel.write(buf);
    }
  }
  
  /**
   * Create a new ouput stream with the given timeout. If the timeout
   * is zero, it will be treated as infinite timeout. The socket's
   * channel will be configured to be non-blocking.
   * 
   * @param channel 
   *        Channel for writing, should also be a {@link SelectableChannel}.  
   *        The channel will be configured to be non-blocking.
   * @param timeout timeout in milliseconds. must not be negative.
   * @throws IOException
   */
  public @OsUntrusted SocketOutputStream(@OsUntrusted WritableByteChannel channel, @OsUntrusted long timeout) 
                                                         throws IOException {
    SocketIOWithTimeout.checkChannelValidity(channel);
    writer = new @OsUntrusted Writer(channel, timeout);
  }
  
  /**
   * Same as SocketOutputStream(socket.getChannel(), timeout):<br><br>
   * 
   * Create a new ouput stream with the given timeout. If the timeout
   * is zero, it will be treated as infinite timeout. The socket's
   * channel will be configured to be non-blocking.
   * 
   * @see SocketOutputStream#SocketOutputStream(WritableByteChannel, long)
   *  
   * @param socket should have a channel associated with it.
   * @param timeout timeout timeout in milliseconds. must not be negative.
   * @throws IOException
   */
  public @OsUntrusted SocketOutputStream(@OsUntrusted Socket socket, @OsUntrusted long timeout) 
                                         throws IOException {
    this(socket.getChannel(), timeout);
  }
  
  @Override
  public void write(@OsUntrusted SocketOutputStream this, @OsUntrusted int b) throws IOException {
    /* If we need to, we can optimize this allocation.
     * probably no need to optimize or encourage single byte writes.
     */
    @OsUntrusted
    byte @OsUntrusted [] buf = new @OsUntrusted byte @OsUntrusted [1];
    buf[0] = (@OsUntrusted byte)b;
    write(buf, 0, 1);
  }
  
  @Override
  public void write(@OsUntrusted SocketOutputStream this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) throws IOException {
    @OsUntrusted
    ByteBuffer buf = ByteBuffer.wrap(b, off, len);
    while (buf.hasRemaining()) {
      try {
        if (write(buf) < 0) {
          throw new @OsUntrusted IOException("The stream is closed");
        }
      } catch (@OsUntrusted IOException e) {
        /* Unlike read, write can not inform user of partial writes.
         * So will close this if there was a partial write.
         */
        if (buf.capacity() > buf.remaining()) {
          writer.close();
        }
        throw e;
      }
    }
  }

  @Override
  public synchronized void close(@OsUntrusted SocketOutputStream this) throws IOException {
    /* close the channel since Socket.getOuputStream().close() 
     * closes the socket.
     */
    writer.channel.close();
    writer.close();
  }

  /**
   * Returns underlying channel used by this stream.
   * This is useful in certain cases like channel for 
   * {@link FileChannel#transferTo(long, long, WritableByteChannel)}
   */
  public @OsUntrusted WritableByteChannel getChannel(@OsUntrusted SocketOutputStream this) {
    return writer.channel; 
  }

  //WritableByteChannle interface 
  
  @Override
  public @OsUntrusted boolean isOpen(@OsUntrusted SocketOutputStream this) {
    return writer.isOpen();
  }

  @Override
  public @OsUntrusted int write(@OsUntrusted SocketOutputStream this, @OsUntrusted ByteBuffer src) throws IOException {
    return writer.doIO(src, SelectionKey.OP_WRITE);
  }
  
  /**
   * waits for the underlying channel to be ready for writing.
   * The timeout specified for this stream applies to this wait.
   *
   * @throws SocketTimeoutException 
   *         if select on the channel times out.
   * @throws IOException
   *         if any other I/O error occurs. 
   */
  public void waitForWritable(@OsUntrusted SocketOutputStream this) throws IOException {
    writer.waitForIO(SelectionKey.OP_WRITE);
  }
  
  /**
   * Transfers data from FileChannel using 
   * {@link FileChannel#transferTo(long, long, WritableByteChannel)}.
   * Updates <code>waitForWritableTime</code> and <code>transferToTime</code>
   * with the time spent blocked on the network and the time spent transferring
   * data from disk to network respectively.
   * 
   * Similar to readFully(), this waits till requested amount of 
   * data is transfered.
   * 
   * @param fileCh FileChannel to transfer data from.
   * @param position position within the channel where the transfer begins
   * @param count number of bytes to transfer.
   * @param waitForWritableTime nanoseconds spent waiting for the socket 
   *        to become writable
   * @param transferTime nanoseconds spent transferring data
   * 
   * @throws EOFException 
   *         If end of input file is reached before requested number of 
   *         bytes are transfered.
   *
   * @throws SocketTimeoutException 
   *         If this channel blocks transfer longer than timeout for 
   *         this stream.
   *          
   * @throws IOException Includes any exception thrown by 
   *         {@link FileChannel#transferTo(long, long, WritableByteChannel)}. 
   */
  public void transferToFully(@OsUntrusted SocketOutputStream this, @OsUntrusted FileChannel fileCh, @OsUntrusted long position, @OsUntrusted int count,
      @OsUntrusted
      LongWritable waitForWritableTime,
      @OsUntrusted
      LongWritable transferToTime) throws IOException {
    @OsUntrusted
    long waitTime = 0;
    @OsUntrusted
    long transferTime = 0;
    while (count > 0) {
      /* 
       * Ideally we should wait after transferTo returns 0. But because of
       * a bug in JRE on Linux (http://bugs.sun.com/view_bug.do?bug_id=5103988),
       * which throws an exception instead of returning 0, we wait for the
       * channel to be writable before writing to it. If you ever see 
       * IOException with message "Resource temporarily unavailable" 
       * thrown here, please let us know.
       * 
       * Once we move to JAVA SE 7, wait should be moved to correct place.
       */
      @OsUntrusted
      long start = System.nanoTime();
      waitForWritable();
      @OsUntrusted
      long wait = System.nanoTime();

      @OsUntrusted
      int nTransfered = (@OsUntrusted int) fileCh.transferTo(position, count, getChannel());
      
      if (nTransfered == 0) {
        //check if end of file is reached.
        if (position >= fileCh.size()) {
          throw new @OsUntrusted EOFException("EOF Reached. file size is " + fileCh.size() + 
                                 " and " + count + " more bytes left to be " +
                                 "transfered.");
        }
        //otherwise assume the socket is full.
        //waitForWritable(); // see comment above.
      } else if (nTransfered < 0) {
        throw new @OsUntrusted IOException("Unexpected return of " + nTransfered + 
                              " from transferTo()");
      } else {
        position += nTransfered;
        count -= nTransfered;
      }
      @OsUntrusted
      long transfer = System.nanoTime();
      waitTime += wait - start;
      transferTime += transfer - wait;
    }
    
    if (waitForWritableTime != null) {
      waitForWritableTime.set(waitTime);
    }
    if (transferToTime != null) {
      transferToTime.set(transferTime);
    }
  }

  /**
   * Call
   * {@link #transferToFully(FileChannel, long, int, MutableRate, MutableRate)}
   * with null <code>waitForWritableTime</code> and <code>transferToTime</code>
   */
  public void transferToFully(@OsUntrusted SocketOutputStream this, @OsUntrusted FileChannel fileCh, @OsUntrusted long position, @OsUntrusted int count)
      throws IOException {
    transferToFully(fileCh, position, count, null, null);
  }

  public void setTimeout(@OsUntrusted SocketOutputStream this, @OsUntrusted int timeoutMs) {
    writer.setTimeout(timeoutMs);
  }
}
