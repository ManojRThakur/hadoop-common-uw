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
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Time;

/**
 * This supports input and output streams for a socket channels. 
 * These streams can have a timeout.
 */
abstract class SocketIOWithTimeout {
  // This is intentionally package private.

  static final @OsUntrusted Log LOG = LogFactory.getLog(SocketIOWithTimeout.class);    
  
  private @OsUntrusted SelectableChannel channel;
  private @OsUntrusted long timeout;
  private @OsUntrusted boolean closed = false;
  
  private static @OsUntrusted SelectorPool selector = new @OsUntrusted SelectorPool();
  
  /* A timeout value of 0 implies wait for ever. 
   * We should have a value of timeout that implies zero wait.. i.e. 
   * read or write returns immediately.
   * 
   * This will set channel to non-blocking.
   */
  @OsUntrusted
  SocketIOWithTimeout(@OsUntrusted SelectableChannel channel, @OsUntrusted long timeout) 
                                                 throws IOException {
    checkChannelValidity(channel);
    
    this.channel = channel;
    this.timeout = timeout;
    // Set non-blocking
    channel.configureBlocking(false);
  }
  
  void close(@OsUntrusted SocketIOWithTimeout this) {
    closed = true;
  }

  @OsUntrusted
  boolean isOpen(@OsUntrusted SocketIOWithTimeout this) {
    return !closed && channel.isOpen();
  }

  @OsUntrusted
  SelectableChannel getChannel(@OsUntrusted SocketIOWithTimeout this) {
    return channel;
  }
  
  /** 
   * Utility function to check if channel is ok.
   * Mainly to throw IOException instead of runtime exception
   * in case of mismatch. This mismatch can occur for many runtime
   * reasons.
   */
  static void checkChannelValidity(@OsUntrusted Object channel) throws IOException {
    if (channel == null) {
      /* Most common reason is that original socket does not have a channel.
       * So making this an IOException rather than a RuntimeException.
       */
      throw new @OsUntrusted IOException("Channel is null. Check " +
                            "how the channel or socket is created.");
    }
    
    if (!(channel instanceof @OsUntrusted SelectableChannel)) {
      throw new @OsUntrusted IOException("Channel should be a SelectableChannel");
    }    
  }
  
  /**
   * Performs actual IO operations. This is not expected to block.
   *  
   * @param buf
   * @return number of bytes (or some equivalent). 0 implies underlying
   *         channel is drained completely. We will wait if more IO is 
   *         required.
   * @throws IOException
   */
  abstract @OsUntrusted int performIO(@OsUntrusted SocketIOWithTimeout this, @OsUntrusted ByteBuffer buf) throws IOException;  
  
  /**
   * Performs one IO and returns number of bytes read or written.
   * It waits up to the specified timeout. If the channel is 
   * not read before the timeout, SocketTimeoutException is thrown.
   * 
   * @param buf buffer for IO
   * @param ops Selection Ops used for waiting. Suggested values: 
   *        SelectionKey.OP_READ while reading and SelectionKey.OP_WRITE while
   *        writing. 
   *        
   * @return number of bytes read or written. negative implies end of stream.
   * @throws IOException
   */
  @OsUntrusted
  int doIO(@OsUntrusted SocketIOWithTimeout this, @OsUntrusted ByteBuffer buf, @OsUntrusted int ops) throws IOException {
    
    /* For now only one thread is allowed. If user want to read or write
     * from multiple threads, multiple streams could be created. In that
     * case multiple threads work as well as underlying channel supports it.
     */
    if (!buf.hasRemaining()) {
      throw new @OsUntrusted IllegalArgumentException("Buffer has no data left.");
      //or should we just return 0?
    }

    while (buf.hasRemaining()) {
      if (closed) {
        return -1;
      }

      try {
        @OsUntrusted
        int n = performIO(buf);
        if (n != 0) {
          // successful io or an error.
          return n;
        }
      } catch (@OsUntrusted IOException e) {
        if (!channel.isOpen()) {
          closed = true;
        }
        throw e;
      }

      //now wait for socket to be ready.
      @OsUntrusted
      int count = 0;
      try {
        count = selector.select(channel, ops, timeout);  
      } catch (@OsUntrusted IOException e) { //unexpected IOException.
        closed = true;
        throw e;
      } 

      if (count == 0) {
        throw new @OsUntrusted SocketTimeoutException(timeoutExceptionString(channel,
                                                                timeout, ops));
      }
      // otherwise the socket should be ready for io.
    }
    
    return 0; // does not reach here.
  }
  
  /**
   * The contract is similar to {@link SocketChannel#connect(SocketAddress)} 
   * with a timeout.
   * 
   * @see SocketChannel#connect(SocketAddress)
   * 
   * @param channel - this should be a {@link SelectableChannel}
   * @param endpoint
   * @throws IOException
   */
  static void connect(@OsUntrusted SocketChannel channel, 
                      @OsUntrusted
                      SocketAddress endpoint, @OsUntrusted int timeout) throws IOException {
    
    @OsUntrusted
    boolean blockingOn = channel.isBlocking();
    if (blockingOn) {
      channel.configureBlocking(false);
    }
    
    try { 
      if (channel.connect(endpoint)) {
        return;
      }

      @OsUntrusted
      long timeoutLeft = timeout;
      @OsUntrusted
      long endTime = (timeout > 0) ? (Time.now() + timeout): 0;
      
      while (true) {
        // we might have to call finishConnect() more than once
        // for some channels (with user level protocols)
        
        @OsUntrusted
        int ret = selector.select((@OsUntrusted SelectableChannel)channel, 
                                  SelectionKey.OP_CONNECT, timeoutLeft);
        
        if (ret > 0 && channel.finishConnect()) {
          return;
        }
        
        if (ret == 0 ||
            (timeout > 0 &&  
              (timeoutLeft = (endTime - Time.now())) <= 0)) {
          throw new @OsUntrusted SocketTimeoutException(
                    timeoutExceptionString(channel, timeout, 
                                           SelectionKey.OP_CONNECT));
        }
      }
    } catch (@OsUntrusted IOException e) {
      // javadoc for SocketChannel.connect() says channel should be closed.
      try {
        channel.close();
      } catch (@OsUntrusted IOException ignored) {}
      throw e;
    } finally {
      if (blockingOn && channel.isOpen()) {
        channel.configureBlocking(true);
      }
    }
  }

  /**
   * This is similar to {@link #doIO(ByteBuffer, int)} except that it
   * does not perform any I/O. It just waits for the channel to be ready
   * for I/O as specified in ops.
   * 
   * @param ops Selection Ops used for waiting
   * 
   * @throws SocketTimeoutException 
   *         if select on the channel times out.
   * @throws IOException
   *         if any other I/O error occurs. 
   */
  void waitForIO(@OsUntrusted SocketIOWithTimeout this, @OsUntrusted int ops) throws IOException {
    
    if (selector.select(channel, ops, timeout) == 0) {
      throw new @OsUntrusted SocketTimeoutException(timeoutExceptionString(channel, timeout,
                                                              ops)); 
    }
  }

  public void setTimeout(@OsUntrusted SocketIOWithTimeout this, @OsUntrusted long timeoutMs) {
    this.timeout = timeoutMs;
  }
    
  private static @OsUntrusted String timeoutExceptionString(@OsUntrusted SelectableChannel channel,
                                               @OsUntrusted
                                               long timeout, @OsUntrusted int ops) {
    
    @OsUntrusted
    String waitingFor;
    switch(ops) {
    
    case SelectionKey.OP_READ :
      waitingFor = "read"; break;
      
    case SelectionKey.OP_WRITE :
      waitingFor = "write"; break;      
      
    case SelectionKey.OP_CONNECT :
      waitingFor = "connect"; break;
      
    default :
      waitingFor = "" + ops;  
    }
    
    return timeout + " millis timeout while " +
           "waiting for channel to be ready for " + 
           waitingFor + ". ch : " + channel;    
  }
  
  /**
   * This maintains a pool of selectors. These selectors are closed
   * once they are idle (unused) for a few seconds.
   */
  private static class SelectorPool {
    
    private static class SelectorInfo {
      @OsUntrusted
      Selector              selector;
      @OsUntrusted
      long                  lastActivityTime;
      @OsUntrusted
      LinkedList<@OsUntrusted SelectorInfo> queue; 
      
      void close(SocketIOWithTimeout.SelectorPool.@OsUntrusted SelectorInfo this) {
        if (selector != null) {
          try {
            selector.close();
          } catch (@OsUntrusted IOException e) {
            LOG.warn("Unexpected exception while closing selector : ", e);
          }
        }
      }    
    }
    
    private static class ProviderInfo {
      @OsUntrusted
      SelectorProvider provider;
      @OsUntrusted
      LinkedList<@OsUntrusted SelectorInfo> queue; // lifo
      @OsUntrusted
      ProviderInfo next;
    }
    
    private static final @OsUntrusted long IDLE_TIMEOUT = 10 * 1000; // 10 seconds.
    
    private @OsUntrusted ProviderInfo providerList = null;
    
    /**
     * Waits on the channel with the given timeout using one of the 
     * cached selectors. It also removes any cached selectors that are
     * idle for a few seconds.
     * 
     * @param channel
     * @param ops
     * @param timeout
     * @return
     * @throws IOException
     */
    @OsUntrusted
    int select(SocketIOWithTimeout.@OsUntrusted SelectorPool this, @OsUntrusted SelectableChannel channel, @OsUntrusted int ops, @OsUntrusted long timeout) 
                                                   throws IOException {
     
      @OsUntrusted
      SelectorInfo info = get(channel);
      
      @OsUntrusted
      SelectionKey key = null;
      @OsUntrusted
      int ret = 0;
      
      try {
        while (true) {
          @OsUntrusted
          long start = (timeout == 0) ? 0 : Time.now();

          key = channel.register(info.selector, ops);
          ret = info.selector.select(timeout);
          
          if (ret != 0) {
            return ret;
          }
          
          /* Sometimes select() returns 0 much before timeout for 
           * unknown reasons. So select again if required.
           */
          if (timeout > 0) {
            timeout -= Time.now() - start;
            if (timeout <= 0) {
              return 0;
            }
          }
          
          if (Thread.currentThread().isInterrupted()) {
            throw new @OsUntrusted InterruptedIOException("Interruped while waiting for " +
                                             "IO on channel " + channel +
                                             ". " + timeout + 
                                             " millis timeout left.");
          }
        }
      } finally {
        if (key != null) {
          key.cancel();
        }
        
        //clear the canceled key.
        try {
          info.selector.selectNow();
        } catch (@OsUntrusted IOException e) {
          LOG.info("Unexpected Exception while clearing selector : ", e);
          // don't put the selector back.
          info.close();
          return ret; 
        }
        
        release(info);
      }
    }
    
    /**
     * Takes one selector from end of LRU list of free selectors.
     * If there are no selectors awailable, it creates a new selector.
     * Also invokes trimIdleSelectors(). 
     * 
     * @param channel
     * @return 
     * @throws IOException
     */
    private synchronized @OsUntrusted SelectorInfo get(SocketIOWithTimeout.@OsUntrusted SelectorPool this, @OsUntrusted SelectableChannel channel) 
                                                         throws IOException {
      @OsUntrusted
      SelectorInfo selInfo = null;
      
      @OsUntrusted
      SelectorProvider provider = channel.provider();
      
      // pick the list : rarely there is more than one provider in use.
      @OsUntrusted
      ProviderInfo pList = providerList;
      while (pList != null && pList.provider != provider) {
        pList = pList.next;
      }      
      if (pList == null) {
        //LOG.info("Creating new ProviderInfo : " + provider.toString());
        pList = new @OsUntrusted ProviderInfo();
        pList.provider = provider;
        pList.queue = new @OsUntrusted LinkedList<@OsUntrusted SelectorInfo>();
        pList.next = providerList;
        providerList = pList;
      }
      
      @OsUntrusted
      LinkedList<@OsUntrusted SelectorInfo> queue = pList.queue;
      
      if (queue.isEmpty()) {
        @OsUntrusted
        Selector selector = provider.openSelector();
        selInfo = new @OsUntrusted SelectorInfo();
        selInfo.selector = selector;
        selInfo.queue = queue;
      } else {
        selInfo = queue.removeLast();
      }
      
      trimIdleSelectors(Time.now());
      return selInfo;
    }
    
    /**
     * puts selector back at the end of LRU list of free selectos.
     * Also invokes trimIdleSelectors().
     * 
     * @param info
     */
    private synchronized void release(SocketIOWithTimeout.@OsUntrusted SelectorPool this, @OsUntrusted SelectorInfo info) {
      @OsUntrusted
      long now = Time.now();
      trimIdleSelectors(now);
      info.lastActivityTime = now;
      info.queue.addLast(info);
    }
    
    /**
     * Closes selectors that are idle for IDLE_TIMEOUT (10 sec). It does not
     * traverse the whole list, just over the one that have crossed 
     * the timeout.
     */
    private void trimIdleSelectors(SocketIOWithTimeout.@OsUntrusted SelectorPool this, @OsUntrusted long now) {
      @OsUntrusted
      long cutoff = now - IDLE_TIMEOUT;
      
      for(@OsUntrusted ProviderInfo pList=providerList; pList != null; pList=pList.next) {
        if (pList.queue.isEmpty()) {
          continue;
        }
        for(@OsUntrusted Iterator<@OsUntrusted SelectorInfo> it = pList.queue.iterator(); it.hasNext();) {
          @OsUntrusted
          SelectorInfo info = it.next();
          if (info.lastActivityTime > cutoff) {
            break;
          }
          it.remove();
          info.close();
        }
      }
    }
  }
}
