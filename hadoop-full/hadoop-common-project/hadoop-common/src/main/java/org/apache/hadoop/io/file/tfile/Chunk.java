/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.io.file.tfile;

import ostrusted.quals.OsUntrusted;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Several related classes to support chunk-encoded sub-streams on top of a
 * regular stream.
 */
final class Chunk {

  /**
   * Prevent the instantiation of class.
   */
  private @OsUntrusted Chunk() {
    // nothing
  }

  /**
   * Decoding a chain of chunks encoded through ChunkEncoder or
   * SingleChunkEncoder.
   */
  static public class ChunkDecoder extends @OsUntrusted InputStream {
    private @OsUntrusted DataInputStream in = null;
    private @OsUntrusted boolean lastChunk;
    private @OsUntrusted int remain = 0;
    private @OsUntrusted boolean closed;

    public @OsUntrusted ChunkDecoder() {
      lastChunk = true;
      closed = true;
    }

    public void reset(Chunk.@OsUntrusted ChunkDecoder this, @OsUntrusted DataInputStream downStream) {
      // no need to wind forward the old input.
      in = downStream;
      lastChunk = false;
      remain = 0;
      closed = false;
    }

    /**
     * Constructor
     * 
     * @param in
     *          The source input stream which contains chunk-encoded data
     *          stream.
     */
    public @OsUntrusted ChunkDecoder(@OsUntrusted DataInputStream in) {
      this.in = in;
      lastChunk = false;
      closed = false;
    }

    /**
     * Have we reached the last chunk.
     * 
     * @return true if we have reached the last chunk.
     * @throws java.io.IOException
     */
    public @OsUntrusted boolean isLastChunk(Chunk.@OsUntrusted ChunkDecoder this) throws IOException {
      checkEOF();
      return lastChunk;
    }

    /**
     * How many bytes remain in the current chunk?
     * 
     * @return remaining bytes left in the current chunk.
     * @throws java.io.IOException
     */
    public @OsUntrusted int getRemain(Chunk.@OsUntrusted ChunkDecoder this) throws IOException {
      checkEOF();
      return remain;
    }

    /**
     * Reading the length of next chunk.
     * 
     * @throws java.io.IOException
     *           when no more data is available.
     */
    private void readLength(Chunk.@OsUntrusted ChunkDecoder this) throws IOException {
      remain = Utils.readVInt(in);
      if (remain >= 0) {
        lastChunk = true;
      } else {
        remain = -remain;
      }
    }

    /**
     * Check whether we reach the end of the stream.
     * 
     * @return false if the chunk encoded stream has more data to read (in which
     *         case available() will be greater than 0); true otherwise.
     * @throws java.io.IOException
     *           on I/O errors.
     */
    private @OsUntrusted boolean checkEOF(Chunk.@OsUntrusted ChunkDecoder this) throws IOException {
      if (isClosed()) return true;
      while (true) {
        if (remain > 0) return false;
        if (lastChunk) return true;
        readLength();
      }
    }

    @Override
    /*
     * This method never blocks the caller. Returning 0 does not mean we reach
     * the end of the stream.
     */
    public @OsUntrusted int available(Chunk.@OsUntrusted ChunkDecoder this) {
      return remain;
    }

    @Override
    public @OsUntrusted int read(Chunk.@OsUntrusted ChunkDecoder this) throws IOException {
      if (checkEOF()) return -1;
      @OsUntrusted
      int ret = in.read();
      if (ret < 0) throw new @OsUntrusted IOException("Corrupted chunk encoding stream");
      --remain;
      return ret;
    }

    @Override
    public @OsUntrusted int read(Chunk.@OsUntrusted ChunkDecoder this, @OsUntrusted byte @OsUntrusted [] b) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public @OsUntrusted int read(Chunk.@OsUntrusted ChunkDecoder this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int off, @OsUntrusted int len) throws IOException {
      if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
        throw new @OsUntrusted IndexOutOfBoundsException();
      }

      if (!checkEOF()) {
        @OsUntrusted
        int n = Math.min(remain, len);
        @OsUntrusted
        int ret = in.read(b, off, n);
        if (ret < 0) throw new @OsUntrusted IOException("Corrupted chunk encoding stream");
        remain -= ret;
        return ret;
      }
      return -1;
    }

    @Override
    public @OsUntrusted long skip(Chunk.@OsUntrusted ChunkDecoder this, @OsUntrusted long n) throws IOException {
      if (!checkEOF()) {
        @OsUntrusted
        long ret = in.skip(Math.min(remain, n));
        remain -= ret;
        return ret;
      }
      return 0;
    }

    @Override
    public @OsUntrusted boolean markSupported(Chunk.@OsUntrusted ChunkDecoder this) {
      return false;
    }

    public @OsUntrusted boolean isClosed(Chunk.@OsUntrusted ChunkDecoder this) {
      return closed;
    }

    @Override
    public void close(Chunk.@OsUntrusted ChunkDecoder this) throws IOException {
      if (closed == false) {
        try {
          while (!checkEOF()) {
            skip(Integer.MAX_VALUE);
          }
        } finally {
          closed = true;
        }
      }
    }
  }

  /**
   * Chunk Encoder. Encoding the output data into a chain of chunks in the
   * following sequences: -len1, byte[len1], -len2, byte[len2], ... len_n,
   * byte[len_n]. Where len1, len2, ..., len_n are the lengths of the data
   * chunks. Non-terminal chunks have their lengths negated. Non-terminal chunks
   * cannot have length 0. All lengths are in the range of 0 to
   * Integer.MAX_VALUE and are encoded in Utils.VInt format.
   */
  static public class ChunkEncoder extends @OsUntrusted OutputStream {
    /**
     * The data output stream it connects to.
     */
    private @OsUntrusted DataOutputStream out;

    /**
     * The internal buffer that is only used when we do not know the advertised
     * size.
     */
    private @OsUntrusted byte buf @OsUntrusted [];

    /**
     * The number of valid bytes in the buffer. This value is always in the
     * range <tt>0</tt> through <tt>buf.length</tt>; elements <tt>buf[0]</tt>
     * through <tt>buf[count-1]</tt> contain valid byte data.
     */
    private @OsUntrusted int count;

    /**
     * Constructor.
     * 
     * @param out
     *          the underlying output stream.
     * @param buf
     *          user-supplied buffer. The buffer would be used exclusively by
     *          the ChunkEncoder during its life cycle.
     */
    public @OsUntrusted ChunkEncoder(@OsUntrusted DataOutputStream out, @OsUntrusted byte @OsUntrusted [] buf) {
      this.out = out;
      this.buf = buf;
      this.count = 0;
    }

    /**
     * Write out a chunk.
     * 
     * @param chunk
     *          The chunk buffer.
     * @param offset
     *          Offset to chunk buffer for the beginning of chunk.
     * @param len
     * @param last
     *          Is this the last call to flushBuffer?
     */
    private void writeChunk(Chunk.@OsUntrusted ChunkEncoder this, @OsUntrusted byte @OsUntrusted [] chunk, @OsUntrusted int offset, @OsUntrusted int len, @OsUntrusted boolean last)
        throws IOException {
      if (last) { // always write out the length for the last chunk.
        Utils.writeVInt(out, len);
        if (len > 0) {
          out.write(chunk, offset, len);
        }
      } else {
        if (len > 0) {
          Utils.writeVInt(out, -len);
          out.write(chunk, offset, len);
        }
      }
    }

    /**
     * Write out a chunk that is a concatenation of the internal buffer plus
     * user supplied data. This will never be the last block.
     * 
     * @param data
     *          User supplied data buffer.
     * @param offset
     *          Offset to user data buffer.
     * @param len
     *          User data buffer size.
     */
    private void writeBufData(Chunk.@OsUntrusted ChunkEncoder this, @OsUntrusted byte @OsUntrusted [] data, @OsUntrusted int offset, @OsUntrusted int len)
        throws IOException {
      if (count + len > 0) {
        Utils.writeVInt(out, -(count + len));
        out.write(buf, 0, count);
        count = 0;
        out.write(data, offset, len);
      }
    }

    /**
     * Flush the internal buffer.
     * 
     * Is this the last call to flushBuffer?
     * 
     * @throws java.io.IOException
     */
    private void flushBuffer(Chunk.@OsUntrusted ChunkEncoder this) throws IOException {
      if (count > 0) {
        writeChunk(buf, 0, count, false);
        count = 0;
      }
    }

    @Override
    public void write(Chunk.@OsUntrusted ChunkEncoder this, @OsUntrusted int b) throws IOException {
      if (count >= buf.length) {
        flushBuffer();
      }
      buf[count++] = (@OsUntrusted byte) b;
    }

    @Override
    public void write(Chunk.@OsUntrusted ChunkEncoder this, @OsUntrusted byte b @OsUntrusted []) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(Chunk.@OsUntrusted ChunkEncoder this, @OsUntrusted byte b @OsUntrusted [], @OsUntrusted int off, @OsUntrusted int len) throws IOException {
      if ((len + count) >= buf.length) {
        /*
         * If the input data do not fit in buffer, flush the output buffer and
         * then write the data directly. In this way buffered streams will
         * cascade harmlessly.
         */
        writeBufData(b, off, len);
        return;
      }

      System.arraycopy(b, off, buf, count, len);
      count += len;
    }

    @Override
    public void flush(Chunk.@OsUntrusted ChunkEncoder this) throws IOException {
      flushBuffer();
      out.flush();
    }

    @Override
    public void close(Chunk.@OsUntrusted ChunkEncoder this) throws IOException {
      if (buf != null) {
        try {
          writeChunk(buf, 0, count, true);
        } finally {
          buf = null;
          out = null;
        }
      }
    }
  }

  /**
   * Encode the whole stream as a single chunk. Expecting to know the size of
   * the chunk up-front.
   */
  static public class SingleChunkEncoder extends @OsUntrusted OutputStream {
    /**
     * The data output stream it connects to.
     */
    private final @OsUntrusted DataOutputStream out;

    /**
     * The remaining bytes to be written.
     */
    private @OsUntrusted int remain;
    private @OsUntrusted boolean closed = false;

    /**
     * Constructor.
     * 
     * @param out
     *          the underlying output stream.
     * @param size
     *          The total # of bytes to be written as a single chunk.
     * @throws java.io.IOException
     *           if an I/O error occurs.
     */
    public @OsUntrusted SingleChunkEncoder(@OsUntrusted DataOutputStream out, @OsUntrusted int size)
        throws IOException {
      this.out = out;
      this.remain = size;
      Utils.writeVInt(out, size);
    }

    @Override
    public void write(Chunk.@OsUntrusted SingleChunkEncoder this, @OsUntrusted int b) throws IOException {
      if (remain > 0) {
        out.write(b);
        --remain;
      } else {
        throw new @OsUntrusted IOException("Writing more bytes than advertised size.");
      }
    }

    @Override
    public void write(Chunk.@OsUntrusted SingleChunkEncoder this, @OsUntrusted byte b @OsUntrusted []) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(Chunk.@OsUntrusted SingleChunkEncoder this, @OsUntrusted byte b @OsUntrusted [], @OsUntrusted int off, @OsUntrusted int len) throws IOException {
      if (remain >= len) {
        out.write(b, off, len);
        remain -= len;
      } else {
        throw new @OsUntrusted IOException("Writing more bytes than advertised size.");
      }
    }

    @Override
    public void flush(Chunk.@OsUntrusted SingleChunkEncoder this) throws IOException {
      out.flush();
    }

    @Override
    public void close(Chunk.@OsUntrusted SingleChunkEncoder this) throws IOException {
      if (closed == true) {
        return;
      }

      try {
        if (remain > 0) {
          throw new @OsUntrusted IOException("Writing less bytes than advertised size.");
        }
      } finally {
        closed = true;
      }
    }
  }
}
