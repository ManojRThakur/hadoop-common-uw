/* Generated By:JavaCC: Do not edit this line. SimpleCharStream.java Version 4.0 */
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

package org.apache.hadoop.record.compiler.generated;

import ostrusted.quals.OsUntrusted;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * An implementation of interface CharStream, where the stream is assumed to
 * contain only ASCII characters (without unicode processing).
 * 
 * @deprecated Replaced by <a href="http://hadoop.apache.org/avro/">Avro</a>.
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SimpleCharStream
{
  public static final @OsUntrusted boolean staticFlag = false;
  @OsUntrusted
  int bufsize;
  @OsUntrusted
  int available;
  @OsUntrusted
  int tokenBegin;
  public @OsUntrusted int bufpos = -1;
  protected @OsUntrusted int bufline @OsUntrusted [];
  protected @OsUntrusted int bufcolumn @OsUntrusted [];

  protected @OsUntrusted int column = 0;
  protected @OsUntrusted int line = 1;

  protected @OsUntrusted boolean prevCharIsCR = false;
  protected @OsUntrusted boolean prevCharIsLF = false;

  protected java.io.Reader inputStream;

  protected @OsUntrusted char @OsUntrusted [] buffer;
  protected @OsUntrusted int maxNextCharInd = 0;
  protected @OsUntrusted int inBuf = 0;
  protected @OsUntrusted int tabSize = 8;

  protected void setTabSize(@OsUntrusted SimpleCharStream this, @OsUntrusted int i) { tabSize = i; }
  protected @OsUntrusted int getTabSize(@OsUntrusted SimpleCharStream this, @OsUntrusted int i) { return tabSize; }


  protected void ExpandBuff(@OsUntrusted SimpleCharStream this, @OsUntrusted boolean wrapAround)
  {
    @OsUntrusted
    char @OsUntrusted [] newbuffer = new @OsUntrusted char @OsUntrusted [bufsize + 2048];
    @OsUntrusted
    int newbufline @OsUntrusted [] = new @OsUntrusted int @OsUntrusted [bufsize + 2048];
    @OsUntrusted
    int newbufcolumn @OsUntrusted [] = new @OsUntrusted int @OsUntrusted [bufsize + 2048];

    try
      {
        if (wrapAround)
          {
            System.arraycopy(buffer, tokenBegin, newbuffer, 0, bufsize - tokenBegin);
            System.arraycopy(buffer, 0, newbuffer,
                             bufsize - tokenBegin, bufpos);
            buffer = newbuffer;

            System.arraycopy(bufline, tokenBegin, newbufline, 0, bufsize - tokenBegin);
            System.arraycopy(bufline, 0, newbufline, bufsize - tokenBegin, bufpos);
            bufline = newbufline;

            System.arraycopy(bufcolumn, tokenBegin, newbufcolumn, 0, bufsize - tokenBegin);
            System.arraycopy(bufcolumn, 0, newbufcolumn, bufsize - tokenBegin, bufpos);
            bufcolumn = newbufcolumn;

            maxNextCharInd = (bufpos += (bufsize - tokenBegin));
          }
        else
          {
            System.arraycopy(buffer, tokenBegin, newbuffer, 0, bufsize - tokenBegin);
            buffer = newbuffer;

            System.arraycopy(bufline, tokenBegin, newbufline, 0, bufsize - tokenBegin);
            bufline = newbufline;

            System.arraycopy(bufcolumn, tokenBegin, newbufcolumn, 0, bufsize - tokenBegin);
            bufcolumn = newbufcolumn;

            maxNextCharInd = (bufpos -= tokenBegin);
          }
      }
    catch (@OsUntrusted Throwable t)
      {
        throw new @OsUntrusted Error(t.getMessage());
      }


    bufsize += 2048;
    available = bufsize;
    tokenBegin = 0;
  }

  protected void FillBuff(@OsUntrusted SimpleCharStream this) throws java.io.IOException
  {
    if (maxNextCharInd == available)
      {
        if (available == bufsize)
          {
            if (tokenBegin > 2048)
              {
                bufpos = maxNextCharInd = 0;
                available = tokenBegin;
              }
            else if (tokenBegin < 0)
              bufpos = maxNextCharInd = 0;
            else
              ExpandBuff(false);
          }
        else if (available > tokenBegin)
          available = bufsize;
        else if ((tokenBegin - available) < 2048)
          ExpandBuff(true);
        else
          available = tokenBegin;
      }

    @OsUntrusted
    int i;
    try {
      if ((i = inputStream.read(buffer, maxNextCharInd,
                                available - maxNextCharInd)) == -1)
        {
          inputStream.close();
          throw new java.io.IOException();
        }
      else
        maxNextCharInd += i;
      return;
    }
    catch(java.io.IOException e) {
      --bufpos;
      backup(0);
      if (tokenBegin == -1)
        tokenBegin = bufpos;
      throw e;
    }
  }

  public @OsUntrusted char BeginToken(@OsUntrusted SimpleCharStream this) throws java.io.IOException
  {
    tokenBegin = -1;
    @OsUntrusted
    char c = readChar();
    tokenBegin = bufpos;

    return c;
  }

  protected void UpdateLineColumn(@OsUntrusted SimpleCharStream this, @OsUntrusted char c)
  {
    column++;

    if (prevCharIsLF)
      {
        prevCharIsLF = false;
        line += (column = 1);
      }
    else if (prevCharIsCR)
      {
        prevCharIsCR = false;
        if (c == '\n')
          {
            prevCharIsLF = true;
          }
        else
          line += (column = 1);
      }

    switch (c)
      {
      case '\r' :
        prevCharIsCR = true;
        break;
      case '\n' :
        prevCharIsLF = true;
        break;
      case '\t' :
        column--;
        column += (tabSize - (column % tabSize));
        break;
      default :
        break;
      }

    bufline[bufpos] = line;
    bufcolumn[bufpos] = column;
  }

  public @OsUntrusted char readChar(@OsUntrusted SimpleCharStream this) throws java.io.IOException
  {
    if (inBuf > 0)
      {
        --inBuf;

        if (++bufpos == bufsize)
          bufpos = 0;

        return buffer[bufpos];
      }

    if (++bufpos >= maxNextCharInd)
      FillBuff();

    @OsUntrusted
    char c = buffer[bufpos];

    UpdateLineColumn(c);
    return (c);
  }

  public @OsUntrusted int getEndColumn(@OsUntrusted SimpleCharStream this) {
    return bufcolumn[bufpos];
  }

  public @OsUntrusted int getEndLine(@OsUntrusted SimpleCharStream this) {
    return bufline[bufpos];
  }

  public @OsUntrusted int getBeginColumn(@OsUntrusted SimpleCharStream this) {
    return bufcolumn[tokenBegin];
  }

  public @OsUntrusted int getBeginLine(@OsUntrusted SimpleCharStream this) {
    return bufline[tokenBegin];
  }

  public void backup(@OsUntrusted SimpleCharStream this, @OsUntrusted int amount) {

    inBuf += amount;
    if ((bufpos -= amount) < 0)
      bufpos += bufsize;
  }

  public @OsUntrusted SimpleCharStream(java.io.Reader dstream, @OsUntrusted int startline,
                          @OsUntrusted
                          int startcolumn, @OsUntrusted int buffersize)
  {
    inputStream = dstream;
    line = startline;
    column = startcolumn - 1;

    available = bufsize = buffersize;
    buffer = new @OsUntrusted char @OsUntrusted [buffersize];
    bufline = new @OsUntrusted int @OsUntrusted [buffersize];
    bufcolumn = new @OsUntrusted int @OsUntrusted [buffersize];
  }

  public @OsUntrusted SimpleCharStream(java.io.Reader dstream, @OsUntrusted int startline,
                          @OsUntrusted
                          int startcolumn)
  {
    this(dstream, startline, startcolumn, 4096);
  }

  public @OsUntrusted SimpleCharStream(java.io.Reader dstream)
  {
    this(dstream, 1, 1, 4096);
  }
  public void ReInit(@OsUntrusted SimpleCharStream this, java.io.Reader dstream, @OsUntrusted int startline,
                     @OsUntrusted
                     int startcolumn, @OsUntrusted int buffersize)
  {
    inputStream = dstream;
    line = startline;
    column = startcolumn - 1;

    if (buffer == null || buffersize != buffer.length)
      {
        available = bufsize = buffersize;
        buffer = new @OsUntrusted char @OsUntrusted [buffersize];
        bufline = new @OsUntrusted int @OsUntrusted [buffersize];
        bufcolumn = new @OsUntrusted int @OsUntrusted [buffersize];
      }
    prevCharIsLF = prevCharIsCR = false;
    tokenBegin = inBuf = maxNextCharInd = 0;
    bufpos = -1;
  }

  public void ReInit(@OsUntrusted SimpleCharStream this, java.io.Reader dstream, @OsUntrusted int startline,
                     @OsUntrusted
                     int startcolumn)
  {
    ReInit(dstream, startline, startcolumn, 4096);
  }

  public void ReInit(@OsUntrusted SimpleCharStream this, java.io.Reader dstream)
  {
    ReInit(dstream, 1, 1, 4096);
  }
  public @OsUntrusted SimpleCharStream(java.io.InputStream dstream, @OsUntrusted String encoding, @OsUntrusted int startline,
                          @OsUntrusted
                          int startcolumn, @OsUntrusted int buffersize) throws java.io.UnsupportedEncodingException
  {
    this(encoding == null ? new java.io.InputStreamReader(dstream) : new java.io.InputStreamReader(dstream, encoding), startline, startcolumn, buffersize);
  }

  public @OsUntrusted SimpleCharStream(java.io.InputStream dstream, @OsUntrusted int startline,
                          @OsUntrusted
                          int startcolumn, @OsUntrusted int buffersize)
  {
    this(new java.io.InputStreamReader(dstream), startline, startcolumn, buffersize);
  }

  public @OsUntrusted SimpleCharStream(java.io.InputStream dstream, @OsUntrusted String encoding, @OsUntrusted int startline,
                          @OsUntrusted
                          int startcolumn) throws java.io.UnsupportedEncodingException
  {
    this(dstream, encoding, startline, startcolumn, 4096);
  }

  public @OsUntrusted SimpleCharStream(java.io.InputStream dstream, @OsUntrusted int startline,
                          @OsUntrusted
                          int startcolumn)
  {
    this(dstream, startline, startcolumn, 4096);
  }

  public @OsUntrusted SimpleCharStream(java.io.InputStream dstream, @OsUntrusted String encoding) throws java.io.UnsupportedEncodingException
  {
    this(dstream, encoding, 1, 1, 4096);
  }

  public @OsUntrusted SimpleCharStream(java.io.InputStream dstream)
  {
    this(dstream, 1, 1, 4096);
  }

  public void ReInit(@OsUntrusted SimpleCharStream this, java.io.InputStream dstream, @OsUntrusted String encoding, @OsUntrusted int startline,
                     @OsUntrusted
                     int startcolumn, @OsUntrusted int buffersize) throws java.io.UnsupportedEncodingException
  {
    ReInit(encoding == null ? new java.io.InputStreamReader(dstream) : new java.io.InputStreamReader(dstream, encoding), startline, startcolumn, buffersize);
  }

  public void ReInit(@OsUntrusted SimpleCharStream this, java.io.InputStream dstream, @OsUntrusted int startline,
                     @OsUntrusted
                     int startcolumn, @OsUntrusted int buffersize)
  {
    ReInit(new java.io.InputStreamReader(dstream), startline, startcolumn, buffersize);
  }

  public void ReInit(@OsUntrusted SimpleCharStream this, java.io.InputStream dstream, @OsUntrusted String encoding) throws java.io.UnsupportedEncodingException
  {
    ReInit(dstream, encoding, 1, 1, 4096);
  }

  public void ReInit(@OsUntrusted SimpleCharStream this, java.io.InputStream dstream)
  {
    ReInit(dstream, 1, 1, 4096);
  }
  public void ReInit(@OsUntrusted SimpleCharStream this, java.io.InputStream dstream, @OsUntrusted String encoding, @OsUntrusted int startline,
                     @OsUntrusted
                     int startcolumn) throws java.io.UnsupportedEncodingException
  {
    ReInit(dstream, encoding, startline, startcolumn, 4096);
  }
  public void ReInit(@OsUntrusted SimpleCharStream this, java.io.InputStream dstream, @OsUntrusted int startline,
                     @OsUntrusted
                     int startcolumn)
  {
    ReInit(dstream, startline, startcolumn, 4096);
  }
  public @OsUntrusted String GetImage(@OsUntrusted SimpleCharStream this)
  {
    if (bufpos >= tokenBegin)
      return new @OsUntrusted String(buffer, tokenBegin, bufpos - tokenBegin + 1);
    else
      return new @OsUntrusted String(buffer, tokenBegin, bufsize - tokenBegin) +
        new @OsUntrusted String(buffer, 0, bufpos + 1);
  }

  public @OsUntrusted char @OsUntrusted [] GetSuffix(@OsUntrusted SimpleCharStream this, @OsUntrusted int len)
  {
    @OsUntrusted
    char @OsUntrusted [] ret = new @OsUntrusted char @OsUntrusted [len];

    if ((bufpos + 1) >= len)
      System.arraycopy(buffer, bufpos - len + 1, ret, 0, len);
    else
      {
        System.arraycopy(buffer, bufsize - (len - bufpos - 1), ret, 0,
                         len - bufpos - 1);
        System.arraycopy(buffer, 0, ret, len - bufpos - 1, bufpos + 1);
      }

    return ret;
  }

  public void Done(@OsUntrusted SimpleCharStream this)
  {
    buffer = null;
    bufline = null;
    bufcolumn = null;
  }

  /**
   * Method to adjust line and column numbers for the start of a token.
   */
  public void adjustBeginLineColumn(@OsUntrusted SimpleCharStream this, @OsUntrusted int newLine, @OsUntrusted int newCol)
  {
    @OsUntrusted
    int start = tokenBegin;
    @OsUntrusted
    int len;

    if (bufpos >= tokenBegin)
      {
        len = bufpos - tokenBegin + inBuf + 1;
      }
    else
      {
        len = bufsize - tokenBegin + bufpos + 1 + inBuf;
      }

    @OsUntrusted
    int i = 0, j = 0, k = 0;
    @OsUntrusted
    int nextColDiff = 0, columnDiff = 0;

    while (i < len &&
           bufline[j = start % bufsize] == bufline[k = ++start % bufsize])
      {
        bufline[j] = newLine;
        nextColDiff = columnDiff + bufcolumn[k] - bufcolumn[j];
        bufcolumn[j] = newCol + columnDiff;
        columnDiff = nextColDiff;
        i++;
      } 

    if (i < len)
      {
        bufline[j] = newLine++;
        bufcolumn[j] = newCol + columnDiff;

        while (i++ < len)
          {
            if (bufline[j = start % bufsize] != bufline[++start % bufsize])
              bufline[j] = newLine++;
            else
              bufline[j] = newLine;
          }
      }

    line = bufline[j];
    column = bufcolumn[j];
  }

}
