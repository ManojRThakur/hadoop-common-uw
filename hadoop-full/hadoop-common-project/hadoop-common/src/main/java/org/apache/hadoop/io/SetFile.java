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
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** A file-based set of keys. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SetFile extends @OsUntrusted MapFile {

  protected @OsUntrusted SetFile() {}                            // no public ctor

  /** 
   * Write a new set file.
   */
  public static class Writer extends MapFile.@OsUntrusted Writer {

    /** Create the named set for keys of the named class. 
     *  @deprecated pass a Configuration too
     */
    public @OsUntrusted Writer(@OsUntrusted FileSystem fs, @OsUntrusted String dirName,
	@OsUntrusted
	Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> keyClass) throws IOException {
      super(new @OsUntrusted Configuration(), fs, dirName, keyClass, NullWritable.class);
    }

    /** Create a set naming the element class and compression type. */
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
                  @OsUntrusted
                  Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> keyClass,
                  SequenceFile.@OsUntrusted CompressionType compress)
      throws IOException {
      this(conf, fs, dirName, WritableComparator.get(keyClass), compress);
    }

    /** Create a set naming the element comparator and compression type. */
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
                  @OsUntrusted
                  WritableComparator comparator,
                  SequenceFile.@OsUntrusted CompressionType compress) throws IOException {
      super(conf, new @OsUntrusted Path(dirName), 
            comparator(comparator), 
            valueClass(NullWritable.class), 
            compression(compress));
    }

    /** Append a key to a set.  The key must be strictly greater than the
     * previous key added to the set. */
    public void append(SetFile.@OsUntrusted Writer this, @OsUntrusted WritableComparable key) throws IOException{
      append(key, NullWritable.get());
    }
  }

  /** Provide access to an existing set file. */
  public static class Reader extends MapFile.@OsUntrusted Reader {

    /** Construct a set reader for the named set.*/
    public @OsUntrusted Reader(@OsUntrusted FileSystem fs, @OsUntrusted String dirName, @OsUntrusted Configuration conf) throws IOException {
      super(fs, dirName, conf);
    }

    /** Construct a set reader for the named set using the named comparator.*/
    public @OsUntrusted Reader(@OsUntrusted FileSystem fs, @OsUntrusted String dirName, @OsUntrusted WritableComparator comparator, @OsUntrusted Configuration conf)
      throws IOException {
      super(new @OsUntrusted Path(dirName), conf, comparator(comparator));
    }

    // javadoc inherited
    @Override
    public @OsUntrusted boolean seek(SetFile.@OsUntrusted Reader this, @OsUntrusted WritableComparable key)
      throws IOException {
      return super.seek(key);
    }

    /** Read the next key in a set into <code>key</code>.  Returns
     * true if such a key exists and false when at the end of the set. */
    public @OsUntrusted boolean next(SetFile.@OsUntrusted Reader this, @OsUntrusted WritableComparable key)
      throws IOException {
      return next(key, NullWritable.get());
    }

    /** Read the matching key from a set into <code>key</code>.
     * Returns <code>key</code>, or null if no match exists. */
    public @OsUntrusted WritableComparable get(SetFile.@OsUntrusted Reader this, @OsUntrusted WritableComparable key)
      throws IOException {
      if (seek(key)) {
        next(key);
        return key;
      } else
        return null;
    }
  }

}
