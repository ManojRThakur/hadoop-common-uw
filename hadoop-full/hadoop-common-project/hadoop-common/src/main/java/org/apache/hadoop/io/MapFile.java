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
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/** A file-based map from keys to values.
 * 
 * <p>A map is a directory containing two files, the <code>data</code> file,
 * containing all keys and values in the map, and a smaller <code>index</code>
 * file, containing a fraction of the keys.  The fraction is determined by
 * {@link Writer#getIndexInterval()}.
 *
 * <p>The index file is read entirely into memory.  Thus key implementations
 * should try to keep themselves small.
 *
 * <p>Map files are created by adding entries in-order.  To maintain a large
 * database, perform updates by copying the previous version of a database and
 * merging in a sorted change list, to create a new version of the database in
 * a new file.  Sorting large change lists can be done with {@link
 * SequenceFile.Sorter}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MapFile {
  private static final @OsUntrusted Log LOG = LogFactory.getLog(MapFile.class);

  /** The name of the index file. */
  public static final @OsUntrusted String INDEX_FILE_NAME = "index";

  /** The name of the data file. */
  public static final @OsUntrusted String DATA_FILE_NAME = "data";

  protected @OsUntrusted MapFile() {}                          // no public ctor

  /** Writes a new map. */
  public static class Writer implements java.io.Closeable {
    private SequenceFile.@OsUntrusted Writer data;
    private SequenceFile.@OsUntrusted Writer index;

    final private static @OsUntrusted String INDEX_INTERVAL = "io.map.index.interval";
    private @OsUntrusted int indexInterval = 128;

    private @OsUntrusted long size;
    private @OsUntrusted LongWritable position = new @OsUntrusted LongWritable();

    // the following fields are used only for checking key order
    private @OsUntrusted WritableComparator comparator;
    private @OsUntrusted DataInputBuffer inBuf = new @OsUntrusted DataInputBuffer();
    private @OsUntrusted DataOutputBuffer outBuf = new @OsUntrusted DataOutputBuffer();
    private @OsUntrusted WritableComparable lastKey;

    /** What's the position (in bytes) we wrote when we got the last index */
    private @OsUntrusted long lastIndexPos = -1;

    /**
     * What was size when we last wrote an index. Set to MIN_VALUE to ensure that
     * we have an index at position zero -- midKey will throw an exception if this
     * is not the case
     */
    private @OsUntrusted long lastIndexKeyCount = Long.MIN_VALUE;


    /** Create the named map for keys of the named class. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
                  @OsUntrusted
                  Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> keyClass, 
                  @OsUntrusted
                  Class valClass) throws IOException {
      this(conf, new @OsUntrusted Path(dirName), keyClass(keyClass), valueClass(valClass));
    }

    /** Create the named map for keys of the named class. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
                  @OsUntrusted
                  Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> keyClass, @OsUntrusted Class valClass,
                  @OsUntrusted
                  CompressionType compress, 
                  @OsUntrusted
                  Progressable progress) throws IOException {
      this(conf, new @OsUntrusted Path(dirName), keyClass(keyClass), valueClass(valClass),
           compression(compress), progressable(progress));
    }

    /** Create the named map for keys of the named class. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
                  @OsUntrusted
                  Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> keyClass, @OsUntrusted Class valClass,
                  @OsUntrusted
                  CompressionType compress, @OsUntrusted CompressionCodec codec,
                  @OsUntrusted
                  Progressable progress) throws IOException {
      this(conf, new @OsUntrusted Path(dirName), keyClass(keyClass), valueClass(valClass),
           compression(compress, codec), progressable(progress));
    }

    /** Create the named map for keys of the named class. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
                  @OsUntrusted
                  Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> keyClass, @OsUntrusted Class valClass,
                  @OsUntrusted
                  CompressionType compress) throws IOException {
      this(conf, new @OsUntrusted Path(dirName), keyClass(keyClass),
           valueClass(valClass), compression(compress));
    }

    /** Create the named map using the named key comparator. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
                  @OsUntrusted
                  WritableComparator comparator, @OsUntrusted Class valClass
                  ) throws IOException {
      this(conf, new @OsUntrusted Path(dirName), comparator(comparator), 
           valueClass(valClass));
    }

    /** Create the named map using the named key comparator. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
                  @OsUntrusted
                  WritableComparator comparator, @OsUntrusted Class valClass,
                  SequenceFile.@OsUntrusted CompressionType compress) throws IOException {
      this(conf, new @OsUntrusted Path(dirName), comparator(comparator),
           valueClass(valClass), compression(compress));
    }

    /** Create the named map using the named key comparator. 
     * @deprecated Use Writer(Configuration, Path, Option...)} instead.
     */
    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
                  @OsUntrusted
                  WritableComparator comparator, @OsUntrusted Class valClass,
                  SequenceFile.@OsUntrusted CompressionType compress,
                  @OsUntrusted
                  Progressable progress) throws IOException {
      this(conf, new @OsUntrusted Path(dirName), comparator(comparator),
           valueClass(valClass), compression(compress),
           progressable(progress));
    }

    /** Create the named map using the named key comparator. 
     * @deprecated Use Writer(Configuration, Path, Option...) instead.
     */
    @Deprecated
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, @OsUntrusted FileSystem fs, @OsUntrusted String dirName,
                  @OsUntrusted
                  WritableComparator comparator, @OsUntrusted Class valClass,
                  SequenceFile.@OsUntrusted CompressionType compress, @OsUntrusted CompressionCodec codec,
                  @OsUntrusted
                  Progressable progress) throws IOException {
      this(conf, new @OsUntrusted Path(dirName), comparator(comparator),
           valueClass(valClass), compression(compress, codec),
           progressable(progress));
    }
    
    // our options are a superset of sequence file writer options
    public static interface Option extends SequenceFile.Writer.@OsUntrusted Option { }
    
    private static class KeyClassOption extends Options.@OsUntrusted ClassOption
                                        implements @OsUntrusted Option {
      @OsUntrusted
      KeyClassOption(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> value) {
        super(value);
      }
    }
    
    private static class ComparatorOption implements @OsUntrusted Option {
      private final @OsUntrusted WritableComparator value;
      @OsUntrusted
      ComparatorOption(@OsUntrusted WritableComparator value) {
        this.value = value;
      }
      @OsUntrusted
      WritableComparator getValue(MapFile.Writer.@OsUntrusted ComparatorOption this) {
        return value;
      }
    }

    public static @OsUntrusted Option keyClass(@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> value) {
      return new @OsUntrusted KeyClassOption(value);
    }
    
    public static @OsUntrusted Option comparator(@OsUntrusted WritableComparator value) {
      return new @OsUntrusted ComparatorOption(value);
    }

    public static SequenceFile.Writer.@OsUntrusted Option valueClass(@OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> value) {
      return SequenceFile.Writer.valueClass(value);
    }
    
    public static 
    SequenceFile.Writer.@OsUntrusted Option compression(@OsUntrusted CompressionType type) {
      return SequenceFile.Writer.compression(type);
    }

    public static 
    SequenceFile.Writer.@OsUntrusted Option compression(@OsUntrusted CompressionType type,
        @OsUntrusted
        CompressionCodec codec) {
      return SequenceFile.Writer.compression(type, codec);
    }

    public static SequenceFile.Writer.@OsUntrusted Option progressable(@OsUntrusted Progressable value) {
      return SequenceFile.Writer.progressable(value);
    }

    @SuppressWarnings("unchecked")
    public @OsUntrusted Writer(@OsUntrusted Configuration conf, 
                  @OsUntrusted
                  Path dirName,
                  SequenceFile.Writer.@OsUntrusted Option @OsUntrusted ... opts
                  ) throws IOException {
      @OsUntrusted
      KeyClassOption keyClassOption = 
        Options.getOption(KeyClassOption.class, opts);
      @OsUntrusted
      ComparatorOption comparatorOption =
        Options.getOption(ComparatorOption.class, opts);
      if ((keyClassOption == null) == (comparatorOption == null)) {
        throw new @OsUntrusted IllegalArgumentException("key class or comparator option "
                                           + "must be set");
      }
      this.indexInterval = conf.getInt(INDEX_INTERVAL, this.indexInterval);

      @OsUntrusted
      Class<@OsUntrusted ? extends @OsUntrusted WritableComparable> keyClass;
      if (keyClassOption == null) {
        this.comparator = comparatorOption.getValue();
        keyClass = comparator.getKeyClass();
      } else {
        keyClass= 
          (@OsUntrusted Class<@OsUntrusted ? extends @OsUntrusted WritableComparable>) keyClassOption.getValue();
        this.comparator = WritableComparator.get(keyClass);
      }
      this.lastKey = comparator.newKey();
      @OsUntrusted
      FileSystem fs = dirName.getFileSystem(conf);

      if (!fs.mkdirs(dirName)) {
        throw new @OsUntrusted IOException("Mkdirs failed to create directory " + dirName);
      }
      @OsUntrusted
      Path dataFile = new @OsUntrusted Path(dirName, DATA_FILE_NAME);
      @OsUntrusted
      Path indexFile = new @OsUntrusted Path(dirName, INDEX_FILE_NAME);

      SequenceFile.Writer.@OsUntrusted Option @OsUntrusted [] dataOptions =
        Options.prependOptions(opts, 
                               SequenceFile.Writer.file(dataFile),
                               SequenceFile.Writer.keyClass(keyClass));
      this.data = SequenceFile.createWriter(conf, dataOptions);

      SequenceFile.Writer.@OsUntrusted Option @OsUntrusted [] indexOptions =
        Options.prependOptions(opts, SequenceFile.Writer.file(indexFile),
            SequenceFile.Writer.keyClass(keyClass),
            SequenceFile.Writer.valueClass(LongWritable.class),
            SequenceFile.Writer.compression(CompressionType.BLOCK));
      this.index = SequenceFile.createWriter(conf, indexOptions);      
    }

    /** The number of entries that are added before an index entry is added.*/
    public @OsUntrusted int getIndexInterval(MapFile.@OsUntrusted Writer this) { return indexInterval; }

    /** Sets the index interval.
     * @see #getIndexInterval()
     */
    public void setIndexInterval(MapFile.@OsUntrusted Writer this, @OsUntrusted int interval) { indexInterval = interval; }

    /** Sets the index interval and stores it in conf
     * @see #getIndexInterval()
     */
    public static void setIndexInterval(@OsUntrusted Configuration conf, @OsUntrusted int interval) {
      conf.setInt(INDEX_INTERVAL, interval);
    }

    /** Close the map. */
    @Override
    public synchronized void close(MapFile.@OsUntrusted Writer this) throws IOException {
      data.close();
      index.close();
    }

    /** Append a key/value pair to the map.  The key must be greater or equal
     * to the previous key added to the map. */
    public synchronized void append(MapFile.@OsUntrusted Writer this, @OsUntrusted WritableComparable key, @OsUntrusted Writable val)
      throws IOException {

      checkKey(key);

      @OsUntrusted
      long pos = data.getLength();      
      // Only write an index if we've changed positions. In a block compressed
      // file, this means we write an entry at the start of each block      
      if (size >= lastIndexKeyCount + indexInterval && pos > lastIndexPos) {
        position.set(pos);                        // point to current eof
        index.append(key, position);
        lastIndexPos = pos;
        lastIndexKeyCount = size;
      }

      data.append(key, val);                      // append key/value to data
      size++;
    }

    private void checkKey(MapFile.@OsUntrusted Writer this, @OsUntrusted WritableComparable key) throws IOException {
      // check that keys are well-ordered
      if (size != 0 && comparator.compare(lastKey, key) > 0)
        throw new @OsUntrusted IOException("key out of order: "+key+" after "+lastKey);
          
      // update lastKey with a copy of key by writing and reading
      outBuf.reset();
      key.write(outBuf);                          // write new key

      inBuf.reset(outBuf.getData(), outBuf.getLength());
      lastKey.readFields(inBuf);                  // read into lastKey
    }

  }
  
  /** Provide access to an existing map. */
  public static class Reader implements java.io.Closeable {
      
    /** Number of index entries to skip between each entry.  Zero by default.
     * Setting this to values larger than zero can facilitate opening large map
     * files using less memory. */
    private @OsUntrusted int INDEX_SKIP = 0;
      
    private @OsUntrusted WritableComparator comparator;

    private @OsUntrusted WritableComparable nextKey;
    private @OsUntrusted long seekPosition = -1;
    private @OsUntrusted int seekIndex = -1;
    private @OsUntrusted long firstPosition;

    // the data, on disk
    private SequenceFile.@OsUntrusted Reader data;
    private SequenceFile.@OsUntrusted Reader index;

    // whether the index Reader was closed
    private @OsUntrusted boolean indexClosed = false;

    // the index, in memory
    private @OsUntrusted int count = -1;
    private @OsUntrusted WritableComparable @OsUntrusted [] keys;
    private @OsUntrusted long @OsUntrusted [] positions;

    /** Returns the class of keys in this file. */
    public @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> getKeyClass(MapFile.@OsUntrusted Reader this) { return data.getKeyClass(); }

    /** Returns the class of values in this file. */
    public @OsUntrusted Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> getValueClass(MapFile.@OsUntrusted Reader this) { return data.getValueClass(); }

    public static interface Option extends SequenceFile.Reader.@OsUntrusted Option {}
    
    public static @OsUntrusted Option comparator(@OsUntrusted WritableComparator value) {
      return new @OsUntrusted ComparatorOption(value);
    }

    static class ComparatorOption implements @OsUntrusted Option {
      private final @OsUntrusted WritableComparator value;
      @OsUntrusted
      ComparatorOption(@OsUntrusted WritableComparator value) {
        this.value = value;
      }
      @OsUntrusted
      WritableComparator getValue(MapFile.Reader.@OsUntrusted ComparatorOption this) {
        return value;
      }
    }

    public @OsUntrusted Reader(@OsUntrusted Path dir, @OsUntrusted Configuration conf,
                  SequenceFile.Reader.@OsUntrusted Option @OsUntrusted ... opts) throws IOException {
      @OsUntrusted
      ComparatorOption comparatorOption = 
        Options.getOption(ComparatorOption.class, opts);
      @OsUntrusted
      WritableComparator comparator =
        comparatorOption == null ? null : comparatorOption.getValue();
      INDEX_SKIP = conf.getInt("io.map.index.skip", 0);
      open(dir, comparator, conf, opts);
    }
 
    /** Construct a map reader for the named map.
     * @deprecated
     */
    @Deprecated
    public @OsUntrusted Reader(@OsUntrusted FileSystem fs, @OsUntrusted String dirName, 
                  @OsUntrusted
                  Configuration conf) throws IOException {
      this(new @OsUntrusted Path(dirName), conf);
    }

    /** Construct a map reader for the named map using the named comparator.
     * @deprecated
     */
    @Deprecated
    public @OsUntrusted Reader(@OsUntrusted FileSystem fs, @OsUntrusted String dirName, @OsUntrusted WritableComparator comparator, 
                  @OsUntrusted
                  Configuration conf) throws IOException {
      this(new @OsUntrusted Path(dirName), conf, comparator(comparator));
    }
    
    protected synchronized void open(MapFile.@OsUntrusted Reader this, @OsUntrusted Path dir,
                                     @OsUntrusted
                                     WritableComparator comparator,
                                     @OsUntrusted
                                     Configuration conf, 
                                     SequenceFile.Reader.@OsUntrusted Option @OsUntrusted ... options
                                     ) throws IOException {
      @OsUntrusted
      Path dataFile = new @OsUntrusted Path(dir, DATA_FILE_NAME);
      @OsUntrusted
      Path indexFile = new @OsUntrusted Path(dir, INDEX_FILE_NAME);

      // open the data
      this.data = createDataFileReader(dataFile, conf, options);
      this.firstPosition = data.getPosition();

      if (comparator == null)
        this.comparator = 
          WritableComparator.get(data.getKeyClass().
                                   asSubclass(WritableComparable.class));
      else
        this.comparator = comparator;

      // open the index
      SequenceFile.Reader.@OsUntrusted Option @OsUntrusted [] indexOptions =
        Options.prependOptions(options, SequenceFile.Reader.file(indexFile));
      this.index = new SequenceFile.@OsUntrusted Reader(conf, indexOptions);
    }

    /**
     * Override this method to specialize the type of
     * {@link SequenceFile.Reader} returned.
     */
    protected SequenceFile.@OsUntrusted Reader 
      createDataFileReader(MapFile.@OsUntrusted Reader this, @OsUntrusted Path dataFile, @OsUntrusted Configuration conf,
                           SequenceFile.Reader.@OsUntrusted Option @OsUntrusted ... options
                           ) throws IOException {
      SequenceFile.Reader.@OsUntrusted Option @OsUntrusted [] newOptions =
        Options.prependOptions(options, SequenceFile.Reader.file(dataFile));
      return new SequenceFile.@OsUntrusted Reader(conf, newOptions);
    }

    private void readIndex(MapFile.@OsUntrusted Reader this) throws IOException {
      // read the index entirely into memory
      if (this.keys != null)
        return;
      this.count = 0;
      this.positions = new @OsUntrusted long @OsUntrusted [1024];

      try {
        @OsUntrusted
        int skip = INDEX_SKIP;
        @OsUntrusted
        LongWritable position = new @OsUntrusted LongWritable();
        @OsUntrusted
        WritableComparable lastKey = null;
        @OsUntrusted
        long lastIndex = -1;
        @OsUntrusted
        ArrayList<@OsUntrusted WritableComparable> keyBuilder = new @OsUntrusted ArrayList<@OsUntrusted WritableComparable>(1024);
        while (true) {
          @OsUntrusted
          WritableComparable k = comparator.newKey();

          if (!index.next(k, position))
            break;

          // check order to make sure comparator is compatible
          if (lastKey != null && comparator.compare(lastKey, k) > 0)
            throw new @OsUntrusted IOException("key out of order: "+k+" after "+lastKey);
          lastKey = k;
          if (skip > 0) {
            skip--;
            continue;                             // skip this entry
          } else {
            skip = INDEX_SKIP;                    // reset skip
          }

	  // don't read an index that is the same as the previous one. Block
	  // compressed map files used to do this (multiple entries would point
	  // at the same block)
	  if (position.get() == lastIndex)
	    continue;

          if (count == positions.length) {
	    positions = Arrays.copyOf(positions, positions.length * 2);
          }

          keyBuilder.add(k);
          positions[count] = position.get();
          count++;
        }

        this.keys = keyBuilder.toArray(new @OsUntrusted WritableComparable @OsUntrusted [count]);
        positions = Arrays.copyOf(positions, count);
      } catch (@OsUntrusted EOFException e) {
        LOG.warn("Unexpected EOF reading " + index +
                              " at entry #" + count + ".  Ignoring.");
      } finally {
	indexClosed = true;
        index.close();
      }
    }

    /** Re-positions the reader before its first key. */
    public synchronized void reset(MapFile.@OsUntrusted Reader this) throws IOException {
      data.seek(firstPosition);
    }

    /** Get the key at approximately the middle of the file. Or null if the
     *  file is empty. 
     */
    public synchronized @OsUntrusted WritableComparable midKey(MapFile.@OsUntrusted Reader this) throws IOException {

      readIndex();
      if (count == 0) {
        return null;
      }
    
      return keys[(count - 1) / 2];
    }
    
    /** Reads the final key from the file.
     *
     * @param key key to read into
     */
    public synchronized void finalKey(MapFile.@OsUntrusted Reader this, @OsUntrusted WritableComparable key)
      throws IOException {

      @OsUntrusted
      long originalPosition = data.getPosition(); // save position
      try {
        readIndex();                              // make sure index is valid
        if (count > 0) {
          data.seek(positions[count-1]);          // skip to last indexed entry
        } else {
          reset();                                // start at the beginning
        }
        while (data.next(key)) {}                 // scan to eof

      } finally {
        data.seek(originalPosition);              // restore position
      }
    }

    /** Positions the reader at the named key, or if none such exists, at the
     * first entry after the named key.  Returns true iff the named key exists
     * in this map.
     */
    public synchronized @OsUntrusted boolean seek(MapFile.@OsUntrusted Reader this, @OsUntrusted WritableComparable key) throws IOException {
      return seekInternal(key) == 0;
    }

    /** 
     * Positions the reader at the named key, or if none such exists, at the
     * first entry after the named key.
     *
     * @return  0   - exact match found
     *          < 0 - positioned at next record
     *          1   - no more records in file
     */
    private synchronized @OsUntrusted int seekInternal(MapFile.@OsUntrusted Reader this, @OsUntrusted WritableComparable key)
      throws IOException {
      return seekInternal(key, false);
    }

    /** 
     * Positions the reader at the named key, or if none such exists, at the
     * key that falls just before or just after dependent on how the
     * <code>before</code> parameter is set.
     * 
     * @param before - IF true, and <code>key</code> does not exist, position
     * file at entry that falls just before <code>key</code>.  Otherwise,
     * position file at record that sorts just after.
     * @return  0   - exact match found
     *          < 0 - positioned at next record
     *          1   - no more records in file
     */
    private synchronized @OsUntrusted int seekInternal(MapFile.@OsUntrusted Reader this, @OsUntrusted WritableComparable key,
        final @OsUntrusted boolean before)
      throws IOException {
      readIndex();                                // make sure index is read

      if (seekIndex != -1                         // seeked before
          && seekIndex+1 < count           
          && comparator.compare(key, keys[seekIndex+1])<0 // before next indexed
          && comparator.compare(key, nextKey)
          >= 0) {                                 // but after last seeked
        // do nothing
      } else {
        seekIndex = binarySearch(key);
        if (seekIndex < 0)                        // decode insertion point
          seekIndex = -seekIndex-2;

        if (seekIndex == -1)                      // belongs before first entry
          seekPosition = firstPosition;           // use beginning of file
        else
          seekPosition = positions[seekIndex];    // else use index
      }
      data.seek(seekPosition);
      
      if (nextKey == null)
        nextKey = comparator.newKey();
     
      // If we're looking for the key before, we need to keep track
      // of the position we got the current key as well as the position
      // of the key before it.
      @OsUntrusted
      long prevPosition = -1;
      @OsUntrusted
      long curPosition = seekPosition;

      while (data.next(nextKey)) {
        @OsUntrusted
        int c = comparator.compare(key, nextKey);
        if (c <= 0) {                             // at or beyond desired
          if (before && c != 0) {
            if (prevPosition == -1) {
              // We're on the first record of this index block
              // and we've already passed the search key. Therefore
              // we must be at the beginning of the file, so seek
              // to the beginning of this block and return c
              data.seek(curPosition);
            } else {
              // We have a previous record to back up to
              data.seek(prevPosition);
              data.next(nextKey);
              // now that we've rewound, the search key must be greater than this key
              return 1;
            }
          }
          return c;
        }
        if (before) {
          prevPosition = curPosition;
          curPosition = data.getPosition();
        }
      }

      return 1;
    }

    private @OsUntrusted int binarySearch(MapFile.@OsUntrusted Reader this, @OsUntrusted WritableComparable key) {
      @OsUntrusted
      int low = 0;
      @OsUntrusted
      int high = count-1;

      while (low <= high) {
        @OsUntrusted
        int mid = (low + high) >>> 1;
        @OsUntrusted
        WritableComparable midVal = keys[mid];
        @OsUntrusted
        int cmp = comparator.compare(midVal, key);

        if (cmp < 0)
          low = mid + 1;
        else if (cmp > 0)
          high = mid - 1;
        else
          return mid;                             // key found
      }
      return -(low + 1);                          // key not found.
    }

    /** Read the next key/value pair in the map into <code>key</code> and
     * <code>val</code>.  Returns true if such a pair exists and false when at
     * the end of the map */
    public synchronized @OsUntrusted boolean next(MapFile.@OsUntrusted Reader this, @OsUntrusted WritableComparable key, @OsUntrusted Writable val)
      throws IOException {
      return data.next(key, val);
    }

    /** Return the value for the named key, or null if none exists. */
    public synchronized @OsUntrusted Writable get(MapFile.@OsUntrusted Reader this, @OsUntrusted WritableComparable key, @OsUntrusted Writable val)
      throws IOException {
      if (seek(key)) {
        data.getCurrentValue(val);
        return val;
      } else
        return null;
    }

    /** 
     * Finds the record that is the closest match to the specified key.
     * Returns <code>key</code> or if it does not exist, at the first entry
     * after the named key.
     * 
-     * @param key       - key that we're trying to find
-     * @param val       - data value if key is found
-     * @return          - the key that was the closest match or null if eof.
     */
    public synchronized @OsUntrusted WritableComparable getClosest(MapFile.@OsUntrusted Reader this, @OsUntrusted WritableComparable key,
      @OsUntrusted
      Writable val)
    throws IOException {
      return getClosest(key, val, false);
    }

    /** 
     * Finds the record that is the closest match to the specified key.
     * 
     * @param key       - key that we're trying to find
     * @param val       - data value if key is found
     * @param before    - IF true, and <code>key</code> does not exist, return
     * the first entry that falls just before the <code>key</code>.  Otherwise,
     * return the record that sorts just after.
     * @return          - the key that was the closest match or null if eof.
     */
    public synchronized @OsUntrusted WritableComparable getClosest(MapFile.@OsUntrusted Reader this, @OsUntrusted WritableComparable key,
        @OsUntrusted
        Writable val, final @OsUntrusted boolean before)
      throws IOException {
     
      @OsUntrusted
      int c = seekInternal(key, before);

      // If we didn't get an exact match, and we ended up in the wrong
      // direction relative to the query key, return null since we
      // must be at the beginning or end of the file.
      if ((!before && c > 0) ||
          (before && c < 0)) {
        return null;
      }

      data.getCurrentValue(val);
      return nextKey;
    }

    /** Close the map. */
    @Override
    public synchronized void close(MapFile.@OsUntrusted Reader this) throws IOException {
      if (!indexClosed) {
        index.close();
      }
      data.close();
    }

  }

  /** Renames an existing map directory. */
  public static void rename(@OsUntrusted FileSystem fs, @OsUntrusted String oldName, @OsUntrusted String newName)
    throws IOException {
    @OsUntrusted
    Path oldDir = new @OsUntrusted Path(oldName);
    @OsUntrusted
    Path newDir = new @OsUntrusted Path(newName);
    if (!fs.rename(oldDir, newDir)) {
      throw new @OsUntrusted IOException("Could not rename " + oldDir + " to " + newDir);
    }
  }

  /** Deletes the named map file. */
  public static void delete(@OsUntrusted FileSystem fs, @OsUntrusted String name) throws IOException {
    @OsUntrusted
    Path dir = new @OsUntrusted Path(name);
    @OsUntrusted
    Path data = new @OsUntrusted Path(dir, DATA_FILE_NAME);
    @OsUntrusted
    Path index = new @OsUntrusted Path(dir, INDEX_FILE_NAME);

    fs.delete(data, true);
    fs.delete(index, true);
    fs.delete(dir, true);
  }

  /**
   * This method attempts to fix a corrupt MapFile by re-creating its index.
   * @param fs filesystem
   * @param dir directory containing the MapFile data and index
   * @param keyClass key class (has to be a subclass of Writable)
   * @param valueClass value class (has to be a subclass of Writable)
   * @param dryrun do not perform any changes, just report what needs to be done
   * @return number of valid entries in this MapFile, or -1 if no fixing was needed
   * @throws Exception
   */
  public static @OsUntrusted long fix(@OsUntrusted FileSystem fs, @OsUntrusted Path dir,
                         @OsUntrusted
                         Class<@OsUntrusted ? extends @OsUntrusted Writable> keyClass,
                         @OsUntrusted
                         Class<@OsUntrusted ? extends @OsUntrusted Writable> valueClass, @OsUntrusted boolean dryrun,
                         @OsUntrusted
                         Configuration conf) throws Exception {
    @OsUntrusted
    String dr = (dryrun ? "[DRY RUN ] " : "");
    @OsUntrusted
    Path data = new @OsUntrusted Path(dir, DATA_FILE_NAME);
    @OsUntrusted
    Path index = new @OsUntrusted Path(dir, INDEX_FILE_NAME);
    @OsUntrusted
    int indexInterval = conf.getInt(Writer.INDEX_INTERVAL, 128);
    if (!fs.exists(data)) {
      // there's nothing we can do to fix this!
      throw new @OsUntrusted Exception(dr + "Missing data file in " + dir + ", impossible to fix this.");
    }
    if (fs.exists(index)) {
      // no fixing needed
      return -1;
    }
    SequenceFile.@OsUntrusted Reader dataReader = 
      new SequenceFile.@OsUntrusted Reader(conf, SequenceFile.Reader.file(data));
    if (!dataReader.getKeyClass().equals(keyClass)) {
      throw new @OsUntrusted Exception(dr + "Wrong key class in " + dir + ", expected" + keyClass.getName() +
                          ", got " + dataReader.getKeyClass().getName());
    }
    if (!dataReader.getValueClass().equals(valueClass)) {
      throw new @OsUntrusted Exception(dr + "Wrong value class in " + dir + ", expected" + valueClass.getName() +
                          ", got " + dataReader.getValueClass().getName());
    }
    @OsUntrusted
    long cnt = 0L;
    @OsUntrusted
    Writable key = ReflectionUtils.newInstance(keyClass, conf);
    @OsUntrusted
    Writable value = ReflectionUtils.newInstance(valueClass, conf);
    SequenceFile.@OsUntrusted Writer indexWriter = null;
    if (!dryrun) {
      indexWriter = 
        SequenceFile.createWriter(conf, 
                                  SequenceFile.Writer.file(index), 
                                  SequenceFile.Writer.keyClass(keyClass), 
                                  SequenceFile.Writer.valueClass
                                    (LongWritable.class));
    }
    try {
      @OsUntrusted
      long pos = 0L;
      @OsUntrusted
      LongWritable position = new @OsUntrusted LongWritable();
      while(dataReader.next(key, value)) {
        cnt++;
        if (cnt % indexInterval == 0) {
          position.set(pos);
          if (!dryrun) indexWriter.append(key, position);
        }
        pos = dataReader.getPosition();
      }
    } catch(@OsUntrusted Throwable t) {
      // truncated data file. swallow it.
    }
    dataReader.close();
    if (!dryrun) indexWriter.close();
    return cnt;
  }


  public static void main(@OsUntrusted String @OsUntrusted [] args) throws Exception {
    @OsUntrusted
    String usage = "Usage: MapFile inFile outFile";
      
    if (args.length != 2) {
      System.err.println(usage);
      System.exit(-1);
    }
      
    @OsUntrusted
    String in = args[0];
    @OsUntrusted
    String out = args[1];

    @OsUntrusted
    Configuration conf = new @OsUntrusted Configuration();
    @OsUntrusted
    FileSystem fs = FileSystem.getLocal(conf);
    MapFile.@OsUntrusted Reader reader = new MapFile.@OsUntrusted Reader(fs, in, conf);
    MapFile.@OsUntrusted Writer writer =
      new MapFile.@OsUntrusted Writer(conf, fs, out,
          reader.getKeyClass().asSubclass(WritableComparable.class),
          reader.getValueClass());

    @OsUntrusted
    WritableComparable key =
      ReflectionUtils.newInstance(reader.getKeyClass().asSubclass(WritableComparable.class), conf);
    @OsUntrusted
    Writable value =
      ReflectionUtils.newInstance(reader.getValueClass().asSubclass(Writable.class), conf);

    while (reader.next(key, value))               // copy all entries
      writer.append(key, value);

    writer.close();
  }

}
