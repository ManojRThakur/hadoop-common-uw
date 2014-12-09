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
package org.apache.hadoop.fs;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import ostrusted.quals.PolyOsTrusted;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.*;

/**
 * This is an implementation of the Hadoop Archive 
 * Filesystem. This archive Filesystem has index files
 * of the form _index* and has contents of the form
 * part-*. The index files store the indexes of the 
 * real files. The index files are of the form _masterindex
 * and _index. The master index is a level of indirection 
 * in to the index file to make the look ups faster. the index
 * file is sorted with hash code of the paths that it contains 
 * and the master index contains pointers to the positions in 
 * index for ranges of hashcodes.
 */

public class HarFileSystem extends @OsUntrusted FileSystem {

  private static final @OsUntrusted Log LOG = LogFactory.getLog(HarFileSystem.class);

  public static final @OsUntrusted String METADATA_CACHE_ENTRIES_KEY = "fs.har.metadatacache.entries";
  public static final @OsUntrusted int METADATA_CACHE_ENTRIES_DEFAULT = 10;

  public static final @OsUntrusted int VERSION = 3;

  private static @OsUntrusted Map<@OsUntrusted URI, @OsUntrusted HarMetaData> harMetaCache;

  // uri representation of this Har filesystem
  private @OsUntrusted URI uri;
  // the top level path of the archive
  // in the underlying file system
  private @OsUntrusted Path archivePath;
  // the har auth
  private @OsUntrusted String harAuth;

  // pointer into the static metadata cache
  private @OsUntrusted HarMetaData metadata;

  private @OsUntrusted FileSystem fs;

  /**
   * public construction of harfilesystem
   */
  public @OsUntrusted HarFileSystem() {
    // Must call #initialize() method to set the underlying file system
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>har</code>
   */
  @Override
  public @OsUntrusted String getScheme(@OsUntrusted HarFileSystem this) {
    return "har";
  }

  /**
   * Constructor to create a HarFileSystem with an
   * underlying filesystem.
   * @param fs underlying file system
   */
  public @OsUntrusted HarFileSystem(@OsUntrusted FileSystem fs) {
    this.fs = fs;
    this.statistics = fs.statistics;
  }
 
  private synchronized void initializeMetadataCache(@OsUntrusted HarFileSystem this, @OsUntrusted Configuration conf) {
    if (harMetaCache == null) {
      @OsUntrusted
      int cacheSize = conf.getInt(METADATA_CACHE_ENTRIES_KEY, METADATA_CACHE_ENTRIES_DEFAULT);
      harMetaCache = Collections.synchronizedMap(new @OsUntrusted LruCache<@OsUntrusted URI, @OsUntrusted HarMetaData>(cacheSize));
    }
  }
 
  /**
   * Initialize a Har filesystem per har archive. The 
   * archive home directory is the top level directory
   * in the filesystem that contains the HAR archive.
   * Be careful with this method, you do not want to go 
   * on creating new Filesystem instances per call to 
   * path.getFileSystem().
   * the uri of Har is 
   * har://underlyingfsscheme-host:port/archivepath.
   * or 
   * har:///archivepath. This assumes the underlying filesystem
   * to be used in case not specified.
   */
  @Override
  public void initialize(@OsUntrusted HarFileSystem this, @OsUntrusted URI name, @OsUntrusted Configuration conf) throws IOException {
    // initialize the metadata cache, if needed
    initializeMetadataCache(conf);

    // decode the name
    @OsUntrusted
    URI underLyingURI = decodeHarURI(name, conf);
    // we got the right har Path- now check if this is 
    // truly a har filesystem
    @OsUntrusted
    Path harPath = archivePath(
      new @OsUntrusted Path(name.getScheme(), name.getAuthority(), name.getPath()));
    if (harPath == null) { 
      throw new @OsUntrusted IOException("Invalid path for the Har Filesystem. " + 
                           name.toString());
    }
    if (fs == null) {
      fs = FileSystem.get(underLyingURI, conf);
    }
    uri = harPath.toUri();
    archivePath = new @OsUntrusted Path(uri.getPath());
    harAuth = getHarAuth(underLyingURI);
    //check for the underlying fs containing
    // the index file
    @OsUntrusted
    Path masterIndexPath = new @OsUntrusted Path(archivePath, "_masterindex");
    @OsUntrusted
    Path archiveIndexPath = new @OsUntrusted Path(archivePath, "_index");
    if (!fs.exists(masterIndexPath) || !fs.exists(archiveIndexPath)) {
      throw new @OsUntrusted IOException("Invalid path for the Har Filesystem. " +
          "No index file in " + harPath);
    }

    metadata = harMetaCache.get(uri);
    if (metadata != null) {
      @OsUntrusted
      FileStatus mStat = fs.getFileStatus(masterIndexPath);
      @OsUntrusted
      FileStatus aStat = fs.getFileStatus(archiveIndexPath);
      if (mStat.getModificationTime() != metadata.getMasterIndexTimestamp() ||
          aStat.getModificationTime() != metadata.getArchiveIndexTimestamp()) {
        // the archive has been overwritten since we last read it
        // remove the entry from the meta data cache
        metadata = null;
        harMetaCache.remove(uri);
      }
    }
    if (metadata == null) {
      metadata = new @OsUntrusted HarMetaData(fs, masterIndexPath, archiveIndexPath);
      metadata.parseMetaData();
      harMetaCache.put(uri, metadata);
    }
  }

  @Override
  public @OsUntrusted Configuration getConf(@OsUntrusted HarFileSystem this) {
    return fs.getConf();
  }

  // get the version of the filesystem from the masterindex file
  // the version is currently not useful since its the first version
  // of archives
  public @OsUntrusted int getHarVersion(@OsUntrusted HarFileSystem this) throws IOException {
    if (metadata != null) {
      return metadata.getVersion();
    }
    else {
      throw new @OsUntrusted IOException("Invalid meta data for the Har Filesystem");
    }
  }

  /*
   * find the parent path that is the 
   * archive path in the path. The last
   * path segment that ends with .har is 
   * the path that will be returned.
   */
  private @OsUntrusted Path archivePath(@OsUntrusted HarFileSystem this, @OsUntrusted Path p) {
    @OsUntrusted
    Path retPath = null;
    @OsUntrusted
    Path tmp = p;
    for (@OsUntrusted int i=0; i< p.depth(); i++) {
      if (tmp.toString().endsWith(".har")) {
        retPath = tmp;
        break;
      }
      tmp = tmp.getParent();
    }
    return retPath;
  }

  /**
   * decode the raw URI to get the underlying URI
   * @param rawURI raw Har URI
   * @return filtered URI of the underlying fileSystem
   */
  private @OsUntrusted URI decodeHarURI(@OsUntrusted HarFileSystem this, @OsUntrusted URI rawURI, @OsUntrusted Configuration conf) throws IOException {
    @OsUntrusted
    String tmpAuth = rawURI.getAuthority();
    //we are using the default file
    //system in the config 
    //so create a underlying uri and 
    //return it
    if (tmpAuth == null) {
      //create a path 
      return FileSystem.getDefaultUri(conf);
    }
    @OsUntrusted
    String authority = rawURI.getAuthority();
    if (authority == null) {
      throw new @OsUntrusted IOException("URI: " + rawURI
          + " is an invalid Har URI since authority==null."
          + "  Expecting har://<scheme>-<host>/<path>.");
    }
 
    @OsUntrusted
    int i = authority.indexOf('-');
    if (i < 0) {
      throw new @OsUntrusted IOException("URI: " + rawURI
          + " is an invalid Har URI since '-' not found."
          + "  Expecting har://<scheme>-<host>/<path>.");
    }
 
    if (rawURI.getQuery() != null) {
      // query component not allowed
      throw new @OsUntrusted IOException("query component in Path not supported  " + rawURI);
    }
 
    @OsUntrusted
    URI tmp;
    try {
      // convert <scheme>-<host> to <scheme>://<host>
      @OsUntrusted
      URI baseUri = new @OsUntrusted URI(authority.replaceFirst("-", "://"));
 
      tmp = new @OsUntrusted URI(baseUri.getScheme(), baseUri.getAuthority(),
            rawURI.getPath(), rawURI.getQuery(), rawURI.getFragment());
    } catch (@OsUntrusted URISyntaxException e) {
      throw new @OsUntrusted IOException("URI: " + rawURI
          + " is an invalid Har URI. Expecting har://<scheme>-<host>/<path>.");
    }
    return tmp;
  }

  private static @OsUntrusted String decodeString(@OsUntrusted String str)
    throws UnsupportedEncodingException {
    return URLDecoder.decode(str, "UTF-8");
  }

  private @OsUntrusted String decodeFileName(@OsUntrusted HarFileSystem this, @OsUntrusted String fname)
    throws UnsupportedEncodingException {
    @OsUntrusted
    int version = metadata.getVersion();
    if (version == 2 || version == 3){
      return decodeString(fname);
    }
    return fname;
  }

  /**
   * return the top level archive.
   */
  @Override
  public @OsUntrusted Path getWorkingDirectory(@OsUntrusted HarFileSystem this) {
    return new @OsUntrusted Path(uri.toString());
  }

  @Override
  public @OsUntrusted Path getInitialWorkingDirectory(@OsUntrusted HarFileSystem this) {
    return getWorkingDirectory();
  }

  @Override
  public @OsUntrusted FsStatus getStatus(@OsUntrusted HarFileSystem this, @OsUntrusted Path p) throws IOException {
    return fs.getStatus(p);
  }

  /**
   * Create a har specific auth 
   * har-underlyingfs:port
   * @param underLyingUri the uri of underlying
   * filesystem
   * @return har specific auth
   */
  private @OsUntrusted String getHarAuth(@OsUntrusted HarFileSystem this, @OsUntrusted URI underLyingUri) {
    @OsUntrusted
    String auth = underLyingUri.getScheme() + "-";
    if (underLyingUri.getHost() != null) {
      auth += underLyingUri.getHost();
      if (underLyingUri.getPort() != -1) {
        auth += ":";
        auth +=  underLyingUri.getPort();
      }
    }
    else {
      auth += ":";
    }
    return auth;
  }

  /**
   * Used for delegation token related functionality. Must delegate to
   * underlying file system.
   */
  @Override
  protected @OsUntrusted URI getCanonicalUri(@OsUntrusted HarFileSystem this) {
    return fs.getCanonicalUri();
  }

  @Override
  protected @PolyOsTrusted URI canonicalizeUri(@OsUntrusted HarFileSystem this, @PolyOsTrusted URI uri) {
    return fs.canonicalizeUri(uri);
  }

  /**
   * Returns the uri of this filesystem.
   * The uri is of the form 
   * har://underlyingfsschema-host:port/pathintheunderlyingfs
   */
  @Override
  public @OsUntrusted URI getUri(@OsUntrusted HarFileSystem this) {
    return this.uri;
  }
  
  @Override
  protected void checkPath(@OsUntrusted HarFileSystem this, @OsUntrusted Path path) {
    fs.checkPath(path);
  }

  @Override
  public @OsUntrusted Path resolvePath(@OsUntrusted HarFileSystem this, @OsUntrusted Path p) throws IOException {
    return fs.resolvePath(p);
  }

  /**
   * this method returns the path 
   * inside the har filesystem.
   * this is relative path inside 
   * the har filesystem.
   * @param path the fully qualified path in the har filesystem.
   * @return relative path in the filesystem.
   */
  private @OsUntrusted Path getPathInHar(@OsUntrusted HarFileSystem this, @OsUntrusted Path path) {
    @OsUntrusted
    Path harPath = new @OsUntrusted Path(path.toUri().getPath());
    if (archivePath.compareTo(harPath) == 0)
      return new @OsUntrusted Path(Path.SEPARATOR);
    @OsUntrusted
    Path tmp = new @OsUntrusted Path(harPath.getName());
    @OsUntrusted
    Path parent = harPath.getParent();
    while (!(parent.compareTo(archivePath) == 0)) {
      if (parent.toString().equals(Path.SEPARATOR)) {
        tmp = null;
        break;
      }
      tmp = new @OsUntrusted Path(parent.getName(), tmp);
      parent = parent.getParent();
    }
    if (tmp != null) 
      tmp = new @OsUntrusted Path(Path.SEPARATOR, tmp);
    return tmp;
  }
  
  //the relative path of p. basically 
  // getting rid of /. Parsing and doing 
  // string manipulation is not good - so
  // just use the path api to do it.
  private @OsUntrusted Path makeRelative(@OsUntrusted HarFileSystem this, @OsUntrusted String initial, @OsUntrusted Path p) {
    @OsUntrusted
    String scheme = this.uri.getScheme();
    @OsUntrusted
    String authority = this.uri.getAuthority();
    @OsUntrusted
    Path root = new @OsUntrusted Path(Path.SEPARATOR);
    if (root.compareTo(p) == 0)
      return new @OsUntrusted Path(scheme, authority, initial);
    @OsUntrusted
    Path retPath = new @OsUntrusted Path(p.getName());
    @OsUntrusted
    Path parent = p.getParent();
    for (@OsUntrusted int i=0; i < p.depth()-1; i++) {
      retPath = new @OsUntrusted Path(parent.getName(), retPath);
      parent = parent.getParent();
    }
    return new @OsUntrusted Path(new @OsUntrusted Path(scheme, authority, initial),
      retPath.toString());
  }
  
  /* this makes a path qualified in the har filesystem
   * (non-Javadoc)
   * @see org.apache.hadoop.fs.FilterFileSystem#makeQualified(
   * org.apache.hadoop.fs.Path)
   */
  @Override
  public @OsUntrusted Path makeQualified(@OsUntrusted HarFileSystem this, @OsUntrusted Path path) {
    // make sure that we just get the 
    // path component 
    @OsUntrusted
    Path fsPath = path;
    if (!path.isAbsolute()) {
      fsPath = new @OsUntrusted Path(archivePath, path);
    }

    @OsUntrusted
    URI tmpURI = fsPath.toUri();
    //change this to Har uri 
    return new @OsUntrusted Path(uri.getScheme(), harAuth, tmpURI.getPath());
  }

  /**
   * Fix offset and length of block locations.
   * Note that this method modifies the original array.
   * @param locations block locations of har part file
   * @param start the start of the desired range in the contained file
   * @param len the length of the desired range
   * @param fileOffsetInHar the offset of the desired file in the har part file
   * @return block locations with fixed offset and length
   */  
  static @OsUntrusted BlockLocation @OsUntrusted [] fixBlockLocations(@OsUntrusted BlockLocation @OsUntrusted [] locations,
                                          @OsUntrusted
                                          long start,
                                          @OsUntrusted
                                          long len,
                                          @OsUntrusted
                                          long fileOffsetInHar) {
    // offset 1 past last byte of desired range
    @OsUntrusted
    long end = start + len;

    for (@OsUntrusted BlockLocation location : locations) {
      // offset of part block relative to beginning of desired file
      // (may be negative if file starts in this part block)
      @OsUntrusted
      long harBlockStart = location.getOffset() - fileOffsetInHar;
      // offset 1 past last byte of har block relative to beginning of
      // desired file
      @OsUntrusted
      long harBlockEnd = harBlockStart + location.getLength();
      
      if (start > harBlockStart) {
        // desired range starts after beginning of this har block
        // fix offset to beginning of relevant range (relative to desired file)
        location.setOffset(start);
        // fix length to relevant portion of har block
        location.setLength(location.getLength() - (start - harBlockStart));
      } else {
        // desired range includes beginning of this har block
        location.setOffset(harBlockStart);
      }
      
      if (harBlockEnd > end) {
        // range ends before end of this har block
        // fix length to remove irrelevant portion at the end
        location.setLength(location.getLength() - (harBlockEnd - end));
      }
    }
    
    return locations;
  }
  
  /**
   * Get block locations from the underlying fs and fix their
   * offsets and lengths.
   * @param file the input file status to get block locations
   * @param start the start of the desired range in the contained file
   * @param len the length of the desired range
   * @return block locations for this segment of file
   * @throws IOException
   */
  @Override
  public @OsUntrusted BlockLocation @OsUntrusted [] getFileBlockLocations(@OsUntrusted HarFileSystem this, @OsUntrusted FileStatus file, @OsUntrusted long start,
                                               @OsUntrusted
                                               long len) throws IOException {
    @OsUntrusted
    HarStatus hstatus = getFileHarStatus(file.getPath());
    @OsUntrusted
    Path partPath = new @OsUntrusted Path(archivePath, hstatus.getPartName());
    @OsUntrusted
    FileStatus partStatus = metadata.getPartFileStatus(partPath);

    // get all part blocks that overlap with the desired file blocks
    @OsUntrusted
    BlockLocation @OsUntrusted [] locations = 
      fs.getFileBlockLocations(partStatus,
                               hstatus.getStartIndex() + start, len);

    return fixBlockLocations(locations, start, len, hstatus.getStartIndex());
  }
  
  /**
   * the hash of the path p inside  the filesystem
   * @param p the path in the harfilesystem
   * @return the hash code of the path.
   */
  public static @OsUntrusted int getHarHash(@OsUntrusted Path p) {
    return (p.toString().hashCode() & 0x7fffffff);
  }
  
  static class Store {
    public @OsUntrusted Store() {
      begin = end = startHash = endHash = 0;
    }
    public @OsUntrusted Store(@OsUntrusted long begin, @OsUntrusted long end, @OsUntrusted int startHash, @OsUntrusted int endHash) {
      this.begin = begin;
      this.end = end;
      this.startHash = startHash;
      this.endHash = endHash;
    }
    public @OsUntrusted long begin;
    public @OsUntrusted long end;
    public @OsUntrusted int startHash;
    public @OsUntrusted int endHash;
  }
  
  /**
   * Get filestatuses of all the children of a given directory. This just reads
   * through index file and reads line by line to get all statuses for children
   * of a directory. Its a brute force way of getting all such filestatuses
   * 
   * @param parent
   *          the parent path directory
   * @param statuses
   *          the list to add the children filestatuses to
   */
  private void fileStatusesInIndex(@OsUntrusted HarFileSystem this, @OsUntrusted HarStatus parent, @OsUntrusted List<@OsUntrusted FileStatus> statuses)
          throws IOException {
    @OsUntrusted
    String parentString = parent.getName();
    if (!parentString.endsWith(Path.SEPARATOR)){
        parentString += Path.SEPARATOR;
    }
    @OsUntrusted
    Path harPath = new @OsUntrusted Path(parentString);
    @OsUntrusted
    int harlen = harPath.depth();
    final @OsUntrusted Map<@OsUntrusted String, @OsUntrusted FileStatus> cache = new @OsUntrusted TreeMap<@OsUntrusted String, @OsUntrusted FileStatus>();

    for (@OsUntrusted HarStatus hstatus : metadata.archive.values()) {
      @OsUntrusted
      String child = hstatus.getName();
      if ((child.startsWith(parentString))) {
        @OsUntrusted
        Path thisPath = new @OsUntrusted Path(child);
        if (thisPath.depth() == harlen + 1) {
          statuses.add(toFileStatus(hstatus, cache));
        }
      }
    }
  }

  /**
   * Combine the status stored in the index and the underlying status. 
   * @param h status stored in the index
   * @param cache caching the underlying file statuses
   * @return the combined file status
   * @throws IOException
   */
  private @OsUntrusted FileStatus toFileStatus(@OsUntrusted HarFileSystem this, @OsUntrusted HarStatus h,
      @OsUntrusted
      Map<@OsUntrusted String, @OsUntrusted FileStatus> cache) throws IOException {
    @OsUntrusted
    FileStatus underlying = null;
    if (cache != null) {
      underlying = cache.get(h.partName);
    }
    if (underlying == null) {
      final @OsUntrusted Path p = h.isDir? archivePath: new @OsUntrusted Path(archivePath, h.partName);
      underlying = fs.getFileStatus(p);
      if (cache != null) {
        cache.put(h.partName, underlying);
      }
    }

    @OsUntrusted
    long modTime = 0;
    @OsUntrusted
    int version = metadata.getVersion();
    if (version < 3) {
      modTime = underlying.getModificationTime();
    } else if (version == 3) {
      modTime = h.getModificationTime();
    }

    return new @OsUntrusted FileStatus(
        h.isDir()? 0L: h.getLength(),
        h.isDir(),
        underlying.getReplication(),
        underlying.getBlockSize(),
        modTime,
        underlying.getAccessTime(),
        underlying.getPermission(),
        underlying.getOwner(),
        underlying.getGroup(),
        makeRelative(this.uri.getPath(), new @OsUntrusted Path(h.name)));
  }

  // a single line parser for hadoop archives status 
  // stored in a single line in the index files 
  // the format is of the form 
  // filename "dir"/"file" partFileName startIndex length 
  // <space separated children>
  private class HarStatus {
    @OsUntrusted
    boolean isDir;
    @OsUntrusted
    String name;
    @OsUntrusted
    List<@OsUntrusted String> children;
    @OsUntrusted
    String partName;
    @OsUntrusted
    long startIndex;
    @OsUntrusted
    long length;
    @OsUntrusted
    long modificationTime = 0;

    public @OsUntrusted HarStatus(@OsUntrusted String harString) throws UnsupportedEncodingException {
      @OsUntrusted
      String @OsUntrusted [] splits = harString.split(" ");
      this.name = decodeFileName(splits[0]);
      this.isDir = "dir".equals(splits[1]) ? true: false;
      // this is equal to "none" if its a directory
      this.partName = splits[2];
      this.startIndex = Long.parseLong(splits[3]);
      this.length = Long.parseLong(splits[4]);

      @OsUntrusted
      int version = metadata.getVersion();
      @OsUntrusted
      String @OsUntrusted [] propSplits = null;
      // propSplits is used to retrieve the metainformation that Har versions
      // 1 & 2 missed (modification time, permission, owner group).
      // These fields are stored in an encoded string placed in different
      // locations depending on whether it's a file or directory entry.
      // If it's a directory, the string will be placed at the partName
      // location (directories have no partName because they don't have data
      // to be stored). This is done because the number of fields in a
      // directory entry is unbounded (all children are listed at the end)
      // If it's a file, the string will be the last field.
      if (isDir) {
        if (version == 3){
          propSplits = decodeString(this.partName).split(" ");
        }
        children = new @OsUntrusted ArrayList<@OsUntrusted String>();
        for (@OsUntrusted int i = 5; i < splits.length; i++) {
          children.add(decodeFileName(splits[i]));
        }
      } else if (version == 3) {
        propSplits = decodeString(splits[5]).split(" ");
      }

      if (propSplits != null && propSplits.length >= 4) {
        modificationTime = Long.parseLong(propSplits[0]);
        // the fields below are stored in the file but are currently not used
        // by HarFileSystem
        // permission = new FsPermission(Short.parseShort(propSplits[1]));
        // owner = decodeString(propSplits[2]);
        // group = decodeString(propSplits[3]);
      }
    }
    public @OsUntrusted boolean isDir(@OsUntrusted HarFileSystem.HarStatus this) {
      return isDir;
    }
    
    public @OsUntrusted String getName(@OsUntrusted HarFileSystem.HarStatus this) {
      return name;
    }
    public @OsUntrusted String getPartName(@OsUntrusted HarFileSystem.HarStatus this) {
      return partName;
    }
    public @OsUntrusted long getStartIndex(@OsUntrusted HarFileSystem.HarStatus this) {
      return startIndex;
    }
    public @OsUntrusted long getLength(@OsUntrusted HarFileSystem.HarStatus this) {
      return length;
    }
    public @OsUntrusted long getModificationTime(@OsUntrusted HarFileSystem.HarStatus this) {
      return modificationTime;
    }
  }
  
  /**
   * return the filestatus of files in har archive.
   * The permission returned are that of the archive
   * index files. The permissions are not persisted 
   * while creating a hadoop archive.
   * @param f the path in har filesystem
   * @return filestatus.
   * @throws IOException
   */
  @Override
  public @OsUntrusted FileStatus getFileStatus(@OsUntrusted HarFileSystem this, @OsUntrusted Path f) throws IOException {
    @OsUntrusted
    HarStatus hstatus = getFileHarStatus(f);
    return toFileStatus(hstatus, null);
  }

  private @OsUntrusted HarStatus getFileHarStatus(@OsUntrusted HarFileSystem this, @OsUntrusted Path f) throws IOException {
    // get the fs DataInputStream for the underlying file
    // look up the index.
    @OsUntrusted
    Path p = makeQualified(f);
    @OsUntrusted
    Path harPath = getPathInHar(p);
    if (harPath == null) {
      throw new @OsUntrusted IOException("Invalid file name: " + f + " in " + uri);
    }
    @OsUntrusted
    HarStatus hstatus = metadata.archive.get(harPath);
    if (hstatus == null) {
      throw new @OsUntrusted FileNotFoundException("File: " +  f + " does not exist in " + uri);
    }
    return hstatus;
  }

  /**
   * @return null since no checksum algorithm is implemented.
   */
  @Override
  public @OsUntrusted FileChecksum getFileChecksum(@OsUntrusted HarFileSystem this, @OsUntrusted Path f) {
    return null;
  }

  /**
   * Returns a har input stream which fakes end of 
   * file. It reads the index files to get the part 
   * file name and the size and start of the file.
   */
  @Override
  public @OsUntrusted FSDataInputStream open(@OsUntrusted HarFileSystem this, @OsUntrusted Path f, @OsUntrusted int bufferSize) throws IOException {
    // get the fs DataInputStream for the underlying file
    @OsUntrusted
    HarStatus hstatus = getFileHarStatus(f);
    if (hstatus.isDir()) {
      throw new @OsUntrusted FileNotFoundException(f + " : not a file in " +
                archivePath);
    }
    return new @OsUntrusted HarFSDataInputStream(fs, new @OsUntrusted Path(archivePath, 
        hstatus.getPartName()),
        hstatus.getStartIndex(), hstatus.getLength(), bufferSize);
  }

  /**
   * Used for delegation token related functionality. Must delegate to
   * underlying file system.
   */
  @Override
  public @OsUntrusted FileSystem @OsUntrusted [] getChildFileSystems(@OsUntrusted HarFileSystem this) {
    return new @OsUntrusted FileSystem @OsUntrusted []{fs};
  }

  @Override
  public @OsUntrusted FSDataOutputStream create(@OsUntrusted HarFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission,
      @OsUntrusted
      boolean overwrite, @OsUntrusted int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize,
      @OsUntrusted
      Progressable progress) throws IOException {
    throw new @OsUntrusted IOException("Har: create not allowed.");
  }

  @SuppressWarnings("deprecation")
  @Override
  public @OsUntrusted FSDataOutputStream createNonRecursive(@OsUntrusted HarFileSystem this, @OsUntrusted Path f, @OsUntrusted boolean overwrite,
      @OsUntrusted
      int bufferSize, @OsUntrusted short replication, @OsUntrusted long blockSize, @OsUntrusted Progressable progress)
      throws IOException {
    throw new @OsUntrusted IOException("Har: create not allowed.");
  }

  @Override
  public @OsUntrusted FSDataOutputStream append(@OsUntrusted HarFileSystem this, @OsUntrusted Path f, @OsUntrusted int bufferSize, @OsUntrusted Progressable progress) throws IOException {
    throw new @OsUntrusted IOException("Har: append not allowed.");
  }

  @Override
  public void close(@OsUntrusted HarFileSystem this) throws IOException {
    super.close();
    if (fs != null) {
      try {
        fs.close();
      } catch(@OsUntrusted IOException ie) {
        //this might already be closed
        // ignore
      }
    }
  }
  
  /**
   * Not implemented.
   */
  @Override
  public @OsUntrusted boolean setReplication(@OsUntrusted HarFileSystem this, @OsUntrusted Path src, @OsUntrusted short replication) throws IOException{
    throw new @OsUntrusted IOException("Har: setReplication not allowed");
  }

  @Override
  public @OsUntrusted boolean rename(@OsUntrusted HarFileSystem this, @OsUntrusted Path src, @OsUntrusted Path dst) throws IOException {
    throw new @OsUntrusted IOException("Har: rename not allowed");
  }

  @Override
  public @OsUntrusted FSDataOutputStream append(@OsUntrusted HarFileSystem this, @OsUntrusted Path f) throws IOException {
    throw new @OsUntrusted IOException("Har: append not allowed");
  }

  /**
   * Not implemented.
   */
  @Override
  public @OsUntrusted boolean delete(@OsUntrusted HarFileSystem this, @OsUntrusted Path f, @OsUntrusted boolean recursive) throws IOException { 
    throw new @OsUntrusted IOException("Har: delete not allowed");
  }

  /**
   * liststatus returns the children of a directory 
   * after looking up the index files.
   */
  @Override
  public @OsUntrusted FileStatus @OsUntrusted [] listStatus(@OsUntrusted HarFileSystem this, @OsUntrusted Path f) throws IOException {
    //need to see if the file is an index in file
    //get the filestatus of the archive directory
    // we will create fake filestatuses to return
    // to the client
    @OsUntrusted
    List<@OsUntrusted FileStatus> statuses = new @OsUntrusted ArrayList<@OsUntrusted FileStatus>();
    @OsUntrusted
    Path tmpPath = makeQualified(f);
    @OsUntrusted
    Path harPath = getPathInHar(tmpPath);
    @OsUntrusted
    HarStatus hstatus = metadata.archive.get(harPath);
    if (hstatus == null) {
      throw new @OsUntrusted FileNotFoundException("File " + f + " not found in " + archivePath);
    }
    if (hstatus.isDir()) {
      fileStatusesInIndex(hstatus, statuses);
    } else {
      statuses.add(toFileStatus(hstatus, null));
    }
    
    return statuses.toArray(new @OsUntrusted FileStatus @OsUntrusted [statuses.size()]);
  }
  
  /**
   * return the top level archive path.
   */
  @Override
  public @OsUntrusted Path getHomeDirectory(@OsUntrusted HarFileSystem this) {
    return new @OsUntrusted Path(uri.toString());
  }

  @Override
  public void setWorkingDirectory(@OsUntrusted HarFileSystem this, @OsUntrusted Path newDir) {
    //does nothing.
  }
  
  /**
   * not implemented.
   */
  @Override
  public @OsUntrusted boolean mkdirs(@OsUntrusted HarFileSystem this, @OsUntrusted Path f, @OsUntrusted FsPermission permission) throws IOException {
    throw new @OsUntrusted IOException("Har: mkdirs not allowed");
  }
  
  /**
   * not implemented.
   */
  @Override
  public void copyFromLocalFile(@OsUntrusted HarFileSystem this, @OsUntrusted boolean delSrc, @OsUntrusted boolean overwrite,
      @OsUntrusted
      Path src, @OsUntrusted Path dst) throws IOException {
    throw new @OsUntrusted IOException("Har: copyfromlocalfile not allowed");
  }

  @Override
  public void copyFromLocalFile(@OsUntrusted HarFileSystem this, @OsUntrusted boolean delSrc, @OsUntrusted boolean overwrite,
      @OsUntrusted
      Path @OsUntrusted [] srcs, @OsUntrusted Path dst) throws IOException {
    throw new @OsUntrusted IOException("Har: copyfromlocalfile not allowed");
  }

  /**
   * copies the file in the har filesystem to a local file.
   */
  @Override
  public void copyToLocalFile(@OsUntrusted HarFileSystem this, @OsUntrusted boolean delSrc, @OsUntrusted Path src, @OsUntrusted Path dst) 
    throws IOException {
    FileUtil.copy(this, src, getLocal(getConf()), dst, false, getConf());
  }
  
  /**
   * not implemented.
   */
  @Override
  public @OsUntrusted Path startLocalOutput(@OsUntrusted HarFileSystem this, @OsUntrusted Path fsOutputFile, @OsUntrusted Path tmpLocalFile) 
    throws IOException {
    throw new @OsUntrusted IOException("Har: startLocalOutput not allowed");
  }
  
  /**
   * not implemented.
   */
  @Override
  public void completeLocalOutput(@OsUntrusted HarFileSystem this, @OsUntrusted Path fsOutputFile, @OsUntrusted Path tmpLocalFile) 
    throws IOException {
    throw new @OsUntrusted IOException("Har: completeLocalOutput not allowed");
  }
  
  /**
   * not implemented.
   */
  @Override
  public void setOwner(@OsUntrusted HarFileSystem this, @OsUntrusted Path p, @OsUntrusted String username, @OsUntrusted String groupname)
    throws IOException {
    throw new @OsUntrusted IOException("Har: setowner not allowed");
  }

  @Override
  public void setTimes(@OsUntrusted HarFileSystem this, @OsUntrusted Path p, @OsUntrusted long mtime, @OsUntrusted long atime) throws IOException {
    throw new @OsUntrusted IOException("Har: setTimes not allowed");
  }

  /**
   * Not implemented.
   */
  @Override
  public void setPermission(@OsUntrusted HarFileSystem this, @OsUntrusted Path p, @OsUntrusted FsPermission permission)
    throws IOException {
    throw new @OsUntrusted IOException("Har: setPermission not allowed");
  }
  
  /**
   * Hadoop archives input stream. This input stream fakes EOF 
   * since archive files are part of bigger part files.
   */
  private static class HarFSDataInputStream extends @OsUntrusted FSDataInputStream {
    /**
     * Create an input stream that fakes all the reads/positions/seeking.
     */
    private static class HarFsInputStream extends @OsUntrusted FSInputStream
        implements @OsUntrusted CanSetDropBehind, @OsUntrusted CanSetReadahead {
      private @OsUntrusted long position;
      private @OsUntrusted long start;
      private @OsUntrusted long end;
      //The underlying data input stream that the
      // underlying filesystem will return.
      private @OsUntrusted FSDataInputStream underLyingStream;
      //one byte buffer
      private @OsUntrusted byte @OsUntrusted [] oneBytebuff = new @OsUntrusted byte @OsUntrusted [1];
      @OsUntrusted
      HarFsInputStream(@OsUntrusted FileSystem fs, @OsUntrusted Path path, @OsUntrusted long start,
          @OsUntrusted
          long length, @OsUntrusted int bufferSize) throws IOException {
        underLyingStream = fs.open(path, bufferSize);
        underLyingStream.seek(start);
        // the start of this file in the part file
        this.start = start;
        // the position pointer in the part file
        this.position = start;
        // the end pointer in the part file
        this.end = start + length;
      }
      
      @Override
      public synchronized @OsUntrusted int available(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this) throws IOException {
        @OsUntrusted
        long remaining = end - underLyingStream.getPos();
        if (remaining > (@OsUntrusted long)Integer.MAX_VALUE) {
          return Integer.MAX_VALUE;
        }
        return (@OsUntrusted int) remaining;
      }
      
      @Override
      public synchronized  void close(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this) throws IOException {
        underLyingStream.close();
        super.close();
      }
      
      //not implemented
      @Override
      public void mark(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this, @OsUntrusted int readLimit) {
        // do nothing 
      }
      
      /**
       * reset is not implemented
       */
      @Override
      public void reset(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this) throws IOException {
        throw new @OsUntrusted IOException("reset not implemented.");
      }
      
      @Override
      public synchronized @OsUntrusted int read(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this) throws IOException {
        @OsUntrusted
        int ret = read(oneBytebuff, 0, 1);
        return (ret <= 0) ? -1: (oneBytebuff[0] & 0xff);
      }
      
      @Override
      public synchronized @OsUntrusted int read(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this, @OsUntrusted byte @OsUntrusted [] b) throws IOException {
        @OsUntrusted
        int ret = read(b, 0, b.length);
        if (ret != -1) {
          position += ret;
        }
        return ret;
      }
      
      /**
       * 
       */
      @Override
      public synchronized @OsUntrusted int read(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int offset, @OsUntrusted int len) 
        throws IOException {
        @OsUntrusted
        int newlen = len;
        @OsUntrusted
        int ret = -1;
        if (position + len > end) {
          newlen = (@OsUntrusted int) (end - position);
        }
        // end case
        if (newlen == 0)
          return ret;
        ret = underLyingStream.read(b, offset, newlen);
        position += ret;
        return ret;
      }
      
      @Override
      public synchronized @OsUntrusted long skip(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this, @OsUntrusted long n) throws IOException {
        @OsUntrusted
        long tmpN = n;
        if (tmpN > 0) {
          if (position + tmpN > end) {
            tmpN = end - position;
          }
          underLyingStream.seek(tmpN + position);
          position += tmpN;
          return tmpN;
        }
        return (tmpN < 0)? -1 : 0;
      }
      
      @Override
      public synchronized @OsUntrusted long getPos(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this) throws IOException {
        return (position - start);
      }
      
      @Override
      public synchronized void seek(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this, @OsUntrusted long pos) throws IOException {
        if (pos < 0 || (start + pos > end)) {
          throw new @OsUntrusted IOException("Failed to seek: EOF");
        }
        position = start + pos;
        underLyingStream.seek(position);
      }

      @Override
      public @OsUntrusted boolean seekToNewSource(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this, @OsUntrusted long targetPos) throws IOException {
        // do not need to implement this
        // hdfs in itself does seektonewsource
        // while reading.
        return false;
      }
      
      /**
       * implementing position readable. 
       */
      @Override
      public @OsUntrusted int read(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this, @OsUntrusted long pos, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int offset, @OsUntrusted int length) 
      throws IOException {
        @OsUntrusted
        int nlength = length;
        if (start + nlength + pos > end) {
          nlength = (@OsUntrusted int) (end - (start + pos));
        }
        return underLyingStream.read(pos + start , b, offset, nlength);
      }
      
      /**
       * position readable again.
       */
      @Override
      public void readFully(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this, @OsUntrusted long pos, @OsUntrusted byte @OsUntrusted [] b, @OsUntrusted int offset, @OsUntrusted int length) 
      throws IOException {
        if (start + length + pos > end) {
          throw new @OsUntrusted IOException("Not enough bytes to read.");
        }
        underLyingStream.readFully(pos + start, b, offset, length);
      }
      
      @Override
      public void readFully(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this, @OsUntrusted long pos, @OsUntrusted byte @OsUntrusted [] b) throws IOException {
          readFully(pos, b, 0, b.length);
      }

      @Override
      public void setReadahead(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this, @OsUntrusted Long readahead) throws IOException {
        underLyingStream.setReadahead(readahead);
      }

      @Override
      public void setDropBehind(HarFileSystem.HarFSDataInputStream.@OsUntrusted HarFsInputStream this, @OsUntrusted Boolean dropBehind) throws IOException {
        underLyingStream.setDropBehind(dropBehind);
      }
    }
  
    /**
     * constructors for har input stream.
     * @param fs the underlying filesystem
     * @param p The path in the underlying filesystem
     * @param start the start position in the part file
     * @param length the length of valid data in the part file
     * @param bufsize the buffer size
     * @throws IOException
     */
    public @OsUntrusted HarFSDataInputStream(@OsUntrusted FileSystem fs, @OsUntrusted Path  p, @OsUntrusted long start, 
        @OsUntrusted
        long length, @OsUntrusted int bufsize) throws IOException {
        super(new @OsUntrusted HarFsInputStream(fs, p, start, length, bufsize));
    }
  }

  private class HarMetaData {
    private @OsUntrusted FileSystem fs;
    private @OsUntrusted int version;
    // the masterIndex of the archive
    private @OsUntrusted Path masterIndexPath;
    // the index file 
    private @OsUntrusted Path archiveIndexPath;

    private @OsUntrusted long masterIndexTimestamp;
    private @OsUntrusted long archiveIndexTimestamp;

    @OsUntrusted
    List<@OsUntrusted Store> stores = new @OsUntrusted ArrayList<@OsUntrusted Store>();
    @OsUntrusted
    Map<@OsUntrusted Path, @OsUntrusted HarStatus> archive = new @OsUntrusted HashMap<@OsUntrusted Path, @OsUntrusted HarStatus>();
    private @OsUntrusted Map<@OsUntrusted Path, @OsUntrusted FileStatus> partFileStatuses = new @OsUntrusted HashMap<@OsUntrusted Path, @OsUntrusted FileStatus>();

    public @OsUntrusted HarMetaData(@OsUntrusted FileSystem fs, @OsUntrusted Path masterIndexPath, @OsUntrusted Path archiveIndexPath) {
      this.fs = fs;
      this.masterIndexPath = masterIndexPath;
      this.archiveIndexPath = archiveIndexPath;
    }

    public @OsUntrusted FileStatus getPartFileStatus(@OsUntrusted HarFileSystem.HarMetaData this, @OsUntrusted Path partPath) throws IOException {
      @OsUntrusted
      FileStatus status;
      status = partFileStatuses.get(partPath);
      if (status == null) {
        status = fs.getFileStatus(partPath);
        partFileStatuses.put(partPath, status);
      }
      return status;
    }

    public @OsUntrusted long getMasterIndexTimestamp(@OsUntrusted HarFileSystem.HarMetaData this) {
      return masterIndexTimestamp;
    }

    public @OsUntrusted long getArchiveIndexTimestamp(@OsUntrusted HarFileSystem.HarMetaData this) {
      return archiveIndexTimestamp;
    }

    private @OsUntrusted int getVersion(@OsUntrusted HarFileSystem.HarMetaData this) {
      return version;
    }

    private void parseMetaData(@OsUntrusted HarFileSystem.HarMetaData this) throws IOException {
      @OsUntrusted
      Text line = new @OsUntrusted Text();
      @OsUntrusted
      long read;
      @OsUntrusted
      FSDataInputStream in = null;
      @OsUntrusted
      LineReader lin = null;

      try {
        in = fs.open(masterIndexPath);
        @OsUntrusted
        FileStatus masterStat = fs.getFileStatus(masterIndexPath);
        masterIndexTimestamp = masterStat.getModificationTime();
        lin = new @OsUntrusted LineReader(in, getConf());
        read = lin.readLine(line);

        // the first line contains the version of the index file
        @OsUntrusted
        String versionLine = line.toString();
        @OsUntrusted
        String @OsUntrusted [] arr = versionLine.split(" ");
        version = Integer.parseInt(arr[0]);
        // make it always backwards-compatible
        if (this.version > HarFileSystem.VERSION) {
          throw new @OsUntrusted IOException("Invalid version " + 
              this.version + " expected " + HarFileSystem.VERSION);
        }

        // each line contains a hashcode range and the index file name
        @OsUntrusted
        String @OsUntrusted [] readStr;
        while(read < masterStat.getLen()) {
          @OsUntrusted
          int b = lin.readLine(line);
          read += b;
          readStr = line.toString().split(" ");
          @OsUntrusted
          int startHash = Integer.parseInt(readStr[0]);
          @OsUntrusted
          int endHash  = Integer.parseInt(readStr[1]);
          stores.add(new @OsUntrusted Store(Long.parseLong(readStr[2]), 
              Long.parseLong(readStr[3]), startHash,
              endHash));
          line.clear();
        }
      } catch (@OsUntrusted IOException ioe) {
        LOG.warn("Encountered exception ", ioe);
        throw ioe;
      } finally {
        IOUtils.cleanup(LOG, lin, in);
      }

      @OsUntrusted
      FSDataInputStream aIn = fs.open(archiveIndexPath);
      try {
        @OsUntrusted
        FileStatus archiveStat = fs.getFileStatus(archiveIndexPath);
        archiveIndexTimestamp = archiveStat.getModificationTime();
        @OsUntrusted
        LineReader aLin;

        // now start reading the real index file
        for (@OsUntrusted Store s: stores) {
          read = 0;
          aIn.seek(s.begin);
          aLin = new @OsUntrusted LineReader(aIn, getConf());
          while (read + s.begin < s.end) {
            @OsUntrusted
            int tmp = aLin.readLine(line);
            read += tmp;
            @OsUntrusted
            String lineFeed = line.toString();
            @OsUntrusted
            String @OsUntrusted [] parsed = lineFeed.split(" ");
            parsed[0] = decodeFileName(parsed[0]);
            archive.put(new @OsUntrusted Path(parsed[0]), new @OsUntrusted HarStatus(lineFeed));
            line.clear();
          }
        }
      } finally {
        IOUtils.cleanup(LOG, aIn);
      }
    }
  }
  
  /*
   * testing purposes only:
   */
  @OsUntrusted
  HarMetaData getMetadata(@OsUntrusted HarFileSystem this) {
    return metadata;
  }

  private static class LruCache<@OsUntrusted K extends java.lang.@OsUntrusted Object, @OsUntrusted V extends java.lang.@OsUntrusted Object> extends @OsUntrusted LinkedHashMap<K, V> {
    private final @OsUntrusted int MAX_ENTRIES;

    public @OsUntrusted LruCache(@OsUntrusted int maxEntries) {
        super(maxEntries + 1, 1.0f, true);
        MAX_ENTRIES = maxEntries;
    }

    @Override
    protected @OsUntrusted boolean removeEldestEntry(HarFileSystem.@OsUntrusted LruCache<K, V> this, Map.@OsUntrusted Entry<@OsUntrusted K, @OsUntrusted V> eldest) {
        return size() > MAX_ENTRIES;
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public @OsUntrusted FsServerDefaults getServerDefaults(@OsUntrusted HarFileSystem this) throws IOException {
    return fs.getServerDefaults();
  }

  @Override
  public @OsUntrusted FsServerDefaults getServerDefaults(@OsUntrusted HarFileSystem this, @OsUntrusted Path f) throws IOException {
    return fs.getServerDefaults(f);
  }

  @Override
  public @OsUntrusted long getUsed(@OsUntrusted HarFileSystem this) throws IOException{
    return fs.getUsed();
  }

  @SuppressWarnings("deprecation")
  @Override
  public @OsUntrusted long getDefaultBlockSize(@OsUntrusted HarFileSystem this) {
    return fs.getDefaultBlockSize();
  }

  @SuppressWarnings("deprecation")
  @Override
  public @OsUntrusted long getDefaultBlockSize(@OsUntrusted HarFileSystem this, @OsUntrusted Path f) {
    return fs.getDefaultBlockSize(f);
  }

  @SuppressWarnings("deprecation")
  @Override
  public @OsUntrusted short getDefaultReplication(@OsUntrusted HarFileSystem this) {
    return fs.getDefaultReplication();
  }

  @Override
  public @OsUntrusted short getDefaultReplication(@OsUntrusted HarFileSystem this, @OsUntrusted Path f) {
    return fs.getDefaultReplication(f);
  }
}
