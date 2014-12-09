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
package org.apache.hadoop.io.nativeio;
import ostrusted.quals.OsUntrusted;

/**
 * Enum representing POSIX errno values.
 */
public enum Errno {

@OsUntrusted  EPERM,

@OsUntrusted  ENOENT,

@OsUntrusted  ESRCH,

@OsUntrusted  EINTR,

@OsUntrusted  EIO,

@OsUntrusted  ENXIO,

@OsUntrusted  E2BIG,

@OsUntrusted  ENOEXEC,

@OsUntrusted  EBADF,

@OsUntrusted  ECHILD,

@OsUntrusted  EAGAIN,

@OsUntrusted  ENOMEM,

@OsUntrusted  EACCES,

@OsUntrusted  EFAULT,

@OsUntrusted  ENOTBLK,

@OsUntrusted  EBUSY,

@OsUntrusted  EEXIST,

@OsUntrusted  EXDEV,

@OsUntrusted  ENODEV,

@OsUntrusted  ENOTDIR,

@OsUntrusted  EISDIR,

@OsUntrusted  EINVAL,

@OsUntrusted  ENFILE,

@OsUntrusted  EMFILE,

@OsUntrusted  ENOTTY,

@OsUntrusted  ETXTBSY,

@OsUntrusted  EFBIG,

@OsUntrusted  ENOSPC,

@OsUntrusted  ESPIPE,

@OsUntrusted  EROFS,

@OsUntrusted  EMLINK,

@OsUntrusted  EPIPE,

@OsUntrusted  EDOM,

@OsUntrusted  ERANGE,

@OsUntrusted  ELOOP,

@OsUntrusted  ENAMETOOLONG,

@OsUntrusted  ENOTEMPTY,


@OsUntrusted  UNKNOWN;
}
