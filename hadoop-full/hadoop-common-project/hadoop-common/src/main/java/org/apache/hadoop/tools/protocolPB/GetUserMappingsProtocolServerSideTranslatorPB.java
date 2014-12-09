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

package org.apache.hadoop.tools.protocolPB;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;

import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserRequestProto;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetGroupsForUserResponseProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class GetUserMappingsProtocolServerSideTranslatorPB implements
    @OsUntrusted
    GetUserMappingsProtocolPB {

  private final @OsUntrusted GetUserMappingsProtocol impl;

  public @OsUntrusted GetUserMappingsProtocolServerSideTranslatorPB(
      @OsUntrusted
      GetUserMappingsProtocol impl) {
    this.impl = impl;
  }

  @Override
  public @OsUntrusted GetGroupsForUserResponseProto getGroupsForUser(
      @OsUntrusted GetUserMappingsProtocolServerSideTranslatorPB this, @OsUntrusted
      RpcController controller, @OsUntrusted GetGroupsForUserRequestProto request)
      throws ServiceException {
    @OsUntrusted
    String @OsUntrusted [] groups;
    try {
      groups = impl.getGroupsForUser(request.getUser());
    } catch (@OsUntrusted IOException e) {
      throw new @OsUntrusted ServiceException(e);
    }
    GetGroupsForUserResponseProto.@OsUntrusted Builder builder = GetGroupsForUserResponseProto
        .newBuilder();
    for (@OsUntrusted String g : groups) {
      builder.addGroups(g);
    }
    return builder.build();
  }
}
