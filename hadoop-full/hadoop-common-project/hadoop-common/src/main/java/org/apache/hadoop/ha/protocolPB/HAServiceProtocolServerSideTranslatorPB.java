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
package org.apache.hadoop.ha.protocolPB;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStatusRequestProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.GetServiceStatusResponseProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAStateChangeRequestInfoProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceStateProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.MonitorHealthRequestProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.MonitorHealthResponseProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToActiveRequestProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToActiveResponseProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToStandbyRequestProto;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.TransitionToStandbyResponseProto;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class is used on the server side. Calls come across the wire for the
 * for protocol {@link HAServiceProtocolPB}.
 * This class translates the PB data types
 * to the native data types used inside the NN as specified in the generic
 * ClientProtocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class HAServiceProtocolServerSideTranslatorPB implements
    @OsUntrusted
    HAServiceProtocolPB {
  private final @OsUntrusted HAServiceProtocol server;
  private static final @OsUntrusted MonitorHealthResponseProto MONITOR_HEALTH_RESP = 
      MonitorHealthResponseProto.newBuilder().build();
  private static final @OsUntrusted TransitionToActiveResponseProto TRANSITION_TO_ACTIVE_RESP = 
      TransitionToActiveResponseProto.newBuilder().build();
  private static final @OsUntrusted TransitionToStandbyResponseProto TRANSITION_TO_STANDBY_RESP = 
      TransitionToStandbyResponseProto.newBuilder().build();
  private static final @OsUntrusted Log LOG = LogFactory.getLog(
      HAServiceProtocolServerSideTranslatorPB.class);
  
  public @OsUntrusted HAServiceProtocolServerSideTranslatorPB(@OsUntrusted HAServiceProtocol server) {
    this.server = server;
  }

  @Override
  public @OsUntrusted MonitorHealthResponseProto monitorHealth(@OsUntrusted HAServiceProtocolServerSideTranslatorPB this, @OsUntrusted RpcController controller,
      @OsUntrusted
      MonitorHealthRequestProto request) throws ServiceException {
    try {
      server.monitorHealth();
      return MONITOR_HEALTH_RESP;
    } catch(@OsUntrusted IOException e) {
      throw new @OsUntrusted ServiceException(e);
    }
  }
  
  private @OsUntrusted StateChangeRequestInfo convert(@OsUntrusted HAServiceProtocolServerSideTranslatorPB this, @OsUntrusted HAStateChangeRequestInfoProto proto) {
    @OsUntrusted
    RequestSource src;
    switch (proto.getReqSource()) {
    case REQUEST_BY_USER:
      src = RequestSource.REQUEST_BY_USER;
      break;
    case REQUEST_BY_USER_FORCED:
      src = RequestSource.REQUEST_BY_USER_FORCED;
      break;
    case REQUEST_BY_ZKFC:
      src = RequestSource.REQUEST_BY_ZKFC;
      break;
    default:
      LOG.warn("Unknown request source: " + proto.getReqSource());
      src = null;
    }
    
    return new @OsUntrusted StateChangeRequestInfo(src);
  }

  @Override
  public @OsUntrusted TransitionToActiveResponseProto transitionToActive(
      @OsUntrusted HAServiceProtocolServerSideTranslatorPB this, @OsUntrusted
      RpcController controller, @OsUntrusted TransitionToActiveRequestProto request)
      throws ServiceException {
    try {
      server.transitionToActive(convert(request.getReqInfo()));
      return TRANSITION_TO_ACTIVE_RESP;
    } catch(@OsUntrusted IOException e) {
      throw new @OsUntrusted ServiceException(e);
    }
  }

  @Override
  public @OsUntrusted TransitionToStandbyResponseProto transitionToStandby(
      @OsUntrusted HAServiceProtocolServerSideTranslatorPB this, @OsUntrusted
      RpcController controller, @OsUntrusted TransitionToStandbyRequestProto request)
      throws ServiceException {
    try {
      server.transitionToStandby(convert(request.getReqInfo()));
      return TRANSITION_TO_STANDBY_RESP;
    } catch(@OsUntrusted IOException e) {
      throw new @OsUntrusted ServiceException(e);
    }
  }

  @Override
  public @OsUntrusted GetServiceStatusResponseProto getServiceStatus(@OsUntrusted HAServiceProtocolServerSideTranslatorPB this, @OsUntrusted RpcController controller,
      @OsUntrusted
      GetServiceStatusRequestProto request) throws ServiceException {
    @OsUntrusted
    HAServiceStatus s;
    try {
      s = server.getServiceStatus();
    } catch(@OsUntrusted IOException e) {
      throw new @OsUntrusted ServiceException(e);
    }
    
    @OsUntrusted
    HAServiceStateProto retState;
    switch (s.getState()) {
    case ACTIVE:
      retState = HAServiceStateProto.ACTIVE;
      break;
    case STANDBY:
      retState = HAServiceStateProto.STANDBY;
      break;
    case INITIALIZING:
    default:
      retState = HAServiceStateProto.INITIALIZING;
      break;
    }
    
    GetServiceStatusResponseProto.@OsUntrusted Builder ret =
      GetServiceStatusResponseProto.newBuilder()
        .setState(retState)
        .setReadyToBecomeActive(s.isReadyToBecomeActive());
    if (!s.isReadyToBecomeActive()) {
      ret.setNotReadyReason(s.getNotReadyReason());
    }
    return ret.build();
  }

  @Override
  public @OsUntrusted long getProtocolVersion(@OsUntrusted HAServiceProtocolServerSideTranslatorPB this, @OsUntrusted String protocol, @OsUntrusted long clientVersion)
      throws IOException {
    return RPC.getProtocolVersion(HAServiceProtocolPB.class);
  }

  @Override
  public @OsUntrusted ProtocolSignature getProtocolSignature(@OsUntrusted HAServiceProtocolServerSideTranslatorPB this, @OsUntrusted String protocol,
      @OsUntrusted
      long clientVersion, @OsUntrusted int clientMethodsHash) throws IOException {
    if (!protocol.equals(RPC.getProtocolName(HAServiceProtocolPB.class))) {
      throw new @OsUntrusted IOException("Serverside implements " +
          RPC.getProtocolName(HAServiceProtocolPB.class) +
          ". The following requested protocol is unknown: " + protocol);
    }

    return ProtocolSignature.getProtocolSignature(clientMethodsHash,
        RPC.getProtocolVersion(HAServiceProtocolPB.class),
        HAServiceProtocolPB.class);
  }
}
