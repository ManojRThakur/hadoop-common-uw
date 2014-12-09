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
package org.apache.hadoop.ha;

import ostrusted.quals.OsUntrusted;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ActiveStandbyElector.ActiveNotFoundException;
import org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.util.ZKUtil.ZKAuthInfo;
import org.apache.hadoop.ha.HealthMonitor.State;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.data.ACL;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@InterfaceAudience.LimitedPrivate("HDFS")
public abstract class ZKFailoverController {

  static final @OsUntrusted Log LOG = LogFactory.getLog(ZKFailoverController.class);
  
  public static final @OsUntrusted String ZK_QUORUM_KEY = "ha.zookeeper.quorum";
  private static final @OsUntrusted String ZK_SESSION_TIMEOUT_KEY = "ha.zookeeper.session-timeout.ms";
  private static final @OsUntrusted int ZK_SESSION_TIMEOUT_DEFAULT = 5*1000;
  private static final @OsUntrusted String ZK_PARENT_ZNODE_KEY = "ha.zookeeper.parent-znode";
  public static final @OsUntrusted String ZK_ACL_KEY = "ha.zookeeper.acl";
  private static final @OsUntrusted String ZK_ACL_DEFAULT = "world:anyone:rwcda";
  public static final @OsUntrusted String ZK_AUTH_KEY = "ha.zookeeper.auth";
  static final @OsUntrusted String ZK_PARENT_ZNODE_DEFAULT = "/hadoop-ha";

  /**
   * All of the conf keys used by the ZKFC. This is used in order to allow
   * them to be overridden on a per-nameservice or per-namenode basis.
   */
  protected static final @OsUntrusted String @OsUntrusted [] ZKFC_CONF_KEYS = new @OsUntrusted String @OsUntrusted [] {
    ZK_QUORUM_KEY,
    ZK_SESSION_TIMEOUT_KEY,
    ZK_PARENT_ZNODE_KEY,
    ZK_ACL_KEY,
    ZK_AUTH_KEY
  };
  
  protected static final @OsUntrusted String USAGE = 
      "Usage: java zkfc [ -formatZK [-force] [-nonInteractive] ]";

  /** Unable to format the parent znode in ZK */
  static final @OsUntrusted int ERR_CODE_FORMAT_DENIED = 2;
  /** The parent znode doesn't exist in ZK */
  static final @OsUntrusted int ERR_CODE_NO_PARENT_ZNODE = 3;
  /** Fencing is not properly configured */
  static final @OsUntrusted int ERR_CODE_NO_FENCER = 4;
  /** Automatic failover is not enabled */
  static final @OsUntrusted int ERR_CODE_AUTO_FAILOVER_NOT_ENABLED = 5;
  /** Cannot connect to ZooKeeper */
  static final @OsUntrusted int ERR_CODE_NO_ZK = 6;
  
  protected @OsUntrusted Configuration conf;
  private @OsUntrusted String zkQuorum;
  protected final @OsUntrusted HAServiceTarget localTarget;

  private @OsUntrusted HealthMonitor healthMonitor;
  private @OsUntrusted ActiveStandbyElector elector;
  protected @OsUntrusted ZKFCRpcServer rpcServer;

  private @OsUntrusted State lastHealthState = State.INITIALIZING;

  /** Set if a fatal error occurs */
  private @OsUntrusted String fatalError = null;

  /**
   * A future nanotime before which the ZKFC will not join the election.
   * This is used during graceful failover.
   */
  private @OsUntrusted long delayJoiningUntilNanotime = 0;

  /** Executor on which {@link #scheduleRecheck(long)} schedules events */
  private @OsUntrusted ScheduledExecutorService delayExecutor =
    Executors.newScheduledThreadPool(1,
        new @OsUntrusted ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("ZKFC Delay timer #%d")
            .build());

  private @OsUntrusted ActiveAttemptRecord lastActiveAttemptRecord;
  private @OsUntrusted Object activeAttemptRecordLock = new @OsUntrusted Object();

  protected @OsUntrusted ZKFailoverController(@OsUntrusted Configuration conf, @OsUntrusted HAServiceTarget localTarget) {
    this.localTarget = localTarget;
    this.conf = conf;
  }
  

  protected abstract @OsUntrusted byte @OsUntrusted [] targetToData(@OsUntrusted ZKFailoverController this, @OsUntrusted HAServiceTarget target);
  protected abstract @OsUntrusted HAServiceTarget dataToTarget(@OsUntrusted ZKFailoverController this, @OsUntrusted byte @OsUntrusted [] data);
  protected abstract void loginAsFCUser(@OsUntrusted ZKFailoverController this) throws IOException;
  protected abstract void checkRpcAdminAccess(@OsUntrusted ZKFailoverController this)
      throws AccessControlException, IOException;
  protected abstract @OsUntrusted InetSocketAddress getRpcAddressToBindTo(@OsUntrusted ZKFailoverController this);
  protected abstract @OsUntrusted PolicyProvider getPolicyProvider(@OsUntrusted ZKFailoverController this);

  /**
   * Return the name of a znode inside the configured parent znode in which
   * the ZKFC will do all of its work. This is so that multiple federated
   * nameservices can run on the same ZK quorum without having to manually
   * configure them to separate subdirectories.
   */
  protected abstract @OsUntrusted String getScopeInsideParentNode(@OsUntrusted ZKFailoverController this);

  public @OsUntrusted HAServiceTarget getLocalTarget(@OsUntrusted ZKFailoverController this) {
    return localTarget;
  }
  
  public @OsUntrusted int run(@OsUntrusted ZKFailoverController this, final @OsUntrusted String @OsUntrusted [] args) throws Exception {
    if (!localTarget.isAutoFailoverEnabled()) {
      LOG.fatal("Automatic failover is not enabled for " + localTarget + "." +
          " Please ensure that automatic failover is enabled in the " +
          "configuration before running the ZK failover controller.");
      return ERR_CODE_AUTO_FAILOVER_NOT_ENABLED;
    }
    loginAsFCUser();
    try {
      return SecurityUtil.doAsLoginUserOrFatal(new @OsUntrusted PrivilegedAction<@OsUntrusted Integer>() {
        @Override
        public @OsUntrusted Integer run() {
          try {
            return doRun(args);
          } catch (@OsUntrusted Exception t) {
            throw new @OsUntrusted RuntimeException(t);
          } finally {
            if (elector != null) {
              elector.terminateConnection();
            }
          }
        }
      });
    } catch (@OsUntrusted RuntimeException rte) {
      throw (@OsUntrusted Exception)rte.getCause();
    }
  }
  

  private @OsUntrusted int doRun(@OsUntrusted ZKFailoverController this, @OsUntrusted String @OsUntrusted [] args)
      throws HadoopIllegalArgumentException, IOException, InterruptedException {
    try {
      initZK();
    } catch (@OsUntrusted KeeperException ke) {
      LOG.fatal("Unable to start failover controller. Unable to connect "
          + "to ZooKeeper quorum at " + zkQuorum + ". Please check the "
          + "configured value for " + ZK_QUORUM_KEY + " and ensure that "
          + "ZooKeeper is running.");
      return ERR_CODE_NO_ZK;
    }
    if (args.length > 0) {
      if ("-formatZK".equals(args[0])) {
        @OsUntrusted
        boolean force = false;
        @OsUntrusted
        boolean interactive = true;
        for (@OsUntrusted int i = 1; i < args.length; i++) {
          if ("-force".equals(args[i])) {
            force = true;
          } else if ("-nonInteractive".equals(args[i])) {
            interactive = false;
          } else {
            badArg(args[i]);
          }
        }
        return formatZK(force, interactive);
      } else {
        badArg(args[0]);
      }
    }

    if (!elector.parentZNodeExists()) {
      LOG.fatal("Unable to start failover controller. "
          + "Parent znode does not exist.\n"
          + "Run with -formatZK flag to initialize ZooKeeper.");
      return ERR_CODE_NO_PARENT_ZNODE;
    }

    try {
      localTarget.checkFencingConfigured();
    } catch (@OsUntrusted BadFencingConfigurationException e) {
      LOG.fatal("Fencing is not configured for " + localTarget + ".\n" +
          "You must configure a fencing method before using automatic " +
          "failover.", e);
      return ERR_CODE_NO_FENCER;
    }

    initRPC();
    initHM();
    startRPC();
    try {
      mainLoop();
    } finally {
      rpcServer.stopAndJoin();
      
      elector.quitElection(true);
      healthMonitor.shutdown();
      healthMonitor.join();
    }
    return 0;
  }

  private void badArg(@OsUntrusted ZKFailoverController this, @OsUntrusted String arg) {
    printUsage();
    throw new @OsUntrusted HadoopIllegalArgumentException(
        "Bad argument: " + arg);
  }

  private void printUsage(@OsUntrusted ZKFailoverController this) {
    System.err.println(USAGE + "\n");
  }

  private @OsUntrusted int formatZK(@OsUntrusted ZKFailoverController this, @OsUntrusted boolean force, @OsUntrusted boolean interactive)
      throws IOException, InterruptedException {
    if (elector.parentZNodeExists()) {
      if (!force && (!interactive || !confirmFormat())) {
        return ERR_CODE_FORMAT_DENIED;
      }
      
      try {
        elector.clearParentZNode();
      } catch (@OsUntrusted IOException e) {
        LOG.error("Unable to clear zk parent znode", e);
        return 1;
      }
    }
    
    elector.ensureParentZNode();
    return 0;
  }

  private @OsUntrusted boolean confirmFormat(@OsUntrusted ZKFailoverController this) {
    @OsUntrusted
    String parentZnode = getParentZnode();
    System.err.println(
        "===============================================\n" +
        "The configured parent znode " + parentZnode + " already exists.\n" +
        "Are you sure you want to clear all failover information from\n" +
        "ZooKeeper?\n" +
        "WARNING: Before proceeding, ensure that all HDFS services and\n" +
        "failover controllers are stopped!\n" +
        "===============================================");
    try {
      return ToolRunner.confirmPrompt("Proceed formatting " + parentZnode + "?");
    } catch (@OsUntrusted IOException e) {
      LOG.debug("Failed to confirm", e);
      return false;
    }
  }

  // ------------------------------------------
  // Begin actual guts of failover controller
  // ------------------------------------------
  
  private void initHM(@OsUntrusted ZKFailoverController this) {
    healthMonitor = new @OsUntrusted HealthMonitor(conf, localTarget);
    healthMonitor.addCallback(new @OsUntrusted HealthCallbacks());
    healthMonitor.start();
  }
  
  protected void initRPC(@OsUntrusted ZKFailoverController this) throws IOException {
    @OsUntrusted
    InetSocketAddress bindAddr = getRpcAddressToBindTo();
    rpcServer = new @OsUntrusted ZKFCRpcServer(conf, bindAddr, this, getPolicyProvider());
  }

  protected void startRPC(@OsUntrusted ZKFailoverController this) throws IOException {
    rpcServer.start();
  }


  private void initZK(@OsUntrusted ZKFailoverController this) throws HadoopIllegalArgumentException, IOException,
      KeeperException {
    zkQuorum = conf.get(ZK_QUORUM_KEY);
    @OsUntrusted
    int zkTimeout = conf.getInt(ZK_SESSION_TIMEOUT_KEY,
        ZK_SESSION_TIMEOUT_DEFAULT);
    // Parse ACLs from configuration.
    @OsUntrusted
    String zkAclConf = conf.get(ZK_ACL_KEY, ZK_ACL_DEFAULT);
    zkAclConf = ZKUtil.resolveConfIndirection(zkAclConf);
    @OsUntrusted
    List<@OsUntrusted ACL> zkAcls = ZKUtil.parseACLs(zkAclConf);
    if (zkAcls.isEmpty()) {
      zkAcls = Ids.CREATOR_ALL_ACL;
    }
    
    // Parse authentication from configuration.
    @OsUntrusted
    String zkAuthConf = conf.get(ZK_AUTH_KEY);
    zkAuthConf = ZKUtil.resolveConfIndirection(zkAuthConf);
    @OsUntrusted
    List<@OsUntrusted ZKAuthInfo> zkAuths;
    if (zkAuthConf != null) {
      zkAuths = ZKUtil.parseAuth(zkAuthConf);
    } else {
      zkAuths = Collections.emptyList();
    }

    // Sanity check configuration.
    Preconditions.checkArgument(zkQuorum != null,
        "Missing required configuration '%s' for ZooKeeper quorum",
        ZK_QUORUM_KEY);
    Preconditions.checkArgument(zkTimeout > 0,
        "Invalid ZK session timeout %s", zkTimeout);
    

    elector = new @OsUntrusted ActiveStandbyElector(zkQuorum,
        zkTimeout, getParentZnode(), zkAcls, zkAuths,
        new @OsUntrusted ElectorCallbacks());
  }
  
  private @OsUntrusted String getParentZnode(@OsUntrusted ZKFailoverController this) {
    @OsUntrusted
    String znode = conf.get(ZK_PARENT_ZNODE_KEY,
        ZK_PARENT_ZNODE_DEFAULT);
    if (!znode.endsWith("/")) {
      znode += "/";
    }
    return znode + getScopeInsideParentNode();
  }

  private synchronized void mainLoop(@OsUntrusted ZKFailoverController this) throws InterruptedException {
    while (fatalError == null) {
      wait();
    }
    assert fatalError != null; // only get here on fatal
    throw new @OsUntrusted RuntimeException(
        "ZK Failover Controller failed: " + fatalError);
  }
  
  private synchronized void fatalError(@OsUntrusted ZKFailoverController this, @OsUntrusted String err) {
    LOG.fatal("Fatal error occurred:" + err);
    fatalError = err;
    notifyAll();
  }
  
  private synchronized void becomeActive(@OsUntrusted ZKFailoverController this) throws ServiceFailedException {
    LOG.info("Trying to make " + localTarget + " active...");
    try {
      HAServiceProtocolHelper.transitionToActive(localTarget.getProxy(
          conf, FailoverController.getRpcTimeoutToNewActive(conf)),
          createReqInfo());
      @OsUntrusted
      String msg = "Successfully transitioned " + localTarget +
          " to active state";
      LOG.info(msg);
      recordActiveAttempt(new @OsUntrusted ActiveAttemptRecord(true, msg));

    } catch (@OsUntrusted Throwable t) {
      @OsUntrusted
      String msg = "Couldn't make " + localTarget + " active";
      LOG.fatal(msg, t);
      
      recordActiveAttempt(new @OsUntrusted ActiveAttemptRecord(false, msg + "\n" +
          StringUtils.stringifyException(t)));

      if (t instanceof @OsUntrusted ServiceFailedException) {
        throw (@OsUntrusted ServiceFailedException)t;
      } else {
        throw new @OsUntrusted ServiceFailedException("Couldn't transition to active",
            t);
      }
/*
* TODO:
* we need to make sure that if we get fenced and then quickly restarted,
* none of these calls will retry across the restart boundary
* perhaps the solution is that, whenever the nn starts, it gets a unique
* ID, and when we start becoming active, we record it, and then any future
* calls use the same ID
*/
      
    }
  }

  /**
   * Store the results of the last attempt to become active.
   * This is used so that, during manually initiated failover,
   * we can report back the results of the attempt to become active
   * to the initiator of the failover.
   */
  private void recordActiveAttempt(
      @OsUntrusted ZKFailoverController this, @OsUntrusted
      ActiveAttemptRecord record) {
    synchronized (activeAttemptRecordLock) {
      lastActiveAttemptRecord = record;
      activeAttemptRecordLock.notifyAll();
    }
  }

  /**
   * Wait until one of the following events:
   * <ul>
   * <li>Another thread publishes the results of an attempt to become active
   * using {@link #recordActiveAttempt(ActiveAttemptRecord)}</li>
   * <li>The node enters bad health status</li>
   * <li>The specified timeout elapses</li>
   * </ul>
   * 
   * @param timeoutMillis number of millis to wait
   * @return the published record, or null if the timeout elapses or the
   * service becomes unhealthy 
   * @throws InterruptedException if the thread is interrupted.
   */
  private @OsUntrusted ActiveAttemptRecord waitForActiveAttempt(@OsUntrusted ZKFailoverController this, @OsUntrusted int timeoutMillis)
      throws InterruptedException {
    @OsUntrusted
    long st = System.nanoTime();
    @OsUntrusted
    long waitUntil = st + TimeUnit.NANOSECONDS.convert(
        timeoutMillis, TimeUnit.MILLISECONDS);
    
    do {
      // periodically check health state, because entering an
      // unhealthy state could prevent us from ever attempting to
      // become active. We can detect this and respond to the user
      // immediately.
      synchronized (this) {
        if (lastHealthState != State.SERVICE_HEALTHY) {
          // early out if service became unhealthy
          return null;
        }
      }

      synchronized (activeAttemptRecordLock) {
        if ((lastActiveAttemptRecord != null &&
            lastActiveAttemptRecord.nanoTime >= st)) {
          return lastActiveAttemptRecord;
        }
        // Only wait 1sec so that we periodically recheck the health state
        // above.
        activeAttemptRecordLock.wait(1000);
      }
    } while (System.nanoTime() < waitUntil);
    
    // Timeout elapsed.
    LOG.warn(timeoutMillis + "ms timeout elapsed waiting for an attempt " +
        "to become active");
    return null;
  }

  private @OsUntrusted StateChangeRequestInfo createReqInfo(@OsUntrusted ZKFailoverController this) {
    return new @OsUntrusted StateChangeRequestInfo(RequestSource.REQUEST_BY_ZKFC);
  }

  private synchronized void becomeStandby(@OsUntrusted ZKFailoverController this) {
    LOG.info("ZK Election indicated that " + localTarget +
        " should become standby");
    try {
      @OsUntrusted
      int timeout = FailoverController.getGracefulFenceTimeout(conf);
      localTarget.getProxy(conf, timeout).transitionToStandby(createReqInfo());
      LOG.info("Successfully transitioned " + localTarget +
          " to standby state");
    } catch (@OsUntrusted Exception e) {
      LOG.error("Couldn't transition " + localTarget + " to standby state",
          e);
      // TODO handle this. It's a likely case since we probably got fenced
      // at the same time.
    }
  }
  

  private synchronized void fenceOldActive(@OsUntrusted ZKFailoverController this, @OsUntrusted byte @OsUntrusted [] data) {
    @OsUntrusted
    HAServiceTarget target = dataToTarget(data);
    
    try {
      doFence(target);
    } catch (@OsUntrusted Throwable t) {
      recordActiveAttempt(new @OsUntrusted ActiveAttemptRecord(false, "Unable to fence old active: " + StringUtils.stringifyException(t)));
      Throwables.propagate(t);
    }
  }
  
  private void doFence(@OsUntrusted ZKFailoverController this, @OsUntrusted HAServiceTarget target) {
    LOG.info("Should fence: " + target);
    @OsUntrusted
    boolean gracefulWorked = new @OsUntrusted FailoverController(conf,
        RequestSource.REQUEST_BY_ZKFC).tryGracefulFence(target);
    if (gracefulWorked) {
      // It's possible that it's in standby but just about to go into active,
      // no? Is there some race here?
      LOG.info("Successfully transitioned " + target + " to standby " +
          "state without fencing");
      return;
    }
    
    try {
      target.checkFencingConfigured();
    } catch (@OsUntrusted BadFencingConfigurationException e) {
      LOG.error("Couldn't fence old active " + target, e);
      recordActiveAttempt(new @OsUntrusted ActiveAttemptRecord(false, "Unable to fence old active"));
      throw new @OsUntrusted RuntimeException(e);
    }
    
    if (!target.getFencer().fence(target)) {
      throw new @OsUntrusted RuntimeException("Unable to fence " + target);
    }
  }


  /**
   * Request from graceful failover to cede active role. Causes
   * this ZKFC to transition its local node to standby, then quit
   * the election for the specified period of time, after which it
   * will rejoin iff it is healthy.
   */
  void cedeActive(@OsUntrusted ZKFailoverController this, final @OsUntrusted int millisToCede)
      throws AccessControlException, ServiceFailedException, IOException {
    try {
      UserGroupInformation.getLoginUser().doAs(new @OsUntrusted PrivilegedExceptionAction<@OsUntrusted Void>() {
        @Override
        public @OsUntrusted Void run() throws Exception {
          doCedeActive(millisToCede);
          return null;
        }
      });
    } catch (@OsUntrusted InterruptedException e) {
      throw new @OsUntrusted IOException(e);
    }
  }
  
  private void doCedeActive(@OsUntrusted ZKFailoverController this, @OsUntrusted int millisToCede) 
      throws AccessControlException, ServiceFailedException, IOException {
    @OsUntrusted
    int timeout = FailoverController.getGracefulFenceTimeout(conf);

    // Lock elector to maintain lock ordering of elector -> ZKFC
    synchronized (elector) {
      synchronized (this) {
        if (millisToCede <= 0) {
          delayJoiningUntilNanotime = 0;
          recheckElectability();
          return;
        }
  
        LOG.info("Requested by " + UserGroupInformation.getCurrentUser() +
            " at " + Server.getRemoteAddress() + " to cede active role.");
        @OsUntrusted
        boolean needFence = false;
        try {
          localTarget.getProxy(conf, timeout).transitionToStandby(createReqInfo());
          LOG.info("Successfully ensured local node is in standby mode");
        } catch (@OsUntrusted IOException ioe) {
          LOG.warn("Unable to transition local node to standby: " +
              ioe.getLocalizedMessage());
          LOG.warn("Quitting election but indicating that fencing is " +
              "necessary");
          needFence = true;
        }
        delayJoiningUntilNanotime = System.nanoTime() +
            TimeUnit.MILLISECONDS.toNanos(millisToCede);
        elector.quitElection(needFence);
      }
    }
    recheckElectability();
  }
  
  /**
   * Coordinate a graceful failover to this node.
   * @throws ServiceFailedException if the node fails to become active
   * @throws IOException some other error occurs
   */
  void gracefulFailoverToYou(@OsUntrusted ZKFailoverController this) throws ServiceFailedException, IOException {
    try {
      UserGroupInformation.getLoginUser().doAs(new @OsUntrusted PrivilegedExceptionAction<@OsUntrusted Void>() {
        @Override
        public @OsUntrusted Void run() throws Exception {
          doGracefulFailover();
          return null;
        }
        
      });
    } catch (@OsUntrusted InterruptedException e) {
      throw new @OsUntrusted IOException(e);
    }
  }

  /**
   * Coordinate a graceful failover. This proceeds in several phases:
   * 1) Pre-flight checks: ensure that the local node is healthy, and
   * thus a candidate for failover.
   * 2) Determine the current active node. If it is the local node, no
   * need to failover - return success.
   * 3) Ask that node to yield from the election for a number of seconds.
   * 4) Allow the normal election path to run in other threads. Wait until
   * we either become unhealthy or we see an election attempt recorded by
   * the normal code path.
   * 5) Allow the old active to rejoin the election, so a future
   * failback is possible.
   */
  private void doGracefulFailover(@OsUntrusted ZKFailoverController this)
      throws ServiceFailedException, IOException, InterruptedException {
    @OsUntrusted
    int timeout = FailoverController.getGracefulFenceTimeout(conf) * 2;
    
    // Phase 1: pre-flight checks
    checkEligibleForFailover();
    
    // Phase 2: determine old/current active node. Check that we're not
    // ourselves active, etc.
    @OsUntrusted
    HAServiceTarget oldActive = getCurrentActive();
    if (oldActive == null) {
      // No node is currently active. So, if we aren't already
      // active ourselves by means of a normal election, then there's
      // probably something preventing us from becoming active.
      throw new @OsUntrusted ServiceFailedException(
          "No other node is currently active.");
    }
    
    if (oldActive.getAddress().equals(localTarget.getAddress())) {
      LOG.info("Local node " + localTarget + " is already active. " +
          "No need to failover. Returning success.");
      return;
    }
    
    // Phase 3: ask the old active to yield from the election.
    LOG.info("Asking " + oldActive + " to cede its active state for " +
        timeout + "ms");
    @OsUntrusted
    ZKFCProtocol oldZkfc = oldActive.getZKFCProxy(conf, timeout);
    oldZkfc.cedeActive(timeout);

    // Phase 4: wait for the normal election to make the local node
    // active.
    @OsUntrusted
    ActiveAttemptRecord attempt = waitForActiveAttempt(timeout + 60000);
    
    if (attempt == null) {
      // We didn't even make an attempt to become active.
      synchronized(this) {
        if (lastHealthState != State.SERVICE_HEALTHY) {
          throw new @OsUntrusted ServiceFailedException("Unable to become active. " +
            "Service became unhealthy while trying to failover.");          
        }
      }
      
      throw new @OsUntrusted ServiceFailedException("Unable to become active. " +
          "Local node did not get an opportunity to do so from ZooKeeper, " +
          "or the local node took too long to transition to active.");
    }

    // Phase 5. At this point, we made some attempt to become active. So we
    // can tell the old active to rejoin if it wants. This allows a quick
    // fail-back if we immediately crash.
    oldZkfc.cedeActive(-1);
    
    if (attempt.succeeded) {
      LOG.info("Successfully became active. " + attempt.status);
    } else {
      // Propagate failure
      @OsUntrusted
      String msg = "Failed to become active. " + attempt.status;
      throw new @OsUntrusted ServiceFailedException(msg);
    }
  }

  /**
   * Ensure that the local node is in a healthy state, and thus
   * eligible for graceful failover.
   * @throws ServiceFailedException if the node is unhealthy
   */
  private synchronized void checkEligibleForFailover(@OsUntrusted ZKFailoverController this)
      throws ServiceFailedException {
    // Check health
    if (this.getLastHealthState() != State.SERVICE_HEALTHY) {
      throw new @OsUntrusted ServiceFailedException(
          localTarget + " is not currently healthy. " +
          "Cannot be failover target");
    }
  }

  /**
   * @return an {@link HAServiceTarget} for the current active node
   * in the cluster, or null if no node is active.
   * @throws IOException if a ZK-related issue occurs
   * @throws InterruptedException if thread is interrupted 
   */
  private @OsUntrusted HAServiceTarget getCurrentActive(@OsUntrusted ZKFailoverController this)
      throws IOException, InterruptedException {
    synchronized (elector) {
      synchronized (this) {
        @OsUntrusted
        byte @OsUntrusted [] activeData;
        try {
          activeData = elector.getActiveData();
        } catch (@OsUntrusted ActiveNotFoundException e) {
          return null;
        } catch (@OsUntrusted KeeperException ke) {
          throw new @OsUntrusted IOException(
              "Unexpected ZooKeeper issue fetching active node info", ke);
        }
        
        @OsUntrusted
        HAServiceTarget oldActive = dataToTarget(activeData);
        return oldActive;
      }
    }
  }

  /**
   * Check the current state of the service, and join the election
   * if it should be in the election.
   */
  private void recheckElectability(@OsUntrusted ZKFailoverController this) {
    // Maintain lock ordering of elector -> ZKFC
    synchronized (elector) {
      synchronized (this) {
        @OsUntrusted
        boolean healthy = lastHealthState == State.SERVICE_HEALTHY;
    
        @OsUntrusted
        long remainingDelay = delayJoiningUntilNanotime - System.nanoTime(); 
        if (remainingDelay > 0) {
          if (healthy) {
            LOG.info("Would have joined master election, but this node is " +
                "prohibited from doing so for " +
                TimeUnit.NANOSECONDS.toMillis(remainingDelay) + " more ms");
          }
          scheduleRecheck(remainingDelay);
          return;
        }
    
        switch (lastHealthState) {
        case SERVICE_HEALTHY:
          elector.joinElection(targetToData(localTarget));
          break;
          
        case INITIALIZING:
          LOG.info("Ensuring that " + localTarget + " does not " +
              "participate in active master election");
          elector.quitElection(false);
          break;
    
        case SERVICE_UNHEALTHY:
        case SERVICE_NOT_RESPONDING:
          LOG.info("Quitting master election for " + localTarget +
              " and marking that fencing is necessary");
          elector.quitElection(true);
          break;
          
        case HEALTH_MONITOR_FAILED:
          fatalError("Health monitor failed!");
          break;
          
        default:
          throw new @OsUntrusted IllegalArgumentException("Unhandled state:" + lastHealthState);
        }
      }
    }
  }
  
  /**
   * Schedule a call to {@link #recheckElectability()} in the future.
   */
  private void scheduleRecheck(@OsUntrusted ZKFailoverController this, @OsUntrusted long whenNanos) {
    delayExecutor.schedule(
        new @OsUntrusted Runnable() {
          @Override
          public void run() {
            try {
              recheckElectability();
            } catch (@OsUntrusted Throwable t) {
              fatalError("Failed to recheck electability: " +
                  StringUtils.stringifyException(t));
            }
          }
        },
        whenNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * @return the last health state passed to the FC
   * by the HealthMonitor.
   */
  @VisibleForTesting
  synchronized @OsUntrusted State getLastHealthState(@OsUntrusted ZKFailoverController this) {
    return lastHealthState;
  }

  private synchronized void setLastHealthState(@OsUntrusted ZKFailoverController this, HealthMonitor.@OsUntrusted State newState) {
    LOG.info("Local service " + localTarget +
        " entered state: " + newState);
    lastHealthState = newState;
  }
  
  @VisibleForTesting
  @OsUntrusted
  ActiveStandbyElector getElectorForTests(@OsUntrusted ZKFailoverController this) {
    return elector;
  }
  
  @VisibleForTesting
  @OsUntrusted
  ZKFCRpcServer getRpcServerForTests(@OsUntrusted ZKFailoverController this) {
    return rpcServer;
  }

  /**
   * Callbacks from elector
   */
  class ElectorCallbacks implements @OsUntrusted ActiveStandbyElectorCallback {
    @Override
    public void becomeActive(@OsUntrusted ZKFailoverController.ElectorCallbacks this) throws ServiceFailedException {
      ZKFailoverController.this.becomeActive();
    }

    @Override
    public void becomeStandby(@OsUntrusted ZKFailoverController.ElectorCallbacks this) {
      ZKFailoverController.this.becomeStandby();
    }

    @Override
    public void enterNeutralMode(@OsUntrusted ZKFailoverController.ElectorCallbacks this) {
    }

    @Override
    public void notifyFatalError(@OsUntrusted ZKFailoverController.ElectorCallbacks this, @OsUntrusted String errorMessage) {
      fatalError(errorMessage);
    }

    @Override
    public void fenceOldActive(@OsUntrusted ZKFailoverController.ElectorCallbacks this, @OsUntrusted byte @OsUntrusted [] data) {
      ZKFailoverController.this.fenceOldActive(data);
    }
    
    @Override
    public @OsUntrusted String toString(@OsUntrusted ZKFailoverController.ElectorCallbacks this) {
      synchronized (ZKFailoverController.this) {
        return "Elector callbacks for " + localTarget;
      }
    }
  }
  
  /**
   * Callbacks from HealthMonitor
   */
  class HealthCallbacks implements HealthMonitor.@OsUntrusted Callback {
    @Override
    public void enteredState(@OsUntrusted ZKFailoverController.HealthCallbacks this, HealthMonitor.@OsUntrusted State newState) {
      setLastHealthState(newState);
      recheckElectability();
    }
  }
  
  private static class ActiveAttemptRecord {
    private final @OsUntrusted boolean succeeded;
    private final @OsUntrusted String status;
    private final @OsUntrusted long nanoTime;
    
    public @OsUntrusted ActiveAttemptRecord(@OsUntrusted boolean succeeded, @OsUntrusted String status) {
      this.succeeded = succeeded;
      this.status = status;
      this.nanoTime = System.nanoTime();
    }
  }

}
