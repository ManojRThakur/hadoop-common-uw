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

package org.apache.hadoop.security.ssl;

import ostrusted.quals.OsUntrusted;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link TrustManager} implementation that reloads its configuration when
 * the truststore file on disk changes.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ReloadingX509TrustManager
  implements @OsUntrusted X509TrustManager, @OsUntrusted Runnable {

  private static final @OsUntrusted Log LOG =
    LogFactory.getLog(ReloadingX509TrustManager.class);

  private @OsUntrusted String type;
  private @OsUntrusted File file;
  private @OsUntrusted String password;
  private @OsUntrusted long lastLoaded;
  private @OsUntrusted long reloadInterval;
  private @OsUntrusted AtomicReference<@OsUntrusted X509TrustManager> trustManagerRef;

  private volatile @OsUntrusted boolean running;
  private @OsUntrusted Thread reloader;

  /**
   * Creates a reloadable trustmanager. The trustmanager reloads itself
   * if the underlying trustore file has changed.
   *
   * @param type type of truststore file, typically 'jks'.
   * @param location local path to the truststore file.
   * @param password password of the truststore file.
   * @param reloadInterval interval to check if the truststore file has
   * changed, in milliseconds.
   * @throws IOException thrown if the truststore could not be initialized due
   * to an IO error.
   * @throws GeneralSecurityException thrown if the truststore could not be
   * initialized due to a security error.
   */
  public @OsUntrusted ReloadingX509TrustManager(@OsUntrusted String type, @OsUntrusted String location,
                                   @OsUntrusted
                                   String password, @OsUntrusted long reloadInterval)
    throws IOException, GeneralSecurityException {
    this.type = type;
    file = new @OsUntrusted File(location);
    this.password = password;
    trustManagerRef = new @OsUntrusted AtomicReference<@OsUntrusted X509TrustManager>();
    trustManagerRef.set(loadTrustManager());
    this.reloadInterval = reloadInterval;
  }

  /**
   * Starts the reloader thread.
   */
  public void init(@OsUntrusted ReloadingX509TrustManager this) {
    reloader = new @OsUntrusted Thread(this, "Truststore reloader thread");
    reloader.setDaemon(true);
    running =  true;
    reloader.start();
  }

  /**
   * Stops the reloader thread.
   */
  public void destroy(@OsUntrusted ReloadingX509TrustManager this) {
    running = false;
    reloader.interrupt();
  }

  /**
   * Returns the reload check interval.
   *
   * @return the reload check interval, in milliseconds.
   */
  public @OsUntrusted long getReloadInterval(@OsUntrusted ReloadingX509TrustManager this) {
    return reloadInterval;
  }

  @Override
  public void checkClientTrusted(@OsUntrusted ReloadingX509TrustManager this, @OsUntrusted X509Certificate @OsUntrusted [] chain, @OsUntrusted String authType)
    throws CertificateException {
    @OsUntrusted
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      tm.checkClientTrusted(chain, authType);
    } else {
      throw new @OsUntrusted CertificateException("Unknown client chain certificate: " +
                                     chain[0].toString());
    }
  }

  @Override
  public void checkServerTrusted(@OsUntrusted ReloadingX509TrustManager this, @OsUntrusted X509Certificate @OsUntrusted [] chain, @OsUntrusted String authType)
    throws CertificateException {
    @OsUntrusted
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      tm.checkServerTrusted(chain, authType);
    } else {
      throw new @OsUntrusted CertificateException("Unknown server chain certificate: " +
                                     chain[0].toString());
    }
  }

  private static final @OsUntrusted X509Certificate @OsUntrusted [] EMPTY = new @OsUntrusted X509Certificate @OsUntrusted [0];
  @Override
  public @OsUntrusted X509Certificate @OsUntrusted [] getAcceptedIssuers(@OsUntrusted ReloadingX509TrustManager this) {
    @OsUntrusted
    X509Certificate @OsUntrusted [] issuers = EMPTY;
    @OsUntrusted
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      issuers = tm.getAcceptedIssuers();
    }
    return issuers;
  }

  @OsUntrusted
  boolean needsReload(@OsUntrusted ReloadingX509TrustManager this) {
    @OsUntrusted
    boolean reload = true;
    if (file.exists()) {
      if (file.lastModified() == lastLoaded) {
        reload = false;
      }
    } else {
      lastLoaded = 0;
    }
    return reload;
  }

  @OsUntrusted
  X509TrustManager loadTrustManager(@OsUntrusted ReloadingX509TrustManager this)
  throws IOException, GeneralSecurityException {
    @OsUntrusted
    X509TrustManager trustManager = null;
    @OsUntrusted
    KeyStore ks = KeyStore.getInstance(type);
    lastLoaded = file.lastModified();
    @OsUntrusted
    FileInputStream in = new @OsUntrusted FileInputStream(file);
    try {
      ks.load(in, password.toCharArray());
      LOG.debug("Loaded truststore '" + file + "'");
    } finally {
      in.close();
    }

    @OsUntrusted
    TrustManagerFactory trustManagerFactory = 
      TrustManagerFactory.getInstance(SSLFactory.SSLCERTIFICATE);
    trustManagerFactory.init(ks);
    @OsUntrusted
    TrustManager @OsUntrusted [] trustManagers = trustManagerFactory.getTrustManagers();
    for (@OsUntrusted TrustManager trustManager1 : trustManagers) {
      if (trustManager1 instanceof @OsUntrusted X509TrustManager) {
        trustManager = (@OsUntrusted X509TrustManager) trustManager1;
        break;
      }
    }
    return trustManager;
  }

  @Override
  public void run(@OsUntrusted ReloadingX509TrustManager this) {
    while (running) {
      try {
        Thread.sleep(reloadInterval);
      } catch (@OsUntrusted InterruptedException e) {
        //NOP
      }
      if (running && needsReload()) {
        try {
          trustManagerRef.set(loadTrustManager());
        } catch (@OsUntrusted Exception ex) {
          LOG.warn("Could not load truststore (keep using existing one) : " +
                   ex.toString(), ex);
        }
      }
    }
  }

}
