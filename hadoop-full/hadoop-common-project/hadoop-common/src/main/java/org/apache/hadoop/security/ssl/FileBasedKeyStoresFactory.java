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
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.text.MessageFormat;

/**
 * {@link KeyStoresFactory} implementation that reads the certificates from
 * keystore files.
 * <p/>
 * if the trust certificates keystore file changes, the {@link TrustManager}
 * is refreshed with the new trust certificate entries (using a
 * {@link ReloadingX509TrustManager} trustmanager).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FileBasedKeyStoresFactory implements @OsUntrusted KeyStoresFactory {

  private static final @OsUntrusted Log LOG =
    LogFactory.getLog(FileBasedKeyStoresFactory.class);

  public static final @OsUntrusted String SSL_KEYSTORE_LOCATION_TPL_KEY =
    "ssl.{0}.keystore.location";
  public static final @OsUntrusted String SSL_KEYSTORE_PASSWORD_TPL_KEY =
    "ssl.{0}.keystore.password";
  public static final @OsUntrusted String SSL_KEYSTORE_KEYPASSWORD_TPL_KEY =
    "ssl.{0}.keystore.keypassword";
  public static final @OsUntrusted String SSL_KEYSTORE_TYPE_TPL_KEY =
    "ssl.{0}.keystore.type";

  public static final @OsUntrusted String SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY =
    "ssl.{0}.truststore.reload.interval";
  public static final @OsUntrusted String SSL_TRUSTSTORE_LOCATION_TPL_KEY =
    "ssl.{0}.truststore.location";
  public static final @OsUntrusted String SSL_TRUSTSTORE_PASSWORD_TPL_KEY =
    "ssl.{0}.truststore.password";
  public static final @OsUntrusted String SSL_TRUSTSTORE_TYPE_TPL_KEY =
    "ssl.{0}.truststore.type";

  /**
   * Default format of the keystore files.
   */
  public static final @OsUntrusted String DEFAULT_KEYSTORE_TYPE = "jks";

  /**
   * Reload interval in milliseconds.
   */
  public static final @OsUntrusted int DEFAULT_SSL_TRUSTSTORE_RELOAD_INTERVAL = 10000;

  private @OsUntrusted Configuration conf;
  private @OsUntrusted KeyManager @OsUntrusted [] keyManagers;
  private @OsUntrusted TrustManager @OsUntrusted [] trustManagers;
  private @OsUntrusted ReloadingX509TrustManager trustManager;

  /**
   * Resolves a property name to its client/server version if applicable.
   * <p/>
   * NOTE: This method is public for testing purposes.
   *
   * @param mode client/server mode.
   * @param template property name template.
   * @return the resolved property name.
   */
  @VisibleForTesting
  public static @OsUntrusted String resolvePropertyName(SSLFactory.@OsUntrusted Mode mode,
                                           @OsUntrusted
                                           String template) {
    return MessageFormat.format(template, mode.toString().toLowerCase());
  }

  /**
   * Sets the configuration for the factory.
   *
   * @param conf the configuration for the factory.
   */
  @Override
  public void setConf(@OsUntrusted FileBasedKeyStoresFactory this, @OsUntrusted Configuration conf) {
    this.conf = conf;
  }

  /**
   * Returns the configuration of the factory.
   *
   * @return the configuration of the factory.
   */
  @Override
  public @OsUntrusted Configuration getConf(@OsUntrusted FileBasedKeyStoresFactory this) {
    return conf;
  }

  /**
   * Initializes the keystores of the factory.
   *
   * @param mode if the keystores are to be used in client or server mode.
   * @throws IOException thrown if the keystores could not be initialized due
   * to an IO error.
   * @throws GeneralSecurityException thrown if the keystores could not be
   * initialized due to a security error.
   */
  @Override
  public void init(@OsUntrusted FileBasedKeyStoresFactory this, SSLFactory.@OsUntrusted Mode mode)
    throws IOException, GeneralSecurityException {

    @OsUntrusted
    boolean requireClientCert =
      conf.getBoolean(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY, true);

    // certificate store
    @OsUntrusted
    String keystoreType =
      conf.get(resolvePropertyName(mode, SSL_KEYSTORE_TYPE_TPL_KEY),
               DEFAULT_KEYSTORE_TYPE);
    @OsUntrusted
    KeyStore keystore = KeyStore.getInstance(keystoreType);
    @OsUntrusted
    String keystoreKeyPassword = null;
    if (requireClientCert || mode == SSLFactory.Mode.SERVER) {
      @OsUntrusted
      String locationProperty =
        resolvePropertyName(mode, SSL_KEYSTORE_LOCATION_TPL_KEY);
      @OsUntrusted
      String keystoreLocation = conf.get(locationProperty, "");
      if (keystoreLocation.isEmpty()) {
        throw new @OsUntrusted GeneralSecurityException("The property '" + locationProperty +
          "' has not been set in the ssl configuration file.");
      }
      @OsUntrusted
      String passwordProperty =
        resolvePropertyName(mode, SSL_KEYSTORE_PASSWORD_TPL_KEY);
      @OsUntrusted
      String keystorePassword = conf.get(passwordProperty, "");
      if (keystorePassword.isEmpty()) {
        throw new @OsUntrusted GeneralSecurityException("The property '" + passwordProperty +
          "' has not been set in the ssl configuration file.");
      }
      @OsUntrusted
      String keyPasswordProperty =
        resolvePropertyName(mode, SSL_KEYSTORE_KEYPASSWORD_TPL_KEY);
      // Key password defaults to the same value as store password for
      // compatibility with legacy configurations that did not use a separate
      // configuration property for key password.
      keystoreKeyPassword = conf.get(keyPasswordProperty, keystorePassword);
      LOG.debug(mode.toString() + " KeyStore: " + keystoreLocation);

      @OsUntrusted
      InputStream is = new @OsUntrusted FileInputStream(keystoreLocation);
      try {
        keystore.load(is, keystorePassword.toCharArray());
      } finally {
        is.close();
      }
      LOG.debug(mode.toString() + " Loaded KeyStore: " + keystoreLocation);
    } else {
      keystore.load(null, null);
    }
    @OsUntrusted
    KeyManagerFactory keyMgrFactory = KeyManagerFactory
        .getInstance(SSLFactory.SSLCERTIFICATE);
      
    keyMgrFactory.init(keystore, (keystoreKeyPassword != null) ?
                                 keystoreKeyPassword.toCharArray() : null);
    keyManagers = keyMgrFactory.getKeyManagers();

    //trust store
    @OsUntrusted
    String truststoreType =
      conf.get(resolvePropertyName(mode, SSL_TRUSTSTORE_TYPE_TPL_KEY),
               DEFAULT_KEYSTORE_TYPE);

    @OsUntrusted
    String locationProperty =
      resolvePropertyName(mode, SSL_TRUSTSTORE_LOCATION_TPL_KEY);
    @OsUntrusted
    String truststoreLocation = conf.get(locationProperty, "");
    if (truststoreLocation.isEmpty()) {
      throw new @OsUntrusted GeneralSecurityException("The property '" + locationProperty +
        "' has not been set in the ssl configuration file.");
    }

    @OsUntrusted
    String passwordProperty = resolvePropertyName(mode,
                                                  SSL_TRUSTSTORE_PASSWORD_TPL_KEY);
    @OsUntrusted
    String truststorePassword = conf.get(passwordProperty, "");
    if (truststorePassword.isEmpty()) {
      throw new @OsUntrusted GeneralSecurityException("The property '" + passwordProperty +
        "' has not been set in the ssl configuration file.");
    }
    @OsUntrusted
    long truststoreReloadInterval =
      conf.getLong(
        resolvePropertyName(mode, SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY),
        DEFAULT_SSL_TRUSTSTORE_RELOAD_INTERVAL);

    LOG.debug(mode.toString() + " TrustStore: " + truststoreLocation);

    trustManager = new @OsUntrusted ReloadingX509TrustManager(truststoreType,
                                                 truststoreLocation,
                                                 truststorePassword,
                                                 truststoreReloadInterval);
    trustManager.init();
    LOG.debug(mode.toString() + " Loaded TrustStore: " + truststoreLocation);

    trustManagers = new @OsUntrusted TrustManager @OsUntrusted []{trustManager};
  }

  /**
   * Releases any resources being used.
   */
  @Override
  public synchronized void destroy(@OsUntrusted FileBasedKeyStoresFactory this) {
    if (trustManager != null) {
      trustManager.destroy();
      trustManager = null;
      keyManagers = null;
      trustManagers = null;
    }
  }

  /**
   * Returns the keymanagers for owned certificates.
   *
   * @return the keymanagers for owned certificates.
   */
  @Override
  public @OsUntrusted KeyManager @OsUntrusted [] getKeyManagers(@OsUntrusted FileBasedKeyStoresFactory this) {
    return keyManagers;
  }

  /**
   * Returns the trustmanagers for trusted certificates.
   *
   * @return the trustmanagers for trusted certificates.
   */
  @Override
  public @OsUntrusted TrustManager @OsUntrusted [] getTrustManagers(@OsUntrusted FileBasedKeyStoresFactory this) {
    return trustManagers;
  }

}
