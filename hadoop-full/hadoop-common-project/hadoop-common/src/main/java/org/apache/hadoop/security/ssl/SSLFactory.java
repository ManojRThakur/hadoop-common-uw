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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.util.ReflectionUtils;
import static org.apache.hadoop.util.PlatformName.IBM_JAVA;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.security.GeneralSecurityException;

/**
 * Factory that creates SSLEngine and SSLSocketFactory instances using
 * Hadoop configuration information.
 * <p/>
 * This SSLFactory uses a {@link ReloadingX509TrustManager} instance,
 * which reloads public keys if the truststore file changes.
 * <p/>
 * This factory is used to configure HTTPS in Hadoop HTTP based endpoints, both
 * client and server.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SSLFactory implements @OsUntrusted ConnectionConfigurator {

  @InterfaceAudience.Private
  public static enum Mode {  @OsUntrusted  CLIENT,  @OsUntrusted  SERVER }

  public static final @OsUntrusted String SSL_REQUIRE_CLIENT_CERT_KEY =
    "hadoop.ssl.require.client.cert";
  public static final @OsUntrusted String SSL_HOSTNAME_VERIFIER_KEY =
    "hadoop.ssl.hostname.verifier";
  public static final @OsUntrusted String SSL_CLIENT_CONF_KEY =
    "hadoop.ssl.client.conf";
  public static final @OsUntrusted String SSL_SERVER_CONF_KEY =
    "hadoop.ssl.server.conf";
  public static final @OsUntrusted String SSLCERTIFICATE = IBM_JAVA?"ibmX509":"SunX509"; 

  public static final @OsUntrusted boolean DEFAULT_SSL_REQUIRE_CLIENT_CERT = false;

  public static final @OsUntrusted String KEYSTORES_FACTORY_CLASS_KEY =
    "hadoop.ssl.keystores.factory.class";

  private @OsUntrusted Configuration conf;
  private @OsUntrusted Mode mode;
  private @OsUntrusted boolean requireClientCert;
  private @OsUntrusted SSLContext context;
  private @OsUntrusted HostnameVerifier hostnameVerifier;
  private @OsUntrusted KeyStoresFactory keystoresFactory;

  /**
   * Creates an SSLFactory.
   *
   * @param mode SSLFactory mode, client or server.
   * @param conf Hadoop configuration from where the SSLFactory configuration
   * will be read.
   */
  public @OsUntrusted SSLFactory(@OsUntrusted Mode mode, @OsUntrusted Configuration conf) {
    this.conf = conf;
    if (mode == null) {
      throw new @OsUntrusted IllegalArgumentException("mode cannot be NULL");
    }
    this.mode = mode;
    requireClientCert = conf.getBoolean(SSL_REQUIRE_CLIENT_CERT_KEY,
                                        DEFAULT_SSL_REQUIRE_CLIENT_CERT);
    @OsUntrusted
    Configuration sslConf = readSSLConfiguration(mode);

    @OsUntrusted
    Class<@OsUntrusted ? extends @OsUntrusted KeyStoresFactory> klass
      = conf.getClass(KEYSTORES_FACTORY_CLASS_KEY,
                      FileBasedKeyStoresFactory.class, KeyStoresFactory.class);
    keystoresFactory = ReflectionUtils.newInstance(klass, sslConf);
  }

  private @OsUntrusted Configuration readSSLConfiguration(@OsUntrusted SSLFactory this, @OsUntrusted Mode mode) {
    @OsUntrusted
    Configuration sslConf = new @OsUntrusted Configuration(false);
    sslConf.setBoolean(SSL_REQUIRE_CLIENT_CERT_KEY, requireClientCert);
    @OsUntrusted
    String sslConfResource;
    if (mode == Mode.CLIENT) {
      sslConfResource = conf.get(SSL_CLIENT_CONF_KEY, "ssl-client.xml");
    } else {
      sslConfResource = conf.get(SSL_SERVER_CONF_KEY, "ssl-server.xml");
    }
    sslConf.addResource(sslConfResource);
    return sslConf;
  }

  /**
   * Initializes the factory.
   *
   * @throws  GeneralSecurityException thrown if an SSL initialization error
   * happened.
   * @throws IOException thrown if an IO error happened while reading the SSL
   * configuration.
   */
  public void init(@OsUntrusted SSLFactory this) throws GeneralSecurityException, IOException {
    keystoresFactory.init(mode);
    context = SSLContext.getInstance("TLS");
    context.init(keystoresFactory.getKeyManagers(),
                 keystoresFactory.getTrustManagers(), null);

    hostnameVerifier = getHostnameVerifier(conf);
  }

  private @OsUntrusted HostnameVerifier getHostnameVerifier(@OsUntrusted SSLFactory this, @OsUntrusted Configuration conf)
    throws GeneralSecurityException, IOException {
    @OsUntrusted
    HostnameVerifier hostnameVerifier;
    @OsUntrusted
    String verifier =
      conf.get(SSL_HOSTNAME_VERIFIER_KEY, "DEFAULT").trim().toUpperCase();
    if (verifier.equals("DEFAULT")) {
      hostnameVerifier = SSLHostnameVerifier.DEFAULT;
    } else if (verifier.equals("DEFAULT_AND_LOCALHOST")) {
      hostnameVerifier = SSLHostnameVerifier.DEFAULT_AND_LOCALHOST;
    } else if (verifier.equals("STRICT")) {
      hostnameVerifier = SSLHostnameVerifier.STRICT;
    } else if (verifier.equals("STRICT_IE6")) {
      hostnameVerifier = SSLHostnameVerifier.STRICT_IE6;
    } else if (verifier.equals("ALLOW_ALL")) {
      hostnameVerifier = SSLHostnameVerifier.ALLOW_ALL;
    } else {
      throw new @OsUntrusted GeneralSecurityException("Invalid hostname verifier: " +
                                         verifier);
    }
    return hostnameVerifier;
  }

  /**
   * Releases any resources being used.
   */
  public void destroy(@OsUntrusted SSLFactory this) {
    keystoresFactory.destroy();
  }
  /**
   * Returns the SSLFactory KeyStoresFactory instance.
   *
   * @return the SSLFactory KeyStoresFactory instance.
   */
  public @OsUntrusted KeyStoresFactory getKeystoresFactory(@OsUntrusted SSLFactory this) {
    return keystoresFactory;
  }

  /**
   * Returns a configured SSLEngine.
   *
   * @return the configured SSLEngine.
   * @throws GeneralSecurityException thrown if the SSL engine could not
   * be initialized.
   * @throws IOException thrown if and IO error occurred while loading
   * the server keystore.
   */
  public @OsUntrusted SSLEngine createSSLEngine(@OsUntrusted SSLFactory this)
    throws GeneralSecurityException, IOException {
    @OsUntrusted
    SSLEngine sslEngine = context.createSSLEngine();
    if (mode == Mode.CLIENT) {
      sslEngine.setUseClientMode(true);
    } else {
      sslEngine.setUseClientMode(false);
      sslEngine.setNeedClientAuth(requireClientCert);
    }
    return sslEngine;
  }

  /**
   * Returns a configured SSLServerSocketFactory.
   *
   * @return the configured SSLSocketFactory.
   * @throws GeneralSecurityException thrown if the SSLSocketFactory could not
   * be initialized.
   * @throws IOException thrown if and IO error occurred while loading
   * the server keystore.
   */
  public @OsUntrusted SSLServerSocketFactory createSSLServerSocketFactory(@OsUntrusted SSLFactory this)
    throws GeneralSecurityException, IOException {
    if (mode != Mode.SERVER) {
      throw new @OsUntrusted IllegalStateException("Factory is in CLIENT mode");
    }
    return context.getServerSocketFactory();
  }

  /**
   * Returns a configured SSLSocketFactory.
   *
   * @return the configured SSLSocketFactory.
   * @throws GeneralSecurityException thrown if the SSLSocketFactory could not
   * be initialized.
   * @throws IOException thrown if and IO error occurred while loading
   * the server keystore.
   */
  public @OsUntrusted SSLSocketFactory createSSLSocketFactory(@OsUntrusted SSLFactory this)
    throws GeneralSecurityException, IOException {
    if (mode != Mode.CLIENT) {
      throw new @OsUntrusted IllegalStateException("Factory is in CLIENT mode");
    }
    return context.getSocketFactory();
  }

  /**
   * Returns the hostname verifier it should be used in HttpsURLConnections.
   *
   * @return the hostname verifier.
   */
  public @OsUntrusted HostnameVerifier getHostnameVerifier(@OsUntrusted SSLFactory this) {
    if (mode != Mode.CLIENT) {
      throw new @OsUntrusted IllegalStateException("Factory is in CLIENT mode");
    }
    return hostnameVerifier;
  }

  /**
   * Returns if client certificates are required or not.
   *
   * @return if client certificates are required or not.
   */
  public @OsUntrusted boolean isClientCertRequired(@OsUntrusted SSLFactory this) {
    return requireClientCert;
  }

  /**
   * If the given {@link HttpURLConnection} is an {@link HttpsURLConnection}
   * configures the connection with the {@link SSLSocketFactory} and
   * {@link HostnameVerifier} of this SSLFactory, otherwise does nothing.
   *
   * @param conn the {@link HttpURLConnection} instance to configure.
   * @return the configured {@link HttpURLConnection} instance.
   *
   * @throws IOException if an IO error occurred.
   */
  @Override
  public @OsUntrusted HttpURLConnection configure(@OsUntrusted SSLFactory this, @OsUntrusted HttpURLConnection conn)
    throws IOException {
    if (conn instanceof @OsUntrusted HttpsURLConnection) {
      @OsUntrusted
      HttpsURLConnection sslConn = (@OsUntrusted HttpsURLConnection) conn;
      try {
        sslConn.setSSLSocketFactory(createSSLSocketFactory());
      } catch (@OsUntrusted GeneralSecurityException ex) {
        throw new @OsUntrusted IOException(ex);
      }
      sslConn.setHostnameVerifier(getHostnameVerifier());
      conn = sslConn;
    }
    return conn;
  }
}
