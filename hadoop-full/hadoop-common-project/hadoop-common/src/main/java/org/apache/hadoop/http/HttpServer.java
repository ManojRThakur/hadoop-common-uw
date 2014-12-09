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
package org.apache.hadoop.http;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.InterruptedIOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLServerSocketFactory;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.ConfServlet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.jmx.JMXJsonServlet;
import org.apache.hadoop.log.LogLevel;
import org.apache.hadoop.metrics.MetricsServlet;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;
import org.mortbay.io.Buffer;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.MimeTypes;
import org.mortbay.jetty.RequestLog;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandler;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.handler.RequestLogHandler;
import org.mortbay.jetty.handler.HandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.FilterMapping;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;
import org.mortbay.util.MultiException;

import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 * Create a Jetty embedded server to answer http requests. The primary goal
 * is to serve up status information for the server.
 * There are three contexts:
 *   "/logs/" -> points to the log directory
 *   "/static/" -> points to common static files (src/webapps/static)
 *   "/" -> the jsp server code from (src/webapps/<name>)
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce", "HBase"})
@InterfaceStability.Evolving
public class HttpServer implements @OsUntrusted FilterContainer {
  public static final @OsUntrusted Log LOG = LogFactory.getLog(HttpServer.class);

  static final @OsUntrusted String FILTER_INITIALIZER_PROPERTY
      = "hadoop.http.filter.initializers";
  static final @OsUntrusted String HTTP_MAX_THREADS = "hadoop.http.max.threads";

  // The ServletContext attribute where the daemon Configuration
  // gets stored.
  public static final @OsUntrusted String CONF_CONTEXT_ATTRIBUTE = "hadoop.conf";
  public static final @OsUntrusted String ADMINS_ACL = "admins.acl";
  public static final @OsUntrusted String SPNEGO_FILTER = "SpnegoFilter";
  public static final @OsUntrusted String NO_CACHE_FILTER = "NoCacheFilter";

  public static final @OsUntrusted String BIND_ADDRESS = "bind.address";

  private @OsUntrusted AccessControlList adminsAcl;

  private @OsUntrusted SSLFactory sslFactory;
  protected final @OsUntrusted Server webServer;
  protected final @OsUntrusted Connector listener;
  protected final @OsUntrusted WebAppContext webAppContext;
  protected final @OsUntrusted boolean findPort;
  protected final @OsUntrusted Map<@OsUntrusted Context, @OsUntrusted Boolean> defaultContexts =
      new @OsUntrusted HashMap<@OsUntrusted Context, @OsUntrusted Boolean>();
  protected final @OsUntrusted List<@OsUntrusted String> filterNames = new @OsUntrusted ArrayList<@OsUntrusted String>();
  static final @OsUntrusted String STATE_DESCRIPTION_ALIVE = " - alive";
  static final @OsUntrusted String STATE_DESCRIPTION_NOT_LIVE = " - not live";

  private final @OsUntrusted boolean listenerStartedExternally;
  
  /**
   * Class to construct instances of HTTP server with specific options.
   */
  public static class Builder {
    @OsUntrusted
    String name;
    @OsTrusted String bindAddress;
    @OsUntrusted
    Integer port;
    @OsUntrusted
    Boolean findPort;
    @OsUntrusted
    Configuration conf;
    @OsUntrusted
    Connector connector;
    @OsUntrusted
    String @OsUntrusted [] pathSpecs;
    @OsUntrusted
    AccessControlList adminsAcl;
    @OsUntrusted
    boolean securityEnabled = false;
    @OsUntrusted
    String usernameConfKey = null;
    @OsUntrusted
    String keytabConfKey = null;
    
    public @OsUntrusted Builder setName(HttpServer.@OsUntrusted Builder this, @OsUntrusted String name){
      this.name = name;
      return this;
    }
    
    public @OsUntrusted Builder setBindAddress(HttpServer.@OsUntrusted Builder this, @OsTrusted String bindAddress){
      this.bindAddress = bindAddress;
      return this;
    }
    
    public @OsUntrusted Builder setPort(HttpServer.@OsUntrusted Builder this, @OsUntrusted int port) {
      this.port = port;
      return this;
    }
    
    public @OsUntrusted Builder setFindPort(HttpServer.@OsUntrusted Builder this, @OsUntrusted boolean findPort) {
      this.findPort = findPort;
      return this;
    }
    
    public @OsUntrusted Builder setConf(HttpServer.@OsUntrusted Builder this, @OsUntrusted Configuration conf) {
      this.conf = conf;
      return this;
    }
    
    public @OsUntrusted Builder setConnector(HttpServer.@OsUntrusted Builder this, @OsUntrusted Connector connector) {
      this.connector = connector;
      return this;
    }
    
    public @OsUntrusted Builder setPathSpec(HttpServer.@OsUntrusted Builder this, @OsUntrusted String @OsUntrusted [] pathSpec) {
      this.pathSpecs = pathSpec;
      return this;
    }
    
    public @OsUntrusted Builder setACL(HttpServer.@OsUntrusted Builder this, @OsUntrusted AccessControlList acl) {
      this.adminsAcl = acl;
      return this;
    }
    
    public @OsUntrusted Builder setSecurityEnabled(HttpServer.@OsUntrusted Builder this, @OsUntrusted boolean securityEnabled) {
      this.securityEnabled = securityEnabled;
      return this;
    }
    
    public @OsUntrusted Builder setUsernameConfKey(HttpServer.@OsUntrusted Builder this, @OsUntrusted String usernameConfKey) {
      this.usernameConfKey = usernameConfKey;
      return this;
    }
    
    public @OsUntrusted Builder setKeytabConfKey(HttpServer.@OsUntrusted Builder this, @OsUntrusted String keytabConfKey) {
      this.keytabConfKey = keytabConfKey;
      return this;
    }
    
    public @OsUntrusted HttpServer build(HttpServer.@OsUntrusted Builder this) throws IOException {
      if (this.name == null) {
        throw new @OsUntrusted HadoopIllegalArgumentException("name is not set");
      }
      if (this.bindAddress == null) {
        throw new @OsUntrusted HadoopIllegalArgumentException("bindAddress is not set");
      }
      if (this.port == null) {
        throw new @OsUntrusted HadoopIllegalArgumentException("port is not set");
      }
      if (this.findPort == null) {
        throw new @OsUntrusted HadoopIllegalArgumentException("findPort is not set");
      }
      
      if (this.conf == null) {
        conf = new @OsUntrusted Configuration();
      }
      
      @OsUntrusted
      HttpServer server = new @OsUntrusted HttpServer(this.name, this.bindAddress, this.port,
      this.findPort, this.conf, this.adminsAcl, this.connector, this.pathSpecs);
      if (this.securityEnabled) {
        server.initSpnego(this.conf, this.usernameConfKey, this.keytabConfKey);
      }
      return server;
    }
  }
  
  /** Same as this(name, bindAddress, port, findPort, null); */
  @Deprecated
  public @OsUntrusted HttpServer(@OsUntrusted String name, @OsTrusted String bindAddress, @OsUntrusted int port, @OsUntrusted boolean findPort
      ) throws IOException {
    this(name, bindAddress, port, findPort, new @OsUntrusted Configuration());
  }
  
  @Deprecated
  public @OsUntrusted HttpServer(@OsUntrusted String name, @OsTrusted String bindAddress, @OsUntrusted int port,
      @OsUntrusted
      boolean findPort, @OsUntrusted Configuration conf, @OsUntrusted Connector connector) throws IOException {
    this(name, bindAddress, port, findPort, conf, null, connector, null);
  }

  /**
   * Create a status server on the given port. Allows you to specify the
   * path specifications that this server will be serving so that they will be
   * added to the filters properly.  
   * 
   * @param name The name of the server
   * @param bindAddress The address for this server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and 
   *        increment by 1 until it finds a free port.
   * @param conf Configuration 
   * @param pathSpecs Path specifications that this httpserver will be serving. 
   *        These will be added to any filters.
   */
  @Deprecated
  public @OsUntrusted HttpServer(@OsUntrusted String name, @OsTrusted String bindAddress, @OsUntrusted int port,
      @OsUntrusted
      boolean findPort, @OsUntrusted Configuration conf, @OsUntrusted String @OsUntrusted [] pathSpecs) throws IOException {
    this(name, bindAddress, port, findPort, conf, null, null, pathSpecs);
  }
  
  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/<name>.
   * @param name The name of the server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and 
   *        increment by 1 until it finds a free port.
   * @param conf Configuration 
   */
  @Deprecated
  public @OsUntrusted HttpServer(@OsUntrusted String name, @OsTrusted String bindAddress, @OsUntrusted int port,
      @OsUntrusted
      boolean findPort, @OsUntrusted Configuration conf) throws IOException {
    this(name, bindAddress, port, findPort, conf, null, null, null);
  }

  @Deprecated
  public @OsUntrusted HttpServer(@OsUntrusted String name, @OsTrusted String bindAddress, @OsUntrusted int port,
      @OsUntrusted
      boolean findPort, @OsUntrusted Configuration conf, @OsUntrusted AccessControlList adminsAcl) 
      throws IOException {
    this(name, bindAddress, port, findPort, conf, adminsAcl, null, null);
  }

  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/<name>.
   * @param name The name of the server
   * @param bindAddress The address for this server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and 
   *        increment by 1 until it finds a free port.
   * @param conf Configuration 
   * @param adminsAcl {@link AccessControlList} of the admins
   */
  @Deprecated
  public @OsUntrusted HttpServer(@OsUntrusted String name, @OsTrusted String bindAddress, @OsUntrusted int port,
      @OsUntrusted
      boolean findPort, @OsUntrusted Configuration conf, @OsUntrusted AccessControlList adminsAcl, 
      @OsUntrusted
      Connector connector) throws IOException {
    this(name, bindAddress, port, findPort, conf, adminsAcl, connector, null);
  }

  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/<name>.
   * @param name The name of the server
   * @param bindAddress The address for this server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and 
   *        increment by 1 until it finds a free port.
   * @param conf Configuration 
   * @param adminsAcl {@link AccessControlList} of the admins
   * @param connector A jetty connection listener
   * @param pathSpecs Path specifications that this httpserver will be serving. 
   *        These will be added to any filters.
   */
  public @OsUntrusted HttpServer(@OsUntrusted String name, @OsTrusted String bindAddress, @OsUntrusted int port,
      @OsUntrusted
      boolean findPort, @OsUntrusted Configuration conf, @OsUntrusted AccessControlList adminsAcl, 
      @OsUntrusted
      Connector connector, @OsUntrusted String @OsUntrusted [] pathSpecs) throws IOException {
    webServer = new @OsUntrusted Server();
    this.findPort = findPort;
    this.adminsAcl = adminsAcl;
    
    if(connector == null) {
      listenerStartedExternally = false;
      if (HttpConfig.isSecure()) {
        sslFactory = new @OsUntrusted SSLFactory(SSLFactory.Mode.SERVER, conf);
        try {
          sslFactory.init();
        } catch (@OsUntrusted GeneralSecurityException ex) {
          throw new @OsUntrusted IOException(ex);
        }
        @OsUntrusted
        SslSocketConnector sslListener = new @OsUntrusted SslSocketConnector() {
          @Override
          protected @OsUntrusted SSLServerSocketFactory createFactory() throws Exception {
            return sslFactory.createSSLServerSocketFactory();
          }
        };
        listener = sslListener;
      } else {
        listener = createBaseListener(conf);
      }
      listener.setHost(bindAddress);
      listener.setPort(port);
    } else {
      listenerStartedExternally = true;
      listener = connector;
    }
    
    webServer.addConnector(listener);

    @OsUntrusted
    int maxThreads = conf.getInt(HTTP_MAX_THREADS, -1);
    // If HTTP_MAX_THREADS is not configured, QueueThreadPool() will use the
    // default value (currently 250).
    @OsUntrusted
    QueuedThreadPool threadPool = maxThreads == -1 ?
        new @OsUntrusted QueuedThreadPool() : new @OsUntrusted QueuedThreadPool(maxThreads);
    threadPool.setDaemon(true);
    webServer.setThreadPool(threadPool);

    final @OsUntrusted String appDir = getWebAppsPath(name);
    @OsUntrusted
    ContextHandlerCollection contexts = new @OsUntrusted ContextHandlerCollection();
    @OsUntrusted
    RequestLog requestLog = HttpRequestLog.getRequestLog(name);

    if (requestLog != null) {
      @OsUntrusted
      RequestLogHandler requestLogHandler = new @OsUntrusted RequestLogHandler();
      requestLogHandler.setRequestLog(requestLog);
      @OsUntrusted
      HandlerCollection handlers = new @OsUntrusted HandlerCollection();
      handlers.setHandlers(new @OsUntrusted Handler @OsUntrusted [] {requestLogHandler, contexts});
      webServer.setHandler(handlers);
    }
    else {
      webServer.setHandler(contexts);
    }

    webAppContext = new @OsUntrusted WebAppContext();
    webAppContext.setDisplayName(name);
    webAppContext.setContextPath("/");
    webAppContext.setWar(appDir + "/" + name);
    webAppContext.getServletContext().setAttribute(CONF_CONTEXT_ATTRIBUTE, conf);
    webAppContext.getServletContext().setAttribute(ADMINS_ACL, adminsAcl);
    addNoCacheFilter(webAppContext);
    webServer.addHandler(webAppContext);

    addDefaultApps(contexts, appDir, conf);
        
    addGlobalFilter("safety", QuotingInputFilter.class.getName(), null);
    final @OsUntrusted FilterInitializer @OsUntrusted [] initializers = getFilterInitializers(conf); 
    if (initializers != null) {
      conf = new @OsUntrusted Configuration(conf);
      conf.set(BIND_ADDRESS, bindAddress);
      for(@OsUntrusted FilterInitializer c : initializers) {
        c.initFilter(this, conf);
      }
    }

    addDefaultServlets();

    if (pathSpecs != null) {
      for (@OsUntrusted String path : pathSpecs) {
        LOG.info("adding path spec: " + path);
        addFilterPathMapping(path, webAppContext);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void addNoCacheFilter(@OsUntrusted HttpServer this, @OsUntrusted WebAppContext ctxt) {
    defineFilter(ctxt, NO_CACHE_FILTER,
      NoCacheFilter.class.getName(), Collections.EMPTY_MAP, new @OsUntrusted String @OsUntrusted [] { "/*"});
  }

  /**
   * Create a required listener for the Jetty instance listening on the port
   * provided. This wrapper and all subclasses must create at least one
   * listener.
   */
  public @OsUntrusted Connector createBaseListener(@OsUntrusted HttpServer this, @OsUntrusted Configuration conf) throws IOException {
    return HttpServer.createDefaultChannelConnector();
  }
  
  @InterfaceAudience.Private
  public static @OsUntrusted Connector createDefaultChannelConnector() {
    @OsUntrusted
    SelectChannelConnector ret = new @OsUntrusted SelectChannelConnector();
    ret.setLowResourceMaxIdleTime(10000);
    ret.setAcceptQueueSize(128);
    ret.setResolveNames(false);
    ret.setUseDirectBuffers(false);
    if(Shell.WINDOWS) {
      // result of setting the SO_REUSEADDR flag is different on Windows
      // http://msdn.microsoft.com/en-us/library/ms740621(v=vs.85).aspx
      // without this 2 NN's can start on the same machine and listen on 
      // the same port with indeterminate routing of incoming requests to them
      ret.setReuseAddress(false);
    }
    ret.setHeaderBufferSize(1024*64);
    return ret;
  }

  /** Get an array of FilterConfiguration specified in the conf */
  private static @OsUntrusted FilterInitializer @OsUntrusted [] getFilterInitializers(@OsUntrusted Configuration conf) {
    if (conf == null) {
      return null;
    }

    @OsUntrusted
    Class<@OsUntrusted ? extends java.lang.@OsUntrusted Object> @OsUntrusted [] classes = conf.getClasses(FILTER_INITIALIZER_PROPERTY);
    if (classes == null) {
      return null;
    }

    @OsUntrusted
    FilterInitializer @OsUntrusted [] initializers = new @OsUntrusted FilterInitializer @OsUntrusted [classes.length];
    for(@OsUntrusted int i = 0; i < classes.length; i++) {
      initializers[i] = (@OsUntrusted FilterInitializer)ReflectionUtils.newInstance(
          classes[i], conf);
    }
    return initializers;
  }

  /**
   * Add default apps.
   * @param appDir The application directory
   * @throws IOException
   */
  protected void addDefaultApps(@OsUntrusted HttpServer this, @OsUntrusted ContextHandlerCollection parent,
      final @OsUntrusted String appDir, @OsUntrusted Configuration conf) throws IOException {
    // set up the context for "/logs/" if "hadoop.log.dir" property is defined. 
    @OsUntrusted
    String logDir = System.getProperty("hadoop.log.dir");
    if (logDir != null) {
      @OsUntrusted
      Context logContext = new @OsUntrusted Context(parent, "/logs");
      logContext.setResourceBase(logDir);
      logContext.addServlet(AdminAuthorizedServlet.class, "/*");
      if (conf.getBoolean(
          CommonConfigurationKeys.HADOOP_JETTY_LOGS_SERVE_ALIASES,
          CommonConfigurationKeys.DEFAULT_HADOOP_JETTY_LOGS_SERVE_ALIASES)) {
        logContext.getInitParams().put(
            "org.mortbay.jetty.servlet.Default.aliases", "true");
      }
      logContext.setDisplayName("logs");
      setContextAttributes(logContext, conf);
      addNoCacheFilter(webAppContext);
      defaultContexts.put(logContext, true);
    }
    // set up the context for "/static/*"
    @OsUntrusted
    Context staticContext = new @OsUntrusted Context(parent, "/static");
    staticContext.setResourceBase(appDir + "/static");
    staticContext.addServlet(DefaultServlet.class, "/*");
    staticContext.setDisplayName("static");
    setContextAttributes(staticContext, conf);
    defaultContexts.put(staticContext, true);
  }
  
  private void setContextAttributes(@OsUntrusted HttpServer this, @OsUntrusted Context context, @OsUntrusted Configuration conf) {
    context.getServletContext().setAttribute(CONF_CONTEXT_ATTRIBUTE, conf);
    context.getServletContext().setAttribute(ADMINS_ACL, adminsAcl);
  }

  /**
   * Add default servlets.
   */
  protected void addDefaultServlets(@OsUntrusted HttpServer this) {
    // set up default servlets
    addServlet("stacks", "/stacks", StackServlet.class);
    addServlet("logLevel", "/logLevel", LogLevel.Servlet.class);
    addServlet("metrics", "/metrics", MetricsServlet.class);
    addServlet("jmx", "/jmx", JMXJsonServlet.class);
    addServlet("conf", "/conf", ConfServlet.class);
  }

  public void addContext(@OsUntrusted HttpServer this, @OsUntrusted Context ctxt, @OsUntrusted boolean isFiltered)
      throws IOException {
    webServer.addHandler(ctxt);
    addNoCacheFilter(webAppContext);
    defaultContexts.put(ctxt, isFiltered);
  }

  /**
   * Add a context 
   * @param pathSpec The path spec for the context
   * @param dir The directory containing the context
   * @param isFiltered if true, the servlet is added to the filter path mapping 
   * @throws IOException
   */
  protected void addContext(@OsUntrusted HttpServer this, @OsUntrusted String pathSpec, @OsUntrusted String dir, @OsUntrusted boolean isFiltered) throws IOException {
    if (0 == webServer.getHandlers().length) {
      throw new @OsUntrusted RuntimeException("Couldn't find handler");
    }
    @OsUntrusted
    WebAppContext webAppCtx = new @OsUntrusted WebAppContext();
    webAppCtx.setContextPath(pathSpec);
    webAppCtx.setWar(dir);
    addContext(webAppCtx, true);
  }

  /**
   * Set a value in the webapp context. These values are available to the jsp
   * pages as "application.getAttribute(name)".
   * @param name The name of the attribute
   * @param value The value of the attribute
   */
  public void setAttribute(@OsUntrusted HttpServer this, @OsUntrusted String name, @OsUntrusted Object value) {
    webAppContext.setAttribute(name, value);
  }

  /** 
   * Add a Jersey resource package.
   * @param packageName The Java package name containing the Jersey resource.
   * @param pathSpec The path spec for the servlet
   */
  public void addJerseyResourcePackage(@OsUntrusted HttpServer this, final @OsUntrusted String packageName,
      final @OsUntrusted String pathSpec) {
    LOG.info("addJerseyResourcePackage: packageName=" + packageName
        + ", pathSpec=" + pathSpec);
    final @OsUntrusted ServletHolder sh = new @OsUntrusted ServletHolder(ServletContainer.class);
    sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass",
        "com.sun.jersey.api.core.PackagesResourceConfig");
    sh.setInitParameter("com.sun.jersey.config.property.packages", packageName);
    webAppContext.addServlet(sh, pathSpec);
  }

  /**
   * Add a servlet in the server.
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   */
  public void addServlet(@OsUntrusted HttpServer this, @OsUntrusted String name, @OsUntrusted String pathSpec,
      @OsUntrusted
      Class<@OsUntrusted ? extends @OsUntrusted HttpServlet> clazz) {
    addInternalServlet(name, pathSpec, clazz, false);
    addFilterPathMapping(pathSpec, webAppContext);
  }

  /**
   * Add an internal servlet in the server. 
   * Note: This method is to be used for adding servlets that facilitate
   * internal communication and not for user facing functionality. For
   * servlets added using this method, filters are not enabled. 
   * 
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   */
  public void addInternalServlet(@OsUntrusted HttpServer this, @OsUntrusted String name, @OsUntrusted String pathSpec,
      @OsUntrusted
      Class<@OsUntrusted ? extends @OsUntrusted HttpServlet> clazz) {
    addInternalServlet(name, pathSpec, clazz, false);
  }

  /**
   * Add an internal servlet in the server, specifying whether or not to
   * protect with Kerberos authentication. 
   * Note: This method is to be used for adding servlets that facilitate
   * internal communication and not for user facing functionality. For
   +   * servlets added using this method, filters (except internal Kerberos
   * filters) are not enabled. 
   * 
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   * @param requireAuth Require Kerberos authenticate to access servlet
   */
  public void addInternalServlet(@OsUntrusted HttpServer this, @OsUntrusted String name, @OsUntrusted String pathSpec, 
      @OsUntrusted
      Class<@OsUntrusted ? extends @OsUntrusted HttpServlet> clazz, @OsUntrusted boolean requireAuth) {
    @OsUntrusted
    ServletHolder holder = new @OsUntrusted ServletHolder(clazz);
    if (name != null) {
      holder.setName(name);
    }
    webAppContext.addServlet(holder, pathSpec);

    if(requireAuth && UserGroupInformation.isSecurityEnabled()) {
       LOG.info("Adding Kerberos (SPNEGO) filter to " + name);
       @OsUntrusted
       ServletHandler handler = webAppContext.getServletHandler();
       @OsUntrusted
       FilterMapping fmap = new @OsUntrusted FilterMapping();
       fmap.setPathSpec(pathSpec);
       fmap.setFilterName(SPNEGO_FILTER);
       fmap.setDispatches(Handler.ALL);
       handler.addFilterMapping(fmap);
    }
  }

  @Override
  public void addFilter(@OsUntrusted HttpServer this, @OsUntrusted String name, @OsUntrusted String classname,
      @OsUntrusted
      Map<@OsUntrusted String, @OsUntrusted String> parameters) {

    final @OsUntrusted String @OsUntrusted [] USER_FACING_URLS = new String @OsUntrusted [] { "*.html", "*.jsp" };
    defineFilter(webAppContext, name, classname, parameters, USER_FACING_URLS);
    LOG.info("Added filter " + name + " (class=" + classname
        + ") to context " + webAppContext.getDisplayName());
    final @OsUntrusted String @OsUntrusted [] ALL_URLS = new String @OsUntrusted [] { "/*" };
    for (Map.@OsUntrusted Entry<@OsUntrusted Context, @OsUntrusted Boolean> e : defaultContexts.entrySet()) {
      if (e.getValue()) {
        @OsUntrusted
        Context ctx = e.getKey();
        defineFilter(ctx, name, classname, parameters, ALL_URLS);
        LOG.info("Added filter " + name + " (class=" + classname
            + ") to context " + ctx.getDisplayName());
      }
    }
    filterNames.add(name);
  }

  @Override
  public void addGlobalFilter(@OsUntrusted HttpServer this, @OsUntrusted String name, @OsUntrusted String classname,
      @OsUntrusted
      Map<@OsUntrusted String, @OsUntrusted String> parameters) {
    final @OsUntrusted String @OsUntrusted [] ALL_URLS = new String @OsUntrusted [] { "/*" };
    defineFilter(webAppContext, name, classname, parameters, ALL_URLS);
    for (@OsUntrusted Context ctx : defaultContexts.keySet()) {
      defineFilter(ctx, name, classname, parameters, ALL_URLS);
    }
    LOG.info("Added global filter '" + name + "' (class=" + classname + ")");
  }

  /**
   * Define a filter for a context and set up default url mappings.
   */
  public void defineFilter(@OsUntrusted HttpServer this, @OsUntrusted Context ctx, @OsUntrusted String name,
      @OsUntrusted
      String classname, @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String> parameters, @OsUntrusted String @OsUntrusted [] urls) {

    @OsUntrusted
    FilterHolder holder = new @OsUntrusted FilterHolder();
    holder.setName(name);
    holder.setClassName(classname);
    holder.setInitParameters(parameters);
    @OsUntrusted
    FilterMapping fmap = new @OsUntrusted FilterMapping();
    fmap.setPathSpecs(urls);
    fmap.setDispatches(Handler.ALL);
    fmap.setFilterName(name);
    @OsUntrusted
    ServletHandler handler = ctx.getServletHandler();
    handler.addFilter(holder, fmap);
  }

  /**
   * Add the path spec to the filter path mapping.
   * @param pathSpec The path spec
   * @param webAppCtx The WebApplicationContext to add to
   */
  protected void addFilterPathMapping(@OsUntrusted HttpServer this, @OsUntrusted String pathSpec,
      @OsUntrusted
      Context webAppCtx) {
    @OsUntrusted
    ServletHandler handler = webAppCtx.getServletHandler();
    for(@OsUntrusted String name : filterNames) {
      @OsUntrusted
      FilterMapping fmap = new @OsUntrusted FilterMapping();
      fmap.setPathSpec(pathSpec);
      fmap.setFilterName(name);
      fmap.setDispatches(Handler.ALL);
      handler.addFilterMapping(fmap);
    }
  }
  
  /**
   * Get the value in the webapp context.
   * @param name The name of the attribute
   * @return The value of the attribute
   */
  public @OsUntrusted Object getAttribute(@OsUntrusted HttpServer this, @OsUntrusted String name) {
    return webAppContext.getAttribute(name);
  }
  
  public @OsUntrusted WebAppContext getWebAppContext(@OsUntrusted HttpServer this){
    return this.webAppContext;
  }

  /**
   * Get the pathname to the webapps files.
   * @param appName eg "secondary" or "datanode"
   * @return the pathname as a URL
   * @throws FileNotFoundException if 'webapps' directory cannot be found on CLASSPATH.
   */
  protected @OsUntrusted String getWebAppsPath(@OsUntrusted HttpServer this, @OsUntrusted String appName) throws FileNotFoundException {
    @OsUntrusted
    URL url = getClass().getClassLoader().getResource("webapps/" + appName);
    if (url == null) 
      throw new @OsUntrusted FileNotFoundException("webapps/" + appName
          + " not found in CLASSPATH");
    @OsUntrusted
    String urlString = url.toString();
    return urlString.substring(0, urlString.lastIndexOf('/'));
  }

  /**
   * Get the port that the server is on
   * @return the port
   */
  public @OsUntrusted int getPort(@OsUntrusted HttpServer this) {
    return webServer.getConnectors()[0].getLocalPort();
  }

  /**
   * Set the min, max number of worker threads (simultaneous connections).
   */
  public void setThreads(@OsUntrusted HttpServer this, @OsUntrusted int min, @OsUntrusted int max) {
    @OsUntrusted
    QueuedThreadPool pool = (@OsUntrusted QueuedThreadPool) webServer.getThreadPool() ;
    pool.setMinThreads(min);
    pool.setMaxThreads(max);
  }

  /**
   * Configure an ssl listener on the server.
   * @param addr address to listen on
   * @param keystore location of the keystore
   * @param storPass password for the keystore
   * @param keyPass password for the key
   * @deprecated Use {@link #addSslListener(InetSocketAddress, Configuration, boolean)}
   */
  @Deprecated
  public void addSslListener(@OsUntrusted HttpServer this, @OsUntrusted InetSocketAddress addr, @OsUntrusted String keystore,
      @OsUntrusted
      String storPass, @OsUntrusted String keyPass) throws IOException {
    if (webServer.isStarted()) {
      throw new @OsUntrusted IOException("Failed to add ssl listener");
    }
    @OsUntrusted
    SslSocketConnector sslListener = new @OsUntrusted SslSocketConnector();
    sslListener.setHost(addr.getHostName());
    sslListener.setPort(addr.getPort());
    sslListener.setKeystore(keystore);
    sslListener.setPassword(storPass);
    sslListener.setKeyPassword(keyPass);
    webServer.addConnector(sslListener);
  }

  /**
   * Configure an ssl listener on the server.
   * @param addr address to listen on
   * @param sslConf conf to retrieve ssl options
   * @param needCertsAuth whether x509 certificate authentication is required
   */
  public void addSslListener(@OsUntrusted HttpServer this, @OsUntrusted InetSocketAddress addr, @OsUntrusted Configuration sslConf,
      @OsUntrusted
      boolean needCertsAuth) throws IOException {
    if (webServer.isStarted()) {
      throw new @OsUntrusted IOException("Failed to add ssl listener");
    }
    if (needCertsAuth) {
      // setting up SSL truststore for authenticating clients
      System.setProperty("javax.net.ssl.trustStore", sslConf.get(
          "ssl.server.truststore.location", ""));
      System.setProperty("javax.net.ssl.trustStorePassword", sslConf.get(
          "ssl.server.truststore.password", ""));
      System.setProperty("javax.net.ssl.trustStoreType", sslConf.get(
          "ssl.server.truststore.type", "jks"));
    }
    @OsUntrusted
    SslSocketConnector sslListener = new @OsUntrusted SslSocketConnector();
    sslListener.setHost(addr.getHostName());
    sslListener.setPort(addr.getPort());
    sslListener.setKeystore(sslConf.get("ssl.server.keystore.location"));
    sslListener.setPassword(sslConf.get("ssl.server.keystore.password", ""));
    sslListener.setKeyPassword(sslConf.get("ssl.server.keystore.keypassword", ""));
    sslListener.setKeystoreType(sslConf.get("ssl.server.keystore.type", "jks"));
    sslListener.setNeedClientAuth(needCertsAuth);
    webServer.addConnector(sslListener);
  }
  
  protected void initSpnego(@OsUntrusted HttpServer this, @OsUntrusted Configuration conf,
      @OsUntrusted
      String usernameConfKey, @OsUntrusted String keytabConfKey) throws IOException {
    @OsUntrusted
    Map<@OsUntrusted String, @OsUntrusted String> params = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted String>();
    @OsUntrusted
    String principalInConf = conf.get(usernameConfKey);
    if (principalInConf != null && !principalInConf.isEmpty()) {
      params.put("kerberos.principal",
                 SecurityUtil.getServerPrincipal(principalInConf, listener.getHost()));
    }
    @OsUntrusted
    String httpKeytab = conf.get(keytabConfKey);
    if (httpKeytab != null && !httpKeytab.isEmpty()) {
      params.put("kerberos.keytab", httpKeytab);
    }
    params.put(AuthenticationFilter.AUTH_TYPE, "kerberos");
  
    defineFilter(webAppContext, SPNEGO_FILTER,
                 AuthenticationFilter.class.getName(), params, null);
  }

  /**
   * Start the server. Does not wait for the server to start.
   */
  public void start(@OsUntrusted HttpServer this) throws IOException {
    try {
      try {
        openListener();
        LOG.info("Jetty bound to port " + listener.getLocalPort());
        webServer.start();
      } catch (@OsUntrusted IOException ex) {
        LOG.info("HttpServer.start() threw a non Bind IOException", ex);
        throw ex;
      } catch (@OsUntrusted MultiException ex) {
        LOG.info("HttpServer.start() threw a MultiException", ex);
        throw ex;
      }
      // Make sure there is no handler failures.
      @OsUntrusted
      Handler @OsUntrusted [] handlers = webServer.getHandlers();
      for (@OsUntrusted int i = 0; i < handlers.length; i++) {
        if (handlers[i].isFailed()) {
          throw new @OsUntrusted IOException(
              "Problem in starting http server. Server handlers failed");
        }
      }
      // Make sure there are no errors initializing the context.
      @OsUntrusted
      Throwable unavailableException = webAppContext.getUnavailableException();
      if (unavailableException != null) {
        // Have to stop the webserver, or else its non-daemon threads
        // will hang forever.
        webServer.stop();
        throw new @OsUntrusted IOException("Unable to initialize WebAppContext",
            unavailableException);
      }
    } catch (@OsUntrusted IOException e) {
      throw e;
    } catch (@OsUntrusted InterruptedException e) {
      throw (@OsUntrusted IOException) new @OsUntrusted InterruptedIOException(
          "Interrupted while starting HTTP server").initCause(e);
    } catch (@OsUntrusted Exception e) {
      throw new @OsUntrusted IOException("Problem starting http server", e);
    }
  }

  /**
   * Open the main listener for the server
   * @throws Exception
   */
  void openListener(@OsUntrusted HttpServer this) throws Exception {
    if (listener.getLocalPort() != -1) { // it's already bound
      return;
    }
    if (listenerStartedExternally) { // Expect that listener was started securely
      throw new @OsUntrusted Exception("Expected webserver's listener to be started " +
          "previously but wasn't");
    }
    @OsUntrusted
    int port = listener.getPort();
    while (true) {
      // jetty has a bug where you can't reopen a listener that previously
      // failed to open w/o issuing a close first, even if the port is changed
      try {
        listener.close();
        listener.open();
        break;
      } catch (@OsUntrusted BindException ex) {
        if (port == 0 || !findPort) {
          @OsUntrusted
          BindException be = new @OsUntrusted BindException(
              "Port in use: " + listener.getHost() + ":" + listener.getPort());
          be.initCause(ex);
          throw be;
        }
      }
      // try the next port number
      listener.setPort(++port);
      Thread.sleep(100);
    }
  }
  
  /**
   * Return the bind address of the listener.
   * @return InetSocketAddress of the listener
   */
  public @OsUntrusted InetSocketAddress getListenerAddress(@OsUntrusted HttpServer this) {
    @OsUntrusted
    int port = listener.getLocalPort();
    if (port == -1) { // not bound, return requested port
      port = listener.getPort();
    }
    return new @OsUntrusted InetSocketAddress(listener.getHost(), port);
  }
  
  /**
   * stop the server
   */
  public void stop(@OsUntrusted HttpServer this) throws Exception {
    @OsUntrusted
    MultiException exception = null;
    try {
      listener.close();
    } catch (@OsUntrusted Exception e) {
      LOG.error("Error while stopping listener for webapp"
          + webAppContext.getDisplayName(), e);
      exception = addMultiException(exception, e);
    }

    try {
      if (sslFactory != null) {
          sslFactory.destroy();
      }
    } catch (@OsUntrusted Exception e) {
      LOG.error("Error while destroying the SSLFactory"
          + webAppContext.getDisplayName(), e);
      exception = addMultiException(exception, e);
    }

    try {
      // clear & stop webAppContext attributes to avoid memory leaks.
      webAppContext.clearAttributes();
      webAppContext.stop();
    } catch (@OsUntrusted Exception e) {
      LOG.error("Error while stopping web app context for webapp "
          + webAppContext.getDisplayName(), e);
      exception = addMultiException(exception, e);
    }
    try {
      webServer.stop();
    } catch (@OsUntrusted Exception e) {
      LOG.error("Error while stopping web server for webapp "
          + webAppContext.getDisplayName(), e);
      exception = addMultiException(exception, e);
    }

    if (exception != null) {
      exception.ifExceptionThrow();
    }

  }

  private @OsUntrusted MultiException addMultiException(@OsUntrusted HttpServer this, @OsUntrusted MultiException exception, @OsUntrusted Exception e) {
    if(exception == null){
      exception = new @OsUntrusted MultiException();
    }
    exception.add(e);
    return exception;
  }

  public void join(@OsUntrusted HttpServer this) throws InterruptedException {
    webServer.join();
  }

  /**
   * Test for the availability of the web server
   * @return true if the web server is started, false otherwise
   */
  public @OsUntrusted boolean isAlive(@OsUntrusted HttpServer this) {
    return webServer != null && webServer.isStarted();
  }

  /**
   * Return the host and port of the HttpServer, if live
   * @return the classname and any HTTP URL
   */
  @Override
  public @OsUntrusted String toString(@OsUntrusted HttpServer this) {
    return listener != null ?
        ("HttpServer at http://" + listener.getHost() + ":" + listener.getLocalPort() + "/"
            + (isAlive() ? STATE_DESCRIPTION_ALIVE : STATE_DESCRIPTION_NOT_LIVE))
        : "Inactive HttpServer";
  }

  /**
   * Checks the user has privileges to access to instrumentation servlets.
   * <p/>
   * If <code>hadoop.security.instrumentation.requires.admin</code> is set to FALSE
   * (default value) it always returns TRUE.
   * <p/>
   * If <code>hadoop.security.instrumentation.requires.admin</code> is set to TRUE
   * it will check that if the current user is in the admin ACLS. If the user is
   * in the admin ACLs it returns TRUE, otherwise it returns FALSE.
   *
   * @param servletContext the servlet context.
   * @param request the servlet request.
   * @param response the servlet response.
   * @return TRUE/FALSE based on the logic decribed above.
   */
  public static @OsUntrusted boolean isInstrumentationAccessAllowed(
    @OsUntrusted
    ServletContext servletContext, @OsUntrusted HttpServletRequest request,
    @OsUntrusted
    HttpServletResponse response) throws IOException {
    @OsUntrusted
    Configuration conf =
      (@OsUntrusted Configuration) servletContext.getAttribute(CONF_CONTEXT_ATTRIBUTE);

    @OsUntrusted
    boolean access = true;
    @OsUntrusted
    boolean adminAccess = conf.getBoolean(
      CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN,
      false);
    if (adminAccess) {
      access = hasAdministratorAccess(servletContext, request, response);
    }
    return access;
  }

  /**
   * Does the user sending the HttpServletRequest has the administrator ACLs? If
   * it isn't the case, response will be modified to send an error to the user.
   * 
   * @param servletContext
   * @param request
   * @param response used to send the error response if user does not have admin access.
   * @return true if admin-authorized, false otherwise
   * @throws IOException
   */
  public static @OsUntrusted boolean hasAdministratorAccess(
      @OsUntrusted
      ServletContext servletContext, @OsUntrusted HttpServletRequest request,
      @OsUntrusted
      HttpServletResponse response) throws IOException {
    @OsUntrusted
    Configuration conf =
        (@OsUntrusted Configuration) servletContext.getAttribute(CONF_CONTEXT_ATTRIBUTE);
    // If there is no authorization, anybody has administrator access.
    if (!conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      return true;
    }

    @OsUntrusted
    String remoteUser = request.getRemoteUser();
    if (remoteUser == null) {
      response.sendError(HttpServletResponse.SC_UNAUTHORIZED,
                         "Unauthenticated users are not " +
                         "authorized to access this page.");
      return false;
    }
    
    if (servletContext.getAttribute(ADMINS_ACL) != null &&
        !userHasAdministratorAccess(servletContext, remoteUser)) {
      response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "User "
          + remoteUser + " is unauthorized to access this page.");
      return false;
    }

    return true;
  }

  /**
   * Get the admin ACLs from the given ServletContext and check if the given
   * user is in the ACL.
   * 
   * @param servletContext the context containing the admin ACL.
   * @param remoteUser the remote user to check for.
   * @return true if the user is present in the ACL, false if no ACL is set or
   *         the user is not present
   */
  public static @OsUntrusted boolean userHasAdministratorAccess(@OsUntrusted ServletContext servletContext,
      @OsUntrusted
      String remoteUser) {
    @OsUntrusted
    AccessControlList adminsAcl = (@OsUntrusted AccessControlList) servletContext
        .getAttribute(ADMINS_ACL);
    @OsUntrusted
    UserGroupInformation remoteUserUGI =
        UserGroupInformation.createRemoteUser(remoteUser);
    return adminsAcl != null && adminsAcl.isUserAllowed(remoteUserUGI);
  }

  /**
   * A very simple servlet to serve up a text representation of the current
   * stack traces. It both returns the stacks to the caller and logs them.
   * Currently the stack traces are done sequentially rather than exactly the
   * same data.
   */
  public static class StackServlet extends @OsUntrusted HttpServlet {
    private static final @OsUntrusted long serialVersionUID = -6284183679759467039L;

    @Override
    public void doGet(HttpServer.@OsUntrusted StackServlet this, @OsUntrusted HttpServletRequest request, @OsUntrusted HttpServletResponse response)
      throws ServletException, IOException {
      if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(),
                                                     request, response)) {
        return;
      }
      response.setContentType("text/plain; charset=UTF-8");
      @OsUntrusted
      PrintWriter out = response.getWriter();
      ReflectionUtils.printThreadInfo(out, "");
      out.close();
      ReflectionUtils.logThreadInfo(LOG, "jsp requested", 1);      
    }
  }
  
  /**
   * A Servlet input filter that quotes all HTML active characters in the
   * parameter names and values. The goal is to quote the characters to make
   * all of the servlets resistant to cross-site scripting attacks.
   */
  public static class QuotingInputFilter implements @OsUntrusted Filter {
    private @OsUntrusted FilterConfig config;

    public static class RequestQuoter extends @OsUntrusted HttpServletRequestWrapper {
      private final @OsUntrusted HttpServletRequest rawRequest;
      public @OsUntrusted RequestQuoter(@OsUntrusted HttpServletRequest rawRequest) {
        super(rawRequest);
        this.rawRequest = rawRequest;
      }
      
      /**
       * Return the set of parameter names, quoting each name.
       */
      @SuppressWarnings("unchecked")
      @Override
      public @OsUntrusted Enumeration<@OsUntrusted String> getParameterNames(HttpServer.QuotingInputFilter.@OsUntrusted RequestQuoter this) {
        return new @OsUntrusted Enumeration<@OsUntrusted String>() {
          private @OsUntrusted Enumeration<@OsUntrusted String> rawIterator =
            rawRequest.getParameterNames();
          @Override
          public @OsUntrusted boolean hasMoreElements() {
            return rawIterator.hasMoreElements();
          }

          @Override
          public @OsUntrusted String nextElement() {
            return HtmlQuoting.quoteHtmlChars(rawIterator.nextElement());
          }
        };
      }
      
      /**
       * Unquote the name and quote the value.
       */
      @Override
      public @OsUntrusted String getParameter(HttpServer.QuotingInputFilter.@OsUntrusted RequestQuoter this, @OsUntrusted String name) {
        return HtmlQuoting.quoteHtmlChars(rawRequest.getParameter
                                     (HtmlQuoting.unquoteHtmlChars(name)));
      }
      
      @Override
      public @OsUntrusted String @OsUntrusted [] getParameterValues(HttpServer.QuotingInputFilter.@OsUntrusted RequestQuoter this, @OsUntrusted String name) {
        @OsUntrusted
        String unquoteName = HtmlQuoting.unquoteHtmlChars(name);
        @OsUntrusted
        String @OsUntrusted [] unquoteValue = rawRequest.getParameterValues(unquoteName);
        if (unquoteValue == null) {
          return null;
        }
        @OsUntrusted
        String @OsUntrusted [] result = new @OsUntrusted String @OsUntrusted [unquoteValue.length];
        for(@OsUntrusted int i=0; i < result.length; ++i) {
          result[i] = HtmlQuoting.quoteHtmlChars(unquoteValue[i]);
        }
        return result;
      }

      @SuppressWarnings("unchecked")
      @Override
      public @OsUntrusted Map<@OsUntrusted String, @OsUntrusted String @OsUntrusted []> getParameterMap(HttpServer.QuotingInputFilter.@OsUntrusted RequestQuoter this) {
        @OsUntrusted
        Map<@OsUntrusted String, @OsUntrusted String @OsUntrusted []> result = new @OsUntrusted HashMap<@OsUntrusted String, @OsUntrusted String @OsUntrusted []>();
        @OsUntrusted
        Map<@OsUntrusted String, @OsUntrusted String @OsUntrusted []> raw = rawRequest.getParameterMap();
        for (Map.@OsUntrusted Entry<@OsUntrusted String, @OsUntrusted String @OsUntrusted []> item: raw.entrySet()) {
          @OsUntrusted
          String @OsUntrusted [] rawValue = item.getValue();
          @OsUntrusted
          String @OsUntrusted [] cookedValue = new @OsUntrusted String @OsUntrusted [rawValue.length];
          for(@OsUntrusted int i=0; i< rawValue.length; ++i) {
            cookedValue[i] = HtmlQuoting.quoteHtmlChars(rawValue[i]);
          }
          result.put(HtmlQuoting.quoteHtmlChars(item.getKey()), cookedValue);
        }
        return result;
      }
      
      /**
       * Quote the url so that users specifying the HOST HTTP header
       * can't inject attacks.
       */
      @Override
      public @OsUntrusted StringBuffer getRequestURL(HttpServer.QuotingInputFilter.@OsUntrusted RequestQuoter this){
        @OsUntrusted
        String url = rawRequest.getRequestURL().toString();
        return new @OsUntrusted StringBuffer(HtmlQuoting.quoteHtmlChars(url));
      }
      
      /**
       * Quote the server name so that users specifying the HOST HTTP header
       * can't inject attacks.
       */
      @Override
      public @OsUntrusted String getServerName(HttpServer.QuotingInputFilter.@OsUntrusted RequestQuoter this) {
        return HtmlQuoting.quoteHtmlChars(rawRequest.getServerName());
      }
    }

    @Override
    public void init(HttpServer.@OsUntrusted QuotingInputFilter this, @OsUntrusted FilterConfig config) throws ServletException {
      this.config = config;
    }

    @Override
    public void destroy(HttpServer.@OsUntrusted QuotingInputFilter this) {
    }

    @Override
    public void doFilter(HttpServer.@OsUntrusted QuotingInputFilter this, @OsUntrusted ServletRequest request, 
                         @OsUntrusted
                         ServletResponse response,
                         @OsUntrusted
                         FilterChain chain
                         ) throws IOException, ServletException {
      @OsUntrusted
      HttpServletRequestWrapper quoted = 
        new @OsUntrusted RequestQuoter((@OsUntrusted HttpServletRequest) request);
      @OsUntrusted
      HttpServletResponse httpResponse = (@OsUntrusted HttpServletResponse) response;

      @OsUntrusted
      String mime = inferMimeType(request);
      if (mime == null) {
        httpResponse.setContentType("text/plain; charset=utf-8");
      } else if (mime.startsWith("text/html")) {
        // HTML with unspecified encoding, we want to
        // force HTML with utf-8 encoding
        // This is to avoid the following security issue:
        // http://openmya.hacker.jp/hasegawa/security/utf7cs.html
        httpResponse.setContentType("text/html; charset=utf-8");
      } else if (mime.startsWith("application/xml")) {
        httpResponse.setContentType("text/xml; charset=utf-8");
      }
      chain.doFilter(quoted, httpResponse);
    }

    /**
     * Infer the mime type for the response based on the extension of the request
     * URI. Returns null if unknown.
     */
    private @OsUntrusted String inferMimeType(HttpServer.@OsUntrusted QuotingInputFilter this, @OsUntrusted ServletRequest request) {
      @OsUntrusted
      String path = ((@OsUntrusted HttpServletRequest)request).getRequestURI();
      @OsUntrusted
      ContextHandler.@OsUntrusted SContext sContext = (@OsUntrusted ContextHandler.@OsUntrusted SContext)config.getServletContext();
      @OsUntrusted
      MimeTypes mimes = sContext.getContextHandler().getMimeTypes();
      @OsUntrusted
      Buffer mimeBuffer = mimes.getMimeByExtension(path);
      return (mimeBuffer == null) ? null : mimeBuffer.toString();
    }

  }
}
