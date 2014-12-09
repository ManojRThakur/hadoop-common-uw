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

package org.apache.hadoop.conf;

import ostrusted.quals.OsUntrusted;
import ostrusted.quals.OsTrusted;
import org.apache.commons.logging.*;

import org.apache.commons.lang.StringEscapeUtils;

import java.util.Collection;
import java.util.Enumeration;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.util.StringUtils;

/**
 * A servlet for changing a node's configuration.
 *
 * Reloads the configuration file, verifies whether changes are
 * possible and asks the admin to approve the change.
 *
 */
public class ReconfigurationServlet extends @OsUntrusted HttpServlet {
  
  private static final @OsUntrusted long serialVersionUID = 1L;

  private static final @OsUntrusted Log LOG =
    LogFactory.getLog(ReconfigurationServlet.class);

  // the prefix used to fing the attribute holding the reconfigurable 
  // for a given request
  //
  // we get the attribute prefix + servlet path
  public static final @OsUntrusted String CONF_SERVLET_RECONFIGURABLE_PREFIX =
    "conf.servlet.reconfigurable.";
  
  @Override
  public void init(@OsUntrusted ReconfigurationServlet this) throws ServletException {
    super.init();
  }

  private @OsUntrusted Reconfigurable getReconfigurable(@OsUntrusted ReconfigurationServlet this, @OsUntrusted HttpServletRequest req) {
    LOG.info("servlet path: " + req.getServletPath());
    LOG.info("getting attribute: " + CONF_SERVLET_RECONFIGURABLE_PREFIX +
             req.getServletPath());
    return (@OsUntrusted Reconfigurable)
      this.getServletContext().getAttribute(CONF_SERVLET_RECONFIGURABLE_PREFIX +
                                            req.getServletPath());
  }

  private void printHeader(@OsUntrusted ReconfigurationServlet this, @OsUntrusted PrintWriter out, @OsUntrusted String nodeName) {
    out.print("<html><head>");
    out.printf("<title>%s Reconfiguration Utility</title>\n", 
               StringEscapeUtils.escapeHtml(nodeName));
    out.print("</head><body>\n");
    out.printf("<h1>%s Reconfiguration Utility</h1>\n",
               StringEscapeUtils.escapeHtml(nodeName));
  }

  private void printFooter(@OsUntrusted ReconfigurationServlet this, @OsUntrusted PrintWriter out) {
    out.print("</body></html>\n");
  }
  
  /**
   * Print configuration options that can be changed.
   */
  private void printConf(@OsUntrusted ReconfigurationServlet this, @OsUntrusted PrintWriter out, @OsUntrusted Reconfigurable reconf) {
    @OsUntrusted Configuration oldConf = reconf.getConf();
    @OsUntrusted Configuration newConf = new @OsUntrusted Configuration();

    @OsUntrusted Collection<ReconfigurationUtil.@OsUntrusted PropertyChange> changes =
      ReconfigurationUtil.getChangedProperties(newConf, oldConf);

    @OsUntrusted boolean changeOK = true;
    
    out.println("<form action=\"\" method=\"post\">");
    out.println("<table border=\"1\">");
    out.println("<tr><th>Property</th><th>Old value</th>");
    out.println("<th>New value </th><th></th></tr>");
    for (ReconfigurationUtil.@OsUntrusted PropertyChange c: changes) {
      out.print("<tr><td>");
      if (!reconf.isPropertyReconfigurable(c.prop)) {
        out.print("<font color=\"red\">" + 
                  StringEscapeUtils.escapeHtml(c.prop) + "</font>");
        changeOK = false;
      } else {
        out.print(StringEscapeUtils.escapeHtml(c.prop));
        out.print("<input type=\"hidden\" name=\"" +
                  StringEscapeUtils.escapeHtml(c.prop) + "\" value=\"" +
                  StringEscapeUtils.escapeHtml(c.newVal) + "\"/>");
      }
      out.print("</td><td>" +
                (c.oldVal == null ? "<it>default</it>" : 
                 StringEscapeUtils.escapeHtml(c.oldVal)) +
                "</td><td>" +
                (c.newVal == null ? "<it>default</it>" : 
                 StringEscapeUtils.escapeHtml(c.newVal)) +
                "</td>");
      out.print("</tr>\n");
    }
    out.println("</table>");
    if (!changeOK) {
      out.println("<p><font color=\"red\">WARNING: properties marked red" +
                  " will not be changed until the next restart.</font></p>");
    }
    out.println("<input type=\"submit\" value=\"Apply\" />");
    out.println("</form>");
  }

  @SuppressWarnings("unchecked")
  private @OsUntrusted Enumeration<@OsUntrusted String> getParams(@OsUntrusted ReconfigurationServlet this, @OsUntrusted HttpServletRequest req) {
    return (@OsUntrusted Enumeration<@OsUntrusted String>) req.getParameterNames();
  }

  /**
   * Apply configuratio changes after admin has approved them.
   */
  @SuppressWarnings("ostrusted:argument.type.incompatible") //buzzsaw, see below
  private void applyChanges(@OsUntrusted ReconfigurationServlet this, @OsUntrusted PrintWriter out, @OsUntrusted Reconfigurable reconf,
                            @OsUntrusted
                            HttpServletRequest req) 
    throws IOException, ReconfigurationException {
    @OsUntrusted Configuration oldConf = reconf.getConf();
    @OsUntrusted Configuration newConf = new @OsUntrusted Configuration();

    @OsUntrusted Enumeration<@OsUntrusted String> params = getParams(req);

    synchronized(oldConf) {
      while (params.hasMoreElements()) {
        @OsUntrusted String rawParam = params.nextElement();
        @OsUntrusted String param = StringEscapeUtils.unescapeHtml( rawParam );

        @SuppressWarnings("ostrusted:cast.unsafe") // Buzzsaw or bug, not validation directly from http request.
        @OsTrusted String value = (@OsTrusted String) StringEscapeUtils.unescapeHtml( req.getParameter(rawParam) );
        if (value != null) {
          if (value.equals( newConf.getRaw( param ) ) || value.equals("default") ||
              value.equals("null") || value.isEmpty()) {
            if ((value.equals("default") || value.equals("null") || 
                 value.isEmpty()) && 
                oldConf.getRaw(param) != null) {
              out.println("<p>Changed \"" + 
                          StringEscapeUtils.escapeHtml(param) + "\" from \"" +
                          StringEscapeUtils.escapeHtml(oldConf.getRaw(param)) +
                          "\" to default</p>");
              reconf.reconfigureProperty(param, null);
            } else if (!value.equals("default") && !value.equals("null") &&
                       !value.isEmpty() && 
                       (oldConf.getRaw(param) == null || 
                        !oldConf.getRaw(param).equals(value))) {
              // change from default or value to different value
              if (oldConf.getRaw(param) == null) {
                out.println("<p>Changed \"" + 
                            StringEscapeUtils.escapeHtml(param) + 
                            "\" from default to \"" +
                            StringEscapeUtils.escapeHtml(value) + "\"</p>");
              } else {
                out.println("<p>Changed \"" + 
                            StringEscapeUtils.escapeHtml(param) + "\" from \"" +
                            StringEscapeUtils.escapeHtml(oldConf. getRaw(param)) +
                            "\" to \"" +
                            StringEscapeUtils.escapeHtml(value) + "\"</p>");
              }
              //ostrusted, value read from a HttpServletReques, the whole intent of this class is
              //to reconfigure from a servlet request, we suggest that this is either checked or secured
              //in some other fashion, so this is a buzzsaw
              reconf.reconfigureProperty(param, value);
            } else {
              LOG.info("property " + param + " unchanged");
            }
          } else {
            // parameter value != newConf value
            out.println("<p>\"" + StringEscapeUtils.escapeHtml(param) + 
                        "\" not changed because value has changed from \"" +
                        StringEscapeUtils.escapeHtml(value) + "\" to \"" +
                        StringEscapeUtils.escapeHtml(newConf.getRaw(param)) +
                        "\" since approval</p>");
          }
        }
      }
    }
  }

  @Override
  protected void doGet(@OsUntrusted ReconfigurationServlet this, @OsUntrusted HttpServletRequest req, @OsUntrusted HttpServletResponse resp)
    throws ServletException, IOException {
    LOG.info("GET");
    @OsUntrusted
    PrintWriter out = resp.getWriter();
    
    @OsUntrusted
    Reconfigurable reconf = getReconfigurable(req);
    @OsUntrusted String nodeName = reconf.getClass().getCanonicalName();

    printHeader(out, nodeName);
    printConf(out, reconf);
    printFooter(out);
  }

  @Override
  protected void doPost(@OsUntrusted ReconfigurationServlet this, @OsUntrusted HttpServletRequest req, @OsUntrusted HttpServletResponse resp)
    throws ServletException, IOException {
    LOG.info("POST");
    @OsUntrusted
    PrintWriter out = resp.getWriter();

    @OsUntrusted
    Reconfigurable reconf = getReconfigurable(req);
    @OsUntrusted
    String nodeName = reconf.getClass().getCanonicalName();

    printHeader(out, nodeName);

    try { 
      applyChanges(out, reconf, req);
    } catch (@OsUntrusted ReconfigurationException e) {
      resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, 
                     StringUtils.stringifyException(e));
      return;
    }

    out.println("<p><a href=\"" + req.getServletPath() + "\">back</a></p>");
    printFooter(out);
  }

}
