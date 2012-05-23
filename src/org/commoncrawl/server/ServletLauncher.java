/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 **/
package org.commoncrawl.server;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.servlet.DefaultServlet;

/**
 * 
 * @author rana
 *
 */
public class ServletLauncher extends DefaultServlet {

  public static final Log LOG = LogFactory.getLog(ServletLauncher.class);

  public static final String SERVLET_REGISTRY_KEY = "servlet.registry";
  ServletRegistry registry = null;

  @Override
  protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

    ServletRegistry theRegistry = null;
    synchronized (this) {
      theRegistry = registry;
      if (theRegistry == null) {
        LOG.info("Querying for Registry");
        String registryClass = getInitParameter(SERVLET_REGISTRY_KEY);
        LOG.info("Registry Class is:" + registryClass);
        if (registryClass != null) {
          try {
            LOG.info("Loading Registry Class");
            Class<ServletRegistry> classObject = (Class<ServletRegistry>) Thread.currentThread()
                .getContextClassLoader().loadClass(registryClass);
            LOG.info("Instantiating Registry Class");
            registry = classObject.newInstance();
            theRegistry = registry;
          } catch (ClassNotFoundException e) {
            LOG.error(StringUtils.stringifyException(e));
          } catch (InstantiationException e) {
            LOG.error(StringUtils.stringifyException(e));
          } catch (IllegalAccessException e) {
            LOG.error(StringUtils.stringifyException(e));
          }

          if (registry == null) {
            LOG.error("Failed to instantiate reigstry!");
          }
        }
      }
    }

    if (theRegistry != null) {
      HttpServlet servlet = theRegistry.getServletForURI(req.getRequestURI());
      if (servlet != null) {
        LOG.info("Dispatching to servlet:" + servlet.getClass().getCanonicalName());
        servlet.service(req, resp);
      } else {
        Request baseRequest = (Request) req;
        baseRequest.setHandled(false);
        // super.service(req, resp);
      }
    }
  }
}
