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
import java.util.Enumeration;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.UnavailableException;

import org.mortbay.jetty.servlet.ServletHolder;

/**
 * 
 * @author rana
 *
 */
public class CustomServletHolder extends ServletHolder {

  class Config implements ServletConfig {
    /* -------------------------------------------------------- */
    public String getInitParameter(String param) {
      return CustomServletHolder.this.getInitParameter(param);
    }

    /* -------------------------------------------------------- */
    public Enumeration getInitParameterNames() {
      return CustomServletHolder.this.getInitParameterNames();
    }

    /* -------------------------------------------------------- */
    public ServletContext getServletContext() {
      return _servletHandler.getServletContext();
    }

    /* -------------------------------------------------------- */
    public String getServletName() {
      return getName();
    }
  }

  public CustomServletHolder(Class servletClass) {
    super(servletClass);
  }

  /*
   * @Override public synchronized Servlet getServlet() throws ServletException
   * { if (getInitParameter("x-hack-nocache") != null) { try { Servlet servlet =
   * (Servlet) newInstance(); servlet.init(new Config()); return servlet; }
   * catch (Throwable e) { throw new ServletException(e); } } else { return
   * super.getServlet(); } }
   */

  @Override
  public void handle(ServletRequest request, ServletResponse response) throws ServletException, UnavailableException,
      IOException {
    if (getInitParameter("x-hack-nocache") != null) {
      try {
        Servlet servlet = (Servlet) newInstance();
        servlet.init(new Config());
        servlet.service(request, response);
        servlet.destroy();
        System.gc();
      } catch (Throwable e) {
        throw new ServletException(e);
      }
    } else {
      super.handle(request, response);
    }
  }
}
