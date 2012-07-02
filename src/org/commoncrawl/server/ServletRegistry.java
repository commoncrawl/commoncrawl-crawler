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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

/**
 * 
 * @author rana
 *
 */
public abstract class ServletRegistry {

  public static class DispatchVector {

    Pattern _urlPattern;

    String _className;

    DispatchVector(String pattern, String className) {
      _urlPattern = Pattern.compile(pattern);
      _className = className;
    }
  }

  public static final Log LOG = LogFactory.getLog(ServletRegistry.class);

  static void addClassPathEntries(File directory, ArrayList<URL> urlListOut) throws MalformedURLException {
    File files[] = directory.listFiles();

    if (files != null) {
      for (File file : files) {
        if (!file.isDirectory()) {
          // LOG.info("Adding ClassPath Entry:" + file.toURL().toString());
          urlListOut.add(file.toURI().toURL());
        } else {
          addClassPathEntries(file, urlListOut);
        }
      }
    }
  }

  private ArrayList<DispatchVector> _registry = new ArrayList<DispatchVector>();

  public HttpServlet getServletForURI(String uri) throws ServletException, IOException {
    for (DispatchVector d : _registry) {
      if (d._urlPattern.matcher(uri).matches()) {
        // LOG.info("Loading Servlet:" + d._className + " for uri:" + uri);
        HttpServlet servlet = loadServlet(d._className);
        if (servlet != null) {

          return servlet;
        }
      }
    }
    return null;
  }

  HttpServlet loadServlet(String servletName) {

    String classesRoot = System.getProperty("commoncrawl.classes.root");

    // LOG.info("Classes Root is:" + classesRoot);

    if (classesRoot == null) {
      classesRoot = new File("/Users/rana/commoncrawl/commoncrawl_trunk/trunk/deploy/bin").getAbsolutePath();
    }
    // LOG.info("Classes Root is:" + classesRoot);
    try {
      ArrayList<URL> urls = new ArrayList<URL>();
      urls.add(new File(classesRoot).toURI().toURL());
      LOG.info("URL is:" + urls.get(0).toString());

      addClassPathEntries(new File(classesRoot, "lib"), urls);

      DynamicClassLoader loader = new DynamicClassLoader(urls.toArray(new URL[0]), ClassLoader.getSystemClassLoader(),
          servletName);

      // LOG.info("Loader Init Successfull");
      try {
        LOG.info("Construct class type:" + servletName);
        Class theClass = loader.loadClass(servletName, true);
        LOG.info("Loaded Class Type:" + theClass);
        return (HttpServlet) theClass.newInstance();
      } catch (ClassNotFoundException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
    } catch (MalformedURLException e1) {
      LOG.error(StringUtils.stringifyException(e1));
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
    return null;
  }

  protected void registerServlet(String pathRegEx, String serveletClassName) {
    _registry.add(new DispatchVector(pathRegEx, serveletClassName));
  }
}
