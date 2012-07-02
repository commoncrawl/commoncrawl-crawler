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
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

/**
 * 
 * @author rana
 *
 */
public class DynamicClassLoader extends URLClassLoader {

  public static final Log LOG = LogFactory.getLog(DynamicClassLoader.class);

  static void addClassPathEntries(File directory, ArrayList<URL> urlListOut) throws MalformedURLException {
    File files[] = directory.listFiles();

    if (files != null) {
      for (File file : files) {
        if (!file.isDirectory()) {
          LOG.info("Adding ClassPath Entry:" + file.toURI().toURL().toString());
          urlListOut.add(file.toURI().toURL());
        } else {
          addClassPathEntries(file, urlListOut);
        }
      }
    }
  }

  public static DynamicClassLoader loaderFromProjectRoot(File projectRoot, ClassLoader parent) {
    try {
      ArrayList<URL> urls = new ArrayList<URL>();
      urls.add(projectRoot.toURI().toURL());

      LOG.info("Project Root is:" + urls.get(0).toString());
      addClassPathEntries(new File(projectRoot, "lib"), urls);

      return new DynamicClassLoader(urls.toArray(new URL[0]), parent, null);
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    return null;
  }

  ClassLoader system = ClassLoader.getSystemClassLoader();

  ClassLoader parent = null;

  String dynamicClass = null;

  public DynamicClassLoader(URL[] urls, ClassLoader parent, String classToLoadDynamically) {
    super(urls, parent);
    this.parent = parent;
    this.dynamicClass = classToLoadDynamically;
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    Class<?> clazz = null;

    // (0) Check our previously loaded local class cache
    clazz = findLoadedClass(name);
    if (clazz != null) {
      ServletRegistry.LOG.info("Returning Class:" + name + " from cache");
      if (resolve)
        resolveClass(clazz);
      return (clazz);
    }

    // Next, try loading the class with the system class loader, to prevent
    // the webapp from overriding J2SE classes

    boolean useSystemLoader = true;

    if (dynamicClass != null) {
      if (name.startsWith(dynamicClass)) {
        useSystemLoader = false;
        LOG.info("**********USING DYNAMIC LOAD FOR CLASS:" + name);
      }
    } else if (name.startsWith("org.commoncrawl")) {
      LOG.info("**********USING DYNAMIC LOAD FOR CLASS:" + name);
      useSystemLoader = false;
    }

    if (useSystemLoader) {
      try {
        clazz = system.loadClass(name);
        if (clazz != null) {
          if (resolve)
            resolveClass(clazz);
          ServletRegistry.LOG.info("Loaded Class:" + name + " From System Loader");
          return (clazz);
        }
      } catch (ClassNotFoundException e) {
        // Ignore
      }
    } else {
      // (2) Search local repositories
      try {
        clazz = findClass(name);
        if (clazz != null) {
          ServletRegistry.LOG.info("Loading Class:" + name + " from local repository");
          if (resolve)
            resolveClass(clazz);
          return (clazz);
        }
      } catch (ClassNotFoundException e) {
        ;
      }
    }

    // (3) Delegate to parent unconditionally
    ClassLoader loader = parent;
    if (loader == null)
      loader = system;
    try {
      clazz = parent.loadClass(name);
      if (clazz != null) {
        ServletRegistry.LOG.info("Loading Class:" + name + "from parent");
        if (resolve)
          resolveClass(clazz);
        return (clazz);
      }
    } catch (ClassNotFoundException e) {
      ;
    }

    ServletRegistry.LOG.info("Class:" + name + " not found");
    throw new ClassNotFoundException(name);

  }

}