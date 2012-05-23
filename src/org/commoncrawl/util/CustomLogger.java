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

package org.commoncrawl.util;

import java.util.Enumeration;

import org.apache.log4j.Appender;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.HierarchyEventListener;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.log4j.spi.LoggerRepository;

/**
 * Hacked implementation of CustomLogger using log4j framework
 * 
 * @author rana
 * 
 */
public class CustomLogger extends Category {

  public CustomLogger(String name) {

    super(name);

    repository = new LoggerRepository() {

      public void addHierarchyEventListener(HierarchyEventListener listener) {
      }

      public void emitNoAppenderWarning(Category cat) {
      }

      public Logger exists(String name) {
        return null;
      }

      public void fireAddAppenderEvent(Category logger, Appender appender) {
      }

      public Enumeration getCurrentCategories() {
        return null;
      }

      public Enumeration getCurrentLoggers() {
        return null;
      }

      public Logger getLogger(String name) {
        return null;
      }

      public Logger getLogger(String name, LoggerFactory factory) {
        return null;
      }

      public Logger getRootLogger() {
        return null;
      }

      public Level getThreshold() {
        return null;
      }

      public boolean isDisabled(int level) {
        return false;
      }

      public void resetConfiguration() {
      }

      public void setThreshold(Level level) {
      }

      public void setThreshold(String val) {
      }

      public void shutdown() {
      }
    };

    this.setLevel(Level.ALL);
  }

}
