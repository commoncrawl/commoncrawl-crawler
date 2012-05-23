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

import org.commoncrawl.util.RuntimeStatsCollector;

/**
 * 
 * @author rana
 *
 */
public final class ServerStats extends RuntimeStatsCollector.Namespace {

  public enum Name {
    CommonServer_ThreadPoolInfo
  }

  public static ServerStats ID = new ServerStats();

  private ServerStats() {
    RuntimeStatsCollector.registerNames(this, Name.values());
  }
}
