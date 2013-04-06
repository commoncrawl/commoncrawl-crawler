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
package org.commoncrawl.io;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * 
 * @author rana
 *
 */
public interface NIODNSResolver {

  /**
   * queue a DNS resolution request
   * 
   * @param client
   *          - the callback interface
   * @param theHost
   *          - the host name to query for
   * @return
   * @throws IOException
   */
  public Future<NIODNSQueryResult> resolve(NIODNSQueryClient client, String theHost, boolean noCache,
      boolean highPriorityRequest, int timeoutValue) throws IOException;

}
