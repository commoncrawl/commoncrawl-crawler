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

import java.net.InetAddress;
import java.util.concurrent.FutureTask;

/**
 * NIODNSQueryClient - status interface for NIODNSResolver
 * 
 * @author rana
 * 
 */
public interface NIODNSQueryClient {

  public enum Status {
    SUCCESS, SERVER_FAILURE, RESOLVER_FAILURE
  }

  /** address resolution failed on the specified host */
  void AddressResolutionFailure(NIODNSResolver source, String hostName, Status status, String errorDesc);

  /**
   * address resolution succeeded for the specified host
   * 
   * @param hostName
   *          - the host name associated with the query results
   * @param cName
   *          - the cannonical name of the specified host name (as returned by
   *          DNS)
   * @param address
   *          - the IP address for the specified host
   */
  void AddressResolutionSuccess(NIODNSResolver source, String hostName, String cName, InetAddress address,
      long addressTTL);

  void DNSResultsAvailable();

  /** task competed **/
  void done(NIODNSResolver source, FutureTask<DNSQueryResult> task);

}
