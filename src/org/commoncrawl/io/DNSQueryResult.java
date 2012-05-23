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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.io.NIODNSQueryClient.Status;

/** 
 * DNSQueryResult - the end result of a DNS Query operation 
 *
 * @author rana
 */
public class DNSQueryResult {

  /** logging **/
  static final Log LOG = LogFactory.getLog(DNSQueryResult.class);

  /** SUCCESS or FAILURE */
  private Status _status = Status.RESOLVER_FAILURE;
  /** error description **/
  private String _errorDesc = "";
  /** the target host name */
  private String _hostName;
  /** the returnrelated ed CNAME */
  private String _cname;
  /** the returned InetAddress */
  private InetAddress _address;
  /** the returned ttl value **/
  private long _ttl;
  /** the associated status callback */
  private NIODNSQueryClient _client;

  NIODNSResolver _source = null;

  /**
   * constructor
   * 
   * @param client
   *          - callback interface into the client
   * @param hostName
   *          - host name associated with this query
   */
  public DNSQueryResult(NIODNSResolver source, NIODNSQueryClient client, String hostName) {
    _source = source;
    _client = client;
    _hostName = hostName;
  }

  public void fireCallback() {
    NIODNSQueryClient client = getConnection();

    if (client != null) {

      if (success()) {
        client.AddressResolutionSuccess(_source, getHostName(), getCName(), getAddress(), getTTL());
      } else {
        client.AddressResolutionFailure(_source, getHostName(), getStatus(), getErrorDescription());
      }
    } else {
      if (getHostName() != null)
        LOG.error("Client no Longer Exists for DNS Resolution Request:" + getHostName());
      else
        LOG.error("Client no Longer Exists for DNS Resolution Request");
    }
  }

  public final InetAddress getAddress() {
    return _address;
  }

  public final String getCName() {
    return _cname;
  }

  public final NIODNSQueryClient getConnection() {
    return _client;
  }

  public String getErrorDescription() {
    if (_errorDesc != null)
      return _errorDesc;
    return "";
  }

  public final String getHostName() {
    return _hostName;
  }

  public final Status getStatus() {
    return _status;
  }

  public final long getTTL() {
    return _ttl;
  }

  public final void setAddress(InetAddress address) {
    _address = address;
  }

  public final void setCName(String cname) {
    _cname = cname;
  }

  public void setErrorDesc(String errorDesc) {
    _errorDesc = errorDesc;
  }

  public final void setStatus(Status theStatus) {
    _status = theStatus;
  }

  public final void setTTL(long ttlValue) {
    _ttl = ttlValue;
  }

  public final boolean success() {
    return _status == Status.SUCCESS;
  }

}