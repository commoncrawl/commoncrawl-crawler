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

package org.commoncrawl.service.dns;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.io.DNSQueryResult;
import org.commoncrawl.io.NIODNSQueryClient;
import org.commoncrawl.io.NIODNSResolver;
import org.commoncrawl.io.NIODNSQueryClient.Status;
import org.commoncrawl.rpc.base.internal.AsyncRequest;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Callback;
import org.commoncrawl.service.dns.DNSQueryInfo;
import org.commoncrawl.service.dns.DNSQueryResponse;
import org.commoncrawl.service.dns.DNSService;
import org.commoncrawl.util.CCStringUtils;
import org.commoncrawl.util.IPAddressUtils;

/** a resolver conforming to the NIODNSResolver Intfc that use the 
 * dns service for dns resolution services 
 * 
 * @author rana 
 */
public class DNSServiceResolver implements NIODNSResolver{

  static final Log LOG = LogFactory.getLog(DNSServiceResolver.class);
  
  private DNSService.AsyncStub _serviceInterface;
  
  public DNSServiceResolver(DNSService.AsyncStub dnsServiceInterface) { 
    _serviceInterface = dnsServiceInterface;
  }
  
  @Override
  public Future<DNSQueryResult> resolve(final NIODNSQueryClient client,final String theHost,boolean noCache,boolean highPriority,final int timeoutValue) throws IOException {
    
    DNSQueryInfo queryInfo = new DNSQueryInfo();
    queryInfo.setHostName(theHost);
    queryInfo.setIsHighPriorityRequest(highPriority);
    // LOG.info("### DNS Sending Hight Priority Request for:" + theHost);
    
    //TODO: PASS TIMEOUT VALUE TO THE DNS SERVER SERVICE 
    
    _serviceInterface.doQuery(queryInfo, new Callback<DNSQueryInfo,DNSQueryResponse>() {

      @Override
      public void requestComplete(AsyncRequest<DNSQueryInfo, DNSQueryResponse> request) {
        
        DNSQueryResult result = new DNSQueryResult(DNSServiceResolver.this,client,theHost);
        
        if (request.getStatus() == AsyncRequest.Status.Success) { 
          if (request.getOutput().getStatus() == DNSQueryResponse.Status.SUCCESS) { 
            result.setStatus(Status.SUCCESS);      
            try {
              result.setAddress(IPAddressUtils.IntegerToInetAddress(request.getOutput().getAddress()));
            } catch (UnknownHostException e) {
              LOG.error(CCStringUtils.stringifyException(e));
            }
            if (request.getOutput().isFieldDirty(DNSQueryResponse.Field_CNAME)) { 
              result.setCName(request.getOutput().getCname());
            }
            result.setTTL(request.getOutput().getTtl());
            
          }
          else { 
            switch (request.getOutput().getStatus()) { 
              case DNSQueryResponse.Status.RESOLVER_FAILURE: result.setStatus(NIODNSQueryClient.Status.RESOLVER_FAILURE);break;
              default:   result.setStatus(NIODNSQueryClient.Status.SERVER_FAILURE);break;
            }
            if (request.getOutput().isFieldDirty(DNSQueryResponse.Field_ERRORDESC)) { 
              result.setErrorDesc(request.getOutput().getErrorDesc());
            }
          }
        }
        else { 
          result.setStatus(NIODNSQueryClient.Status.SERVER_FAILURE);
          result.setErrorDesc("RPC Failure");
          LOG.error("DNS RPC Failure while querying IP for host:" + theHost);
        }
        
        result.fireCallback();
      } 
      
    });
    
    return null;
    
  }

}
