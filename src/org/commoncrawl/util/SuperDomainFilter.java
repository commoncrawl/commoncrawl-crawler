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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.io.NIODNSCache;

/**
 * 
 * @author rana
 *
 */
public class SuperDomainFilter {
  
  public static final Log LOG = LogFactory.getLog(SuperDomainFilter.class);
  
  private NIODNSCache _cache = new NIODNSCache();
  
  public SuperDomainFilter(InputStream inputStream)throws IOException { 
    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
    
    String line = null;
    
    while ((line = bufferedReader.readLine()) != null) { 
      if (line.length() != 0) { 
        if (line.charAt(0) == '.')
          line = line.substring(1);
        _cache.cacheIPAddressForHost(line,1,Long.MAX_VALUE,null).markAsSuperNode();
      }
    }
  }
  
  public String getSuperDomainName(String fullQualifiedDomainName) { 
    NIODNSCache.Node node = _cache.findNode(fullQualifiedDomainName);
    
    // if we found a node then this domain, then truncate to the specified super domain name ...
    if (node != null && node.isSuperNode()) { 
      return node.getFullName();
    }
    return fullQualifiedDomainName;
  }
}
