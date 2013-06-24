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

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

/**
 * 
 * @author rana
 *
 */
public class SuperDomainList {
  
  private static final Log LOG = LogFactory.getLog(SuperDomainList.class);
  
  public static long domainFingerprintGivenName(String name) { 
    return FPGenerator.std64.fp(name);
  }
  
  public static final Set<Long> loadSuperDomainIdList(Configuration conf,Path superDomainIdFile)throws IOException { 
    
    LOG.info("Loading Super Domain File From:" + superDomainIdFile);
    
    FileSystem fs = FileSystem.get(superDomainIdFile.toUri(),conf);
    
    TreeSet<Long> superDomainSet = new TreeSet<Long>();
    
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, superDomainIdFile, conf);
    try { 
      TextBytes domainName = new TextBytes();
      
      while(reader.next(domainName)) { 
        superDomainSet.add(domainFingerprintGivenName(domainName.toString()));
      }
    }
    finally { 
      reader.close();
    }
    return superDomainSet;
  }
}
