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
import java.nio.charset.Charset;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.ImmutableMultimap;

public class TLDNamesCollection {

  private static final Log LOG = LogFactory.getLog(TLDNamesCollection.class);
	
	public static Collection<String> getSecondaryNames(String tldName) { 
		initialize();
		return tldToSecondaryNameMap.get(tldName);
	}
	
	private static ImmutableMultimap<String, String> tldToSecondaryNameMap = null;
	private static void initialize() { 
		synchronized (TLDNamesCollection.class) {
	    if (tldToSecondaryNameMap == null) { 

	    	try {
	    		
	    		ImmutableMultimap.Builder<String, String> builder = new ImmutableMultimap.Builder<String,String>();
	    		
	    			    		
	  		  InputStream inputStream = TLDNamesCollection.class.getResourceAsStream("/conf/effective_tld_list.txt");
	  		  
	  		  try { 
	  		  
	  		  	BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("UTF-8")));
	  	  	
	  		  	String line = null;
	  		  	
	  		  			  	
	  		  	while ( (line = reader.readLine()) != null) { 
	  		  		if (!line.startsWith("//")) { 
	  		  			if (line.length() != 0) { 
	  			  			int indexOfDot = line.lastIndexOf(".");
	  			  			if (indexOfDot == -1) { 
	  			  				builder.put(line.trim(),"");
	  			  			}
	  			  			else { 
	  			  				String leftSide = line.substring(0,indexOfDot).trim();
	  			  				String rightSide = line.substring(indexOfDot + 1).trim();
	  			  				builder.put(rightSide,leftSide);
	  			  			}
	  		  			}
	  		  		}
	  		  	}
	  		  	
	  		  	tldToSecondaryNameMap = builder.build();
	  		  }
	  		  finally { 
	  		  	inputStream.close();
	  		  }
	  	  }
	  	  catch (IOException e) { 
	  	  	LOG.error(CCStringUtils.stringifyException(e));
	  	  	throw new RuntimeException(e);
	  	  }
			}
		}
	}
}
