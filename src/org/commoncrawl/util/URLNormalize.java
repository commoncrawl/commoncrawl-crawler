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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

/** 
 * helper routines dealing with url normalization 
 *
 * @author rana
 */
public class URLNormalize {

  public static String wwwNormalizeHost(String url) { 
    if (url.startsWith("www.") || url.startsWith("WWW.")) { 
      return url.substring(4);
    }
    return url;
  }
  
  public static String wwwNormalize(String url) { 
    return tweakURL(true, url);
  }
  
  public static String wwwDeNormalize(String url) { 
    return tweakURL(false, url);
  }
  
  public static boolean isWWWNormalized(String url) { 
    return url.startsWith("+");
  }
  
  public static String stripNormalizationMetadata(String url) { 
    if (isWWWNormalized(url)) { 
      return url.substring(1);
    }
    return url;
  }
  
  private static String  tweakURL(boolean normalize,String url) {
    
    boolean modify = false;
    
    if (!normalize && url.startsWith("+")) { 
      modify = true;
      url = url.substring(1);
    }
    
    if (modify || normalize) { 
      GoogleURL urlObject = new GoogleURL(url);
      
      if (urlObject.isValid()) { 
        
        if (normalize && (urlObject.getHost().startsWith("www.") || urlObject.getHost().startsWith("WWW."))) {
          modify = true;
        }

        if (modify) { 
          
          StringBuilder urlOut = new StringBuilder();
          if (normalize) { 
            urlOut.append("+");
          }
          urlOut.append(urlObject.getScheme());
          urlOut.append("://");
          if (urlObject.getUserName() != GoogleURL.emptyString) { 
            urlOut.append(urlObject.getUserName());
            if (urlObject.getPassword() != GoogleURL.emptyString) { 
              urlOut.append(":");
              urlOut.append(urlObject.getPassword());
            }
            urlOut.append("@");
          }
          if (normalize) { 
            urlOut.append(urlObject.getHost().substring(4));
          }
          else { 
            urlOut.append("www.");
            urlOut.append(urlObject.getHost());
          }
          if (urlObject.getPort() != GoogleURL.emptyString) { 
            urlOut.append(":");
            urlOut.append(urlObject.getPort());
          }
          if (urlObject.getPath() != GoogleURL.emptyString) { 
            urlOut.append(urlObject.getPath());
          }
          if (urlObject.getQuery() != GoogleURL.emptyString) { 
            urlOut.append("?");
            urlOut.append(urlObject.getQuery());
          }
          if (urlObject.getRef() != GoogleURL.emptyString) { 
            urlOut.append("#");
            urlOut.append(urlObject.getRef());
          }
          return urlOut.toString();
        }
      }
    }
    return url;
  }
  
  
  public static class WWWNormalizedURLComparator extends WritableComparator { 
    
    public WWWNormalizedURLComparator() { 
      super(Text.class,true);
    }
    
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      // adjust for the + metadata symbol in source and target urls ...
      if (b1[s1] == '+') { 
        s1 += 1;
        l1 -= 1;
      }
      if (b2[s2] == '+') { 
        s2 += 1;
        l2 -= 1;
      }
      
      return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
    }
  }
  
  @Test
  public void testname() throws Exception {
    String preNormalizedURL = "http:///ahad:password@www.google.com/././../.../foobar http://foobarz.com/foobarz{1}?FOO=10#1282838383838";
    System.out.println("Pre-Normalized URL:"+ preNormalizedURL);
    System.out.println("Normalized URL:"+ wwwNormalize(preNormalizedURL));
    System.out.println("Stripping Normalized Metadata for Normalized URL:"+ stripNormalizationMetadata(wwwNormalize(preNormalizedURL)));
    System.out.println("DeNormalized URL:"+ wwwDeNormalize(wwwNormalize(preNormalizedURL)));
    
  }
}
