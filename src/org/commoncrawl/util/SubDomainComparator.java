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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 
 *
 * @author rana
 *
 */
public class SubDomainComparator {

  Pattern blogspotRegEx = Pattern.compile("(.*)([.]blogspot[.]com)");
  Pattern askypRegEx = Pattern.compile("(.*)([.]askyp[.]com)");
  Pattern stumbleuponRegEx = Pattern.compile("(.*)([.]stumbleupon[.]com)");
  Pattern overblogRegEx = Pattern.compile("(.*)([.]overblog[.]com)");
  
  public synchronized boolean isEqual(String superDomain, String subdomain1, String subdomain2) {
    if (superDomain.equals("blogspot.com")) { 
      Matcher subdomain1Matcher = blogspotRegEx.matcher(subdomain1);
      Matcher subdomain2Matcher = blogspotRegEx.matcher(subdomain2);
      
      if (subdomain1Matcher.matches() && subdomain2Matcher.matches()) { 
        String domain1Prefix = subdomain1Matcher.group(1);
        String domain2Prefix = subdomain2Matcher.group(1);
        if (domain1Prefix != null && domain2Prefix != null) {
          if (domain1Prefix.equals("www") || domain1Prefix.equals("members") 
              || domain2Prefix.equals("www") || domain2Prefix.equals("members")) { 
            return false;
          }
          return true;
        }
        
      }
      return false;
    }
    else { 
      Pattern pattern = null;
      
      if (superDomain.equals("askyp.com")) { 
        pattern = askypRegEx;
      }
      else if (superDomain.equals("stumbleupon.com")) { 
        pattern = stumbleuponRegEx;
      }
      else if (superDomain.equals("overblog.com")) { 
        pattern = overblogRegEx;
      }

      if (pattern != null) { 
        if (pattern.matcher(subdomain1).matches() && pattern.matcher(subdomain2).matches()) { 
          return true;
        }
      }
    }
    return false;
  }

}
 