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

import java.net.URL;


/**
 * 
 * @author rana
 *
 */
public class URLFingerprint {
  
  public static long generate64BitURLFPrint(URL theURL) { 
    return FPGenerator.std64.fp(theURL.toString());
  }

  public static long generate64BitURLFPrint(String theURL) { 
    return FPGenerator.std64.fp(theURL);
  }
  
  /*
  public static long generate64BitHostFP(URL theURL) { 
    return FPGenerator.std64.fp(theURL.getHost());
  }
  */
  
  public static int generate32BitHostFP(String hostname) { 
    return hostname.hashCode();
  }
}
