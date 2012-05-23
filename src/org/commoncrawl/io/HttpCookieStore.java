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

import java.net.URL;

/**
 * abstract interface defining the functionality of a cookie store ...
 * 
 * @author rana
 * 
 */
public interface HttpCookieStore {

  /**
   * retrieve http cookies from the cookie store given a url
   * 
   * @param url
   *          the url idenitifying the domain/path for which cookies need to be
   *          retrieved
   * @return the http cookie string (if any) for the domain/path
   */
  public String GetCookies(URL url);

  /**
   * set / update cookies in the cookie store given a url
   * 
   * @param urlObject
   *          the url indentifying the domain/path context for the passed on
   *          cookie
   * @param cookie
   *          the set cookie line, as received from the http protocol headers
   * @return (true) if setCookie operation succeeded
   */
  public boolean setCookie(URL urlObject, String cookie);

}
