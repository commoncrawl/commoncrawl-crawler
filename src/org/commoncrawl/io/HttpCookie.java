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

/**
 * abstract interface defining an http cookie
 * 
 * @author rana
 * 
 */

public interface HttpCookie {
  /**
   * returns the creation date of this cookie
   * 
   * @return date as a long value
   */
  public long CreationDate();

  /**
   * does this cookie have an expire time set
   * 
   * @return returns a boolean (true) if expire time set
   */
  public boolean DoesExpire();

  /**
   * returns the domain name associated with this cookie
   * 
   * @return domain name string
   */
  public String Domain();

  /**
   * returns the expiry date for this cookie
   * 
   * @return expiry date as a long (UTC)
   */
  public long ExpiryDate();

  /**
   * is this an http only cookie
   * 
   * @return returns a boolean (true) if the cookie is an http-only cookie
   */
  public boolean IsHttpOnly();

  /**
   * is this a persistent cookie
   * 
   * @return returns a boolean (true) if this cookie is persistent
   */
  public boolean IsPersistent();

  /**
   * is this cookie secure
   * 
   * @return returns a boolean (true) if the cookie is secure
   */
  public boolean IsSecure();

  /**
   * returns the last access date for this cookie
   * 
   * @return date as a long value
   */
  public long LastAccessDate();

  /**
   * returns the name associated with this cookie
   * 
   * @return name as string
   */
  public String Name();

  /**
   * returns the path associated with this cookie
   * 
   * @return path as string
   */
  public String Path();

  /**
   * returns the value associated with this cookie
   * 
   * @return value as string
   */
  public String Value();
}
