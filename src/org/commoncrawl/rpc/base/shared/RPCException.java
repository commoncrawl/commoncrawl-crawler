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
package org.commoncrawl.rpc.base.shared;

import java.io.IOException;

/**
 * 
 * @author rana
 *
 */
public class RPCException extends IOException {

  /**
   * 
   */
  private static final long serialVersionUID = -2660559646576235804L;

  public RPCException(Exception e) {
    super(e);
  }

  public RPCException(String string) {
    super(string);
  }
}
