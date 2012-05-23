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

import org.apache.hadoop.io.MD5Hash;

/**
 * 
 * @author rana
 *
 */
public class MD5Signature {

  public static FlexBuffer calculate(FlexBuffer content) {
    FlexBuffer signature = new FlexBuffer();

    if (content.getCount() != 0) {
      signature.set(MD5Hash.digest(content.get(), content.getOffset(),
          content.getCount()).getDigest());
    }
    return signature;
  }
}
