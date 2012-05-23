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

/**
 * 
 * @author rana
 *
 */
public class GoogleURLComponent {

  public GoogleURLComponent() { 
    begin =0;
    len = -1;
  }

  // Normal constructor: takes an offset and a length.
  public GoogleURLComponent(int b, int l) { 
    begin = b;
    len = l;
  }

  public int end() {
    return begin + len;
  }

  // Returns true if this component is valid, meaning the length is given. Even
  // valid components may be empty to record the fact that they exist.
  public boolean is_valid() {
    return (len != -1);
  }

  // Returns true if the given component is specified on false, the component
  // is either empty or invalid.
  public boolean is_nonempty() {
    return (len > 0);
  }

  public void reset() {
    begin = 0;
    len = -1;
  }


  public int begin;  // Byte offset in the string of this component.
  public int len;    // Will be -1 if the component is unspecified.

}
