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
 * Utility function that operate on a java 
 * bytearray 
 * @author rana
 *
 */
public class ByteArrayUtils {
  /**
   * Search the data byte array for the first occurrence of the byte array
   * pattern.
   */
  public static final int indexOf(byte[] data,int offset,int length, byte[] pattern) {
    int[] failure = computeFailure(pattern);

    int j = 0;

    for (int i = 0; i < length; i++) {
      while (j > 0 && pattern[j] != data[offset + i]) {
        j = failure[j - 1];
      }
      if (pattern[j] == data[offset + i]) {
        j++;
      }
      if (j == pattern.length) {
        return offset + (i - pattern.length + 1);
      }
    }
    return -1;
  }

  /**
   * Computes the failure function using a boot-strapping process, where the
   * pattern is matched against itself.
   */
  private static final int[] computeFailure(byte[] pattern) {
    int[] failure = new int[pattern.length];

    int j = 0;
    for (int i = 1; i < pattern.length; i++) {
      while (j > 0 && pattern[j] != pattern[i]) {
        j = failure[j - 1];
      }
      if (pattern[j] == pattern[i]) {
        j++;
      }
      failure[i] = j;
    }

    return failure;
  }

  
  public static long parseLong(byte[] s,int offset,int length, int radix)
      throws NumberFormatException {
    if (s == null) {
      throw new NumberFormatException("null");
    }

    if (radix < Character.MIN_RADIX) {
      throw new NumberFormatException("radix " + radix
          + " less than Character.MIN_RADIX");
    }
    if (radix > Character.MAX_RADIX) {
      throw new NumberFormatException("radix " + radix
          + " greater than Character.MAX_RADIX");
    }

    long result = 0;
    boolean negative = false;
    int i = 0, len = length;
    long limit = -Long.MAX_VALUE;
    long multmin;
    int digit;

    if (len > 0) {
      char firstChar = (char) s[offset];
      if (firstChar < '0') { // Possible leading "-"
        if (firstChar == '-') {
          negative = true;
          limit = Long.MIN_VALUE;
        } else
          throw new NumberFormatException();

        if (len == 1) // Cannot have lone "-"
          throw new NumberFormatException();
        i++;
      }
      multmin = limit / radix;
      while (i < len) {
        // Accumulating negatively avoids surprises near MAX_VALUE
        digit = Character.digit((char)s[offset + i++], radix);
        if (digit < 0) {
          throw new NumberFormatException();
        }
        if (result < multmin) {
          throw new NumberFormatException();
        }
        result *= radix;
        if (result < limit + digit) {
          throw new NumberFormatException();
        }
        result -= digit;
      }
    } else {
      throw new NumberFormatException();
    }
    return negative ? result : -result;
  }  
}
