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

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;

import sun.misc.Unsafe;

/**
 * Utility function that operate on a java 
 * bytearray 
 * @author rana
 *
 */
@SuppressWarnings("restriction")
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
  
  
  static final Unsafe theUnsafe;

  /** The offset to the first element in a byte array. */
  static final int BYTE_ARRAY_BASE_OFFSET;

  static {
    theUnsafe = (Unsafe) AccessController.doPrivileged(
        new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            try {
              Field f = Unsafe.class.getDeclaredField("theUnsafe");
              f.setAccessible(true);
              return f.get(null);
            } catch (NoSuchFieldException e) {
              // It doesn't matter what we throw;
              // it's swallowed in getBestComparer().
              throw new Error();
            } catch (IllegalAccessException e) {
              throw new Error();
            }
          }
        });

    BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);

    // sanity check - this should never fail
    if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
      throw new AssertionError();
    }
  }

  static final boolean littleEndian =
    ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

  
  /**
   * Returns true if x1 is less than x2, when both values are treated as
   * unsigned.
   */
  static boolean lessThanUnsigned(long x1, long x2) {
    return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
  }

  /**
   * Lexicographically compare two arrays.
   *
   * @param buffer1 left operand
   * @param buffer2 right operand
   * @param offset1 Where to start comparing in the left buffer
   * @param offset2 Where to start comparing in the right buffer
   * @param length1 How much to compare from the left buffer
   * @param length2 How much to compare from the right buffer
   * @return 0 if equal, < 0 if left is less than right, etc.
   */
  public static int compareBytes(byte[] buffer1, int offset1, int length1,
      byte[] buffer2, int offset2, int length2) {
    // Short circuit equal case
    if (buffer1 == buffer2 &&
        offset1 == offset2 &&
        length1 == length2) {
      return 0;
    }
    int minLength = Math.min(length1, length2);
    int minWords = minLength / Longs.BYTES;
    int offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
    int offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
     * time is no slower than comparing 4 bytes at a time even on 32-bit.
     * On the other hand, it is substantially faster on 64-bit.
     */
    for (int i = 0; i < minWords * Longs.BYTES; i += Longs.BYTES) {
      long lw = theUnsafe.getLong(buffer1, offset1Adj + (long) i);
      long rw = theUnsafe.getLong(buffer2, offset2Adj + (long) i);
      long diff = lw ^ rw;

      if (diff != 0) {
        if (!littleEndian) {
          return lessThanUnsigned(lw, rw) ? -1 : 1;
        }

        // Use binary search
        int n = 0;
        int y;
        int x = (int) diff;
        if (x == 0) {
          x = (int) (diff >>> 32);
          n = 32;
        }

        y = x << 16;
        if (y == 0) {
          n += 16;
        } else {
          x = y;
        }

        y = x << 8;
        if (y == 0) {
          n += 8;
        }
        return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
      }
    }

    // The epilogue to cover the last (minLength % 8) elements.
    for (int i = minWords * Longs.BYTES; i < minLength; i++) {
      int result = UnsignedBytes.compare(
          buffer1[offset1 + i],
          buffer2[offset2 + i]);
      if (result != 0) {
        return result;
      }
    }
    return length1 - length2;
  }
  
}
