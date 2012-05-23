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

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * IPV4 Address Helpers
 * 
 * @author rana
 * 
 */
public class IPAddressUtils {

  private final static int INADDR4SZ = 4;

  /*
   * Converts IPv4 address in its textual presentation form into its numeric
   * binary form.
   * 
   * @param src a String representing an IPv4 address in standard format
   * 
   * @return a byte array representing the IPv4 numeric address
   */
  public static byte[] textToNumericFormatV4(String src) {
    if (src.length() == 0) {
      return null;
    }

    byte[] res = new byte[INADDR4SZ];
    String[] s = src.split("\\.", -1);
    long val;
    try {
      switch (s.length) {
        case 1:
          /*
           * When only one part is given, the value is stored directly in the
           * network address without any byte rearrangement.
           */

          val = Long.parseLong(s[0]);
          if (val < 0 || val > 0xffffffffL)
            return null;
          res[0] = (byte) ((val >> 24) & 0xff);
          res[1] = (byte) (((val & 0xffffff) >> 16) & 0xff);
          res[2] = (byte) (((val & 0xffff) >> 8) & 0xff);
          res[3] = (byte) (val & 0xff);
          break;
        case 2:
          /*
           * When a two part address is supplied, the last part is interpreted
           * as a 24-bit quantity and placed in the right most three bytes of
           * the network address. This makes the two part address format
           * convenient for specifying Class A network addresses as net.host.
           */

          val = Integer.parseInt(s[0]);
          if (val < 0 || val > 0xff)
            return null;
          res[0] = (byte) (val & 0xff);
          val = Integer.parseInt(s[1]);
          if (val < 0 || val > 0xffffff)
            return null;
          res[1] = (byte) ((val >> 16) & 0xff);
          res[2] = (byte) (((val & 0xffff) >> 8) & 0xff);
          res[3] = (byte) (val & 0xff);
          break;
        case 3:
          /*
           * When a three part address is specified, the last part is
           * interpreted as a 16-bit quantity and placed in the right most two
           * bytes of the network address. This makes the three part address
           * format convenient for specifying Class B net- work addresses as
           * 128.net.host.
           */
          for (int i = 0; i < 2; i++) {
            val = Integer.parseInt(s[i]);
            if (val < 0 || val > 0xff)
              return null;
            res[i] = (byte) (val & 0xff);
          }
          val = Integer.parseInt(s[2]);
          if (val < 0 || val > 0xffff)
            return null;
          res[2] = (byte) ((val >> 8) & 0xff);
          res[3] = (byte) (val & 0xff);
          break;
        case 4:
          /*
           * When four parts are specified, each is interpreted as a byte of
           * data and assigned, from left to right, to the four bytes of an IPv4
           * address.
           */
          for (int i = 0; i < 4; i++) {
            val = Integer.parseInt(s[i]);
            if (val < 0 || val > 0xff)
              return null;
            res[i] = (byte) (val & 0xff);
          }
          break;
        default:
          return null;
      }
    } catch (NumberFormatException e) {
      return null;
    }
    return res;
  }

  public static int IPV4AddressStrToInteger(String ipAddressStr)
      throws UnknownHostException {
    return IPV4AddressToInteger(textToNumericFormatV4(ipAddressStr));
  }

  public static int IPV4AddressToInteger(byte[] addr) {

    int hostAddress = addr[3] & 0xFF;

    hostAddress |= ((addr[2] << 8) & 0xFF00);
    hostAddress |= ((addr[1] << 16) & 0xFF0000);
    hostAddress |= ((addr[0] << 24) & 0xFF000000);

    return hostAddress;
  }

  public static InetAddress IntegerToInetAddress(int ipAddress)
      throws UnknownHostException {
    byte[] array = new byte[4];

    array[0] = (byte) ((ipAddress >> 24) & 0xFF);
    array[1] = (byte) ((ipAddress >> 16) & 0xFF);
    array[2] = (byte) ((ipAddress >> 8) & 0xFF);
    array[3] = (byte) (ipAddress & 0xFF);

    return InetAddress.getByAddress(array);
  }

  public static String IntegerToIPAddressString(int ess) {
    String ipAddressOut = "";

    try {
      ipAddressOut = IntegerToInetAddress(ess).getHostAddress();
    } catch (UnknownHostException e) {
      // TODO: LOG THIS ?
    }
    return ipAddressOut;
  }

}
