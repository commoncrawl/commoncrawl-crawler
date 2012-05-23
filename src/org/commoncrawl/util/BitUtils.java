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
 * Helpers to write bit level data
 * 
 * @author rana
 * 
 */
public class BitUtils {

  /**
   * a writeable bitstream
   * 
   * @author rana
   * 
   */
  public static class BitStream {

    public int    nbits; // number of bits in the control stream
    public byte[] bits; // the actual bits of the control stream

    public BitStream() {
      nbits = 0; // number of bits in the control stream
      bits = new byte[4]; // the actual bits of the control stream
    }

    public BitStream(byte[] bits, int nbits) {
      this.nbits = nbits;
      this.bits = bits;
    }

    // add a bit to the encoding
    public void addbit(int b) {

      int len = bits.length;
      // need to grow the bit list
      if (nbits == len * 8) {
        int newlen = (int) (len * 1.5) + 1;
        byte tmp[] = new byte[newlen];
        System.arraycopy(bits, 0, tmp, 0, bits.length);
        bits = tmp;
      }
      if (b == 1)
        bits[(nbits >> 3)] |= (1 << (nbits & 0x7));
      nbits++;
    }
  }

  /**
   * a bitstream reader
   * 
   * @author rana
   * 
   */
  public static class BitStreamReader {

    BitStream _bitStream;
    int       _currPos;

    public BitStreamReader(BitStream bitStream) {
      _bitStream = bitStream;
      _currPos = 0;
    }

    public boolean hasNext() {
      return _currPos < _bitStream.nbits;
    }

    // get the value of the next bit in the stream
    public int getbit() {
      int bit = (_bitStream.bits[(_currPos >> 3)] >> (_currPos & 0x7)) & 0x1;
      _currPos++;
      return bit;
    }
  }

}
