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
public class RiceCoding {

  private int size; // number of items stored

  private int nbits;  // number of bits to store the items

  private int m;    // parameter to this encoding

  private byte[] bits;  // the actual bits

  // add a bit to the encoding
  public void addbit(int b) {

    int len = bits.length;
    // need to grow the bit list
    if (nbits == len * 8) {
      int newlen = (int)(len * 1.5) + 1;
      byte tmp[] = new byte[newlen];
      System.arraycopy(bits, 0, tmp, 0, bits.length);
      bits = tmp;
    }
    if (b == 1)
      bits[(nbits >> 3)] |= (1 << (nbits & 0x7));
    nbits++;
  }



  // get the value of the n-th bit
  public int getbit(int n) {
    return (bits[(n >> 3)] >> (n & 0x7)) & 0x1;
  }

  // construct a new encoding object
  public RiceCoding(int mval) {

    bits = new byte[4];
    m = mval;
    if (m < 0 || m > 64)
      throw new RuntimeException("m < 0 || m > 64");
  }
  
  /* construct a rice coding object from previously encoded data 
   * 
   */
  public RiceCoding(int mval,int size,int bitCount,byte[] bits) { 
    this.m = mval;
    this.size = size;
    this.nbits = bitCount;
    this.bits = bits;
  }


  // get the number of items
  public int getSize() {
    return size;
  }

  // get the number of bits
  public int getNumBits() {
    return nbits;
  }
  
  // get the number of bits as bytes 
  public int getNumBitsAsBytes() { 
    return (nbits + 7) / 8;
  }
  
  public int getMValue() { 
    return m;
  }
  
  /** get the underlying storage bits
   *  NOTE: array may have been overallocated. use getNumBits to idenitfy number of valid elements 
   * @return byte array used to store underlying bits  
   */
  public byte[] getBits() { 
    return bits;
  }

  // add an item to the encoding
  public void addItem(long val) {

    if (val < 1)

      throw new IllegalArgumentException("val < 1");

    size++;

    long x = val - 1;

    long q = x >> m;

    long r = x & ((1L << m) - 1L);

    // encode the first (unary) part
    while (q-- > 0)
      addbit(1);

    addbit(0);

    // encode the binary part
    if (m > 0) {

      long mask = (1L << (m - 1));

      while (mask != 0) {
        addbit((r & mask) != 0 ? 1 : 0);

        mask >>= 1;
      }
    }
  }



  // get the items in this encoding and return them in a vector
  public long[] getItems() {

    long items[] = new long[size];

    int currbit = 0;

    for (int i = 0; i < size; i++) {

      long unary = 0;

      while (getbit(currbit) != 0) {
        unary++;
        currbit++;
      }

      currbit++;
      long binary = 0;
      for (int j = 1; j <= m; j++)
        binary = (binary << 1) | getbit(currbit++);
      
      items[i] = (unary << m) + binary + 1;

    }
    return items;
  }
  
  public static final class RiceCodeReader { 
    
    private int m;
    private int currbit = 0;
    private int nbits;
    private byte[] bits;
    private int offset;
    
    public RiceCodeReader(int mValue,int totalBits,byte[] array,int offset) { 
      this.m = mValue;
      this.nbits = totalBits;
      this.bits = array;
      this.offset = offset;
      this.currbit = 0;
    }
    
    public boolean hasNext() { 
      return currbit < nbits;
    }

    public int getbit() { 
      return getbit(currbit++);      
    }
    
    // get the value of the n-th bit
    private int getbit(int bitNo) {
      return (bits[offset + (bitNo >> 3)] >> (bitNo & 0x7)) & 0x1;
    }
    
    public long nextValue() { 
      
      long unary = 0;

      while (getbit(currbit) != 0) {
        unary++;
        currbit++;
      }

      currbit++;
      long binary = 0;
      for (int j = 1; j <= m; j++)
        binary = (binary << 1) | getbit(currbit++);      
      
      return (unary << m) + binary + 1;
    }
    
    
  }
  
  public static void main(String[] args) {
    RiceCoding coder = new RiceCoding(4);
    coder.addItem(17);
  }
  
}

