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

import java.io.File;


/**
 * 
 * @author rana
 *
 */
public class LongBitSet
{
    //--------------------------------------------------------------------
    private static final long WORD_MASK = 0xffffffffffffffffL;


    public static void main(String[] args)
    {
        LongBitSet bits = new LongBitSet(2500000000L);

        bits.set(2400000000L);
        System.out.println( bits.get(2399999999L) );
        System.out.println( bits.get(2400000000L) );
        System.out.println( bits.get(2400000001L) );
    }



    //--------------------------------------------------------------------
    private final long[] BITS;


    //--------------------------------------------------------------------
    public LongBitSet(long size)
    {
        BITS = new long[ (int)((size - 1) >> 6) + 1 ];
    }
    private LongBitSet(long[] bits)
    {
        BITS = bits;
    }


    //--------------------------------------------------------------------
    public boolean get(long index)
    {
        int i = wordIndex(index);
        return (BITS[i] & (1L << index)) != 0;
    }

    public void set(long index, boolean value)
    {
        if (value) {
            set(index);
        } else {
            clear(index);
        }
    }


    //--------------------------------------------------------------------
    public void set(long index)
    {
        int i = wordIndex(index);
        BITS[i] |= 1L << index;
    }

    public void clear(long index)
    {
        int i = wordIndex(index);
        BITS[i] &= ~(1L << index);
    }


    //--------------------------------------------------------------------
    public long nextSetBit(long fromIndex)
    {
        int  u    = wordIndex(fromIndex);
        long word = BITS[u] & (WORD_MASK << fromIndex);

        while (true) {
            if (word != 0)
                return (((long) u) * Long.SIZE) +
                       Long.numberOfTrailingZeros(word);

            if (++u == BITS.length) return -1;
            word = BITS[u];
        }
    }


    //--------------------------------------------------------------------
    public int length()
    {
        return BITS.length * Long.SIZE;
    }


    //--------------------------------------------------------------------
    private static int wordIndex(long bitIndex)
    {
        return (int)(bitIndex >> 6);
    }


    public static void persist(LongBitSet bits, File into)
    {
        PersistentLongs.persist(bits.BITS, into);
    }

    public static LongBitSet retrieve(File from)
    {
        long[] bits = PersistentLongs.retrieve(from);
        return bits == null
               ? null
               : new LongBitSet(bits);
    }
}