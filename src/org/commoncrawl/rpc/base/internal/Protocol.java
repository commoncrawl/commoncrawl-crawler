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

package org.commoncrawl.rpc.base.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.TextBytes;

/**
 * 
 * @author rana
 *
 */
public interface Protocol {

  /**
   * Write a fields header
   * 
   * @param out
   *          Output stream
   * @param fieldCount
   *          number of fields being serialized
   * @throws IOException
   *           Indicates error in serialization
   */
  // public void beginFields(DataOutput out) throws IOException;

  /**
   * Write a field header
   * 
   * @param b
   *          Byte to be serialized
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @throws IOException
   *           Indicates error in serialization
   */
  public void beginField(DataOutput out, String fieldName, int fildId) throws IOException;

  /**
   * Write a fields footer
   * 
   * @param out
   *          Output stream
   */
  public void endFields(DataOutput out) throws IOException;

  /**
   * Write a field footer (for a previously written field header)
   * 
   * @param b
   *          Byte to be serialized
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @throws IOException
   *           Indicates error in serialization
   */
  // public void endField(DataOutput out,String fieldName,int fildId) throws
  // IOException;

  /**
   * Read a boolean from serialized record.
   * 
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  boolean readBool(DataInput in) throws IOException;

  /**
   * Read a byte from serialized record.
   * 
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  byte readByte(DataInput in) throws IOException;

  /**
   * Read a double-precision number from serialized record.
   * 
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  double readDouble(DataInput in) throws IOException;

  /**
   * Read a field header
   * 
   * @param in
   *          Input Stream
   * @returns the field id of the current field
   * @throws IOException
   *           Indicates error in serialization
   */
  public int readFieldId(DataInput in) throws IOException;

  /**
   * Read byte array from serialized record.
   * 
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  FlexBuffer readFlexBuffer(DataInput in) throws IOException;

  /**
   * Read a single-precision float from serialized record.
   * 
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  float readFloat(DataInput in) throws IOException;

  /**
   * Read an integer from serialized record.
   * 
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  int readInt(DataInput in) throws IOException;

  /**
   * Read a long integer from serialized record.
   * 
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  long readLong(DataInput in) throws IOException;

  /**
   * Read a UTF-8 encoded string from serialized record.
   * 
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  String readString(DataInput in) throws IOException;

  /**
   * Read a UTF-8 encoded string encoded via TextBytes.
   * 
   * @param DataInputStream
   * @return TextBytes object
   */
  TextBytes readTextBytes(DataInput in) throws IOException;

  /**
   * Read a variable bit rate encoded integer from serialized record.
   * 
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  int readVInt(DataInput in) throws IOException;

  /**
   * Read a variable bit rate encoded long integer from serialized record.
   * 
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  long readVLong(DataInput in) throws IOException;

  /**
   * Write a boolean to serialized record.
   * 
   * @param b
   *          Boolean to be serialized
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @throws IOException
   *           Indicates error in serialization
   */
  public void writeBool(DataOutput out, boolean b) throws IOException;

  /**
   * Write a byte to serialized record.
   * 
   * @param b
   *          Byte to be serialized
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @throws IOException
   *           Indicates error in serialization
   */
  public void writeByte(DataOutput out, byte b) throws IOException;

  /**
   * Write a double precision floating point number to serialized record.
   * 
   * @param d
   *          Double to be serialized
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @throws IOException
   *           Indicates error in serialization
   */
  public void writeDouble(DataOutput out, double d) throws IOException;

  /**
   * Write a buffer to serialized record.
   * 
   * @param buf
   *          Buffer to be serialized
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @throws IOException
   *           Indicates error in serialization
   */
  public void writeFlexBuffer(DataOutput out, FlexBuffer buf) throws IOException;

  /**
   * Write a single-precision float to serialized record.
   * 
   * @param f
   *          Float to be serialized
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @throws IOException
   *           Indicates error in serialization
   */
  public void writeFloat(DataOutput out, float f) throws IOException;

  /**
   * Write an integer to serialized record.
   * 
   * @param i
   *          Integer to be serialized
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @throws IOException
   *           Indicates error in serialization
   */
  public void writeInt(DataOutput out, int i) throws IOException;

  /**
   * Write a long integer to serialized record.
   * 
   * @param l
   *          Long to be serialized
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @throws IOException
   *           Indicates error in serialization
   */
  public void writeLong(DataOutput out, long l) throws IOException;

  /**
   * Write a unicode string to serialized record.
   * 
   * @param s
   *          String to be serialized
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @throws IOException
   *           Indicates error in serialization
   */
  public void writeString(DataOutput out, String s) throws IOException;

  /**
   * Write a TextBytes record to stream
   */
  public void writeTextBytes(DataOutput out, TextBytes text) throws IOException;

  /**
   * Write an variable bit rate encoded integer to serialized record.
   * 
   * @param i
   *          Integer to be serialized
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @throws IOException
   *           Indicates error in serialization
   */
  public void writeVInt(DataOutput out, int i) throws IOException;

  /**
   * Write a variable bit rate encoded long integer to serialized record.
   * 
   * @param l
   *          Long to be serialized
   * @param tag
   *          Used by tagged serialization formats (such as XML)
   * @throws IOException
   *           Indicates error in serialization
   */
  public void writeVLong(DataOutput out, long l) throws IOException;

}
