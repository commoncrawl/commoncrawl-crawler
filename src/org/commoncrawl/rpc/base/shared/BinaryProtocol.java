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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.commoncrawl.util.FlexBuffer;
import org.commoncrawl.util.TextBytes;

/**
 * 
 * @author rana
 *
 */
public final class BinaryProtocol {

  public static final int RAW_PROTOCOL_VERSION = 1;

  public static final int FIELD_ID_ENCODING_MODE_UNKNOWN = 0;
  public static final int FIELD_ID_ENCODING_MODE_SHORT = 1;
  public static final int FIELD_ID_ENCODING_MODE_VINT = 2;

  public static int DEFAULT_PROTOCOL_ENCODING_MODE = FIELD_ID_ENCODING_MODE_VINT;

  private int _currentMode;
  private static final int DEFAULT_MODE_STACK_CAPACITY = 20;
  int _currentModeIdx = -1;
  int _modeStack[] = new int[DEFAULT_MODE_STACK_CAPACITY];
  int _nestingLevel = 0;
  boolean _skipping = false;

  public BinaryProtocol() {
    pushFieldIdEncodingMode(FIELD_ID_ENCODING_MODE_UNKNOWN);
  }

  // @Override
  public void beginField(DataOutput out, String fieldName, int fieldId) throws IOException {
    if (_currentMode == FIELD_ID_ENCODING_MODE_UNKNOWN) {
      throw new IOException("Unknown Field Id Encoding Mode!");
    }
    if (_currentMode == FIELD_ID_ENCODING_MODE_SHORT)
      out.writeShort(fieldId);
    else
      WritableUtils.writeVInt(out, fieldId);
  }

  // start encoding fields
  public void beginFields(DataOutput out) throws IOException {
    // use default encoding type...
    pushFieldIdEncodingMode(DEFAULT_PROTOCOL_ENCODING_MODE);

    // if nesting level == 0 && encoding mode is new encoding mode
    if (_nestingLevel++ == 0 && _currentMode == FIELD_ID_ENCODING_MODE_VINT) {
      // write out protocol version
      out.writeByte(RAW_PROTOCOL_VERSION);
    }
  }

  // @Override
  public void endFields(DataOutput out) throws IOException {
    if (_currentMode == FIELD_ID_ENCODING_MODE_UNKNOWN) {
      throw new IOException("Unknown Field Id Encoding Mode!");
    }
    if (_currentMode == FIELD_ID_ENCODING_MODE_SHORT)
      out.writeShort(-1);
    else
      WritableUtils.writeVInt(out, -1);

    // ok pop encoding mode
    popFieldIdEncodingMode();
    // reduce nesting level
    _nestingLevel--;
  }

  public int getFieldIdEncodingMode() {
    return _currentMode;
  }

  public void initializeSkipStream(DataInputBuffer stream) throws IOException {
    stream.mark(Integer.MAX_VALUE);
    byte firstByte = stream.readByte();
    if (firstByte == 0 || firstByte == -1) {
      // System.out.println("Protocol: Shifted to SHORT MODE");
      _currentMode = FIELD_ID_ENCODING_MODE_SHORT;
      // reset to head ... no version byte
      stream.reset();
    } else {
      // System.out.println("Protocol: Shifted to VINT MODE");
      _currentMode = FIELD_ID_ENCODING_MODE_VINT;
      stream.mark(Integer.MAX_VALUE);
    }
    _skipping = true;
  }

  public int popFieldIdEncodingMode() {
    _currentMode = _modeStack[--_currentModeIdx];
    return _currentMode;
  }

  public void pushFieldIdEncodingMode(int mode) {
    if (_currentModeIdx + 1 == _modeStack.length) {
      int temp[] = new int[_modeStack.length * 2];
      System.arraycopy(_modeStack, 0, temp, 0, _currentModeIdx + 1);
      _modeStack = temp;
    }
    _modeStack[++_currentModeIdx] = mode;
    _currentMode = mode;
  }

  // @Override
  public boolean readBool(DataInput in) throws IOException {
    return in.readBoolean();
  }

  // @Override
  public byte readByte(DataInput in) throws IOException {
    return in.readByte();
  }

  // @Override
  public double readDouble(DataInput in) throws IOException {
    return in.readDouble();
  }

  // @Override
  public int readFieldId(DataInput in) throws IOException {

    int fieldIdOut = -1;

    // read first byte no matter what
    byte firstByte = in.readByte();
    // if mode is not VINT MODE
    if (_currentMode != FIELD_ID_ENCODING_MODE_VINT) {
      // ok if first byte is zero, then this is the old short encoding style ..
      if (_currentMode == FIELD_ID_ENCODING_MODE_SHORT || (firstByte == 0 || firstByte == -1)) {
        if (_currentMode == FIELD_ID_ENCODING_MODE_UNKNOWN) {
          // System.out.println("Protocol: Shifted to SHORT MODE");
          // increment nesting level ...
          _nestingLevel++;
          // set mode
          pushFieldIdEncodingMode(FIELD_ID_ENCODING_MODE_SHORT);
        }
        // return second byte
        fieldIdOut = (((firstByte << 8)) | (in.readByte() & 0xff));
      } else if (_currentMode == FIELD_ID_ENCODING_MODE_UNKNOWN) {

        if (_nestingLevel++ == 0) {
          // System.out.println("Protocol: Skipping Version Byte");
          // skip version byte
          firstByte = in.readByte();
        }
        // System.out.println("Protocol: Shifted to VINT MODE");
        // shift to vint encoding mode ...
        pushFieldIdEncodingMode(FIELD_ID_ENCODING_MODE_VINT);
      }
    }

    if (_currentMode == FIELD_ID_ENCODING_MODE_VINT) {
      // ok a little messier ...
      int len = WritableUtils.decodeVIntSize(firstByte);
      if (len == 1) {
        fieldIdOut = firstByte;
      } else {
        long i = 0;
        for (int idx = 0; idx < len - 1; idx++) {
          byte b = in.readByte();
          i = i << 8;
          i = i | (b & 0xFF);
        }
        fieldIdOut = (int) (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
      }
    }

    if (fieldIdOut == -1 && !_skipping) {
      // ok pop encoding mode off stack
      popFieldIdEncodingMode();
      // reduce nesting level ...
      _nestingLevel--;

      // System.out.println("Protocol: POPED EncodingMode. NestLevel:" +
      // _nestingLevel);
    }
    return fieldIdOut;
  }

  // @Override
  public FlexBuffer readFlexBuffer(DataInput in) throws IOException {
    FlexBuffer buffer = null;

    int length = in.readInt();
    if (length != 0) {
      byte[] data = new byte[length];
      in.readFully(data);
      buffer = new FlexBuffer(data, 0, data.length);
    } else {
      buffer = new FlexBuffer();
    }
    return buffer;
  }

  // @Override
  public float readFloat(DataInput in) throws IOException {
    return in.readFloat();
  }

  // @Override
  public int readInt(DataInput in) throws IOException {
    return in.readInt();
  }

  // @Override
  public long readLong(DataInput in) throws IOException {
    return in.readLong();
  }

  // @Override
  public void readTextBytes(DataInput in, TextBytes stringBytes) throws IOException {
    if (_currentMode == FIELD_ID_ENCODING_MODE_SHORT) {
      // old encoding style
      int utfLen = in.readUnsignedShort();
      // ok set count in underlying object
      stringBytes.setLength(utfLen);
      // now if length != 0
      if (utfLen != 0) {
        // access the underlying buffer object
        FlexBuffer buffer = stringBytes.getBuffer();
        // make sure copy on write status
        buffer.copyOnWrite();
        // ok read in buytes
        in.readFully(buffer.get(), buffer.getOffset(), utfLen);
      }
    }
    // ok read in new way
    else {
      stringBytes.readFields(in);
    }
  }

  public int readVInt(DataInput in) throws IOException {
    return WritableUtils.readVInt(in);
  }

  public long readVLong(DataInput in) throws IOException {
    return WritableUtils.readVLong(in);
  }

  public final void skipBool(DataInput in) throws IOException {
    in.skipBytes(1);
  }

  public void skipByte(DataInput in) throws IOException {
    in.skipBytes(1);
  }

  public void skipDouble(DataInput in) throws IOException {
    in.skipBytes(8);
  }

  public void skipFlexBuffer(DataInput in) throws IOException {
    int length = in.readInt();
    if (length != 0)
      in.skipBytes(length);
  }

  public void skipFloat(DataInput in) throws IOException {
    in.skipBytes(4);
  }

  public void skipInt(DataInput in) throws IOException {
    in.skipBytes(4);
  }

  public void skipLong(DataInput in) throws IOException {
    if (in.skipBytes(8) != 8)
      throw new IOException("Seek Failed!");
  }

  public void skipTextBytes(DataInput in) throws IOException {
    int utflen = 0;
    if (_currentMode == FIELD_ID_ENCODING_MODE_SHORT) {
      utflen = in.readUnsignedShort();
    } else {
      utflen = WritableUtils.readVInt(in);
    }
    if (utflen != 0)
      in.skipBytes(utflen);
  }

  public void skipVInt(DataInput in) throws IOException {
    WritableUtils.readVInt(in);
  }

  public void skipVLong(DataInput in) throws IOException {
    WritableUtils.readVLong(in);
  }

  // @Override
  public void writeBool(DataOutput out, boolean b) throws IOException {
    out.writeBoolean(b);
  }

  // @Override
  public void writeByte(DataOutput out, byte b) throws IOException {
    out.writeByte(b);
  }

  // @Override
  public void writeDouble(DataOutput out, double d) throws IOException {
    out.writeDouble(d);
  }

  // @Override
  public void writeFlexBuffer(DataOutput out, FlexBuffer buf) throws IOException {
    out.writeInt(buf.getCount());
    if (buf.getCount() != 0) {
      out.write(buf.get(), buf.getOffset(), buf.getCount());
    }
  }

  // @Override
  public void writeFloat(DataOutput out, float f) throws IOException {
    out.writeFloat(f);
  }

  // @Override
  public void writeInt(DataOutput out, int i) throws IOException {
    out.writeInt(i);
  }

  // @Override
  public void writeLong(DataOutput out, long l) throws IOException {
    out.writeLong(l);
  }

  // @Override
  public void writeTextBytes(DataOutput out, TextBytes s) throws IOException {
    if (_currentMode == FIELD_ID_ENCODING_MODE_SHORT) {
      if (s.getLength() > 65535) {
        throw new IOException("String Length Exceeds Max Encoding Length of 65535");
      } else {
        // encode the old fashioned way ...
        out.writeShort(s.getLength());
        if (s.getLength() != 0) {
          // and the bytes
          out.write(s.getBytes(), s.getOffset(), s.getLength());
        }
      }
    } else {
      // do it the new way
      s.write(out);
    }
  }

  public void writeVInt(DataOutput out, int i) throws IOException {
    WritableUtils.writeVInt(out, i);
  }

  public void writeVLong(DataOutput out, long l) throws IOException {
    WritableUtils.writeVLong(out, l);
  }
}
