package org.commoncrawl.io;

/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Sun designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Sun in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 */

/*
 * @(#)NIOStreamEncoder.java 1.24 07/05/05
 */
/*Derrived FROM Decoder in Java Source */

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;

public class NIOStreamEncoder extends Writer {

  private volatile boolean isOpen = true;

  private Charset cs;

  // -- Public methods corresponding to those in OutputStreamWriter --

  // All synchronization and state/argument checking is done in these public
  // methods; the concrete stream-encoder subclasses defined below need not
  // do any such checking.

  private CharsetEncoder encoder;

  private ByteBuffer bb;

  // Exactly one of these is non-null
  private NIOBufferList ch;

  // Leftover first char in a surrogate pair
  private boolean haveLeftoverChar = false;

  private char leftoverChar;

  private CharBuffer lcb = null;

  public NIOStreamEncoder(NIOBufferList ch, CharsetEncoder enc) throws IOException {
    this.ch = ch;
    this.cs = enc.charset();
    this.encoder = enc;
    this.bb = ch.allocateBuffer();
  }

  @Override
  public void close() throws IOException {
    synchronized (lock) {
      if (!isOpen)
        return;
      implClose();
      isOpen = false;
    }
  }

  // -- Charset-based stream encoder impl --

  String encodingName() {
    return cs.name();
  }

  private void ensureOpen() throws IOException {
    if (!isOpen)
      throw new IOException("Stream closed");
  }

  @Override
  public void flush() throws IOException {
    synchronized (lock) {
      ensureOpen();
      implFlush();
    }
  }

  public void flushBuffer() throws IOException {
    synchronized (lock) {
      if (isOpen())
        implFlushBuffer();
      else
        throw new IOException("Stream closed");
    }
  }

  private void flushLeftoverChar(CharBuffer cb, boolean endOfInput) throws IOException {
    if (!haveLeftoverChar && !endOfInput)
      return;
    if (lcb == null)
      lcb = CharBuffer.allocate(2);
    else
      lcb.clear();
    if (haveLeftoverChar)
      lcb.put(leftoverChar);
    if ((cb != null) && cb.hasRemaining())
      lcb.put(cb.get());
    lcb.flip();
    while (lcb.hasRemaining() || endOfInput) {
      CoderResult cr = encoder.encode(lcb, bb, endOfInput);
      if (cr.isUnderflow()) {
        if (lcb.hasRemaining()) {
          leftoverChar = lcb.get();
          if (cb != null && cb.hasRemaining())
            flushLeftoverChar(cb, endOfInput);
          return;
        }
        break;
      }
      if (cr.isOverflow()) {
        assert bb.position() > 0;
        writeBytes();
        continue;
      }
      cr.throwException();
    }
    haveLeftoverChar = false;
  }

  public String getEncoding() {
    if (isOpen())
      return encodingName();
    return null;
  }

  void implClose() throws IOException {
    flushLeftoverChar(null, true);
    try {
      for (;;) {
        CoderResult cr = encoder.flush(bb);
        if (cr.isUnderflow())
          break;
        if (cr.isOverflow()) {
          assert bb.position() > 0;
          writeBytes();
          continue;
        }
        cr.throwException();
      }

      if (bb.position() > 0)
        writeBytes();
    } catch (IOException x) {
      encoder.reset();
      throw x;
    }
  }

  void implFlush() throws IOException {
    implFlushBuffer();
  }

  void implFlushBuffer() throws IOException {
    if (bb.position() > 0)
      writeBytes();
  }

  void implWrite(char cbuf[], int off, int len) throws IOException {
    CharBuffer cb = CharBuffer.wrap(cbuf, off, len);

    if (haveLeftoverChar)
      flushLeftoverChar(cb, false);

    while (cb.hasRemaining()) {
      CoderResult cr = encoder.encode(cb, bb, false);
      if (cr.isUnderflow()) {
        assert (cb.remaining() <= 1) : cb.remaining();
        if (cb.remaining() == 1) {
          haveLeftoverChar = true;
          leftoverChar = cb.get();
        }
        break;
      }
      if (cr.isOverflow()) {
        assert bb.position() > 0;
        writeBytes();
        continue;
      }
      cr.throwException();
    }
  }

  private boolean isOpen() {
    return isOpen;
  }

  @Override
  public void write(char cbuf[], int off, int len) throws IOException {
    synchronized (lock) {
      ensureOpen();
      if ((off < 0) || (off > cbuf.length) || (len < 0) || ((off + len) > cbuf.length) || ((off + len) < 0)) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return;
      }
      implWrite(cbuf, off, len);
    }
  }

  @Override
  public void write(int c) throws IOException {
    char cbuf[] = new char[1];
    cbuf[0] = (char) c;
    write(cbuf, 0, 1);
  }

  @Override
  public void write(String str, int off, int len) throws IOException {
    /* Check the len before creating a char buffer */
    if (len < 0)
      throw new IndexOutOfBoundsException();
    char cbuf[] = new char[len];
    str.getChars(off, off + len, cbuf, 0);
    write(cbuf, 0, len);
  }

  private void writeBytes() throws IOException {

    // bb.flip();
    int lim = bb.limit();
    int pos = bb.position();
    assert (pos <= lim);
    int rem = (pos <= lim ? lim - pos : 0);

    if (rem > 0) {
      ch.write(bb);
      // allocate a new buffer since we surrundered existing buffer to NIOBuffer
      bb = ch.allocateBuffer();
    } else {
      // clear existing buffer ...
      bb.clear();
    }
  }
}