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

package org.commoncrawl.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;

/**
 * 
 * @author rana
 *
 */
public abstract class AsyncWebServerRequest {

  /** logging **/
  private static final Log LOG = LogFactory.getLog(AsyncWebServerRequest.class);

  final String requestName;
  final Writer writer;
  PrintWriter out = null;

  public IOException exceptionOut = null;

  public AsyncWebServerRequest(String requestName) {
    this.requestName = requestName;
    this.out = null;
    this.writer = null;
  }

  public AsyncWebServerRequest(String requestName, Writer incomingWriter) {
    this.requestName = requestName;
    this.writer = incomingWriter;
    if (incomingWriter != null) {
      this.out = new PrintWriter(incomingWriter);
    }
  }

  public void dispatch(EventLoop eventLoop) {

    final Semaphore waitState = new Semaphore(0);

    eventLoop.setTimer(new Timer(0, false, new Timer.Callback() {

      public void timerFired(Timer timer) {
        boolean isClosure = false;
        try {
          isClosure = handleRequest(waitState);
        } catch (IOException exception) {
          exceptionOut = exception;
          if (out != null) {
            (out).print("<pre>WebRequest:" + requestName + " Failed with exception: "
                + StringUtils.stringifyException(exception) + " </pre>");
          }
        } finally {
          if (!isClosure) {
            waitState.release();
          }
        }
      }
    }));

    waitState.acquireUninterruptibly();
  }

  public IOException getException() {
    return exceptionOut;
  }

  public abstract boolean handleRequest(Semaphore completionSemaphore) throws IOException;
}
