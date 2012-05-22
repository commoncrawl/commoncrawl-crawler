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

package org.commoncrawl.async;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ConcurrentTask - runs inside of an event loop
 * 
 * @author rana
 *
 * @param <V>
 */
public class ConcurrentTask<V> extends FutureTask<V> {

  /** logging **/
  private static final Log      LOG = LogFactory.getLog(ConcurrentTask.class);

  private EventLoop             _eventLoop;
  private CompletionCallback<V> _callback;

  public static interface CompletionCallback<V> {

    void taskComplete(V loadResult);

    void taskFailed(Exception e);
  }

  public ConcurrentTask(EventLoop eventLoop, Callable<V> callable,
      CompletionCallback<V> callback) {
    super(callable);

    _eventLoop = eventLoop;
    _callback = callback;
  }

  protected void done() {

    // schedule an async event to process loaded results ...
    Timer timer = new Timer(1, false, new Timer.Callback() {

      public void timerFired(Timer timer) {

        try {
          _callback.taskComplete(get());
        } catch (InterruptedException e) {
          LOG.error(e);
        } catch (ExecutionException e) {
          LOG.error(e);
          _callback.taskFailed(e);
        } catch (CancellationException e) {
          LOG.error(e);
          _callback.taskFailed(e);
        }
      }
    });
    // schedule the timer in the server's main event loop thread ...
    _eventLoop.setTimer(timer);
  }
}
