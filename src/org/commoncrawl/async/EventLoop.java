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

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.io.internal.NIODNSLocalResolver;
import org.commoncrawl.io.internal.NIOSocketSelector;
import org.junit.Test;

/**
 * Async Event Loop used to dispatch messages and events
 * 
 * @author rana
 *
 */
public final class EventLoop implements Runnable {

  /** logging **/
  private static final Log          LOG                = LogFactory
                                                           .getLog(EventLoop.class);

  NIODNSLocalResolver               _resolver;
  NIOSocketSelector                 _selector;
  Thread                            _eventThread;
  TimerRegistry                     _timerRegistry     = new TimerRegistry(this);
  boolean                           _shutdown          = false;
  long                              _loopCount         = 0;

  NIOSocketSelector.TimeUsageDetail _selectorTimeUsage = new NIOSocketSelector.TimeUsageDetail();

  public EventLoop() {
    init(Executors.newFixedThreadPool(1));
  }

  public EventLoop(ExecutorService resolverThreadPool) {
    init(resolverThreadPool);
  }

  private void init(ExecutorService resolverThreadPool) {
    try {
      _selector = new NIOSocketSelector(this);
      _resolver = new NIODNSLocalResolver(this, resolverThreadPool, true);

    } catch (IOException e) {
      LOG.fatal("Unable to initialize NIO Selector");
      throw new RuntimeException("Unable to initialize NIO Selector");
    }

  }

  public NIOSocketSelector getSelector() {
    return _selector;
  }

  public NIODNSLocalResolver getResolver() {
    return _resolver;
  }

  public void start() {
    if (_eventThread != null) {
      LOG.fatal("Invalid Call State");
      throw new RuntimeException("Invalid Call State");
    }

    _shutdown = false;
    _eventThread = new Thread(this);
    _eventThread.start();
  }

  public void stop() {
    if (_eventThread == null) {
      throw new RuntimeException("Invalid Call State");
    }
    // set shutdown flag ...
    _shutdown = true;

    // and wakeup the socket selector ...
    try {
      _selector.wakeup();
    } catch (IOException e) {
      LOG.fatal("IOException encountered in Selector.wakeup!");
      throw new RuntimeException(e);
    }

    // if NOT called from event thread itself, wait for event thread to die ...
    if (Thread.currentThread() != _eventThread) {
      try {
        LOG.info("Waiting for Event Thread to DIE TID:"
            + Thread.currentThread().getId());
        _eventThread.join();
        _eventThread = null;
        LOG.info("Event Thread DEAD. Exiting EventLoop TID:"
            + Thread.currentThread().getId());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }

  public void wakeup() {

    if (_selector != null) {
      try {
        _selector.wakeup();
      } catch (IOException e) {
        LOG.fatal("IOException encountered in Selector.wakeup!");
        throw new RuntimeException(e);
      }
    }

  }

  public Thread getEventThread() {
    return _eventThread;
  }

  public boolean waitForIO(long waitTime) throws IOException {

    long timeStart = System.currentTimeMillis();
    long timeEnd = timeStart + waitTime;

    while (!_shutdown) {

      long nextFireTime = _timerRegistry.fireTimers();

      waitTime = timeEnd - System.currentTimeMillis();

      if (waitTime > 0) {
        if (nextFireTime != 0) {
          waitTime = Math.max(1, Math.min(nextFireTime
              - System.currentTimeMillis(), waitTime));
        }

        if (_selector.poll(waitTime, _selectorTimeUsage) != 0) {
          return false;
        }
      } else {
        break;
      }
    }
    return true;
  }

  public void run() {

    try {
      while (!_shutdown) {

        long waitTime = 0;
        long nextFireTime = _timerRegistry.fireTimers();

        if (nextFireTime != 0) {
          waitTime = Math.max(1, nextFireTime - System.currentTimeMillis());
        }

        try {
          // long timeStart = System.currentTimeMillis();
          _resolver.poll();
          // long timeEnd = System.currentTimeMillis();

          // timeStart = System.currentTimeMillis();
          _selector.poll(waitTime, _selectorTimeUsage);
          // timeEnd = System.currentTimeMillis();

          /**
           * if (++_loopCount % 100 == 0) JVMStats.dumpMemoryStats();
           **/
        } catch (IOException e) {
          LOG.error(StringUtils.stringifyException(e));
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
      LOG.fatal("Unhandled Exception in Event Loop:"
          + StringUtils.stringifyException(e));
      System.out.println("Unhandled Exception in Event Loop:"
          + StringUtils.stringifyException(e));
    }
    LOG.info("Event Loop Existing Run Loop");
  }

  public void setTimer(Timer t) {
    _timerRegistry.setTimer(t);
  }

  public void cancelTimer(Timer t) {
    _timerRegistry.cancelTimer(t);
  }

  public void queueAsyncCallback(final Callback callback) {
    setTimer(new Timer(0, false, new Timer.Callback() {

      @Override
      public void timerFired(Timer timer) {
        callback.execute();
      }

    }));
  }

  public <ResultType> void queueAsyncCallbackWithResult(
      final CallbackWithResult<ResultType> callback, final ResultType result) {
    setTimer(new Timer(0, false, new Timer.Callback() {

      @Override
      public void timerFired(Timer timer) {
        callback.execute(result);
      }

    }));
  }

  @Test
  public void testEventLoop() throws Exception {

    final EventLoop eventLoop = new EventLoop();

    eventLoop.start();
    eventLoop.setTimer(new Timer(1000, false, new Timer.Callback() {

      public void timerFired(Timer timer) {
        System.out.println("Timer 1 Fired");
      }

    }));

    eventLoop.setTimer(new Timer(500, false, new Timer.Callback() {

      public void timerFired(Timer timer) {
        System.out.println("Timer 2 Fired");
      }

    }));

    eventLoop.setTimer(new Timer(3000, false, new Timer.Callback() {

      public void timerFired(Timer timer) {
        System.out.println("Timer 3 Fired");
        eventLoop.stop();
      }

    }));

    System.out.println("Wait for Timer to Fire");
    eventLoop.getEventThread().join();
    System.out.println("Event loop stopped. Shutting down.");
  }
}
