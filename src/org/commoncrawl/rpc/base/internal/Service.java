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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author rana
 *
 */
public interface Service {

  public static class AsyncStub {

    private AsyncClientChannel _channel;

    public AsyncStub(AsyncClientChannel channel) {
      _channel = channel;
    }

    public AsyncClientChannel getChannel() {
      return _channel;
    }
  }

  public static class BlockingStub<StubType extends AsyncStub> {

    /** default RPC Timeout **/
    private static final int DEFAULT_RPC_TIMEOUT = 60000;

    private StubType _asyncStub;
    private int _rpcTimeout = DEFAULT_RPC_TIMEOUT;

    public BlockingStub(StubType asyncStub) {
      _asyncStub = asyncStub;
    }

    public StubType getAsyncStub() {
      return _asyncStub;
    }

    public int getRPCTimeout() {
      return _rpcTimeout;
    }

    public void setRPCTimeout(int milliseconds) {
      _rpcTimeout = milliseconds;
    }

    protected boolean waitForResult(CountDownLatch latch) throws IOException {

      // if called from event thread ...
      if (Thread.currentThread() == _asyncStub.getChannel().getClient().getEventThread()) {
        // pump events until timeout is reached

        long timeoutTime = System.currentTimeMillis() + getRPCTimeout();

        do {
          _asyncStub.getChannel().getClient().waitForIO(1);
        } while (latch.getCount() == 1 || System.currentTimeMillis() >= timeoutTime);
      } else {
        try {
          latch.await(getRPCTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          // Should never happen ... ?
          // TODO: IGNORE OR THROW RUNTIME EXCEPTION ?
          throw new RuntimeException(e);
        }
      }

      return latch.getCount() == 0;
    }
  }

  public static class Specification {

    public String _name;

    public Dispatcher _dispatcher;

    public Specification(String name, Dispatcher dispatcher) {
      _name = name;
      _dispatcher = dispatcher;
    }
  }

}
