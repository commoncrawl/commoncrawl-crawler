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
import java.util.HashMap;
import java.util.Map;

import org.commoncrawl.rpc.base.internal.Frame.IncomingFrame;
import org.commoncrawl.rpc.base.shared.RPCException;

/**
 * 
 * @author rana
 *
 */
public class Server {

  private static class ServerChannelMapItem {

    public Map<String, Service.Specification> _serviceMap = new HashMap<String, Service.Specification>();

  }

  private final Map<AsyncServerChannel, ServerChannelMapItem> _channelMap = new HashMap<AsyncServerChannel, ServerChannelMapItem>();

  public void dispatchRequest(AsyncServerChannel serverChannel, AsyncClientChannel source, IncomingFrame frame)
      throws RPCException {

    Service.Specification spec = null;
    synchronized (_channelMap) {
      ServerChannelMapItem mapItem = _channelMap.get(serverChannel);

      if (mapItem != null) {
        spec = mapItem._serviceMap.get(frame._service);
      }
    }

    if (spec == null) {
      throw new RPCException("Uknown Service Name:" + frame._service);
    }
    // dispatch the request via the service's dispatcher ...
    AsyncContext context = spec._dispatcher.dispatch(this, frame, serverChannel, source);

    // if not completed immediately, add this request to pending request context
    // list
    if (!context.isComplete()) {
      // TODO: IS THIS NECESSARY ?
    }
  }

  public final void registerService(AsyncServerChannel channel, Service.Specification specification) {

    synchronized (_channelMap) {

      ServerChannelMapItem item = _channelMap.get(channel);
      if (item == null) {
        item = new ServerChannelMapItem();
        _channelMap.put(channel, item);
      } else {
        if (item._serviceMap.get(specification._name) != null) {
          throw new RuntimeException("Invalid call to register Service. The specified channel - service specification "
              + "association already exists!");
        }
      }
      item._serviceMap.put(specification._name, specification);

    }
  }

  public void start() throws IOException {
    synchronized (_channelMap) {
      for (AsyncServerChannel channel : _channelMap.keySet()) {
        channel.open();
      }
    }
  }

  public void stop() {
    synchronized (_channelMap) {
      for (AsyncServerChannel channel : _channelMap.keySet()) {
        channel.close();
      }
    }
  }

}
