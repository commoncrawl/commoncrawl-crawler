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

import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.rpc.base.shared.RPCStruct;

/**
 * 
 * @author rana
 *
 * @param <InputType>
 * @param <OutputType>
 */
public class AsyncContext<InputType extends RPCStruct, OutputType extends RPCStruct> {

  AsyncClientChannel _clientChannel;
  AsyncServerChannel _serverChannel;
  int _requestId;
  InputType _input;
  OutputType _output;
  AsyncRequest.Status _status = AsyncRequest.Status.Success;
  String _errorDescription;
  boolean _isComplete = false;

  public AsyncContext(AsyncServerChannel serverChannel, AsyncClientChannel clientChannel, int requestId,
      InputType input, OutputType output) {

    _clientChannel = clientChannel;
    _serverChannel = serverChannel;
    _requestId = requestId;
    _input = input;
    _output = output;

  }

  public void completeRequest() throws RPCException {
    _serverChannel.sendResponse(this);
  }

  public AsyncClientChannel getClientChannel() {
    return _clientChannel;
  }

  public String getErrorDesc() {
    return _errorDescription;
  }

  public InputType getInput() {
    return _input;
  }

  public OutputType getOutput() {
    return _output;
  }

  public int getRequestId() {
    return _requestId;
  }

  public AsyncServerChannel getServerChannel() {
    return _serverChannel;
  }

  public AsyncRequest.Status getStatus() {
    return _status;
  }

  boolean isComplete() {
    return _isComplete;
  }

  public void setErrorDesc(String errorDesc) {
    _errorDescription = errorDesc;
  }

  public void setOutput(OutputType output) {
    _output = output;
  }

  public void setStatus(AsyncRequest.Status status) {
    _status = status;
  }
}
