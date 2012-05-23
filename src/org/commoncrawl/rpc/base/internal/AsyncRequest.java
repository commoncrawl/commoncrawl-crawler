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

import org.commoncrawl.rpc.base.shared.RPCStruct;

/**
 * 
 * @author rana
 *
 * @param <InputType>
 * @param <OutputType>
 */
public class AsyncRequest<InputType extends RPCStruct, OutputType extends RPCStruct> {

  public static interface Callback<InputType extends RPCStruct, OutputType extends RPCStruct> {
    void requestComplete(AsyncRequest<InputType, OutputType> request);
  }

  public enum Status {
    Success, Error_RPCFailed, Error_ServerError, Error_RequestFailed
  }

  String _service;

  String _method;
  int _id;
  InputType _input;
  OutputType _output;
  Status _status = Status.Error_RPCFailed;
  String _errorDesc;
  Callback<InputType, OutputType> _callback = null;

  public AsyncRequest(String serviceName, String method, InputType input, OutputType output, Callback callback) {
    _service = serviceName;
    _method = method;
    _input = input;
    _output = output;
    _callback = callback;
  }

  public Callback<InputType, OutputType> getCallback() {
    return _callback;
  }

  String getErrorDesc() {
    return _errorDesc;
  }

  public InputType getInput() {
    return _input;
  }

  String getMethod() {
    return _method;
  }

  public OutputType getOutput() {
    return _output;
  }

  public int getRequestId() {
    return _id;
  }

  String getService() {
    return _service;
  }

  public Status getStatus() {
    return _status;
  }

  void setErrorDesc(String errorDesc) {
    _errorDesc = errorDesc;
  }

  public void setRequestId(int requestId) {
    _id = requestId;
  }

  public void setStatus(Status status) {
    _status = status;
  }
}
