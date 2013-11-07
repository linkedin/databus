package com.linkedin.databus2.core.container.request;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/



public abstract class BaseBinaryCommandParser implements BinaryCommandParser
{
  protected IDatabusRequest _cmd;
  protected ErrorResponse _err;

  @Override
  public void startNew()
  {
    _cmd = null;
    _err = null;
  }

  @Override
  public IDatabusRequest getCommand()
  {
    return _cmd;
  }

  @Override
  public ErrorResponse getError()
  {
    return _err;
  }

  protected void setCommand(IDatabusRequest cmd)
  {
    _cmd = cmd;
  }

  protected void setError(ErrorResponse err)
  {
    _err = err;
  }

}
