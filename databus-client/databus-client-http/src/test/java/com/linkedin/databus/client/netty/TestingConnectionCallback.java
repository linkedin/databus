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
package com.linkedin.databus.client.netty;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.databus.core.async.AbstractActorMessageQueue;

/**
 * A helper class for testing of network connections.
 */
public class TestingConnectionCallback extends AbstractActorMessageQueue
{
  TestResponseProcessors.TestConnectionStateMessage _lastMsg = null;
  List<TestResponseProcessors.TestConnectionStateMessage> _allMsgs =
      new ArrayList<TestResponseProcessors.TestConnectionStateMessage>();

  @Override
  protected boolean executeAndChangeState(Object message)
  {
    if (message instanceof TestResponseProcessors.TestConnectionStateMessage)
    {
      TestResponseProcessors.TestConnectionStateMessage m = (TestResponseProcessors.TestConnectionStateMessage)message;
      boolean success = true;
      _lastMsg = m;
      _allMsgs.add(m);

      return success;
    }
    else
    {
      return super.executeAndChangeState(message);
    }
  }

  public TestingConnectionCallback(String name)
  {
    super(name);
  }

  public TestResponseProcessors.TestConnectionStateMessage getLastMsg()
  {
    return _lastMsg;
  }

  public void clearLastMsg()
  {
    _lastMsg = null;
    _allMsgs.clear();
  }

  public static TestingConnectionCallback createAndStart(String name)
  {
    TestingConnectionCallback result = new TestingConnectionCallback(name);
    Thread runThread = new Thread(result, name + "-callback");
    runThread.setDaemon(true);
    runThread.start();

    return result;
  }

  public List<TestResponseProcessors.TestConnectionStateMessage> getAllMsgs()
  {
    return _allMsgs;
  }

  @Override
  protected void onShutdown()
  {
  }
}
