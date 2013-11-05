package com.linkedin.ggtest;


/*
 *
 *  *
 *  * Copyright 2013 LinkedIn Corp. All rights reserved
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus.monitoring.mbean.GGParserStatistics.TransactionInfo;
import com.linkedin.databus2.ggParser.XmlStateMachine.TransactionState;
import com.linkedin.databus2.ggParser.XmlStateMachine.TransactionSuccessCallBack;


public class DummyTransactionCallback  implements TransactionSuccessCallBack
{

  int numUpdatesSeen;
  List<TransactionState.PerSourceTransactionalUpdate> dbUpdates;
  private static final Logger LOG = Logger.getLogger(DummyTransactionCallback.class.getName());


  @Override
  public void onTransactionEnd(List<TransactionState.PerSourceTransactionalUpdate> dbUpdates, TransactionInfo ti)
  {
    numUpdatesSeen = dbUpdates.size();
    this.dbUpdates = dbUpdates;
    LOG.info("The callback has seen: "+ numUpdatesSeen + " updates ");
  }

}
