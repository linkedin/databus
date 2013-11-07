/*
 * Copyright 2013 LinkedIn Corp. All rights reserved
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.linkedin.databus2.ggParser.XmlStateMachine;


import com.linkedin.databus2.core.DatabusException;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.log4j.Logger;


public class RootState extends AbstractStateTransitionProcessor
{
  public final static String MODULE = TransactionState.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);

  public RootState()
  {
    super(STATETYPE.STARTELEMENT, ROOT);
  }

  @Override
  public void onEndElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws Exception
  {
    _currentStateType = STATETYPE.ENDELEMENT;
    LOG.info("Finished processing XML successfully, shutting down parser");
    xmlStreamReader.nextTag();
    // stateMachine.setProcessState(stateMachine.shutdownState);
    return;
  }

  @Override
  public void onStartElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {
    _currentStateType = STATETYPE.STARTELEMENT;
    xmlStreamReader.nextTag();
    setNextStateProcessor(stateMachine, xmlStreamReader);
  }


    @Override
    public void cleanUpState(StateMachine stateMachine, XMLStreamReader xmlStreamReader) {

    }
}