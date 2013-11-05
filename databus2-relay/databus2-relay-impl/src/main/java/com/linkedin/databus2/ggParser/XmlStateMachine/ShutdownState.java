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


public class ShutdownState extends AbstractStateTransitionProcessor
{
  public final static String MODULE = TransactionState.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);

  public ShutdownState()
  {
    super(STATETYPE.ENDELEMENT, ROOT);
  }

  @Override
  public void onEndElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws Exception
  {
  }

  @Override
  public void onStartElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {

  }

  @Override
  public void processElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {
    LOG.info("The XmlStream parser is shutdown, no further processing possible");
    xmlStreamReader.next();
  }

  @Override
  public void setNextStateProcessor(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException
  {
    LOG.info("No-op");
    return;
  }

    @Override
    public void cleanUpState(StateMachine stateMachine, XMLStreamReader xmlStreamReader) {
    }
}