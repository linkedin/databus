package com.linkedin.databus2.ggParser.XmlStateMachine;

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


import com.linkedin.databus2.core.DatabusException;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;


/**
 * This interface defines the standard methods each state transition class(xml processing element) should implement.
 * Upon processing the current state, the processElement function should move the cursor to the next element.
 */
public interface StateProcessor
{

  public enum STATETYPE
  {
    STARTELEMENT,
    ENDELEMENT
  }

  public void onEndElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws Exception;

  public void onStartElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException;

  /**
   * Perform any operations that have to performed with the current element/tag
   * @param stateMachine
   * @param xmlStreamReader
   * @throws DatabusException
   * @throws XMLStreamException
   */
  public void processElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws Exception;

  /**
   * Set the next state processor by advancing the cursor the START_ELEMENT of the next tag
   * @param stateMachine
   * @param xmlStreamReader
   */
  public void setNextStateProcessor(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException;

  /**
   * All processing in this state are complete and have been picked up by the other states as needed
   * @param stateMachine
   * @param xmlStreamReader
   */
  public void cleanUpState(StateMachine stateMachine, XMLStreamReader xmlStreamReader);
}