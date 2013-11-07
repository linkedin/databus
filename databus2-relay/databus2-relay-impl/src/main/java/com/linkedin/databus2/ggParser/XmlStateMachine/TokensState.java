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
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig.MissingValueBehavior;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.log4j.Logger;


public class TokensState extends AbstractStateTransitionProcessor
{
  public final static String MODULE = TokensState.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);
  private long scn;
  private boolean _isReplicated;

  public TokensState()
  {
    super(STATETYPE.STARTELEMENT, TOKENSSTATE);
  }

  public long getScn() {
      return scn;
  }

  public void setScn(long scn) {
      this.scn = scn;
  }

  public boolean isReplicated()
  {
    return _isReplicated;
  }

  public void setReplicated(boolean replicated)
  {
    _isReplicated = replicated;
  }


  @Override
  public void cleanUpState(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
  {
     setScn(TokenState.ERRORSCN);
     _isReplicated = false;
  }

  @Override
  public void onEndElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {
    _currentStateType = STATETYPE.ENDELEMENT;
    if(LOG.isDebugEnabled())
      LOG.debug("picking scn from token state: " + stateMachine.tokenState.getScn());

    if( (ReplicationBitSetterStaticConfig.SourceType.TOKEN == stateMachine.getReplicationBitConfig().getSourceType())
        && (!stateMachine.tokenState.isSeenReplicationField())
        && (MissingValueBehavior.STOP_WITH_ERROR == stateMachine.getReplicationBitConfig().getMissingValueBehavior()))
      throw new DatabusException("The replication field was not seen in the trail files in the tokens, this field is mandatory! The scn associated is: "+ stateMachine.tokenState.getScn());

    setScn(stateMachine.tokenState.getScn());
    
    if ( stateMachine.tokenState.isSeenReplicationField())
      _isReplicated = stateMachine.tokenState.isReplicated();
    else
      _isReplicated = StateMachineHelper.verifyReplicationStatus(stateMachine.getReplicationValuePattern(),
                                                                 null, stateMachine.getReplicationBitConfig().getMissingValueBehavior());
    
    stateMachine.tokenState.cleanUpState(stateMachine,xmlStreamReader);
    xmlStreamReader.nextTag();
    setNextStateProcessor(stateMachine, xmlStreamReader);
  }

  @Override
  public void onStartElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {
    _currentStateType = STATETYPE.STARTELEMENT;
    xmlStreamReader.nextTag();
    setNextStateProcessor(stateMachine, xmlStreamReader);
  }


}