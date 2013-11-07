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
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.log4j.Logger;


public class TokenState extends AbstractStateTransitionProcessor
{
  public final static String MODULE = TokenState.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);
  public static final String TOKENATTRNAME = "name";
  public static final String TOKENSCN = "TK-CSN";
  public static final long ERRORSCN = -1;
  private long scn;
  private boolean _replicatedBit;
  private boolean _seenReplicationField;

  public TokenState()
  {
    super(STATETYPE.STARTELEMENT, TOKENSTATE);
  }

  public boolean isSeenReplicationField()
  {
    return _seenReplicationField;
  }

  public long getScn() {
    return scn;
  }

  public void setScn(long scn) {
    this.scn = scn;
  }

  public boolean isReplicated()
  {
    return _replicatedBit;
  }


  /**
   * Verify if the this is an scn field, if yes, then get the scn value
   * @param attributeValue The value of the attribute (TK-CSN, TK-UNAME etc.)
   * @param  tokenValue The value of token (The actual scn)
   * @return True if scn attribute is seen, false otherwise
   */
  private boolean verifyAndSetScnField(String attributeValue, String tokenValue)
  {
    if(attributeValue.equals(TOKENSCN))
    {
      try{
        String scn = tokenValue;
        this.scn =  Long.valueOf(scn);
        if(LOG.isDebugEnabled())
          LOG.debug("Event with SCN:" + scn + " processed");
      }
      catch(NumberFormatException e)
      {
        LOG.error("Unable to convert the scn:" + scn + " to long",e);
        this.scn = ERRORSCN;
      }
      return true;
    }
    return false;
  }

  /**
   * The method verifies if the token set is an replicated event. If it is, then marks the tokenstate as replicated
   * @param stateMachine Instance of the state machine
   * @param attributeValue The value of the current attribute being read. E.g., <token name=\"uname\" value=\"attributeValue\"....
   * @param tokenValue The actual value of the token field (xml value)
   * @throws XMLStreamException
   * @throws DatabusException
   * @return true if the replicated field is seen, false if not seen. (This does not have anything to do with the actual value of the field, just that the field was seen).
   */
  private boolean verifyAndSetReplicatedFlag(StateMachine stateMachine,
                                          String attributeValue,
                                          String tokenValue)
      throws XMLStreamException, DatabusException
  {
    ReplicationBitSetterStaticConfig.SourceType sourceType = stateMachine.getReplicationBitConfig().getSourceType();
    if(sourceType == ReplicationBitSetterStaticConfig.SourceType.TOKEN)
    {
      boolean isReplicatedDbUpdate = attributeValue.equalsIgnoreCase(stateMachine.getReplicationBitConfig().getFieldName());
      if( isReplicatedDbUpdate)
      {
        String replicationFieldValue = tokenValue;
        _replicatedBit = StateMachineHelper.verifyReplicationStatus(stateMachine.getReplicationValuePattern(),
                                                                    replicationFieldValue,stateMachine.getReplicationBitConfig().getMissingValueBehavior());
        _seenReplicationField = true;
        return true;
      }
    }
    return false;
  }


  @Override
  public void cleanUpState(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
  {
    this.scn = ERRORSCN;
    _replicatedBit = false;
    _seenReplicationField = false;
  }

  public void onEndElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {
    _currentStateType = STATETYPE.ENDELEMENT;
    xmlStreamReader.nextTag(); //Move to the next start tag
    setNextStateProcessor(stateMachine, xmlStreamReader);
  }

  @Override
  public void onStartElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {
    _currentStateType = STATETYPE.STARTELEMENT;
    //Cache the attribute values because getElementText() moves the xml cursor the the END_ELEMENT of the current tag.
    HashMap<String, String> attributeMap = XmlStreamReaderHelper.getAttributeMap(xmlStreamReader);
    String tokenValue = xmlStreamReader.getElementText();

    for(Map.Entry<String, String> attributeEntrySet : attributeMap.entrySet())
    {
      String attributeName = attributeEntrySet.getKey();
      String attributeValue = attributeEntrySet.getValue();
      if(!attributeName.equals(TOKENATTRNAME))
      {
        LOG.info("Unknown attribute name seen: " + attributeName);
        continue;
      }

      if(verifyAndSetReplicatedFlag(stateMachine, attributeValue, tokenValue))
        continue;
      else if (verifyAndSetScnField(attributeValue, tokenValue))
        continue;
      else
      {
        if(LOG.isDebugEnabled())
          LOG.info("Unknown attribute value seen" + attributeValue + " seen");
      }
    }

    //Adding a new invariant to get rid of checkAndMovetoNextTagSet, each state upon processing the element should have the stream reader at the end tag
    if(!xmlStreamReader.isEndElement())
      throw new DatabusException("The process element is expected to leave the xml stream reader at end element");
    setNextStateProcessor(stateMachine, xmlStreamReader);
  }
}