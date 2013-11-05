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
import java.util.HashMap;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.log4j.Logger;


public class ColumnState extends AbstractStateTransitionProcessor
{
  public final static String MODULE = ColumnState.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);

  public static final String FIELDNAMEATTR = "name";
  public static final String KEYNAMEATTR = "key";
  public static final String STATUSATTR = "status";
  private HashMap<String, EventField> eventFields;

  public ColumnState()
  {
    super(STATETYPE.STARTELEMENT, COLUMNSTATE);
  }

  public HashMap<String, EventField> getEventFields()
  {
    return eventFields;
  }

  public void setEventFields(HashMap<String, EventField> eventFields)
  {
    this.eventFields = eventFields;
  }

  @Override
  public void onEndElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws Exception
  {
    _currentStateType = STATETYPE.ENDELEMENT;
    xmlStreamReader.nextTag();
    setNextStateProcessor(stateMachine,xmlStreamReader);
  }

  @Override
  public void onStartElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {
    _currentStateType = STATETYPE.STARTELEMENT;
    String currentField = null;
    boolean isKey = false;
    boolean isNull = false;
    for(int i = 0; i < xmlStreamReader.getAttributeCount() ; i++)
    {
      if(xmlStreamReader.getAttributeName(i).getLocalPart().equals(FIELDNAMEATTR))
      {
        currentField = xmlStreamReader.getAttributeValue(i);
      }
      else if(xmlStreamReader.getAttributeName(i).getLocalPart().equals(KEYNAMEATTR))
      {
        try{
          isKey = Boolean.valueOf(xmlStreamReader.getAttributeValue(i));
          if(LOG.isDebugEnabled())
            LOG.debug("Key field located: " +  currentField);
        }
        catch(NumberFormatException e){
          LOG.error("Unable to interpret key field: ",e);
        }
      }
      else if(xmlStreamReader.getAttributeName(i).getLocalPart().equals(STATUSATTR))
      {
        if(xmlStreamReader.getAttributeValue(i).equals("null"))
          isNull = true;

      }
    }

    if(currentField == null || currentField.length()==0)
    {
      LOG.error("Unable to parse current column field type: " + currentField);
      XmlStreamReaderHelper.checkAndMoveToNextTagSet(xmlStreamReader);
      setNextStateProcessor(stateMachine,xmlStreamReader);
      return;
    }

    String currentFieldValue = xmlStreamReader.getElementText();
    getEventFields().put(currentField,new EventField(isKey,currentFieldValue, isNull));
    if(LOG.isDebugEnabled())
      LOG.debug("Processed the field " + currentField + " with value " + currentFieldValue + " and isNull: " + isNull);

    setNextStateProcessor(stateMachine, xmlStreamReader);
  }


  @Override
  public void cleanUpState(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
  {
    setEventFields(null);
  }


  public static class EventField
  {
    public EventField(boolean key, String val, boolean isNull) {
      isKey = key;
      this.val = val;
      this.isNull = isNull;
    }

    boolean isKey;
    String val;


    boolean isNull;

    public String getVal() {
      return val;
    }

    public boolean isKey() {
      return isKey;
    }

    public boolean isNull()
    {
      return isNull;
    }
  }
}
