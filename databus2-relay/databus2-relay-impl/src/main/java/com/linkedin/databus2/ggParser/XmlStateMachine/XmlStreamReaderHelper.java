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
import com.linkedin.databus2.schemas.SchemaRegistryService;
import java.util.HashMap;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;


public class XmlStreamReaderHelper
{
  public final static String MODULE = XmlStreamReaderHelper.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);

  /**
   * Custom implementation of getElementText(). Reads all the text till the end of the element.
   * Should be called only for XMLStreamReader is in START_ELEMENT state.
   *
   * @param xmlStreamReader
   * @throws XMLStreamException
   */
  public static String getTextChunkedReader(XMLStreamReader xmlStreamReader)
      throws XMLStreamException, DatabusException
  {


    if(xmlStreamReader.getEventType() != XMLStreamReader.START_ELEMENT)
      throw new DatabusException("The XmlStreamReader is not pointing to a start element");

    StringBuffer sb = new StringBuffer();
      while(xmlStreamReader.hasNext())
      {

       int eventType = xmlStreamReader.next();
       if(eventType == XMLStreamConstants.CHARACTERS
              || eventType == XMLStreamConstants.CDATA
              || eventType == XMLStreamConstants.SPACE
              || eventType == XMLStreamConstants.ENTITY_REFERENCE) {
          sb.append(xmlStreamReader.getText());
          } else if(eventType == XMLStreamConstants.PROCESSING_INSTRUCTION
              || eventType == XMLStreamConstants.COMMENT) {
            // skip
            LOG.error("Unknown state while processing characters (in method getText())");
          } else if(eventType == XMLStreamConstants.END_DOCUMENT) {
            throw new DatabusException("Unexpected end of document when reading element text content");
          } else if(eventType == XMLStreamConstants.START_ELEMENT) {
            throw new DatabusException("Element text content may not contain START_ELEMENT");
          } else {
            throw new DatabusException("Unexpected event type "+eventType);
          }
        }
    return sb.toString();
  }

  /**
   * Skip new lines after the end elements.
   * This function should be called when the cursor is pointing to CHARACTER and leaves the cursor
   * at END_ELEMENT OR START_ELEMENT
   * @param xmlStreamReader
   */
  public static void skipNewLines(XMLStreamReader xmlStreamReader)
      throws XMLStreamException
  {
    if(xmlStreamReader.isCharacters())
    {
      while(xmlStreamReader.isCharacters() && xmlStreamReader.hasNext())
        xmlStreamReader.next();
    }
  }


  public static void checkAndMoveNext(XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {
    if(xmlStreamReader.hasNext())
      xmlStreamReader.next();
    else
      throw new DatabusException("XmlStream does not have any more data");
  }

  public static void checkAndMoveToNextTag(XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {
    if(xmlStreamReader.hasNext())
      xmlStreamReader.nextTag();
    else
      throw new DatabusException("XmlStream does not have any more data");
  }

  /**
   * It moves the cursor to the next immediate tag set (next immediate ancestor or descendant) (START_ELEMENT OR END_ELEMENT).
   * Case 1: <start1>xxx</start1> <start2>yyy</start2>
   * Called  anywhere from start1 tag set moves the cursor the <start2>'s START_ELEMENT
   * Case 2:  <start2><start1>xxx</start1> </start2>
   * Called  anywhere from start1 tag set moves the cursor the <start2>'s END_ELEMENT
   * Case 3:<start2><start1>xxx</start1> </start2>
   * Called from <start2> will set the cursor to the <start1>
   * Accepts both END_ELEMENT AND START_ELEMENT cursor states
   * @param xmlStreamReader
   * @throws DatabusException
   * @throws XMLStreamException
   */
  @Deprecated
  public static void checkAndMoveToNextTagSet(XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {
    if(xmlStreamReader.isEndElement())
    {
      if(xmlStreamReader.hasNext())
        xmlStreamReader.nextTag();
      else
        throw new DatabusException("XmlStream does not have any more data");
    }
    else if(xmlStreamReader.isStartElement())
    {
      XmlStreamReaderHelper.checkAndMoveNext(xmlStreamReader);
      if(xmlStreamReader.isCharacters())
      {
        XmlStreamReaderHelper.checkAndMoveToNextTag(xmlStreamReader);
      }

      if(xmlStreamReader.isEndElement())
        XmlStreamReaderHelper.checkAndMoveToNextTag(xmlStreamReader);

      if(!xmlStreamReader.isStartElement() && !xmlStreamReader.isEndElement())
        throw new DatabusException("Inconsistent state, expected cursor state START_ELEMENT or END_ELEMENT");

    }
    else
      throw new DatabusException("Unable to move to nextTag, expected START_ELEMENT or END_ELEMENT");
  }

  public static HashMap<String, String> getAttributeMap(XMLStreamReader xmlStreamReader)
  {

    HashMap<String,String> attributeMap = new HashMap<String, String>();
    int attributeCount =  xmlStreamReader.getAttributeCount();
    for(int i = 0; i < attributeCount; i++)
    {
      String attributeName = xmlStreamReader.getAttributeName(i).getLocalPart();
      String attributeValue = xmlStreamReader.getAttributeValue(i);
      attributeMap.put(attributeName,attributeValue);
    }
    return attributeMap;
  }
}