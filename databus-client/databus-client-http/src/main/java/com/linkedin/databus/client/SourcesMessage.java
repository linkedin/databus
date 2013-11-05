package com.linkedin.databus.client;
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


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.container.request.RegisterResponseMetadataEntry;

/**
 * A message class for dealing with databus sources.
 *
 * <p>Current message types are:
 * <ul>
 *   <li>SET_SOURCES_IDS - sets the ids for sources</li>
 *   <li>SET_SOURCES_SCHEMAS - sets the schemas for sources</li>
 * </ul>
 *
 * @author cbotev
 *
 */
public class SourcesMessage
{

  public enum TypeId
  {
    SET_SOURCES_IDS,
    SET_SOURCES_SCHEMAS
  }

  private TypeId _typeId;
  private List<IdNamePair> _sources;
  private String _sourcesIdListString;
  private Map<Long, List<RegisterResponseEntry>> _sourcesSchemas;
  private final List<RegisterResponseMetadataEntry> _metadataSchemas;

  private SourcesMessage(TypeId typeId,
                         List<IdNamePair> sources,
                         String sourcesIdListString,
                         Map<Long, List<RegisterResponseEntry>> sourcesSchemas)
  {
    this(typeId,sources,sourcesIdListString,sourcesSchemas,null);
  }

  private SourcesMessage(TypeId typeId,
          List<IdNamePair> sources,
          String sourcesIdListString,
          Map<Long, List<RegisterResponseEntry>> sourcesSchemas,
          List<RegisterResponseMetadataEntry> metadataSchemas)
  {
      super();
      _typeId = typeId;
      _sources = sources;
      _sourcesIdListString = sourcesIdListString;
      _sourcesSchemas = sourcesSchemas;
      _metadataSchemas = metadataSchemas;
  }

  /**
   * Creates a new SET_SOURCES_IDS message
   * @param  sources            the sources id list
   * @return the new message object
   */
  public static SourcesMessage createSetSourcesIdsMessage(List<IdNamePair> sources,
                                                          String sourcesIdListString)
  {
    return new SourcesMessage(TypeId.SET_SOURCES_IDS, sources, sourcesIdListString, null);
  }

  /**
   * Creates a new SET_SOURCES_SCHEMAS message
   * @param  sourcesSchemas            the sources schemas maps
   * @param  metadataSchemas           the schemas describing metadata carried in the event; can be null
   * @return the new message object
   */
  public static SourcesMessage createSetSourcesSchemasMessage(
      Map<Long, List<RegisterResponseEntry>> sourcesSchemas,
      List<RegisterResponseMetadataEntry> metadataSchemas)
  {
    return new SourcesMessage(TypeId.SET_SOURCES_SCHEMAS, null, null, sourcesSchemas,metadataSchemas);
  }

  /**
   * Creates a new SET_SOURCES_SCHEMAS message
   * @param  sourcesSchemas            the sources schemas maps
   * @return the new message object
   */
  public static SourcesMessage createSetSourcesSchemasMessage(
      Map<Long, List<RegisterResponseEntry>> sourcesSchemas)
  {
    return new SourcesMessage(TypeId.SET_SOURCES_SCHEMAS, null, null, sourcesSchemas,null);
  }

  /**
   * Reuses an existing message object and makes it SET_SOURCES_IDS message
   * @param  sources             the new sources ids list
   * @return the message object
   */
  public SourcesMessage switchToSetSourcesIds(List<IdNamePair> sources, String sourcesIdListString)
  {
    _typeId = TypeId.SET_SOURCES_IDS;
    _sources = sources;
    _sourcesIdListString = sourcesIdListString;
    _sourcesSchemas = null;

    return this;
  }

  public static SourcesMessage createSetSourcesIdsMessage(Collection<IdNamePair> sources)
  {
    return new SourcesMessage(TypeId.SET_SOURCES_IDS,
                              new ArrayList<IdNamePair>(sources),
                              calcSourcesIdListString(sources),
                              null);
  }

  private static String calcSourcesIdListString(Collection<IdNamePair> sources)
  {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (IdNamePair pair: sources)
    {
      if (!first)
      {
        sb.append(",");
      }
      sb.append(pair.getId());
      first = false;
    }
    return sb.toString();

  }

  /**
   * Reuses an existing message object and makes it SET_SOURCES_SCHEMAS message
   * @param  sourcesSchemas            the new sources schemas maps
   * @return the message object
   */
  public SourcesMessage switchToSetSourcesSchemas(Map<Long, List<RegisterResponseEntry>> sourcesSchemas)
  {
    _typeId = TypeId.SET_SOURCES_IDS;
    _sources = null;
    _sourcesSchemas = sourcesSchemas;

    return this;
  }

  /** Returns the type of the current message*/
  public TypeId getTypeId()
  {
    return _typeId;
  }

  /** Returns the list of sources Id-Name pairs; meaningful only for SET_SOURCES_IDS messages */
  public List<IdNamePair> getSources()
  {
    return _sources;
  }

  /** Returns the list of sources ids as a string; meaningful only for SET_SOURCES_IDS messages */
  public String getSourcesIdListString()
  {
    return _sourcesIdListString;
  }

  /** Returns the sources Id-to-Schema map; meaningful only for SET_SOURCES_SCHEMAS messages */
  public Map<Long, List<RegisterResponseEntry>> getSourcesSchemas()
  {
    return _sourcesSchemas;
  }


  /** Returns list of metadata schemas ; meaningful only for SET_SOURCES_SCHEMAS messages */
  public List<RegisterResponseMetadataEntry> getMetadataSchemas()
  {
    return _metadataSchemas;
  }

  @Override
  public String toString()
  {
    return _typeId.toString();
  }

}
