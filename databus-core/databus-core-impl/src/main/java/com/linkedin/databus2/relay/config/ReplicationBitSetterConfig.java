package com.linkedin.databus2.relay.config;

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

import java.util.Arrays;

import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig.MissingValueBehavior;
import com.linkedin.databus2.relay.config.ReplicationBitSetterStaticConfig.SourceType;

/**
 * Configuration Builder for ReplicationBitSetter.
 * The setter used regular reg-ex match (case-insensitive) on the column/token value for each event to decide if replication bit has to be set.
 * All the configs defined here are case-insensitive.
 */
public class ReplicationBitSetterConfig implements
    ConfigBuilder<ReplicationBitSetterStaticConfig>
{
  // By default, Replication BitSetter logic is turned-off.
  public final static String DEFAULT_REPLICATION_BIT_SETTER_SOURCE_TYPE = SourceType.NONE.toString();

  // By default, MissingValue behavior should be to stop and throw exception.
  public final static String DEFAULT_MISSING_VALUE_BEHAVIOR = MissingValueBehavior.STOP_WITH_ERROR.toString();

  /**
   *  Source Type for looking up the fieldName and value to decide if an event has to be set replicated or not
   */
  private String _sourceType;

  /**
   *  Field Name to inspect for setting the replicated bit
   */
  private String _fieldName;

  /**
   * The Regex pattern for the value extracted for the fieldName which would identify if an event is replicated
   */
  private String _remoteUpdateValueRegex;

  // Behavior when the fetcher did not find the replication field/token
  private String _missingValueBehavior;

  /**
   * Constructs Config builder with default configs for replication bit setter
   */
  public ReplicationBitSetterConfig()
  {
    _sourceType = DEFAULT_REPLICATION_BIT_SETTER_SOURCE_TYPE;
    _fieldName = DbusConstants.GG_REPLICATION_BIT_SETTER_FIELD_NAME;
    _remoteUpdateValueRegex = DbusConstants.GG_REPLICATION_BIT_SETTER_VALUE;
    _missingValueBehavior = DEFAULT_MISSING_VALUE_BEHAVIOR;
  }


  @Override
  public ReplicationBitSetterStaticConfig build() throws InvalidConfigException
  {
    SourceType type = null;

    try
    {
      type = SourceType.valueOf(_sourceType);
    } catch (IllegalArgumentException iae) {
      throw new InvalidConfigException("Source Types should be one of (" + Arrays.asList(SourceType.values()) + ") but is (" + _sourceType + ")");
    }

    MissingValueBehavior missingValueForDelete = null;
    try
    {
      missingValueForDelete = MissingValueBehavior.valueOf(_missingValueBehavior);
    } catch ( IllegalArgumentException iae) {
      throw new InvalidConfigException("Missing Value For Delete Behavior should be one of (" + Arrays.asList(MissingValueBehavior.values()) + ") but is (" + _missingValueBehavior + ")");
    }

    return new ReplicationBitSetterStaticConfig(type, _fieldName, _remoteUpdateValueRegex, missingValueForDelete);
  }

  /**
   *  @return Source Type for looking up fieldName and value to decide if an event has to be set replicated or not
   */
  public String getSourceType()
  {
    return _sourceType;
  }

  public void setSourceType(String sourceType)
  {
    this._sourceType = sourceType;
  }

  /**
   *  @return the field Name to inspect for setting the replicated bit in the event
   */
  public String getFieldName()
  {
    return _fieldName;
  }

  public void setFieldName(String fieldName)
  {
    this._fieldName = fieldName;
  }

  /**
   * @return : the Regex pattern for the value extracted for the fieldName which would identify if an event is replicated or not
   */
  public String getRemoteUpdateValueRegex()
  {
    return _remoteUpdateValueRegex;
  }

  public void setRemoteUpdateValueRegex(String remoteUpdateValueRegex)
  {
    this._remoteUpdateValueRegex = remoteUpdateValueRegex;
  }


  public String getMissingValueBehavior()
  {
    return _missingValueBehavior;
  }


  public void setMissingValueBehavior(String missingValueBehavior)
  {
    _missingValueBehavior = missingValueBehavior;
  }
}
