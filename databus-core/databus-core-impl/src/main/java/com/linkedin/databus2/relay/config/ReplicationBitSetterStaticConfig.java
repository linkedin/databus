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

/**
 *
 * Config for ReplicationBitSetter
 */
public class ReplicationBitSetterStaticConfig
{
    /**
     * Type of source which decides if an event should be marked replicated or not
     */
    public static enum SourceType
    {
      COLUMN, // The deciding field is a column (For e.g: GG_STATUS = 'g' in the case of Oracle)
      TOKEN, // The deciding field is a token from the trail file. (e.g: <token name="TK-UNAME">GGADMIN</token> )
      NONE // No field. No Check will be done for setting replicating bit
    };

    /**
     * Behavior when the fetcher did not find the replication field/token
     */
    public static enum MissingValueBehavior
    {
      STOP_WITH_ERROR,      // Throw Exception & stop Processing
      TREAT_EVENT_LOCAL,    // Treat as local event
      TREAT_EVENT_REPLICATED  // Treat as replicated event
    };

    // Source Type for looking up the fieldName and value to decide if an event has to be set replicated or not
    private final SourceType _sourceType;

    // Field Name to inspect for setting the replicated bit
    private final String _fieldName;

    // The Regex pattern for the value extracted for the fieldName which would identify if an event is replicated or not
    private final String _remoteUpdateValueRegex;

    // Behavior when the fetcher did not find the replication field/token
    private final MissingValueBehavior _missingValueBehavior;

    public SourceType getSourceType()
    {
      return _sourceType;
    }

    public MissingValueBehavior getMissingValueBehavior()
    {
      return _missingValueBehavior;
    }

    public String getFieldName()
    {
      return _fieldName;
    }

    public String getRemoteUpdateValueRegex()
    {
      return _remoteUpdateValueRegex;
    }

    public ReplicationBitSetterStaticConfig(SourceType sourceType,
                                            String fieldName,
                                            String remoteUpdateValueRegex,
                                            MissingValueBehavior missingValueForDeleteBehavior)
    {
      super();
      _sourceType = sourceType;
      _fieldName = fieldName;
      _remoteUpdateValueRegex = remoteUpdateValueRegex;
      _missingValueBehavior = missingValueForDeleteBehavior;
    }

    @Override
    public String toString()
    {
      return "ReplicationBitSetterStaticConfig [_sourceType=" + _sourceType
          + ", _fieldName=" + _fieldName + ", _remoteUpdateValueRegex="
          + _remoteUpdateValueRegex + ", _missingValueBehavior=" + _missingValueBehavior
          + "]";
    }
}
