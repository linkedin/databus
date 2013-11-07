package com.linkedin.databus.core;
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


import org.apache.log4j.Logger;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;


public class RelayEventTraceOption
{  
  public static final String MODULE = RelayEventTraceOption.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public enum Option
  {
    none,
    file
  }
  
  private final Option _option;
  private final boolean _isAppendOnly;
  private final String _filename;
  private final boolean _isNeedFileSuffix;
  
  public RelayEventTraceOption(Option traceOption,
                               String traceFilename,
                               boolean isAppendOnly,
                               boolean needFileSuffix)
  {
    _option = traceOption;
    _filename = traceFilename;
    _isAppendOnly = isAppendOnly;
    _isNeedFileSuffix = needFileSuffix;
  }

  public RelayEventTraceOption(Option traceOption)
  {
    this(traceOption, null, false, false);
  }

  public Option getOption()
  {
    return _option;
  }
  
  public String getFilename()
  {
    return _filename;
  }

  public boolean isAppendOnly()
  {
    return _isAppendOnly;
  }
  
  public boolean isNeedFileSuffix() {
    return _isNeedFileSuffix;
  }
  
  public static class RelayEventTraceOptionBuilder implements ConfigBuilder<RelayEventTraceOption>
  {
    public static final String DEFAULT_TRACE_OPTION_FILENAME = "databus_relay_event_trace";

    private String _option;
    private boolean _isAppendOnly;
    private String _filename;
    private boolean _isNeedFileSuffix; //if true - the trace file will have a .{pSourceName} extension
    
    public RelayEventTraceOptionBuilder()
    {
      _option = Option.none.toString();
      _isAppendOnly = false;
      _isNeedFileSuffix = false;
      _filename = DEFAULT_TRACE_OPTION_FILENAME;
    }
    
    public RelayEventTraceOptionBuilder(RelayEventTraceOptionBuilder other)
    {
      _option = other._option;
      _isAppendOnly = other._isAppendOnly;
      _filename = other._filename;
      _isNeedFileSuffix = other._isNeedFileSuffix;
    }

    public String getOption()
    {
      return _option;
    }

    public boolean isAppendOnly()
    {
      return _isAppendOnly;
    }
    
    /** if true - the trace file will have a .{pSourceName} extension */
    public boolean isNeedFileSuffix()
    {
      return _isNeedFileSuffix;
    }

    public String getFilename()
    {
      return _filename;
    }

    public void setOption(String option)
    {
      this._option = option;
    }

    public void setAppendOnly(boolean _isAppendOnly)
    {
      this._isAppendOnly = _isAppendOnly;
    }
    
    /** set to true if the trace file needs to have a .{pSourceName} extension 
     * could be set as:
     * databus.relay.eventBuffer.trace.needFileSuffix=true
     */
    public void setNeedFileSuffix(boolean needFileSuffix)
    {
      this._isNeedFileSuffix = needFileSuffix;
    }

    public void setFilename(String _filename)
    {
      this._filename = _filename;
    }

    @Override
    public RelayEventTraceOption build() throws InvalidConfigException
    {
      LOG.info("Relay event trace option: " + _option);
      LOG.info("Relay event trace filename: " + _filename);
      LOG.info("Relay event trace file append only: " + _isAppendOnly);
      LOG.info("Relay event trace need file suffix: " + _isNeedFileSuffix);
      return new RelayEventTraceOption(Option.valueOf(_option), _filename, _isAppendOnly, _isNeedFileSuffix);
    }
    
    
    
  }

}
