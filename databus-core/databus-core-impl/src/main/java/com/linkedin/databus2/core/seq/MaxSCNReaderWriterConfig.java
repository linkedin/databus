package com.linkedin.databus2.core.seq;
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


import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;

public class MaxSCNReaderWriterConfig implements ConfigBuilder<MaxSCNReaderWriterStaticConfig>
{

  private String _type;
  private FileMaxSCNHandler.Config _file;
  private MysqlMaxSCNHandler.Config _mysql;
  private MaxSCNReaderWriter _existing;

  public MaxSCNReaderWriterConfig()
  {
    _type = MaxSCNReaderWriterStaticConfig.Type.FILE.toString();
    _existing = null;
    _file = new FileMaxSCNHandler.Config();
    _mysql = new MysqlMaxSCNHandler.Config();
  }

  public MysqlMaxSCNHandler.Config getMysql() {
    return _mysql;
  }

  public void setMysql(MysqlMaxSCNHandler.Config _mysql) {
    this._mysql = _mysql;
  }

  public String getType()
  {
    return _type;
  }

  public void setType(String type)
  {
    _type = type;
  }

  public FileMaxSCNHandler.Config getFile()
  {
    return _file;
  }

  public void setFile(FileMaxSCNHandler.Config file)
  {
    _file = file;
  }

  public MaxSCNReaderWriter fixmeGetExisting()
  {
    return _existing;
  }

  public void fixmeSetExisting(MaxSCNReaderWriter existing)
  {
    _existing = existing;
  }

  @Override
  public MaxSCNReaderWriterStaticConfig build() throws InvalidConfigException
  {
    MaxSCNReaderWriterStaticConfig.Type handlerType = null;
    try
    {
      handlerType = MaxSCNReaderWriterStaticConfig.Type.valueOf(_type);
    }
    catch (Exception e)
    {
      throw new InvalidConfigException("invalid max scn reader/writer type:" + _type );
    }

    if (MaxSCNReaderWriterStaticConfig.Type.EXISTING == handlerType && null == _existing)
    {
      throw new InvalidConfigException("No existing max scn reader/writer specified ");
    }

    return new MaxSCNReaderWriterStaticConfig(handlerType, _file.build(), _mysql.build(),  _existing);
  }

}
