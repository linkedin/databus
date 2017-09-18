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


import com.linkedin.databus2.core.seq.FileMaxSCNHandler.StaticConfig;
import org.apache.log4j.Logger;

/**
 * Static configuration for the SCN reader/writer
 * @author cbotev
 *
 */
public class MaxSCNReaderWriterStaticConfig
{
  public static final String MODULE = MaxSCNReaderWriterStaticConfig.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  /**
   * The type of the SCN reader/writer
   * <ul>
   *   <li>DISABLED - no scn is persisted </li>
   *   <li>FILE - standard file based </li>
   *   <li>EXISTING - inject existing implementation </li>
   * </ul>
   */
  public enum Type
  {
    DISABLED,
    FILE,
    EXISTING,
    IN_MEMORY,
    MYSQL
  }

  private final Type _type;
  private final FileMaxSCNHandler.StaticConfig _file;
  private final MysqlMaxSCNHandler.StaticConfig _mysql;
  private final MaxSCNReaderWriter _existing;

  public MaxSCNReaderWriterStaticConfig(Type type,
                                     StaticConfig file,
                                        MysqlMaxSCNHandler.StaticConfig  mysql,
                                     MaxSCNReaderWriter existing)
  {
    super();
    _type = type;
    _file = file;
    _existing = existing;
    _mysql = mysql;
  }

  /** Type of of the MaxSCN handler */
  public Type getType()
  {
    return _type;
  }

  /**
   * The configuration for the file-system based MaxSCN handler; used only if {@link #getType()}
   * returns FILE . */
  public FileMaxSCNHandler.StaticConfig getFile()
  {
    return _file;
  }

  /**
   * Wired MaxSCN handler; used only if {@link #getType()} returns EXISTING . This setting makes
   * sense only of Spring-based configuration.
   *
   * NOTE: Non-standard naming to avoid being considered part of the bean interface. */
  public MaxSCNReaderWriter obtainExisting()
  {
    return _existing;
  }

  public MaxSCNReaderWriter createOrUseExisting()
  {
    MaxSCNReaderWriter result = null;

    switch (_type)
    {
      case FILE:
      try
      {
        result = FileMaxSCNHandler.create(_file);
      }
      catch (Exception e)
      {
        LOG.error("Unable to create FileMaxSCNHandler:" + e.getMessage(), e);
      }
      break;
      case IN_MEMORY: result = new InMemorySequenceNumberHandler(); break;
      case EXISTING: result = _existing; break;
      case DISABLED: result = null; break;
      default: throw new RuntimeException("unknown scn reader/writer type: " + _type.toString());
    }

    return result;
  }

  public SequenceNumberHandlerFactory createFactory()
  {
    SequenceNumberHandlerFactory result = null;

    switch (_type)
    {
      case FILE:
      try
      {
        FileMaxSCNHandler.Config configBuilder = new FileMaxSCNHandler.Config();
        configBuilder.setFlushItvl(_file.getFlushItvl());
        configBuilder.setInitVal(_file.getInitVal());
        configBuilder.setKey(_file.getKey());
        configBuilder.setScnDir(_file.getScnDir().getAbsolutePath());
        result = new FileMaxSCNHandlerFactory(configBuilder);
      }
      catch (Exception e)
      {
        LOG.error("Unable to create FileMaxSCNHandler:" + e.getMessage(), e);
      }
      break;
      case IN_MEMORY: result = new InMemorySequenceNumberHandlerFactory(-1); break;
      case DISABLED: result = null; break;
      case MYSQL : {
        MysqlMaxSCNHandler.Config configBuilder = new MysqlMaxSCNHandler.Config();
        configBuilder.setJdbcUrl(_mysql.getJdbcUrl());
        configBuilder.setScnTable(_mysql.getScnTable());
        configBuilder.setDriverClass(_mysql.getDriverClass());
        configBuilder.setDbPassword(_mysql.getDbPassword());
        configBuilder.setDbUser(_mysql.getDbUser());
        configBuilder.setFlushItvl(_mysql.getFlushItvl());
        configBuilder.setInitVal(_mysql.getInitVal());
        configBuilder.setUpsertSCNQuery(_mysql.getUpsertSCNQuery());
        configBuilder.setGetSCNQuery(_mysql.getGetSCNQuery());
        configBuilder.setScnColumnName(_mysql.getScnColumnName());

        result = new MysqlMaxSCNHandlerFactory(configBuilder);
      }break;
      default: throw new RuntimeException("unknown scn reader/writer type: " + _type.toString());
    }

    return result;
  }

}
