package com.linkedin.databus.bootstrap.common;

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
 * This class provides helper queries used for execution in the BootstrapDBCleaner context
 * This is a singleton class, and expected to be held by multiple objects and invoked in
 * various thread contexts. So, the methods are expected to be thread-safe by only operating
 * with thread-local storage
 */
public class BootstrapDBCleanerQueryHelper
{
  private static BootstrapDBCleanerQueryHelper _singletonObj = null;

  public static BootstrapDBCleanerQueryHelper getInstance()
  {
    if (_singletonObj == null)
    {
      _singletonObj = new BootstrapDBCleanerQueryHelper();
    }
    return _singletonObj;
  }

  private BootstrapDBCleanerQueryHelper()
  {
    // Singleton class
  }

  public String getMarkDeletedStmt()
  {
    StringBuilder sql = new StringBuilder();

    sql.append("UPDATE bootstrap_loginfo ");
    sql.append(" set deleted = 1");
    sql.append(" where srcid = ?");
    sql.append(" and logid = ?");
    return sql.toString();
  }

  public String getCandidateLogIdsForSrcStmt()
  {
    StringBuilder sql = new StringBuilder();
    sql.append("select logid, minwindowscn, maxwindowscn from bootstrap_loginfo ");
    sql.append("where srcid = ?");
    sql.append(" and maxwindowscn < ? ");
    sql.append(" and deleted = 0 ");
    sql.append(" and logid < ( select max(logid) from bootstrap_loginfo where srcid = ? )");
    sql.append(" order by logid desc");
    return sql.toString();
  }

  /*
   *
   * @return the SrcTable name corresponding to the passed logInfo
   */
  public String getSrcTable(int srcid)
  {
    return "tab_" + srcid;
  }

  public String getUpdateLogStartSCNStmt()
  {
    StringBuilder sql = new StringBuilder();
    sql.append("update bootstrap_sources set logstartscn = ?");
    sql.append(" where id = ?");
    return sql.toString();
  }

  public String getFirstLogTableWithGreaterSCNStmt()
  {
    StringBuilder sql = new StringBuilder();
    sql.append("select logid, minwindowscn, maxwindowscn from bootstrap_loginfo ");
    sql.append("where srcid = ?");
    sql.append(" and minwindowscn > ? ");
    sql.append(" order by logid asc limit 1");
    return sql.toString();
  }

}
