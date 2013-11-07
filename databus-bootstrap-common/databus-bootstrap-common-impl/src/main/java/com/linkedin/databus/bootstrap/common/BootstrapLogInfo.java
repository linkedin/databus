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

public class BootstrapLogInfo
{
  private long minWindowSCN;
  private long maxWindowSCN;
  private short srcId;
  private int logId;

  public long getMinWindowSCN()
  {
    return minWindowSCN;
  }

  public void setMinWindowSCN(long minWindowSCN)
  {
    this.minWindowSCN = minWindowSCN;
  }

  public long getMaxWindowSCN()
  {
    return maxWindowSCN;
  }

  public void setMaxWindowSCN(long maxWindowSCN)
  {
    this.maxWindowSCN = maxWindowSCN;
  }

  public short getSrcId()
  {
    return srcId;
  }

  public void setSrcId(short srcId)
  {
    this.srcId = srcId;
  }

  public int getLogId()
  {
    return logId;
  }

  public void setLogId(int logId)
  {
    this.logId = logId;
  }

  /*
   * @param logInfo LogInfo
   *
   * @return the LogTable name corresponding to the passed logInfo
   */
  public String getLogTable()
  {
    return "log_" + getSrcId() + "_" + getLogId();
  }

  @Override
  public String toString()
  {
    return "LogInfo [minWindowSCN=" + minWindowSCN + ", maxWindowSCN="
        + maxWindowSCN + ", srcId=" + srcId + ", logId=" + logId + "]";
  }
}
