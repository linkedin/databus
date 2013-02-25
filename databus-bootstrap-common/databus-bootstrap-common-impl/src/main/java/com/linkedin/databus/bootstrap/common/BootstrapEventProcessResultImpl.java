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


import com.linkedin.databus.bootstrap.api.BootstrapEventProcessResult;

public class BootstrapEventProcessResultImpl implements BootstrapEventProcessResult
{
  private boolean _isClientBufferLimitExceeded;
  private boolean _isDropped;
  private long _processedRowCount;
  
  public BootstrapEventProcessResultImpl(long processedRowCount, boolean isLimitExceeded, boolean dropped)
  {
    _isClientBufferLimitExceeded = isLimitExceeded;
    _processedRowCount = processedRowCount;
    _isDropped = dropped;
  }
  
  @Override
  public long getProcessedRowCount()
  {
    return _processedRowCount;
  }

  @Override
  public boolean isClientBufferLimitExceeded()
  {
    return _isClientBufferLimitExceeded;
  }
  
  @Override
  public boolean isDropped()
  {
    return _isDropped;
  }
  

  @Override
  public String toString()
  {
    return new String("isLimitExcceed=" + _isClientBufferLimitExceeded +
                      "; processedRowCount=" + _processedRowCount);
  }
}
