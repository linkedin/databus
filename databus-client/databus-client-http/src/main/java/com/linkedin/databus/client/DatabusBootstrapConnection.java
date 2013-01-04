package com.linkedin.databus.client;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus2.core.filter.DbusKeyFilter;

public interface DatabusBootstrapConnection
    extends DatabusServerConnection
{
  void requestTargetScn(Checkpoint checkpoint, DatabusBootstrapConnectionStateMessage stateReuse);

  void requestStartScn(Checkpoint checkpoint, DatabusBootstrapConnectionStateMessage stateReuse, String sourceNames);

  void requestStream(String sourcesIdList, DbusKeyFilter filter, int freeBufferSpace, Checkpoint cp,
                     DatabusBootstrapConnectionStateMessage stateReuse);

}
