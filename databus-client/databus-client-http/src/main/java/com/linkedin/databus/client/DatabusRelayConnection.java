package com.linkedin.databus.client;

import com.linkedin.databus.core.CheckpointMult;
import com.linkedin.databus.core.util.Range;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilter;

public interface DatabusRelayConnection 
   extends DatabusServerConnection
{

  void requestSources(DatabusRelayConnectionStateMessage stateReuse);

  void requestRegister(String sourcesIdList, DatabusRelayConnectionStateMessage stateReuse);

  void requestStream(String sourcesIdList, DbusKeyCompositeFilter filter,
		             int freeBufferSpace, CheckpointMult cp, Range keyRange,
                     DatabusRelayConnectionStateMessage stateReuse);

  /* Callback to let the Relay client request streamFromLatestSCN from the relay */
  void enableReadFromLatestScn(boolean enable);
}
