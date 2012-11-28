/**
 * 
 */
package com.linkedin.databus.bootstrap.test;

import java.io.IOException;
import java.sql.SQLException;

import com.linkedin.databus.bootstrap.api.BootstrapEventCallback;
import com.linkedin.databus.bootstrap.api.BootstrapProcessingException;
import com.linkedin.databus.bootstrap.common.BootstrapConfig;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.bootstrap.server.BootstrapProcessor;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.util.InvalidConfigException;

/**
 * @author ksurlake
 *
 */
public class DatabusBootstrapClient {
	
	private Checkpoint _initState;
	private Checkpoint _currState;
	private BootstrapProcessor _dbProcessor;
	private int _catchupSource = 0;
	private int _snapshotSource = 0;
	private String sources[];
	
	public DatabusBootstrapClient(Checkpoint initCheckpoint, String bootstrapSources[]) 
	       throws InstantiationException, IllegalAccessException, ClassNotFoundException, 
	              SQLException, IOException, InvalidConfigException
	{
		_initState = initCheckpoint;
		_currState = _initState;
		sources = bootstrapSources;
		init();
	}
	
	public DatabusBootstrapClient(String bootstrapSources[]) 
	       throws InstantiationException, IllegalAccessException, ClassNotFoundException, 
	              SQLException, IOException, InvalidConfigException
	{
		init();
		
		long startScn = 0; /* getProcessor().getCurrentScn();*/
		long targetScn = startScn;
		
		sources = bootstrapSources;
		
		_initState = new Checkpoint();
		_initState.setBootstrapStartScn(startScn);
		_initState.setBootstrapTargetScn(targetScn);
		_initState.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
		_initState.setSnapshotSource(sources[0]);
		_initState.setSnapshotOffset(0);
		_initState.setBootstrapSinceScn(Long.valueOf(12345));
		_currState = _initState;
	}
	
	private void init() throws InstantiationException, IllegalAccessException, 
	                           ClassNotFoundException, SQLException, IOException,
	                           InvalidConfigException
	{
	  BootstrapConfig configBuilder = new BootstrapConfig();
	  BootstrapReadOnlyConfig staticConfig = configBuilder.build();
	  _dbProcessor = new BootstrapProcessor(staticConfig, null);
	}
	
	public void setSources(String bootstrapSources[])
	{
	  sources = bootstrapSources;
	}
	
	public Checkpoint getNextBatch(int batchSize, BootstrapEventCallback callBack) 
	  throws SQLException, BootstrapProcessingException, BootstrapDatabaseTooOldException
	{
		boolean phaseCompleted = false;
		
		if (_currState.getConsumptionMode() == DbusClientMode.ONLINE_CONSUMPTION)
		{
			return _currState;
		}
		
		if (_currState.getConsumptionMode() == DbusClientMode.BOOTSTRAP_SNAPSHOT)
		{
			phaseCompleted = getProcessor().streamSnapShotRows(_currState, callBack);
			_currState.bootstrapCheckPoint();
			
			// If there are no more rows to be fetched from the current source, move to the catchup phase.
			if (phaseCompleted)
			{
				_catchupSource = 0;
				_currState.setCatchupSource(sources[_catchupSource]);
				_currState.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
				_currState.setWindowScn(_currState.getBootstrapStartScn());
			//	_currState.setBootstrapTargetScn(getProcessor().getCurrentScn());
				_currState.setWindowOffset(0);
			}
		}
		else
		{
		    phaseCompleted = getProcessor().streamCatchupRows(_currState, callBack);
			_currState.bootstrapCheckPoint();
			
			// If there are no more rows to be fetched from the current source, go the next source
			if (phaseCompleted)
			{	
				// if We are done with catchup, move to the next snapshot source
				if (_catchupSource == _snapshotSource)
				{
					_catchupSource = 0;
					_snapshotSource++;
					
					_currState.setBootstrapStartScn(_currState.getBootstrapTargetScn());
					
					// If we are done snapshotting all sources, we are done
					if (_snapshotSource == sources.length)
					{
						_currState.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
					}
					else
					{
						_currState.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
						_currState.setSnapshotSource(sources[_snapshotSource]);
						_currState.setSnapshotOffset(0);
					}
				}
				else
				{
					_catchupSource++;
					_currState.setCatchupSource(sources[_catchupSource]);
					_currState.setWindowScn(_currState.getBootstrapStartScn());
					_currState.setWindowOffset(0);					
				}
			}
			
		}
		
		return _currState;
	}
	
	private BootstrapProcessor getProcessor()
	{
		return _dbProcessor;
	}
}
