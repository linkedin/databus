package com.linkedin.databus.bootstrap.producer;

import com.linkedin.databus.bootstrap.api.BootstrapProducerStatus;
import com.linkedin.databus.bootstrap.common.BootstrapConn;
import com.linkedin.databus.bootstrap.common.BootstrapDBCleaner;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO;
import com.linkedin.databus.bootstrap.common.BootstrapProducerStatsCollector;
import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.pub.CheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.monitoring.mbean.StatsCollectors;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.container.request.BootstrapDBException;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class DatabusBootstrapProducer extends DatabusHttpClientImpl
   implements BootstrapProducerCallback.ErrorCaseHandler
{
  public static final String MODULE = DatabusBootstrapProducer.class.getName();
  public static final Logger LOG    = Logger.getLogger(MODULE);

  private final HashSet<SourceInfo> _registeredPhysicalSources;
  private final List<String> _registeredSources;

  private final BootstrapApplierThread _applierThread;
  private final BootstrapDBPeriodicTriggerThread _periodicCleaner;
  private final BootstrapDBDiskSpaceTriggerThread _diskSpaceTriggerCleaner;

  private final BootstrapDBMetaDataDAO _dbDao;
  private final Map<String, Integer> _srcNameIdMap;

  protected final StatsCollectors<BootstrapProducerStatsCollector> _bootstrapProducerStatsCollectors;
  private final BootstrapProducerStatsCollector _applierStatsCollector;
  private final BootstrapProducerStaticConfig _bootstrapProducerStaticConfig;



		/**
   * @param config
   * @throws SQLException
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws DatabusClientException
   * @throws DatabusException
   * @throws BootstrapDatabaseTooOldException
   * @throws Exception
   */

  public DatabusBootstrapProducer(BootstrapProducerConfig config)
         throws IOException, InvalidConfigException, InstantiationException, IllegalAccessException,
                ClassNotFoundException, SQLException, DatabusClientException, DatabusException,
                BootstrapDBException
  {
    this(config.build());
  }

  public DatabusBootstrapProducer(BootstrapProducerStaticConfig bootstrapProducerStaticConfig)
         throws IOException, InvalidConfigException, InstantiationException, IllegalAccessException,
                ClassNotFoundException, SQLException, DatabusClientException, DatabusException,
                BootstrapDBException
  {
     super(bootstrapProducerStaticConfig.getClient());
      _registeredPhysicalSources = new HashSet<SourceInfo>();
      decouplePhysicalSources();
    _bootstrapProducerStaticConfig = bootstrapProducerStaticConfig;
    _registeredSources = new ArrayList<String>();
    _applierStatsCollector = new BootstrapProducerStatsCollector(getContainerStaticConfig().getId(), "bootstrapApplier", true, true, getMbeanServer(), null);
    _bootstrapProducerStatsCollectors = new StatsCollectors<BootstrapProducerStatsCollector>();

    _applierThread = new BootstrapApplierThread(MODULE,
                                               _registeredSources,
                                               _bootstrapProducerStaticConfig,
                                               _applierStatsCollector);
    BootstrapConn conn = new BootstrapConn();
    _dbDao = new BootstrapDBMetaDataDAO(conn,
    							bootstrapProducerStaticConfig.getBootstrapDBHostname(),
    							bootstrapProducerStaticConfig.getBootstrapDBUsername(),
    							bootstrapProducerStaticConfig.getBootstrapDBPassword(),
    							bootstrapProducerStaticConfig.getBootstrapDBName(),
    							false);
    _srcNameIdMap = new HashMap<String,Integer>();
    conn.initBootstrapConn(false,
    		 bootstrapProducerStaticConfig.getBootstrapDBUsername(),
    		 bootstrapProducerStaticConfig.getBootstrapDBPassword(),
    		 bootstrapProducerStaticConfig.getBootstrapDBHostname(),
    		 bootstrapProducerStaticConfig.getBootstrapDBName());
    initBootstrapDBMetadata();

    // callback should only be registered after DBMetadata is initialized.
			LOG.info("The Bootstrap Producer is configured for " + _registeredPhysicalSources.size() + " sources");
    for(SourceInfo sourceInfo : _registeredPhysicalSources)
    {
				LOG.info("Creating BootstrapProducer callback for PhysicalSource: " + sourceInfo.getPhysicalSourceName());
				BootstrapProducerStatsCollector producerStatsCollector = new BootstrapProducerStatsCollector(getContainerStaticConfig().getId(),
                                                                                                   sourceInfo.getPhysicalSourceName(), true, true, getMbeanServer(), sourceInfo.getLogicalSources());
      _bootstrapProducerStatsCollectors.addStatsCollector(sourceInfo.getPhysicalSourceName(), producerStatsCollector);
      registerProducerCallback(sourceInfo.getLogicalSources(), producerStatsCollector);
    }
    validateAndRepairBootstrapDBCheckpoint();

    BootstrapDBCleaner cleaner = new BootstrapDBCleaner("DBCleaner", _bootstrapProducerStaticConfig.getCleaner(), _bootstrapProducerStaticConfig, _applierThread, _registeredSources);
    _diskSpaceTriggerCleaner = new BootstrapDBDiskSpaceTriggerThread(cleaner, _bootstrapProducerStaticConfig.getCleaner().getDiskSpaceTrigger());
    _periodicCleaner = new BootstrapDBPeriodicTriggerThread(cleaner, _bootstrapProducerStaticConfig.getCleaner().getPeriodSpaceTrigger());
  }


  /**
   * The method helps identify the Physical sources that are listed in the bootstrap producer config (Which is really the client config)
   * The Physical sources are identified by comparing the list of subscriptions/logical sources.
   * There are two acceptable cases:
   * 1. Two relays provide different logical sources -> Two different physical sources
   * 2. Two relays provide the same logical sources (for load balancing/fault tolerance) -> Only one physical source
   * (The multitenant-client library underneath is aware that two relays provide the same logical sources)
   * @return A set of Physical sources which each contains a list of logical sources.
   *
   */
  private void decouplePhysicalSources()
  {
    DatabusHttpClientImpl.RuntimeConfig clientRtConfig = getClientConfigManager().getReadOnlyConfig();

    for (ServerInfo relayInfo: clientRtConfig.getRelays())
    {

       if(relayInfo == null || relayInfo.getSources() == null)
           LOG.error("No sources specified in the client config for the bootstrap producer");

			 SourceInfo sourceInfo = new SourceInfo(relayInfo.getPhysicalSourceName(), relayInfo.getSources());
			 _registeredPhysicalSources.add(sourceInfo);
    }
  }


  /**
   *
   *
   * Compares the Checkpoint  and bootstrap_producer_state's SCN to check against the possibility of gap in event consumption!!
   * If gap is found, makes best-effort to repair it.
   *
   * Three Scenario that should be allowed
   *
   * 1. Bootstrap Producer started after seeding
   *       In this case checkpoint SCN should not be greater than producer SCN.
   * 2. Bootstrap Producer started after adding sources without seeding
   *       In this case the producer SCN is "-1". The checkpoint "could" be empty
   * 3. Bootstrap Producer bounced
   *       In this case checkpoint SCN should not be greater than producer SCN.
   * @throws SQLException
   * @throws BootstrapDBException if there is a gap and cannot be repaired
   * @throws IOException                                                                                                    `
   */
  private void validateAndRepairBootstrapDBCheckpoint() throws SQLException, BootstrapDBException, IOException
  {
	 LOG.info("Validating bootstrap DB checkpoints !!");

	 for (List<DatabusSubscription> subsList : _relayGroups.keySet())
	 {
		 List<String> sourceNames = DatabusSubscription.getStrList(subsList);
		 long scn = -1;
		 try
		 {
			 scn = _dbDao.getMinWindowSCNFromStateTable(sourceNames, "bootstrap_producer_state");
		 } catch (BootstrapDBException ex ) {
			 LOG.error("Got exception while trying to fetch SCN from bootstrap_producer_state for sources :" + sourceNames, ex);
			 throw ex;
		 }


		 CheckpointPersistenceProvider provider = getCheckpointPersistenceProvider();
		 Checkpoint cp = provider.loadCheckpoint(sourceNames);
		 LOG.info("Bootstrap Producer SCN :" + scn + ", Checkpoint :" + cp);

		 if ( null != cp )
		 {
			if ( cp.getConsumptionMode() != DbusClientMode.ONLINE_CONSUMPTION)
			{
				 // Bootstrapping bootstrap Producer not yet supported !!
				 String msg = "Bootstrap Producer starting from non-online consumption mode for sources :" + sourceNames + ", Ckpt :" + cp;
				 LOG.error(msg);
				 throw new BootstrapDBException(msg);
			} else {
				String msg = null;
				if ( ((cp.getWindowScn() > scn) && ( scn > -1)) ||
				     ((cp.getWindowScn() < scn) && ( scn > -1)))
				{

					 if (((cp.getWindowScn() > scn) && ( scn > -1)))
						 LOG.warn("Non-Empty checkpint. Bootstrap Producer is at SCN:" + scn + ", while checkpoint is :"
				                      + cp + ", Could result in gap in event consumption. Repairing ckpt !!");
					 else
						 LOG.info("Non-Empty checkpoint. Bootstrap Producer is at SCN:" + scn + ", while checkpoint is :"
				                      + cp + ", Copying producer Scn to checkpoint !!");

					 cp.setWindowScn(scn);
					 cp.setWindowOffset(-1);
					 try
					 {
						 provider.removeCheckpoint(sourceNames);
						 provider.storeCheckpoint(sourceNames, cp);

						 // Check if persisted properly
						 cp = provider.loadCheckpoint(sourceNames);
						 if ( (null == cp)
							  || (cp.getWindowScn() != scn)
							  || (cp.getWindowOffset() != -1)
							  || (cp.getConsumptionMode() != DbusClientMode.ONLINE_CONSUMPTION))
						 {
							 msg = "Unable to repair and store the new checkpoint (" + cp + ") to make it same as producer SCN (" + scn + ") !!";
							 LOG.fatal(msg);
							 throw new BootstrapDBException(msg);
						 }
					 } catch ( IOException ex) {
						 msg = "Unable to repair and store the new checkpoint (" + cp + ") to make it same as producer SCN (" + scn + ") !!";
						 LOG.fatal(msg,ex);
						 throw new BootstrapDBException(msg);
					 }
				}
			}
		 } else {
			 /**
			  * Currently since bootstrapping is not available, a null ckpt would result in flexible checkpoint and could result in gap !!
			  */
			 if (scn > -1)
			 {
				 String msg = "Empty checkpoint. Bootstrap Producer SCN is at SCN:" + scn
						      + ", while checkpoint is null !! Could result in gap in event consumption. Repairing ckpt !!";
				 LOG.warn(msg);
				 cp = new Checkpoint();
				 cp.setWindowScn(scn);
				 cp.setWindowOffset(-1);
				 cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
				 try
				 {
					 provider.removeCheckpoint(sourceNames);
					 provider.storeCheckpoint(sourceNames, cp);

					 // Check if persisted properly
					 cp = provider.loadCheckpoint(sourceNames);
					 if ( (null == cp)
							 || (cp.getWindowScn() != scn)
							 || (cp.getWindowOffset() != -1)
							 || (cp.getConsumptionMode() != DbusClientMode.ONLINE_CONSUMPTION))
					 {
						 LOG.fatal("Unable to repair and store the checkpoint (" + cp + ") to make it same as producer SCN (" + scn + ") !!");
						 throw new BootstrapDBException(msg);
					 }
				 } catch ( IOException ex) {
					 msg = "Unable to repair and store the checkpoint (" + cp + ") to make it same as producer SCN (" + scn + ") !!";
					 LOG.fatal(msg,ex);
					 throw new BootstrapDBException(msg);
				 }
			 }
		 }
	 }
	 LOG.info("Validating bootstrap DB checkpoints done successfully!!");
  }

  private void registerProducerCallback(List<String> logicalSourceList, BootstrapProducerStatsCollector statsCollector) throws SQLException, DatabusClientException
  {

    // create callback for producer to populate data into log_* tables
    BootstrapProducerCallback bootstrapCallback =
      new BootstrapProducerCallback(_bootstrapProducerStaticConfig, statsCollector, this, logicalSourceList);
    registerDatabusStreamListener(bootstrapCallback, logicalSourceList,null);
  }

  private void initBootstrapDBMetadata() throws SQLException, BootstrapDatabaseTooOldException
  {
    DatabusHttpClientImpl.RuntimeConfig clientRtConfig = getClientConfigManager().getReadOnlyConfig();

    // create source list
    for (ServerInfo relayInfo: clientRtConfig.getRelays())
    {
      _registeredSources.addAll(relayInfo.getSources());

      for (String source : _registeredSources)
      {
    	BootstrapDBMetaDataDAO.SourceStatusInfo srcIdStatus = _dbDao.getSrcIdStatusFromDB(source, false);

		if (0 > srcIdStatus.getSrcId())
        {
		  int newState = BootstrapProducerStatus.NEW;

		  if ( ! _bootstrapProducerStaticConfig.isBootstrapDBStateCheck())
		  {
	          // TO allow test framework to listen to relay directly,DBStateCheck flag is used
			  newState = BootstrapProducerStatus.ACTIVE;
		  }
          _dbDao.addNewSourceInDB(source,newState);
        }

        srcIdStatus = _dbDao.getSrcIdStatusFromDB(source, false);
        _srcNameIdMap.put(source,srcIdStatus.getSrcId());

        if ( _bootstrapProducerStaticConfig.isBootstrapDBStateCheck())
        {
        	if (! BootstrapProducerStatus.isReadyForConsumption(srcIdStatus.getStatus()))
        		throw new BootstrapDatabaseTooOldException("Bootstrap DB is not ready to read from relay !! Status :" + srcIdStatus);
        }
      }
    }
  }

  public List<String> getRegisteredSources()
  {
    return _registeredSources;
  }

  @Override
  public void doStart()
  {
    super.doStart();
    if(_bootstrapProducerStaticConfig.getRunApplierThreadOnStart())
    	_applierThread.start();
    _diskSpaceTriggerCleaner.start();
    _periodicCleaner.start();
    LOG.info(DatabusBootstrapProducer.class.getName() + " is running ...");
  }

  public static void main(String [] args) throws Exception
  {
    //    Properties startupProps = BootstrapConfig.loadConfigProperties(args);
    Properties startupProps = ServerContainer.processCommandLineArgs(args);


    BootstrapProducerConfig producerConfig = new BootstrapProducerConfig();
    ConfigLoader<BootstrapProducerStaticConfig> staticProducerConfigLoader =
        new ConfigLoader<BootstrapProducerStaticConfig>("databus.bootstrap.",producerConfig);
    BootstrapProducerStaticConfig staticProducerConfig =  staticProducerConfigLoader.loadConfig(startupProps);

    DatabusBootstrapProducer bootstrapProducer = new DatabusBootstrapProducer(staticProducerConfig);
    bootstrapProducer.registerShutdownHook();
    bootstrapProducer.startAndBlock();
  }

  @Override
  protected void doShutdown()
  {
    super.doShutdown();
    if (_applierThread.isAlive())
    {
      _applierThread.shutdownAsynchronously();
      _applierThread.interrupt();
      _applierThread.awaitShutdownUniteruptibly();
    }

    if (_periodicCleaner.isAlive())
    {
    	_periodicCleaner.shutdownAsynchronously();
    	_periodicCleaner.interrupt();
    	_periodicCleaner.awaitShutdownUniteruptibly();
    }

    if (_diskSpaceTriggerCleaner.isAlive())
    {
    	_diskSpaceTriggerCleaner.shutdownAsynchronously();
    	_diskSpaceTriggerCleaner.interrupt();
    	_diskSpaceTriggerCleaner.awaitShutdownUniteruptibly();
    }
  }

  public StatsCollectors<BootstrapProducerStatsCollector> getProducerStatsCollectors() {
	  return _bootstrapProducerStatsCollectors;
  }

  public BootstrapProducerStatsCollector getApplierStatsCollector() {
	  return _applierStatsCollector;
  }


  @Override
  public void onErrorRetryLimitExceeded(String message, Throwable exception)
  {
	 LOG.fatal("Error Retry Limit reached. Message :(" + message + "). Stopping Bootstrap Producer Service. Exception Received :", exception);
	 doShutdown();
  }

	/* This class holds information about the physical source and the list of logical sources that belong to them
	 */
	private class SourceInfo
	{
			private String PhysicalSourceName;
			private List<String> logicalSources;


			public List<String> getLogicalSources() {
					return logicalSources;
			}

			public void setLogicalSources(List<String> logicalSources) {
					this.logicalSources = logicalSources;
			}

			public String getPhysicalSourceName() {
					return PhysicalSourceName;
			}

			public void setPhysicalSourceName(String physicalSourceName) {
					PhysicalSourceName = physicalSourceName;
			}


			private SourceInfo(String physicalSourceName, List<String> logicalSources) {
					PhysicalSourceName = physicalSourceName;
					this.logicalSources = logicalSources;
			}

			@Override
			public boolean equals(Object obj) {

					if (null == obj)
							return false;
					if (!(obj instanceof SourceInfo))
							return false;
					SourceInfo other = (SourceInfo) obj;
					return this.PhysicalSourceName.equals(other.getPhysicalSourceName());
			}
	}
}
