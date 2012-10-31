package com.linkedin.databus3.espresso.client.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import com.linkedin.databus.client.consumer.DelegatingDatabusCombinedConsumer;
import com.linkedin.databus.client.data_model.MultiPartitionSCN;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DatabusCombinedConsumer;
import com.linkedin.databus.client.pub.DatabusV3Consumer;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.store.HelixPropertyStore;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.store.zk.ZkHelixPropertyStore;

// TODO : Change this consumer to DelegatingDatabusCombinedConsumer to PartitionMultiplexingConsumer
public class EspressoTestValidatingConsumer extends DelegatingDatabusCombinedConsumer implements DatabusV3Consumer
{

   public static final Logger LOG = 
		 Logger.getLogger(EspressoTestValidatingConsumer.class);

   // A flag to ensure that only one startConsumption call is made
   boolean _startConsumptionCalled = false;

   // SCN Map of the last event consumed ( per partition )
   private Map<String, EspressoSCN> _lastScn = new HashMap<String,EspressoSCN> ();
   // variables used for monitoring status of a transaction
   private final TransactionTracker _tracker;
   private String _currentPartition;
   
   private final ConsistencyChecker _consistencyChecker;
   private final DatabusSubscription[] _subs;

   private static String _zkAddress = null; 
   private static String _clusterName = null; 
   private static String _dbName = null;

   public static final String PROPERTYSTORE = "/PROPERTYSTORE";
   public static final String MASTER_GEN_INFO = "/PARTITION_GEN_INFO_V2";  

   private HighwaterMarkBackingStore<ZNRecord> _hwmBackingStore; 
   private MasterHighwaterMarkStore _masterHwmStore;  
   private HelixPropertyStore<ZNRecord> _partitionStateStore;
   private static final long LONG_LONG_SLEEP = 1000000000; //1B ms sleep
   private boolean _ignoreSCNHoles = false;
   public EspressoTestValidatingConsumer(DatabusCombinedConsumer delegate, Logger log,
       long maxPartitionTimeMs, boolean bootstrapEnabled, String zkAddress, String clusterName, String dbName, boolean ignoreSCNHoles,
       DatabusSubscription... subs) throws DatabusException
   {
	   super(delegate,log);
	   _tracker = new TransactionTracker();
	   _consistencyChecker = new ConsistencyChecker(_tracker);
	   _currentPartition = null;
	   _subs = subs;
	   _zkAddress = zkAddress;
	   _clusterName = clusterName;
	   _dbName = dbName;
	   _ignoreSCNHoles = ignoreSCNHoles;
	   _partitionStateStore = new ZkHelixPropertyStore<ZNRecord>(
			_zkAddress, new ZNRecordSerializer(), "/" + _clusterName
					+ PROPERTYSTORE);    
	   _hwmBackingStore = new PropertyStoreBasedHighwaterMarkStore(
			_partitionStateStore);
	   _masterHwmStore = new DefaultMasterHighwaterMarkStore(_hwmBackingStore,
	   		MASTER_GEN_INFO);
   }
  
   @Override
   public ConsumerCallbackResult onStartConsumption() 
   { 
	 if ( _startConsumptionCalled ) { 
		 LOG.error ( "Start Consumption Called Multiple Times");
	 }
     try
     {
         return super.onStartConsumption();
     }
     finally
     {
         _startConsumptionCalled = true;
     }
   }

   @Override
   public ConsumerCallbackResult onStopConsumption() {
	   try {
		 if ( !_startConsumptionCalled ) {
		     LOG.error ( "Stop Consumption Called without Start Consumption being called");
		     //***hack***for testing purposes, we dont want to be able to recover from this..
		     //putting a really long sleep here guarantees that client will not be able to progress, and test scripts will ERROR out waiting for client to reach hwm
		     Thread.sleep (LONG_LONG_SLEEP);		       
		     return ConsumerCallbackResult.ERROR;		     
		 }
	   }catch (Exception ex) {
		     LOG.error(ex.toString());
		     return ConsumerCallbackResult.ERROR;		   
	   }
       return super.onStopConsumption();
   }

   @Override
   public ConsumerCallbackResult onStartDataEventSequence(SCN startScn)
   {
	   try {
       // An existing Transaction cannot be in flight..
       if ( _tracker.isInProgress() == true ) {
	       LOG.error("Multiple Transactions in flight for event:" + startScn);
	       //***hack***for testing purposes, we dont want to be able to recover from this..
	       //putting a really long sleep here guarantees that client will not be able to progress, and test scripts will ERROR out waiting for client to reach hwm
	       Thread.sleep (LONG_LONG_SLEEP);
	       
	       return ConsumerCallbackResult.ERROR;
       }
       if ( _currentPartition != null ) {
	       LOG.error("Current Partition should be null for a new transaction but is:" + _currentPartition + " instead" );
	       //***hack***for testing purposes, we dont want to be able to recover from this..
	       //putting a really long sleep here guarantees that client will not be able to progress, and test scripts will ERROR out waiting for client to reach hwm
	       Thread.sleep (LONG_LONG_SLEEP);
	       return ConsumerCallbackResult.ERROR;
       }
       MultiPartitionSCN startScnMp = (MultiPartitionSCN) startScn;
	   if (!(startScn instanceof MultiPartitionSCN))
	   {
	       LOG.error("SCN not instance of MultiPartitionSCN partition:" + startScn.toString());
	       //***hack***for testing purposes, we dont want to be able to recover from this..
	       //putting a really long sleep here guarantees that client will not be able to progress, and test scripts will ERROR out waiting for client to reach hwm
	       Thread.sleep (LONG_LONG_SLEEP);
	       
	       return ConsumerCallbackResult.ERROR;
	   }
	   LOG.debug("Starting transaction partition for SCN=" + startScnMp);

	   } catch (Exception e) {
		     LOG.error(e.toString());
		     return ConsumerCallbackResult.ERROR;		   
	   }
     return super.onStartDataEventSequence(startScn);     
   }

   @Override
   public ConsumerCallbackResult onEndDataEventSequence(SCN endScn)
   {
	 MultiPartitionSCN endScnMp = (MultiPartitionSCN) endScn;
     EspressoSCN esnScnEsp = new EspressoSCN (endScnMp.partitionSeq(Integer.parseInt(_currentPartition)));
     LOG.debug("Ending transaction for partition=" + _currentPartition + 
    		 ",eventScn=" + esnScnEsp );
	 try
	 {
     
         if (!(endScn instanceof MultiPartitionSCN))
         {
	        LOG.error("SCN for partition " + _currentPartition + " not instance of MultiPartitionSCN "
            + esnScnEsp);
 	         //***hack***for testing purposes, we dont want to be able to recover from this..
	         //putting a really long sleep here guarantees that client will not be able to progress, and test scripts will ERROR out waiting for client to reach hwm
	         Thread.sleep (LONG_LONG_SLEEP);		 	        
            return ConsumerCallbackResult.ERROR;
	     }	    
	     if (!_consistencyChecker.onEndDataEventSequence(esnScnEsp))		 
	     {
		     //transaction is completed reset all status
		     _tracker.reset();
		     _currentPartition = null;
		     LOG.error("EOW failed consistancy check");
  	         //***hack***for testing purposes, we dont want to be able to recover from this..
	         //putting a really long sleep here guarantees that client will not be able to progress, and test scripts will ERROR out waiting for client to reach hwm
	         Thread.sleep (LONG_LONG_SLEEP);		 
	         return ConsumerCallbackResult.ERROR;
	     }
	     boolean discardTxn = _tracker.isToBeDiscarded();	      
	     if (!discardTxn)
	     {
		     // fake commit.	    	 
	    	 _tracker.commit();
	         _lastScn .put (_currentPartition, esnScnEsp);	     	     
	         //reset the currentPartition
	         _currentPartition = null;
	     }
	 }
	 catch(Exception e)
	 {
	     LOG.error("Exception applying transaction partition=" + _currentPartition + ",eventScn=" +
	    		  + endScnMp.partitionSeq(Integer.parseInt(_currentPartition)));
	     LOG.error(e.toString());
	     return ConsumerCallbackResult.ERROR;
	 }
	   
     return super.onEndDataEventSequence(endScn);
   }
   
   @Override
   public ConsumerCallbackResult onDataEvent(DbusEvent e, DbusEventDecoder eventDecoder)
   {	 
     long createTime = e.timestampInNanos();
     try
     {
     
     EspressoSCN scn = new EspressoSCN(e.sequence());
     
     //This is the first onDataEvent callback in this window.
     //A little bit of a hack. This logic should really be in OnStartDataEventSequence
     //but in the OnStartDataEventSequence we do not know which partition the next sequence of events     
     // is coming from.     
     if ( _currentPartition == null) {
  	   //clear the previous status 
         boolean discardTxn = false;    	 
    	_currentPartition = Short.toString(e.physicalPartitionId());         	
    	
        if (!_consistencyChecker.onStartDataEventSequence(scn))
        {
 	       //***hack***for testing purposes, we dont want to be able to recover from this..
	       //putting a really long sleep here guarantees that client will not be able to progress, and test scripts will ERROR out waiting for client to reach hwm
	       Thread.sleep (LONG_LONG_SLEEP);		         	
           return ConsumerCallbackResult.ERROR;
        }
        if (_lastScn.get(_currentPartition) != null )
        {
          // Databus guarantees at least once delivery. We allow duplicate events
          // from old generations as well.
          if (scn.getGeneration() < _lastScn.get(_currentPartition).getGeneration())
          {
            // Should be a rare case, print a warning here.
            LOG.warn("Re-delivery of event from a previous generation:partition=" 
            + _currentPartition + ",lastScn=" + _lastScn.get(_currentPartition) + ",eventScn=" + scn);
            discardTxn = true;            
          }
          else if (scn.getGeneration() > _lastScn.get(_currentPartition).getGeneration())
          {
            try
            {
              Partition partition = new Partition ( _dbName, (int)Integer.valueOf(_currentPartition));
              if ( !_ignoreSCNHoles )
              {
                  ConsistencyCheckerGlobal chkGbl = new ConsistencyCheckerGlobal ( partition, _masterHwmStore);                            
                  chkGbl .checkConsistency(new EspressoSCN(_lastScn .get(_currentPartition).getGeneration(), _lastScn.get(_currentPartition).getSequence()+1),
                      _lastScn.get(_currentPartition).getGeneration(),
                      scn.getGeneration());
              }
            }
            catch (Exception ex)
            {
              LOG.error("ClusterManager global consistency check failed:partition=" + _currentPartition + ":" + ex);
    	      //***hack***for testing purposes, we dont want to be able to recover from this..
   	          //putting a really long sleep here guarantees that client will not be able to progress, and test scripts will ERROR out waiting for client to reach hwm
   	          Thread.sleep (LONG_LONG_SLEEP);
              return ConsumerCallbackResult.ERROR;
            }
          } 
          else
          {
            // Generation numbers are equal, compare sequence numbers.
            if (scn.getSequence() <= _lastScn.get(_currentPartition).getSequence())
            {
              
              // Keep this log in debug level, may be a common case in databus.
              LOG.debug("Re-delivery of event within current generation:partition=" + 
                   _currentPartition +",lastScn=" + _lastScn.get(_currentPartition) + ",eventScn=" + scn);
              discardTxn = true;              
            }
            else if (scn.getSequence() == _lastScn.get(_currentPartition).getSequence() + 1)
            {
                // We got event in strict order.
            	discardTxn = false;
            }
            else
            {
                // We skipped events.
                if ( !_ignoreSCNHoles)
            	{
                    LOG.error("Detected missing events:partition=" + 
                    _currentPartition + ",lastScn=" + _lastScn.get(_currentPartition).getSequence() + ",eventScn=" + scn);
    	            //***hack***for testing purposes, we dont want to be able to recover from this..
   	                //putting a really long sleep here guarantees that client will not be able to progress, and test scripts will ERROR out waiting for client to reach hwm
   	                Thread.sleep (LONG_LONG_SLEEP);		         	
                    return ConsumerCallbackResult.ERROR;
            	} else {
                    LOG.warn("Detected missing events but Ignore SCN Holes Flag is set to TRUE:partition=" + 
                    _currentPartition + ",lastScn=" + _lastScn.get(_currentPartition).getSequence() + ",eventScn=" + scn);            		
            	}
            }
          }
        }
        try
        {
          _tracker.begin(scn, _currentPartition, discardTxn);
        }
        catch(Exception ex)
        {
          return ConsumerCallbackResult.ERROR;
        }    	 
     } // end check for first event in a window 
     
     if ( !_currentPartition .equals(Short.toString(e.physicalPartitionId()))) {
    	 //partitions mismatch
	      LOG.error(": New parition found in middle of a window:" +
	    			  "(CurrentPartition)"+ _currentPartition+ 
	    			  "(NewPartition)" + Short.toString(e.physicalPartitionId()));
	      //***hack***for testing purposes, we dont want to be able to recover from this..
	      //putting a really long sleep here guarantees that client will not be able to progress, and test scripts will ERROR out waiting for client to reach hwm
	      Thread.sleep (LONG_LONG_SLEEP);	      
    	 return ConsumerCallbackResult.ERROR;
     }     
     if (!_consistencyChecker.onDataEvent(scn))
     {
       _tracker.reset();    	 
	   //***hack***for testing purposes, we dont want to be able to recover from this..
       //putting a really long sleep here guarantees that client will not be able to progress, and test scripts will ERROR out waiting for client to reach hwm
       Thread.sleep (LONG_LONG_SLEEP);		         	       
       return ConsumerCallbackResult.ERROR;
     }
     
     //check to see that the srcId for this event is in the subscription list
     //boolean subForEventFound = false;    
     //for ( DatabusSubscription sub : _subs) {
    //	 LOG.info("Physical Partition Name(sub):" + sub.getPhysicalPartition().getName() + " currentPartition:" + _currentPartition  + "\n");
     //    if ( sub.getPhysicalPartition().getName() .equalsIgnoreCase(_currentPartition) ){
      //  	 LOG.info("Sub Logical Partition srcId(sub):" + sub.getLogicalPartition().getId() + " event SrcId:" + e.srcId()  + "\n");
     //   	 if ( sub.getLogicalPartition().getId() == e.srcId() ) {
     //   		 
     //   		 subForEventFound = true;
      //  	 }
      //   }
     //}
     
     //On DataEvent has a event with Src Id we cannot match in the subscription. This is an error
     //if (!subForEventFound) {
  	   //***hack***for testing purposes, we dont want to be able to recover from this..
         //putting a really long sleep here guarantees that client will not be able to progress, and test scripts will ERROR out waiting for client to reach hwm
 //        Thread.sleep (LONG_LONG_SLEEP);		         	       
 //        return ConsumerCallbackResult.ERROR;    	 
  //   }
     
     // We don't get the create time when we start the transaction
     // so set it from here. Ideally, we need to set the create time only for
     // the first data event.
     _tracker.setCreateTime(createTime);     
     // make up a fake request
     String req = _currentPartition + ":" + scn.toString();
     _tracker.addRequest(req);     
     
     } catch (Exception e2) {
	     LOG.error(e2.toString());
	     return ConsumerCallbackResult.ERROR;		   
     }
     return super.onDataEvent(e, eventDecoder);
   }

   @Override
   public ConsumerCallbackResult onRollback(SCN rollbackScn)
   {
     MultiPartitionSCN rollbackScnMp = (MultiPartitionSCN) rollbackScn;
	  if (_tracker.isInProgress())
	  {
	      // A rollback is happening in the middle of a transaction in progress. Reset it.
	      LOG.info("Rolling back within a transaction: Partition=" +
	    			 _currentPartition+ "SCN= " +
	    			 + rollbackScnMp.partitionSeq(Integer.parseInt(_currentPartition)));
	      _currentPartition = null;
	      _tracker.reset();
	  }
     // TODO Auto-generated method stub
     return super.onRollback(rollbackScn);
   }

   @Override
   public ConsumerCallbackResult onCheckpoint(SCN checkpointScn)
   {
     MultiPartitionSCN checkpointScnMp = (MultiPartitionSCN) checkpointScn;
     //cannot call onCheckpoint in the middle of a winodw!
     try {
     if (_tracker.isInProgress())
	 {
	     LOG.error("consumer Checkpointing in middle of a Window " + 
		 _currentPartition+ "SCN= " +
		 + checkpointScnMp.partitionSeq(Integer.parseInt(_currentPartition)));
	     _tracker.reset();
	     _currentPartition = null;
	      //***hack***for testing purposes, we dont want to be able to recover from this..
	      //putting a really long sleep here guarantees that client will not be able to progress, and test scripts will ERROR out waiting for client to reach hwm
	      Thread.sleep (LONG_LONG_SLEEP);		         		     
		  return ConsumerCallbackResult.ERROR;
	  }
     } catch ( Exception e) {
	     LOG.error(e.toString());     
	     return ConsumerCallbackResult.ERROR;
     }
     return super.onCheckpoint(checkpointScn);
   }

   @Override
   public ConsumerCallbackResult onStartBootstrapSequence(SCN startScn)
   {
     return super.onStartBootstrapSequence(startScn);
   }

   @Override
   public ConsumerCallbackResult onEndBootstrapSequence(SCN endScn)
   {
     return super.onEndBootstrapSequence(endScn);
   }

   @Override
   public ConsumerCallbackResult onBootstrapRollback(SCN batchCheckpointScn)
   {
     return super.onBootstrapRollback(batchCheckpointScn);
   }

   @Override
   public ConsumerCallbackResult onBootstrapCheckpoint(SCN
batchCheckpointScn)
   {
     return super.onBootstrapCheckpoint(batchCheckpointScn);
   }

   @Override
   public boolean canBootstrap()
   {
     return false;
   }
}
   
