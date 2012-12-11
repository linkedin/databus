package com.linkedin.databus3.espresso.client.test;

import org.apache.log4j.Logger;

 public class ConsistencyChecker
 {
     private final TransactionTracker _tracker;
     private String _partition;
     public static final Logger LOG = 
    		 Logger.getLogger(ConsistencyChecker.class);
     
     ConsistencyChecker(TransactionTracker tracker)
     {
       _tracker = tracker;   
       _partition = null;
     }
     
     public void setPartition (String p) {
    	 _partition = p;
     }

     // If a we are not in a transaction boundary, then return false.
     // We are in a transaction boundary if:
     //     a. We have not seen any events
     //  OR b. The last event we saw was onEndDataEventSequence()
     public boolean onStartDataEventSequence(EspressoSCN scn)
     {
       if (_tracker.isInProgress())
       {
         LOG.error("Starting a transaction for partition " + _partition + " with SCN=" + scn +
                       " when already in one with SCN=" + _tracker.getSCN());
         return false;
       }
       return true;
     }

     // If we in a transaction boundary, return false.
     // If the SCN of the event does not match the one we received in onStartDataEventSequence()
     // then return false.
     private boolean onDataEvent(EspressoSCN scn, boolean dataEvent)
     {
       if (!_tracker.isInProgress())
       {
         LOG.error("Cannot get a " + (dataEvent ? "data event" : "end sequence event")
                    + "outside of a transaction for partition " + _partition);
         return false;
       }
       if (!scn.equals(_tracker.getSCN()))
       {
         LOG.error("Transaction start Partition=" + _partition + ",SCN(" + _tracker.getSCN() + ") != " +
                    (dataEvent ? "row" : "end") +  "SCN(" + scn + ")");
         return false;
       }
       return true;
     }

     public boolean onDataEvent(EspressoSCN scn)
     {
       return onDataEvent(scn, true);
     }

     public boolean onEndDataEventSequence(EspressoSCN scn)
     {
       if (!_tracker.isToBeDiscarded() && _tracker.getNumRequests() == 0)
       {
         LOG.error("No accumulated requests for partition " + _partition + " in SCN=" + scn);
         return false;
       }
       return onDataEvent(scn, false);
     }

}
