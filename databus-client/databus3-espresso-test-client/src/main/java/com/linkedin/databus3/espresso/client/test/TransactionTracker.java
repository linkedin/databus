package com.linkedin.databus3.espresso.client.test;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

   public class TransactionTracker
   {
     private EspressoSCN _scn;
     List <String> _espReqList;
     private String _partition;
     private long _createTime;
     private boolean _discardTransaction;
     public static final Logger LOG = 
    		 Logger.getLogger(TransactionTracker.class);
     public TransactionTracker()
     {
       _partition = null;
       _espReqList = new ArrayList<String>();
       reset();
     }
     
     public void SetParition (String p) {
    	 _partition = p;
     }

     public void reset()
     {
       _scn = null;
       _espReqList.clear();
       _createTime = 0L;
       _discardTransaction = false;
       _partition = null;
     }

     public void begin(EspressoSCN scn, String p, boolean discardTransaction) throws Exception
     {
       if (_scn != null)
       {
         LOG.error("Transaction currently in progress:SCN=" + _scn + ",partition=" + _partition);
         throw new Exception("Transaction currently in progress(SCN=" + _scn + "),partition=" + _partition);
       }
       _scn = scn;
       _partition = p;
       _discardTransaction = discardTransaction;
     }

     public EspressoSCN getSCN()
     {
       return _scn;
     }

     public void addRequest(String req)
     {
       if (!_discardTransaction)
       {
         _espReqList.add(req);
       }
     }

     /**
      * Commits all the requests accumulated so far into the local database
      * via the request processor.
      *
      * Resets the object in preparation for the next transaction, even if there
      * are errors when committing the current transaction.
      * @param  startingPrimarySCN the primarySCN when dbus client is started. it is used to determine whether we should skip
      *         primary writes.
      * @throws EspressoException
      * @throws InvalidEspressoRequestException
      */
     public void commit() throws Exception 
     {
       try
       {
         if (_discardTransaction)
         {
           return;
         }
         if (_espReqList.size() == 0) {
           // Happens only if we are using EspressoDatabusStreamConsumer.MockConsistencyChecker
           LOG.warn("Ignoring null data:SCN=" + _scn + ",partition=" + _partition);
           return;
         }
       }
       finally
       {
         reset();
       }
     }

     public boolean isInProgress()
     {
       return (_scn != null || _partition != null);
     }

     public boolean isToBeDiscarded()
     {
       return _discardTransaction;
     }

     public int getNumRequests()
     {
       return _espReqList.size();
     }

     public void setCreateTime(long createTime)
     {
       if (_createTime != 0L)
       {
         _createTime = createTime;
       }
     }

   }
