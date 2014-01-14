package com.linkedin.databus2.ggParser.XmlStateMachine;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.log4j.Logger;

import com.linkedin.databus.monitoring.mbean.GGParserStatistics.TransactionInfo;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.producers.gg.GGEventGenerationFactory;


public class TransactionState extends AbstractStateTransitionProcessor
{
  public final static String MODULE = TransactionState.class.getName();
  public final static Logger LOG = Logger.getLogger(MODULE);
  public static final String TRANSACTIONTIMESTAMPATTR = "timestamp";
  //when calculating transaction size we need to adjust it for the size of the first element. See parseElement()
  public static final int TRANSACTION_ELEMENT_SIZE = "<transaction timestamp=\"2013-07-29:13:26:15.000000\">".length();
  public TransactionSuccessCallBack _transactionSuccessCallBack;
  private final long  UNINITIALIZEDTS = -1;
  private long _currentTimeStamp = UNINITIALIZEDTS;
  private String _lastSeenTimestampStr = null;
  private long _startTransProcessingTimeNs = 0; //nano seconds
  private long _startTransLocation = 0;
  private long _transactionSize = 0;


  public TransactionState(TransactionSuccessCallBack transactionSuccessCallBack)
  {
    super(STATETYPE.STARTELEMENT,TRANSACTION);
    _transactionSuccessCallBack = transactionSuccessCallBack;
  }

  public long getCurrentTimeStamp()
  {
    return _currentTimeStamp;
  }

  public String getLastSeenTxnTimestampStr()
  {
    return _lastSeenTimestampStr;
  }

  @Override
  public void cleanUpState(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
  {
    _currentTimeStamp = UNINITIALIZEDTS;
    _startTransProcessingTimeNs = 0;
    _transactionSize = 0;
    _startTransLocation = 0;
  }

  @Override
  public void onEndElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws Exception
  {

    _currentStateType = STATETYPE.ENDELEMENT;
    if(LOG.isDebugEnabled())
       LOG.debug("The current transaction has " + stateMachine.dbUpdateState.getSourceDbUpdatesMap().size() + " DbUpdates");
    if(_transactionSuccessCallBack == null)
    {
      throw new DatabusException("No callback specified for the transaction state! Cannot proceed without a callback");
    }

    long endTransactionLocation = xmlStreamReader.getLocation().getCharacterOffset();
    _transactionSize =  endTransactionLocation - _startTransLocation;
    // collect stats
    long trTime = System.nanoTime() - _startTransProcessingTimeNs;
    long scn = stateMachine.dbUpdateState.getScn();
    TransactionInfo trInfo = new TransactionInfo(_transactionSize, trTime, _currentTimeStamp, scn);


    if(stateMachine.dbUpdateState.getSourceDbUpdatesMap().size() == 0)
    {
      if(LOG.isDebugEnabled())
         LOG.debug("The current transaction contains no dbUpdates, giving empty callback");
      _transactionSuccessCallBack.onTransactionEnd(null, trInfo);
    }
    else{
      List<PerSourceTransactionalUpdate> dbUpdates = sortDbUpdates(stateMachine.dbUpdateState.getSourceDbUpdatesMap());
      _transactionSuccessCallBack.onTransactionEnd(dbUpdates, trInfo);
    }

    stateMachine.dbUpdateState.cleanUpState(stateMachine,xmlStreamReader);
    cleanUpState(stateMachine,xmlStreamReader);
    xmlStreamReader.nextTag();
    setNextStateProcessor(stateMachine,xmlStreamReader);
  }

  @Override
  public void onStartElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {

    _currentStateType = STATETYPE.STARTELEMENT;
    for(int i = 0; i < xmlStreamReader.getAttributeCount() ; i++)
    {
      if(xmlStreamReader.getAttributeName(i).getLocalPart().equals(TRANSACTIONTIMESTAMPATTR))
      {
        StringBuilder timeStamp = new StringBuilder(xmlStreamReader.getAttributeValue(i));
        _lastSeenTimestampStr = timeStamp.toString();
        String correctedTimestamp = timeStamp.append("000").toString(); //The timestamp given by golden gate does not have nanoseconds accuracy needed by oracle timestamp
        _currentTimeStamp = GGEventGenerationFactory.ggTimeStampStringToNanoSeconds(correctedTimestamp);
      }
    }


    if(_currentTimeStamp == UNINITIALIZEDTS)
      throw new DatabusException("Unable to locate timestamp in the transaction tag in the xml");

    // start of new transaction
    if(_startTransProcessingTimeNs == 0) {
      _startTransProcessingTimeNs = System.nanoTime();
      _startTransLocation = xmlStreamReader.getLocation().getCharacterOffset();
      // this is location of the END of the <transaction timestamp="..."> entity
      // so we need to adjust the size of the transactions in bytes
      _startTransLocation -= TRANSACTION_ELEMENT_SIZE;
    }


    //create dbupdates list
    stateMachine.dbUpdateState.setSourceDbUpdatesMap(new HashMap<Integer, HashSet<DbUpdateState.DBUpdateImage>>());
    xmlStreamReader.nextTag();
    setNextStateProcessor(stateMachine,xmlStreamReader);
  }

  public void setCallBack(TransactionSuccessCallBack transactionSuccessCallBack)
  {
    _transactionSuccessCallBack = transactionSuccessCallBack;
  }

  private List<PerSourceTransactionalUpdate> sortDbUpdates(HashMap<Integer, HashSet<DbUpdateState.DBUpdateImage>> dbUpdates)
  {
    List<PerSourceTransactionalUpdate> sourceTransactionalUpdates = new ArrayList<PerSourceTransactionalUpdate>(dbUpdates.size());
    for(Map.Entry<Integer,HashSet<DbUpdateState.DBUpdateImage>> _entry: dbUpdates.entrySet())
    {
      sourceTransactionalUpdates.add(new PerSourceTransactionalUpdate(_entry.getKey(),_entry.getValue()));
    }
    Collections.sort(sourceTransactionalUpdates);
    return sourceTransactionalUpdates;
  }

  /**
   * The class is to hold the data associated with a particular source and the corrosponding dbUpdate
   */
  public static class PerSourceTransactionalUpdate implements Comparable<PerSourceTransactionalUpdate>{

    private int _sourceId;
    private Set<DbUpdateState.DBUpdateImage> _dbUpdate;


    public int getSourceId()
    {
      return _sourceId;
    }

    public Set<DbUpdateState.DBUpdateImage> getDbUpdatesSet()
    {
      return _dbUpdate;
    }

    public int getNumDbUpdates()
    {
      if ( null == _dbUpdate)
        return 0;

      return _dbUpdate.size();
    }

    public PerSourceTransactionalUpdate(int sourceId, Set<DbUpdateState.DBUpdateImage> dbUpdate)
    {
      this._sourceId = sourceId;
      this._dbUpdate = dbUpdate;
    }

    @Override
    public int compareTo(PerSourceTransactionalUpdate perSourceTransactionalUpdate)
    {
      if(this.getSourceId() == perSourceTransactionalUpdate.getSourceId())
      {
        LOG.error("The transactional update list cannot contain duplicate source IDs");
        return 0;
      }
      else if (this.getSourceId() < perSourceTransactionalUpdate.getSourceId())
        return -1;
      else
        return 1;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PerSourceTransactionalUpdate that = (PerSourceTransactionalUpdate) o;

      if (_sourceId != that._sourceId) return false;
      if (_dbUpdate != null ? !_dbUpdate.equals(that._dbUpdate) : that._dbUpdate != null) return false;

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = _sourceId;
      result = 31 * result + (_dbUpdate != null ? _dbUpdate.hashCode() : 0);
      return result;
    }
  }
}
