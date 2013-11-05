package com.linkedin.databus.client;
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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jboss.netty.channel.Channel;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.BootstrapCheckpointHandler;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventFactory;
import com.linkedin.databus.core.DbusEventInternalReadable;
import com.linkedin.databus.core.InternalDatabusEventsListener;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.container.request.RegisterResponseMetadataEntry;

public class ConnectionState implements DatabusRelayConnectionStateMessage,
                                        DatabusBootstrapConnectionStateMessage
{

  public enum StateId
  {
    INITIAL,
    PICK_SERVER,
    REQUEST_SOURCES,
    SOURCES_REQUEST_SENT,
    SOURCES_RESPONSE_SUCCESS,
    SOURCES_REQUEST_ERROR,
    SOURCES_RESPONSE_ERROR,
    REQUEST_REGISTER,
    REGISTER_REQUEST_SENT,
    REGISTER_RESPONSE_SUCCESS,
    REGISTER_REQUEST_ERROR,
    REGISTER_RESPONSE_ERROR,
    REQUEST_STREAM,
    STREAM_REQUEST_SENT,
    STREAM_REQUEST_SUCCESS,
    STREAM_REQUEST_ERROR,
    STREAM_RESPONSE_ERROR,
    STREAM_RESPONSE_DONE,
    CLOSED,
    BOOTSTRAP_REQUESTED,
    BOOTSTRAP,
    REQUEST_START_SCN,
    START_SCN_REQUEST_SENT,
    START_SCN_RESPONSE_SUCCESS,
    START_SCN_REQUEST_ERROR,
    START_SCN_RESPONSE_ERROR,
    REQUEST_TARGET_SCN,
    TARGET_SCN_REQUEST_SENT,
    TARGET_SCN_RESPONSE_SUCCESS,
    TARGET_SCN_REQUEST_ERROR,
    TARGET_SCN_RESPONSE_ERROR,
    START_SNAPSHOT_PHASE,
    START_CATCHUP_PHASE,
    BOOTSTRAP_DONE
  }

  private StateId _stateId;

  //INIITIAL
  private final DbusEventBuffer _dataEventsBuffer;
  private DatabusHttpClientImpl.StaticConfig _clientConfig;
  protected final List<DatabusSubscription> _subscriptions;
  private final List<String> _sourcesNames;
  protected  BootstrapCheckpointHandler _bstCheckpointHandler = null;
  private final String _sourcesNameList;

  //PICK_SERVER extends INITIAL
  private boolean _isRelayFellOff = false;

  //REQUEST_SOURCES extends PICK_SERVER
  private InetSocketAddress _serverInetAddress;
  private ServerInfo _currentServerInfo;
  private DatabusRelayConnection _relayConnection;

  //REQUEST_START_SCN extends PICK_SERVER
  private DatabusBootstrapConnection _bootstrapConnection;

  //SOURCES_RESPONSE_SUCCESS extends REQUEST_SOURCES
  private List<IdNamePair> _sources;
  protected Map<Long, IdNamePair> _sourcesIdMap = new HashMap<Long, IdNamePair>(20);
  protected Map<String, IdNamePair> _sourcesNameMap = new HashMap<String, IdNamePair>(20);
  //private DbusKeyCompositeFilter _filter = new DbusKeyCompositeFilter();

  //REQUEST_SOURCES_SCHEMAS extends SOURCES_READY
  private String _sourcesIdListString;
  private String _subsListString;

  //SOURCES_SCHEMAS_READY extends READ_SOURCES_SCHEMAS
  private Map<Long, List<RegisterResponseEntry>> _sourcesSchemas;  // mandatory, else can't make callbacks to consumer
  private Map<Long, List<RegisterResponseEntry>> _keysSchemas;
  private List<RegisterResponseMetadataEntry> _metadataSchemas;

  //REQUEST_DATA_EVENTS extends SOURCES_SCHEMAS_READY
  private Checkpoint _checkpoint;
  private boolean _scnRegress = false;
  private boolean _flexibleCheckpointRequest = false;

  private final List<InternalDatabusEventsListener> _readEventListeners;


  //READ_DATA_EVENTS extends REQUEST_DATA_EVENTS
  private ChunkedBodyReadableByteChannel _readChannel;

  //START_SNAPSHOT_PHASE

  private Channel _startScnChannel;


  private ServerInfo _currentBSServerInfo;

  private String _hostName, _svcName;

  //DATA_EVENTS_DONE extends READ_DATE_EVENTS

  protected ConnectionState(DbusEventBuffer dataEventsBuffer, List<String> sourcesNames, List<DatabusSubscription> subs)
  {
    _stateId = StateId.INITIAL;
    _dataEventsBuffer = dataEventsBuffer;
    _readEventListeners = new ArrayList<InternalDatabusEventsListener>();

    if(subs != null)
    {
      _subscriptions = new ArrayList<DatabusSubscription>(subs);
      _sourcesNames = DatabusSubscription.getStrList(_subscriptions);
    }
    else if(sourcesNames != null)
    {
      _subscriptions = DatabusSubscription.createSubscriptionList(sourcesNames);
      _sourcesNames = new ArrayList<String>(sourcesNames);
    }
    else
    {
      throw new IllegalArgumentException("both sources and subscriptions are null");
    }

    StringBuilder sb = new StringBuilder();
    boolean firstSource = true;
    for (String sourceName: _sourcesNames)
    {
      if (! firstSource) sb.append(',');
      sb.append(sourceName);
      firstSource = false;
    }
   _sourcesNameList = sb.toString();
   _hostName = "";
   _svcName = "";

    // The bootstrap checkpoint handler will be constructed when we get the final list of sources from the relay.
  }

  // This method is overridden for V3
  public void createBootstrapCheckpointHandler()
  {
    assert _bstCheckpointHandler == null : "BootstrapCheckpointHandler already initialized " + _bstCheckpointHandler.toString();
    _bstCheckpointHandler = new BootstrapCheckpointHandler(_sourcesNames);
  }

  public void expandSubscriptions()
  {
    // For V3, the table names are known after the relay responds with the source list,
    // so this method is overridden to replace _subscriptions and _sourcesNames accordingly.
  }

  public StateId getStateId()
  {
    return _stateId;
  }

  public ConnectionState switchToPickServer()
  {
    _stateId = StateId.PICK_SERVER;

    return this;
  }

  public ConnectionState switchToRequestSources(ServerInfo relayInfo,
                                                InetSocketAddress relayInetAddr,
                                                DatabusRelayConnection relayConnection)
  {
    //TODO HIGH : add checks for valid input states
    _stateId = StateId.REQUEST_SOURCES;
    _serverInetAddress = relayInetAddr;
    _currentServerInfo = relayInfo;
    _relayConnection = relayConnection;
    return this;
  }

  @Override
  public void switchToSourcesRequestError()
  {
    _stateId = StateId.SOURCES_REQUEST_ERROR;
  }

  @Override
  public void switchToSourcesResponseError()
  {
    _stateId = StateId.SOURCES_RESPONSE_ERROR;
  }

  @Override
  public void switchToSourcesSuccess(List<IdNamePair> sources, String hostName, String svcName)
  {
    _stateId = StateId.SOURCES_RESPONSE_SUCCESS;
    setSourcesIds(sources);
    setTrackingInfo(hostName, svcName);
  }

  public void setSourcesIds(List<IdNamePair> sources)
  {
    _sources = sources;
    _sourcesIdMap.clear();
    _sourcesNameMap.clear();
    for (IdNamePair source: _sources)
    {
      _sourcesIdMap.put(source.getId(), source);
      _sourcesNameMap.put(source.getName(), source);
    }
    expandSubscriptions();
  }

  public void setTrackingInfo(String hostName, String svcName)
  {
    _hostName = hostName;
    _svcName = svcName;
  }

  public String getHostName()
  {
    return _hostName;
  }

  public String getSvcName()
  {
    return _svcName;
  }

  // This method is overridden for V3
  public DbusEventInternalReadable createEopEvent(Checkpoint cp, DbusEventFactory eventFactory)
      throws DatabusException
  {
    return eventFactory.createLongKeyEOPEvent(cp.getWindowScn(), (short) 0);
  }

  public ConnectionState switchToRequestSourcesSchemas(String sourcesIdListString,
                                                       String subsListString)
  {
    _stateId = StateId.REQUEST_REGISTER;
    setSourcesIdListString(sourcesIdListString) ;
    setSubsListString(subsListString);
    return this;
  }

  public void setSourcesIdListString(String sourcesIdListString)
  {
    _sourcesIdListString = sourcesIdListString;
  }

  public void setSubsListString(String subsListString)
  {
    _subsListString = subsListString;
  }

  @Override
  public void switchToRegisterSuccess(Map<Long, List<RegisterResponseEntry>> sourcesSchemas,
                                      Map<Long, List<RegisterResponseEntry>> keysSchemas,
                                      List<RegisterResponseMetadataEntry> metadataSchemas)
  {
    _stateId = StateId.REGISTER_RESPONSE_SUCCESS;
    setSourcesSchemas(sourcesSchemas);
    setKeysSchemas(keysSchemas);      // OK if null
    setMetadataSchemas(metadataSchemas);  // OK if null
  }

  public void setSourcesSchemas(Map<Long, List<RegisterResponseEntry>> sourcesSchemas)
  {
    _sourcesSchemas = sourcesSchemas;
  }

  public void setKeysSchemas(Map<Long, List<RegisterResponseEntry>> keysSchemas)
  {
    _keysSchemas = keysSchemas;
  }

  public void setMetadataSchemas(List<RegisterResponseMetadataEntry> metadataSchemas)
  {
    _metadataSchemas = metadataSchemas;
  }

  public ConnectionState switchToRequestStream(Checkpoint checkpoint)
  {
    _stateId = StateId.REQUEST_STREAM;
    if (_checkpoint != checkpoint || _readEventListeners.isEmpty())
    {
      _checkpoint = checkpoint;
      _readEventListeners.clear();
      _readEventListeners.add(_checkpoint);
    }

    return this;
  }


  @Override
  public void switchToStreamSuccess(ChunkedBodyReadableByteChannel readChannel)
  {
    _stateId = StateId.STREAM_REQUEST_SUCCESS;
    _readChannel = readChannel;
  }

  public void switchToStreamResponseDone()
  {
    _stateId = StateId.STREAM_RESPONSE_DONE;
  }

  public ConnectionState switchToClosed()
  {
    _stateId = StateId.CLOSED;
    return this;
  }

  @Override
  public void switchToRegisterRequestError()
  {
    _stateId = StateId.REGISTER_REQUEST_ERROR;
  }

  @Override
  public void switchToRegisterResponseError()
  {
    _stateId = StateId.REGISTER_RESPONSE_ERROR;
  }

  public DatabusRelayConnection getRelayConnection()
  {
    return _relayConnection;
  }

  @Override
  public void switchToStreamRequestError()
  {
    _stateId = StateId.STREAM_REQUEST_ERROR;
  }

  @Override
  public void switchToStreamResponseError()
  {
    _stateId = StateId.STREAM_RESPONSE_ERROR;
  }

  public ConnectionState switchToRequestTargetScn(Checkpoint ckpt)
  {
    _stateId = StateId.REQUEST_TARGET_SCN;
    setCheckpoint(ckpt);
    return this;
  }

  @Override
  public void switchToTargetScnRequestError()
  {
    _stateId = StateId.TARGET_SCN_REQUEST_ERROR;
  }

  @Override
  public void switchToTargetScnResponseError()
  {
    _stateId = StateId.TARGET_SCN_RESPONSE_ERROR;
  }

  @Override
  public void switchToTargetScnSuccess()
  {
    _stateId = StateId.TARGET_SCN_RESPONSE_SUCCESS;
  }

  public ConnectionState bootstrapServerSelected(InetSocketAddress serviceInetAddr,
                                                 DatabusBootstrapConnection bootstrapConnection,
                                                 ServerInfo currentBSServerInfo)
  {
    _serverInetAddress = serviceInetAddr;
    _bootstrapConnection = bootstrapConnection;
    _currentBSServerInfo = currentBSServerInfo;
    return this;
  }


  public ConnectionState switchToRequestStartScn(Checkpoint ckpt)
  {

    _stateId = StateId.REQUEST_START_SCN;
    setCheckpoint(ckpt);
    return this;
  }

  @Override
  public void switchToStartScnRequestError()
  {
    _stateId = StateId.START_SCN_REQUEST_ERROR;
  }

  @Override
  public void switchToStartScnResponseError()
  {
    _stateId = StateId.START_SCN_RESPONSE_ERROR;
  }

  @Override
  public void switchToStartScnSuccess(Checkpoint ckpt, DatabusBootstrapConnection bootstrapConnection, ServerInfo bsServerInfo)
  {
    _stateId = StateId.START_SCN_RESPONSE_SUCCESS;
    _currentBSServerInfo = bsServerInfo;

    if ( ckpt != _checkpoint)
    {
    	_checkpoint = ckpt;
    	_readEventListeners.clear();
    	_readEventListeners.add(_checkpoint);
    }

    if ( null != bootstrapConnection)
    {
    	_bootstrapConnection = bootstrapConnection;
    }
  }

  public void switchToBootstrap(Checkpoint cp)
  {
    _stateId = StateId.BOOTSTRAP;
    _checkpoint = cp;
  }

  public Channel getStartScnChannel()
  {
    return _startScnChannel;
  }

  public List<String> getSourcesNames()
  {
    return _sourcesNames;
  }

  public List<DatabusSubscription> getSubscriptions()
  {
    return _subscriptions;
  }

  public String getSourcesNameList()
  {
    return _sourcesNameList;
  }


  public InetSocketAddress getServerInetAddress()
  {
    return _serverInetAddress;
  }

  public List<IdNamePair> getSources()
  {
    return _sources;
  }

  public String getSourcesIdListString()
  {
    return _sourcesIdListString;
  }

  public String getSubsListString()
  {
    return _subsListString;
  }

  public Map<Long, IdNamePair> getSourceIdMap()
  {
    return _sourcesIdMap;
  }

  public Map<String, IdNamePair> getSourcesNameMap()
  {
    return _sourcesNameMap;
  }

  public Map<Long, List<RegisterResponseEntry>> getSourcesSchemas()
  {
    return _sourcesSchemas;
  }

  public Map<Long, List<RegisterResponseEntry>> getKeysSchemas()
  {
    return _keysSchemas;
  }

  public List<RegisterResponseMetadataEntry> getMetadataSchemas()
  {
    return _metadataSchemas;
  }

  public DbusEventBuffer getDataEventsBuffer()
  {
    return _dataEventsBuffer;
  }

  public ChunkedBodyReadableByteChannel getReadChannel()
  {
    return _readChannel;
  }

  public void setCheckpoint(Checkpoint cp)
  {
    //intentional object comparison!
    if (cp == _checkpoint) return;

    if (null != _checkpoint)
    {
      _readEventListeners.remove(_checkpoint);
    }
    _checkpoint = cp;
    _readEventListeners.add(_checkpoint);
  }

  public Checkpoint getCheckpoint()
  {
    return _checkpoint;
  }

  public List<InternalDatabusEventsListener> getListeners()
  {
    return _readEventListeners;
  }

  public DatabusHttpClientImpl.StaticConfig getClientConfig()
  {
    return _clientConfig;
  }


  @Override
  public String toString()
  {
    return _stateId.toString();
  }

  public ServerInfo getCurrentServerInfo()
  {
    return _currentServerInfo;
  }


  public DatabusBootstrapConnection getBootstrapConnection()
  {
    return _bootstrapConnection;
  }

  public void clearBootstrapState()
  {
    _readEventListeners.clear();
  }

  public void setRelayConnection(DatabusRelayConnection conn)
  {
	  _relayConnection = conn;
  }

  public void setBootstrapConnection(DatabusBootstrapConnection conn)
  {
	  _bootstrapConnection = conn;
  }

  @Override
  public void switchToSourcesRequestSent() {
	  _stateId = StateId.SOURCES_REQUEST_SENT;
  }

  @Override
  public void swichToRegisterRequestSent() {
	  _stateId = StateId.REGISTER_REQUEST_SENT;
  }

  @Override
  public void switchToStreamRequestSent() {
	  _stateId = StateId.STREAM_REQUEST_SENT;
  }

  @Override
  public void switchToBootstrapRequested() {
	  _stateId = StateId.BOOTSTRAP_REQUESTED;
  }

  @Override
  public void switchToBootstrapDone()
  {
	  _stateId = StateId.BOOTSTRAP_DONE;
  }

  @Override
  public void switchToStartScnRequestSent()
  {
	  _stateId = StateId.START_SCN_REQUEST_SENT;
  }

  @Override
  public void switchToTargetScnRequestSent()
  {
	  _stateId = StateId.TARGET_SCN_REQUEST_SENT;
  }

  public ServerInfo getCurrentBSServerInfo()
  {
	  return _currentBSServerInfo;
  }

  public void setCurrentBSServerInfo(ServerInfo serverInfo)
  {
	 _currentBSServerInfo = serverInfo;
  }

  public boolean isRelayFellOff()
  {
	  return _isRelayFellOff;
  }

  public void setRelayFellOff(boolean retryOnFellOff)
  {
	 _isRelayFellOff = retryOnFellOff;
  }

  public boolean isSCNRegress()
  {
	  return _scnRegress;
  }

  public void setSCNRegress(boolean regress)
  {
	  _scnRegress = regress;
  }

  public boolean isFlexibleCheckpointRequest()
  {
	  return _flexibleCheckpointRequest;
  }

  public void setFlexibleCheckpointRequest(boolean flexibleCheckpointRequest)
  {
	  _flexibleCheckpointRequest = flexibleCheckpointRequest;
  }

  /**
   * @return the bstCheckpointHandler
   */
  protected BootstrapCheckpointHandler getBstCheckpointHandler()
  {
    return _bstCheckpointHandler;
  }

}
