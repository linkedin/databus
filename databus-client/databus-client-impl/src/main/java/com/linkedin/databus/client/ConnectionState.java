package com.linkedin.databus.client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.netty.channel.Channel;

import com.linkedin.databus.client.pub.DatabusServerCoordinates;
import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.InternalDatabusEventsListener;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;

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
  private DbusEventBuffer _dataEventsBuffer;
  private DatabusHttpClientImpl.StaticConfig _clientConfig;
  private final List<DatabusSubscription> _subscriptions;
  private String _sourcesNameList;

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
  private Map<Long, IdNamePair> _sourcesIdMap = new HashMap<Long, IdNamePair>(20);
  private Map<String, IdNamePair> _sourcesNameMap = new HashMap<String, IdNamePair>(20);
  //private DbusKeyCompositeFilter _filter = new DbusKeyCompositeFilter();

  //REQUEST_SOURCES_SCHEMAS extends SOURCES_READY
  private String _sourcesIdListString;
  private String _subsListString;

  //SOURCES_SCHEMAS_READY extends READ_SOURCES_SCHEMAS
  private Map<Long, List<RegisterResponseEntry>> _sourcesSchemas;

  //REQUEST_DATA_EVENTS extends SOURCES_SCHEMAS_READY
  private Checkpoint _checkpoint;
  private boolean _scnRegress = false;
  private boolean _flexibleCheckpointRequest = false;
  
  private List<InternalDatabusEventsListener> _readEventListeners;

  
  //READ_DATA_EVENTS extends REQUEST_DATA_EVENTS
  private ChunkedBodyReadableByteChannel _readChannel;

  //START_SNAPSHOT_PHASE

  private Channel _startScnChannel;

  
  private ServerInfo _currentBSServerInfo;
  
  //DATA_EVENTS_DONE extends READ_DATE_EVENTS

  private ConnectionState(DbusEventBuffer dataEventsBuffer,
                          List<String> sourcesNames, List<DatabusSubscription> subs)
  {
    _stateId = StateId.INITIAL;
    _dataEventsBuffer = dataEventsBuffer;
    _readEventListeners = new ArrayList<InternalDatabusEventsListener>();

    if(subs != null){
      _subscriptions = subs;
    } else if(sourcesNames != null) {
      _subscriptions = DatabusSubscription.createSubscriptionList(sourcesNames);
    }else {
      throw new IllegalArgumentException("both sources and subscriptions are null");
    }

    StringBuilder sb = new StringBuilder();
    boolean firstSource = true;
    for (String sourceName: sourcesNames)
    {
      if (! firstSource) sb.append(',');
      sb.append(sourceName);
      firstSource = false;
    }
   _sourcesNameList = sb.toString();
  }

  public StateId getStateId()
  {
    return _stateId;
  }

  public static ConnectionState create(DbusEventBuffer dataEventsBuffer,
                                       List<String> sourcesNames, List<DatabusSubscription> subs)
  {
    return new ConnectionState(dataEventsBuffer, sourcesNames, subs);
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
  public void switchToSourcesSuccess(List<IdNamePair> sources)
  {
    _stateId = StateId.SOURCES_RESPONSE_SUCCESS;
    setSourcesIds(sources);
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
  public void switchToRegisterSuccess(
      Map<Long, List<RegisterResponseEntry>> sourcesSchemas)
  {
    _stateId = StateId.REGISTER_RESPONSE_SUCCESS;
    setSourcesSchemas(sourcesSchemas);
  }

  public void setSourcesSchemas(Map<Long, List<RegisterResponseEntry>> sourcesSchemas)
  {
    _sourcesSchemas = sourcesSchemas;
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

  public ConnectionState switchToRequestTargetScn(InetSocketAddress serviceInetAddr,
                                                  Map<Long, IdNamePair> sourceIdMap,
                                                  Map<Long, List<RegisterResponseEntry>> sourcesSchemas,
                                                  Checkpoint ckpt,
                                                  DatabusBootstrapConnection bootstrapConnection)
  {
    _stateId = StateId.REQUEST_TARGET_SCN;
    _serverInetAddress = serviceInetAddr;
    _sourcesIdMap = sourceIdMap;
    _sourcesSchemas = sourcesSchemas;
    _checkpoint = ckpt;
    _bootstrapConnection = bootstrapConnection;
    _readEventListeners.add(_checkpoint);
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

  public ConnectionState switchToRequestStartScn(InetSocketAddress serviceInetAddr,
                                                 Map<Long, IdNamePair> sourceIdMap,
                                                 Map<Long, List<RegisterResponseEntry>> sourcesSchemas,
                                                 Checkpoint ckpt,
                                                 DatabusBootstrapConnection bootstrapConnection,
                                                 ServerInfo currentBSServerInfo)
  {
    _stateId = StateId.REQUEST_START_SCN;
    _serverInetAddress = serviceInetAddr;
    _sourcesIdMap = sourceIdMap;
    _sourcesSchemas = sourcesSchemas;
    _checkpoint = ckpt;
    _bootstrapConnection = bootstrapConnection;
    _readEventListeners.add(_checkpoint);
    _currentBSServerInfo = currentBSServerInfo;
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
    return DatabusSubscription.getStrList(_subscriptions);
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
	  _checkpoint = cp;
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
}
