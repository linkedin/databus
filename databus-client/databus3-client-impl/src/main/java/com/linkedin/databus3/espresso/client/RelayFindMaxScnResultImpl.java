package com.linkedin.databus3.espresso.client;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.SingleSourceSCN;
import com.linkedin.databus.client.pub.DatabusServerCoordinates;
import com.linkedin.databus.client.pub.RelayFindMaxSCNResult;
import com.linkedin.databus.client.pub.SCN;
import com.linkedin.databus.core.BufferInfoResponse;
import com.linkedin.databus.core.util.Utils;

public class RelayFindMaxScnResultImpl implements RelayFindMaxSCNResult
{
  Map<DatabusServerCoordinates, BufferInfoResponse> _resultBufferInfoResponseMap;
  
  Map<DatabusServerCoordinates, ResultCode> _requestResultMap;
  
  SummaryCode _summaryCode = SummaryCode.FAIL;
  
  SingleSourceSCN _maxSCN, _minSCN;
  
  Set<DatabusServerCoordinates> _maxSCNRelaySet;
  
  public static final String MODULE = RelayFindMaxScnResultImpl.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  
  public RelayFindMaxScnResultImpl()
  {  
  }

  @Override
  public Map<DatabusServerCoordinates, BufferInfoResponse> getResultSCNMap()
  {
    return _resultBufferInfoResponseMap;
  }

  @Override
  public SCN getMaxSCN()
  {
    return _maxSCN;
  }

  @Override
  public SCN getMinSCN()
  {
    return _minSCN;
  }

  @Override
  public Map<DatabusServerCoordinates, ResultCode> getRequestResultMap()
  {
    return _requestResultMap;
  }

  @Override
  public Set<DatabusServerCoordinates> getMaxScnRelays()
  {
    return _maxSCNRelaySet;
  }

  @Override
  public SummaryCode getResultSummary()
  {
    return _summaryCode;
  }
  
  void setResult(Map<DatabusServerCoordinates, ResultCode> requestResultMap, Map<DatabusServerCoordinates, BufferInfoResponse> bufferInfoResponseMap)
  {
    _resultBufferInfoResponseMap = bufferInfoResponseMap;
    _requestResultMap = requestResultMap;
    _maxSCNRelaySet = new HashSet<DatabusServerCoordinates>();
    
    long maxSCNVal = -1, minSCNVal = -1;
    
    int successCount = 0;
    for(DatabusServerCoordinates coordinate : _requestResultMap.keySet())
    {
      if(_requestResultMap.get(coordinate) == ResultCode.SUCCESS)
      {
        successCount ++;
      }
      if(maxSCNVal < bufferInfoResponseMap.get(coordinate).getMaxScn())
      {
        maxSCNVal = bufferInfoResponseMap.get(coordinate).getMaxScn();
        // Update the minSCNVal whenever maxSCNVal is updated, as this minSCN corresponds 
        // to the minSCN in the relay which has the maxSCN
        minSCNVal = bufferInfoResponseMap.get(coordinate).getMinScn();
        LOG.info("Updating maxSCNVal to " + maxSCNVal + " minSCNVal to " + minSCNVal + " from relay " + coordinate.getAddress());
        _maxSCNRelaySet.clear();
      }
      
      if(maxSCNVal == bufferInfoResponseMap.get(coordinate).getMaxScn())
      {
        _maxSCNRelaySet.add(coordinate);
      }
    }
    _maxSCN = new SingleSourceSCN(-1, maxSCNVal);
    _minSCN = new SingleSourceSCN(-1, minSCNVal);
    if(successCount == _requestResultMap.size())
    {
      _summaryCode = SummaryCode.SUCCESS;
    }
    else if(successCount == 0)
    {
      _summaryCode = SummaryCode.FAIL;
    }
    else
    {
      _summaryCode = SummaryCode.PARTIAL_SUCCESS;
    }
  }

  public void setResultSummary(SummaryCode code)
  {
	  _summaryCode = code;
  }

  /**
   * A test hook to create an object with trivial values
   * @return
   */
  static public RelayFindMaxSCNResult createTestRelayFindMaxScn()
  {
	  RelayFindMaxScnResultImpl rfms = new RelayFindMaxScnResultImpl();
	  rfms.setResultSummary(SummaryCode.SUCCESS);
	  rfms._maxSCN = new SingleSourceSCN(-1, 100);
	  rfms._minSCN = new SingleSourceSCN(-1, 10);
	  rfms._maxSCNRelaySet = new HashSet<DatabusServerCoordinates>();
	  DatabusServerCoordinates dsc = new DatabusServerCoordinates(new InetSocketAddress("localhost", Utils.getAvailablePort(8888)), "relay1");
	  rfms._maxSCNRelaySet.add(dsc);
	  return rfms;
  }
  
  @Override
  public String toString() {
	return "RelayFindMaxScnResultImpl [_resultBufferInfoResponseMap="
			+ _resultBufferInfoResponseMap + ", _requestResultMap="
			+ _requestResultMap + ", _summaryCode=" + _summaryCode
			+ ", _maxSCN=" + _maxSCN + ", _minSCN=" + _minSCN
			+ ", _maxSCNRelaySet=" + _maxSCNRelaySet
			+ "]";
  }
  
}
