package com.linkedin.databus2.core.mbean;

import java.util.Hashtable;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.linkedin.databus.core.DatabusComponentStatus;

public class DatabusReadOnlyStatus extends BaseDatabusMBean implements DatabusReadOnlyStatusMBean
{
  private final DatabusComponentStatus _status;
  private final String _name;
  private final long _ownerId;

  public DatabusReadOnlyStatus(String name, DatabusComponentStatus status, long ownerId)
  {
    _status = status;
    _name = name;
    _ownerId = ownerId;
  }

  @Override
  public String getStatus()
  {
    return _status.getStatus().toString();
  }

  @Override
  public String getStatusMessage()
  {
    return _status.getMessage();
  }

  @Override
  public String getComponentName()
  {
    return _name;
  }

  @Override
  public int getStatusCode()
  {
    return _status.getStatus().getCode();
  }

  @Override
  public ObjectName generateObjectName() throws MalformedObjectNameException
  {
    Hashtable<String, String> nameProps = generateBaseMBeanProps();
    nameProps.put("name", sanitizeString(_name));
    nameProps.put("ownerId", Long.toString(_ownerId));

    return new ObjectName(JMX_DOMAIN, nameProps);
  }

  @Override
  public int getRetriesNum()
  {
    return _status.getRetriesNum();
  }

  @Override
  public int getRemainingRetriesNum()
  {
    return _status.getRetriesLeft();
  }

  @Override
  public long getCurrentRetryLatency()
  {
    return _status.getLastRetrySleepMs();
  }

  @Override
  public long getTotalRetryTime()
  {
    return _status.getRetriesCounter().getTotalRetryTime();
  }

  private String sanitizeString(String s)
  {
    return s.replaceAll("[.,;]", "_");
  }

  @Override
  public long getUptimeMs()
  {
    return _status.getUptimeMs();
  }
}
