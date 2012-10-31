/**
 *
 */
package com.linkedin.databus2.core.container.monitoring.mbean;

import java.util.Hashtable;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.monitoring.mbean.AbstractMonitoringMBean;
import com.linkedin.databus.core.util.JmxUtil;
import com.linkedin.databus2.core.container.netty.ServerContainer;

/**
 * @author lgao
 *
 */
public class DatabusComponentAdmin implements DatabusComponentAdminMBean
{
  public static final String MODULE = DatabusComponentAdmin.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String JMX_DOMAIN = "com.linkedin.databus2";

  private final String _componentName;
  private final ServerContainer _serverContainer;

  private final MBeanServer _mbeanServer;
  private final ObjectName _mbeanObjectName;


  /**
   * Constructor
   */
  public DatabusComponentAdmin(ServerContainer serverContainer, MBeanServer mbeanServer, String componentName)
  {
    _serverContainer = serverContainer;
    _mbeanServer = mbeanServer;
    _componentName = componentName;
    _mbeanObjectName = createMBeanObjectName();
  }

  /**
   * @see com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdminMBean#getStatus()
   */
  @Override
  public String getStatus()
  {
    return _serverContainer.getStatus().toString();
  }

  @Override
  public int getStatusCode()
  {
    return _serverContainer.getStatus().getCode();
  }

  /**
   * @see com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdminMBean#getStatusMessage()
   */
  @Override
  public String getStatusMessage()
  {
    return _serverContainer.getStatusMessage();
  }

  /**
   * @see com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdminMBean#getContainerId()
   */
  @Override
  public long getContainerId()
  {
    return _serverContainer.getContainerStaticConfig().getId();
  }

  /**
   * @see com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdminMBean#getHttpPort()
   */
  @Override
  public long getHttpPort()
  {
    return _serverContainer.getContainerStaticConfig().getHttpPort();
  }

  /**
   * @see com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdminMBean#getComponentName()
   */
  @Override
  public String getComponentName()
  {
    return _componentName;
  }

  /**
   * @see com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdminMBean#pause()
   */
  @Override
  public void pause()
  {
    _serverContainer.pause();
  }

  /**
   * @see com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdminMBean#resume()
   */
  @Override
  public void resume()
  {
    _serverContainer.resume();
  }

  /**
   * @see com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdminMBean#shutdown()
   */
  @Override
  public void shutdown()
  {
    _serverContainer.awaitShutdown();
  }

  private ObjectName createMBeanObjectName()
  {
    ObjectName jmxName = null;
    try
    {
      Hashtable<String, String> mbeanProps = new Hashtable<String, String>(5);
      mbeanProps.put("name", _componentName);
      mbeanProps.put("type", DatabusComponentAdmin.class.getSimpleName());
      mbeanProps.put("ownerId", Long.toString(getContainerId()));
      jmxName = new ObjectName(AbstractMonitoringMBean.JMX_DOMAIN, mbeanProps);
    }
    catch (Exception e)
    {
      LOG.error("Error creating JMX object name", e);
    }
    return jmxName;
  }

  public void registerAsMBean()
  {
    if (null != _mbeanServer)
    {
      try
      {
        _mbeanServer.registerMBean(this, _mbeanObjectName);
        LOG.info("MBean registered " + _mbeanObjectName);
      }
      catch (Exception e)
      {
        LOG.error("JMX registration failed", e);
      }
    }
  }

  public void unregisterAsMBeans()
  {
    if (null != _mbeanServer)
    {
      try
      {
        JmxUtil.unregisterMBeanSafely(_mbeanServer, _mbeanObjectName, LOG);
        LOG.info("MBean unregistered " + _mbeanObjectName);
      }
      catch (Exception e)
      {
        LOG.error("JMX deregistration failed", e);
      }
    }
  }

/*  public void setComponentStatus(DatabusComponentStatus newStatus)
  {
    _status = newStatus;
  }

  public DatabusComponentStatus getComponentStatus()
  {
    return _status;
  }
  */
}
