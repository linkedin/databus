package com.linkedin.databus.bootstrap.server;
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


import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.pub.ServerInfo;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;

public abstract class BootstrapRequestProcessorBase
	implements RequestProcessor
{
	public static final String MODULE = BootstrapRequestProcessorBase.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	public final static String CHECKPOINT_PARAM  = "checkPoint";

	/**
	 *  We do lazy initializing of bootstrap-server as this class instantiation happens before we decide on
	 *  which port the service will be up
	 */
	//Flag to indicate if bootstrap server info is initialized.
	private boolean _isServerInfoInitialized;

	// Bootstrap Server HostPort String
	protected String _serverHostPort;

	// Bootstrap Server Info for comparison with incoming request
	protected ServerInfo _serverInfo;

	protected final ExecutorService _executorService;
	protected final BootstrapServerStaticConfig _config;
	protected final BootstrapHttpServer _bootstrapServer;

	public BootstrapRequestProcessorBase(ExecutorService executorService,
			BootstrapServerStaticConfig config,
			BootstrapHttpServer bootstrapServer)
		throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
	{
		_config = config;
		_executorService = executorService;
		_bootstrapServer = bootstrapServer;
		_isServerInfoInitialized = false;
	}

	  @Override
	  public final DatabusRequest process(DatabusRequest request) throws IOException,
	      RequestProcessingException
	  {
	        initBootstrapServerInfo();

		  	String ckptStr = request.getParams().getProperty(CHECKPOINT_PARAM);
		  	Checkpoint ckpt = null;

		  	if ( null != ckptStr)
		  	{
		  		ckpt = new Checkpoint(ckptStr);

		  		String bsServerInfo = ckpt.getBootstrapServerInfo();

		  		if ((null != bsServerInfo) && (null != _serverInfo))
		  		{
		  			ServerInfo expServerInfo = null;
		  			try
		  			{
		  				expServerInfo = ServerInfo.buildServerInfoFromHostPort(bsServerInfo.trim(),DbusConstants.HOSTPORT_DELIMITER);
		  			} catch (Exception ex) {
		  				LOG.error("Unable to fetch ServerInfo from ckpt. Passed ServerInfo was :" + bsServerInfo.trim());
		  			}

		  			if ((null != expServerInfo) && (! _serverInfo.equals(expServerInfo)))
		  			{
		  				String msg = "Bootstrap Server Request should be served by different host : " + bsServerInfo + ", This instance is :" + _serverHostPort;
		  				LOG.error(msg);
		  				throw new RequestProcessingException(msg);
		  			}
		  		}
		  	}

		    DatabusRequest req = doProcess(request);

	    	if ( null != _serverHostPort)
	    	{
	    		req.getResponseContent().setMetadata(DbusConstants.SERVER_INFO_HOSTPORT_HEADER_PARAM, _serverHostPort);
	    	}

	    	return req;
	  }

	  protected abstract DatabusRequest doProcess(DatabusRequest request)
			  throws IOException, RequestProcessingException;

	  @Override
	  public ExecutorService getExecutorService()
	  {
	    return _executorService;
	  }

	  private void initBootstrapServerInfo()
	  {
	    if ( !_isServerInfoInitialized )
	    {
	      String host = null;

	      try {
	        host = InetAddress.getLocalHost().getHostName();
	      } catch (UnknownHostException e) {
	        LOG.error("Unable to fetch the local hostname !! Trying to fetch IP Address !!", e);
	        try
	        {
	          host = InetAddress.getLocalHost().getHostAddress();
	        } catch (UnknownHostException e1) {
	          LOG.error("Unable to fetch the local IP Address too !! Giving up", e1);
	          host = null;
	        }
	      }
	      if ( null != host )
	      {
	        int port = _bootstrapServer.getHttpPort();
	        _serverHostPort = host + DbusConstants.HOSTPORT_DELIMITER + port;
	        ServerInfo serverInfo = null;
	        try {
	          serverInfo = ServerInfo.buildServerInfoFromHostPort(_serverHostPort, DbusConstants.HOSTPORT_DELIMITER);
	        } catch (Exception e) {
	          LOG.error("Unable to build serverInfo from string (" + _serverHostPort +")", e);
	        }
	        _serverInfo = serverInfo;
	      } else {
	        _serverHostPort = null;
	        _serverInfo = null;
	        LOG.error("Unable to fetch local address !! Clients connecting to this bootstrap server will restart bootstrap on failures !!");
	      }
	      _isServerInfoInitialized = true;
	    }
	  }
}
