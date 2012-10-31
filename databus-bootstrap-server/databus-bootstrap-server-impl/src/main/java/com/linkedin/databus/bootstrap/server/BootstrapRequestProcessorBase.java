package com.linkedin.databus.bootstrap.server;

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
	  
	protected final String _serverHostPort;
	protected final ServerInfo _serverInfo;
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
			int port = config.getDb().getContainer().getHttpPort();
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
	}

	  @Override
	  public final DatabusRequest process(DatabusRequest request) throws IOException,
	      RequestProcessingException
	  {
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
}
