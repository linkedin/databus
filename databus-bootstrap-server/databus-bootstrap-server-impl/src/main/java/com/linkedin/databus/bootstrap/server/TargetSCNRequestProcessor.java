/**
 *
 */
package com.linkedin.databus.bootstrap.server;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.bootstrap.api.BootstrapProcessingException;
import com.linkedin.databus.bootstrap.common.BootstrapConn;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO;
import com.linkedin.databus.bootstrap.common.BootstrapHttpStatsCollector;
import com.linkedin.databus2.core.container.request.BootstrapDatabaseTooOldException;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.RequestProcessingException;

/**
 * @author lgao
 *
 */

public class TargetSCNRequestProcessor extends BootstrapRequestProcessorBase
{
  public static final String MODULE = TargetSCNRequestProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public final static String COMMAND_NAME = "targetSCN";
  public final static String SOURCE_PARAM  = "source";


  public TargetSCNRequestProcessor(ExecutorService executorService,
		  						   BootstrapServerStaticConfig config,
                                   BootstrapHttpServer bootstrapServer)
     throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
  {
	  super(executorService,config,bootstrapServer);
  }


  @Override
  protected DatabusRequest doProcess(DatabusRequest request)
		  throws IOException, RequestProcessingException
  {
    BootstrapHttpStatsCollector bootstrapStatsCollector = _bootstrapServer.getBootstrapStatsCollector();
    long startTime = System.currentTimeMillis();
    
    int srcId = -1;
    long targetScn = -1;
    String source = request.getRequiredStringParam(SOURCE_PARAM);
    BootstrapSCNProcessor processor = null;
    try
    {
    	processor = new BootstrapSCNProcessor(_config, _bootstrapServer.getInboundEventStatisticsCollector());

    	try
    	{
    		// get src id from db
    		BootstrapDBMetaDataDAO.SourceStatusInfo srcIdStatus = processor.getSrcIdStatusFromDB(source, true);
    	    
    		if ( !srcIdStatus.isValidSource())
    	    	throw new BootstrapProcessingException("Bootstrap DB not servicing source :" + source);
    	    
    		srcId = srcIdStatus.getSrcId();

    		// select target scn
    		targetScn = processor.getSourceTargetScn(srcId);
    	}
    	catch (BootstrapDatabaseTooOldException tooOldException)
    	{
    	    if (bootstrapStatsCollector != null) 
    	    {
    	    	bootstrapStatsCollector.registerErrTargetSCN();
    	    	bootstrapStatsCollector.registerErrDatabaseTooOld();
    	    }

    		LOG.error("The bootstrap database is too old!", tooOldException);
    		throw new RequestProcessingException(tooOldException);
    	}
    	catch (SQLException e)
    	{
    		if (bootstrapStatsCollector != null) 
    	    {
    			bootstrapStatsCollector.registerErrTargetSCN();
    			bootstrapStatsCollector.registerErrSqlException();
    	    }

    		LOG.error("Error encountered while fetching targetSCN from database.", e);
    		throw new RequestProcessingException(e);
    	}

    	ObjectMapper mapper = new ObjectMapper();
    	StringWriter out = new StringWriter(1024);
    	mapper.writeValue(out, String.valueOf(targetScn));
    	byte[] resultBytes = out.toString().getBytes();
    	request.getResponseContent().write(ByteBuffer.wrap(resultBytes));
    	LOG.info("targetSCN: " + targetScn);
    } catch (Exception ex) {
    	LOG.error("Got exception while calculating targetSCN", ex);
    	throw new RequestProcessingException(ex);
    } finally {
    	if ( null != processor)
    		processor.shutdown();
    }
    
    if (bootstrapStatsCollector != null) 
    {
    	bootstrapStatsCollector.registerTargetSCNReq(System.currentTimeMillis()-startTime);
    }

    return request;
  }
}
