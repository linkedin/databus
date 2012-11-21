package com.linkedin.databus.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class SCNRegressMessage 
{
	public static final String  MODULE               = SCNRegressMessage.class.getName();
	public static final Logger  LOG                  = Logger.getLogger(MODULE);

	private static ObjectMapper mapper               = new ObjectMapper();

	private Checkpoint cp;
	
	public SCNRegressMessage()
	{		
	}
	
	public SCNRegressMessage(Checkpoint ckpt)
	{
		this.cp = ckpt;
	}

	public Checkpoint getCheckpoint()
	{
		return cp;
	}

	public void setCheckpoint(Checkpoint ckpt)
	{
		this.cp = ckpt;
	}

	public static String toJSONString(SCNRegressMessage message)
	{
		String msgStr = message.getCheckpoint().toString();
		return msgStr;
	}
	
	public static SCNRegressMessage getRegressMessage(String serializedMessage)
	{
		SCNRegressMessage message = null;
		
		try {
			Checkpoint cp = new Checkpoint(serializedMessage);
			message = new SCNRegressMessage(cp);
		} catch (JsonParseException e) {
			LOG.error("JSON parse error : " + e.getMessage(), e);
		} catch (JsonMappingException e) {
			LOG.error("JSON mapping error : " + e.getMessage(), e);
		} catch (IOException e) {
			LOG.error("JSON IO error: " + e.getMessage(), e);
		}
		return message;
	}

	@Override
	public String toString() {
		return "SCNRegressMessage [cp=" + cp + "]";
	}
}
