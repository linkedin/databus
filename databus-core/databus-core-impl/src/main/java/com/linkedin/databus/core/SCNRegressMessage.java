package com.linkedin.databus.core;
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
