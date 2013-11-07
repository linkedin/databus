package com.linkedin.databus2.core.container.request;
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



public class InvalidRequestParamValueException extends RequestProcessingException {

	private static final long serialVersionUID = 1L;
	
	private final String _cmdName;
	private final String _paramName;
	private final String _paramValue;
	
	public InvalidRequestParamValueException(String cmdName, String paramName, String paramValue) {
		super("Invalid value for " + cmdName + "." + paramName + ": " + paramValue);
		_cmdName = cmdName;
		_paramName = paramName;
		_paramValue = paramValue;
	}
	
	public String getParamName() {
		return _paramName;
	}
	
	public String getParamValue() {
		return _paramValue;
	}

	public String getCmdName() {
		return _cmdName;
	}
	
	

}
