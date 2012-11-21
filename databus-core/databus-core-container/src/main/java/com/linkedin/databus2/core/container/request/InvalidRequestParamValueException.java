package com.linkedin.databus2.core.container.request;


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
