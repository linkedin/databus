package com.linkedin.databus2.core.container.request;

import org.jboss.netty.handler.codec.http.HttpMethod;


public class InvalidRequestType extends RequestProcessingException {
	private static final long serialVersionUID = 1L;
	
	private final HttpMethod _methodType;
	private final String _commandName;

	public InvalidRequestType(HttpMethod methodType, String commandName) {
		super("Request type " + methodType + " is not supported for command " + commandName);
		_methodType = methodType;
		_commandName = commandName;
	}
	
	public HttpMethod getMethodType() {
		return _methodType;
	}
	
	public String getCommandName() {
		return _commandName;
	}
	
	
	
}
