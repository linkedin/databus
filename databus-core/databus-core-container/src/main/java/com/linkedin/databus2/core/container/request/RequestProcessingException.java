package com.linkedin.databus2.core.container.request;

public class RequestProcessingException extends Exception {
	
	private static final long serialVersionUID = 1L;

	public RequestProcessingException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	public RequestProcessingException(String arg0) {
		super(arg0);
	}

	public RequestProcessingException(Throwable arg0) {
		super(arg0);
	}

}
