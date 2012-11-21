package com.linkedin.databus3.espresso.client;

import org.codehaus.jackson.io.JsonStringEncoder;

import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.client.pub.StartResult;

public class StartResultImpl implements StartResult
{

	private boolean _success;
	private String _errorMessage;
	private Exception _exception;

	public StartResultImpl()
	{
		_success = true;
	}

	public static StartResult createFailedStartResult(RegistrationId id, String errorMessage, Exception ex)
	{
		StartResultImpl result = new StartResultImpl();
		result.setSuccess(false);
		result.setErrorMessage("Start for id (" + id + ") failed. Reason :" + errorMessage);
		result.setException(ex);
		return result;
	}

	public static StartResult createSuccessStartResult()
	{
		StartResultImpl result = new StartResultImpl();
		result.setSuccess(true);
		return result;
	}

	@Override
	public String getErrorMessage() {
		return _errorMessage;
	}

	@Override
	public Exception getException() {
		return _exception;
	}

	@Override
	public boolean getSuccess() {
		return _success;
	}

	public void setErrorMessage(String errorMessage) {
		this._errorMessage = errorMessage;
	}

	public void setException(Exception exception) {
		this._exception = exception;
	}

	public void setSuccess(boolean success) {
		this._success = success;
	}

	@Override
  public String toString()
	{
	  if (_success)
	  {
	    return "{\"success\":\"true\"}";
	  }
	  else
	  {
	    JsonStringEncoder enc = JsonStringEncoder.getInstance();
	    String encMessage = null == _errorMessage ? "" : new String(enc.quoteAsString(_errorMessage));
	    String encException = null == _exception ? "" : _exception.getClass().getName() +
	        (null == _exception.getMessage() ? "" :
	            ":" +new String(enc.quoteAsString(_exception.getMessage())));
	    return "{\"success\":\"false\", \"errorMessage\":\"" + encMessage +
	           "\",\"exception\":\"" + encException + "\"}";
	  }
	}
}
