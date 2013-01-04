package com.linkedin.databus2.core.container.request;

import com.linkedin.databus2.core.DatabusException;

public class ProcessorRegistrationConflictException extends DatabusException
{
	private static final long serialVersionUID = 1L;

	private final String _commandName;

	public ProcessorRegistrationConflictException(String commandName) {
		super("Processor for the command has already been registered: " + commandName);
		_commandName = commandName;
	}

	public String getCommandName() {
		return _commandName;
	}

}
