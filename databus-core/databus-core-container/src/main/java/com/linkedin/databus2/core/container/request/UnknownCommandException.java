package com.linkedin.databus2.core.container.request;

public class UnknownCommandException extends Exception {

	private static final long serialVersionUID = 1L;

	private final String _commandName;

	public UnknownCommandException(String commandName) {
		super("Unknown databus command: " + commandName);
		_commandName = commandName;
	}

	public String getCommandName() {
		return _commandName;
	}

}
