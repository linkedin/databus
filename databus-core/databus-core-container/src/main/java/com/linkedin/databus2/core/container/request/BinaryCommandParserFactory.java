package com.linkedin.databus2.core.container.request;

import org.jboss.netty.channel.Channel;

/**
 * The interface for binary command parsers that are used to parse a TCP stream and insert the
 * command object into the Netty pipeline.
 * */
public interface BinaryCommandParserFactory
{

  /** Creates a binary parser for a command */
  BinaryCommandParser createParser(Channel channel);

}
