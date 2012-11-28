package com.linkedin.databus2.core.container.request;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * The factory interface for execution handlers which listen on a Netty pipeline for a
 * particular type of command objects and execute the commands represented by the objects.*/
public interface RequestExecutionHandlerFactory
{

  /** Creates a handler for the execution of a particular types of request */
  SimpleChannelHandler createHandler(Channel channel);

}
