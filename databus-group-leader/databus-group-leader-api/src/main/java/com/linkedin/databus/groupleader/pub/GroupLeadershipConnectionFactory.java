/**
 * $Id: GroupLeadershipConnectionFactory.java 154385 2010-12-08 22:05:55Z mstuart $ */
package com.linkedin.databus.groupleader.pub;


/**
 *
 * @author Mitch Stuart
 * @version $Revision: 154385 $
 */
public interface GroupLeadershipConnectionFactory
{
  GroupLeadershipConnection getConnection();
}
