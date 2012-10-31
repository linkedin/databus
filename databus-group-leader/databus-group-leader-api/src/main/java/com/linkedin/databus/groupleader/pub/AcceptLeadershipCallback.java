/**
 * $Id: AcceptLeadershipCallback.java 154385 2010-12-08 22:05:55Z mstuart $ */
package com.linkedin.databus.groupleader.pub;

/**
 * This is the callback that will be called when a group member must accept leadership.
 *
 * @author Mitch Stuart
 * @version $Revision: 154385 $
 */
public interface AcceptLeadershipCallback
{
  void doAcceptLeadership(GroupLeadershipSession groupLeadershipSession);
}
