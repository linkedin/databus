/**
 *
 */
package com.linkedin.databus2.core.container.monitoring.mbean;

/**
 * Commong interface for all databus admin mbeans
 * @author lgao
 *
 */
public interface DatabusComponentAdminMBean
{
  /**** =============== getters ================ ******/

  /**
   * The status of the dbus component: running, paused, suspendedOnError")
   * @return status string
   */
  String getStatus();

  /**
   * The detailed message about the status
   * @return detailed status message string
   */
  String getStatusMessage();

  /**
   * The container ID of the dbus component
   * @return container ID
   */
  long getContainerId();

  /**
   * The HTTP port on which the component is listening
   * @return http port number
   */
  long getHttpPort();

  /**
   * The dbus component name
   * @return component name
   */
  String getComponentName();

  /**
   * A number representation of of the status for graphing/alerts. Larger value means worse
   * */
  int getStatusCode();

  /**** =============== setters ================ ******/

  /**
   * Pause the component temporarily
   */
  void pause();

  /**
   * Resume the component paused previously
   */
  void resume();

  /**
   * Shutdown the component permanently
   */
  void shutdown();
}
