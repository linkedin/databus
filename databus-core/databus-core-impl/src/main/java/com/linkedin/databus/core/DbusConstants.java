package com.linkedin.databus.core;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


public final class DbusConstants {

	public static final long  NUM_NSECS_IN_MSEC= 1000000;
	public static final long  NUM_NSECS_IN_SEC= 1000000000;

    public static final String SERVER_INFO_HOSTPORT_HEADER_PARAM = "X-DBUS2-BS-HOSTPORT";
    public static final String HOSTPORT_DELIMITER = ":";

    public static final String APP_NAME = "com.linkedin.app.name";
    public static final String INSTANCE_NAME = "com.linkedin.app.instance";
    public static final String ENV_NAME = "com.linkedin.app.env";
    public static final String MACHINE_NAME = "com.linkedin.app.machine";
    public static final String CONTAINER_NAME = "com.linkedin.app.container";

    // The delimiter used when merging compound keys
    public static final String COMPOUND_KEY_DELIMITER = "\t";
    // The separator used in the AVRO schema
    public static final String COMPOUND_KEY_SEPARATOR = ",";

    public static final String UNKNOWN_ENV = "UNKNOWN";
    public static final String UNKNOWN_APP_NAME = "UNKNOWN";
    public static final String UNKNOWN_INSTANCE = "UNKNOWN";
    public static final String UNKNOWN_SERVICE_ID = "UNKNOWN";
    public static final String UNKNOWN_HOST = "UNKNOWN";
    public static final String DEFAULT_VAL = "UNKNOWN";
    private static final String UNKNOWN_FULL_SERVICE_ID =
        getServiceIdentifier(UNKNOWN_ENV, UNKNOWN_APP_NAME, UNKNOWN_INSTANCE);

    public final static String GG_REPLICATION_BIT_SETTER_FIELD_NAME = "GG_STATUS";
    public final static String GG_REPLICATION_BIT_SETTER_VALUE = "g";
    public final static String ISO_8859_1 = "ISO-8859-1";
    public final static String XML_VERSION = "1.0";

    public static String getAppName()
    {
        return System.getProperty(APP_NAME, UNKNOWN_APP_NAME);
    }

    public static void setAppName(String appName)
    {
        System.setProperty(APP_NAME, appName);
    }

    public static String getInstanceName()
    {
        return System.getProperty(INSTANCE_NAME, UNKNOWN_INSTANCE);
    }

    public static void setInstanceName(String instanceName)
    {
        System.setProperty(INSTANCE_NAME, instanceName);
    }

    public static String getEnvName()
    {
        return System.getProperty(ENV_NAME, UNKNOWN_ENV);
    }

    public static void setEnvName(String envName)
    {
        System.setProperty(ENV_NAME, envName);
    }

    public static String getMachineName()
    {
        return System.getProperty(MACHINE_NAME, UNKNOWN_HOST);
    }

    public static void setMachineName(String machineName)
    {
        System.setProperty(MACHINE_NAME, machineName);
    }

    public static String getContainerName()
    {
        return System.getProperty(CONTAINER_NAME, DEFAULT_VAL);
    }

    public static void setContainerName(String containerName)
    {
        System.setProperty(CONTAINER_NAME, containerName);
    }

    public static String getServiceIdentifier(String environment, String appName, String instance)
    {
      return environment + "/" + appName + "/" + instance;
    }

    public static String getServiceIdentifier()
    {
        String svcId = getServiceIdentifier(DbusConstants.getEnvName(), DbusConstants.getAppName(),
                                            DbusConstants.getInstanceName());
        // In case of integration test case, these environment properties may not be set
        if (svcId.equals(UNKNOWN_FULL_SERVICE_ID))
        {
        	svcId = UNKNOWN_SERVICE_ID;
        }
    	return svcId;
    }

}
