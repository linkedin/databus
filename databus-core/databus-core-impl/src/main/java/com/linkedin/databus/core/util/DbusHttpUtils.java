package com.linkedin.databus.core.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.linkedin.databus.core.DbusConstants;

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

public class DbusHttpUtils
{

    public static String getLocalHostName()
    {
      try
      {
        return getHostName(InetAddress.getLocalHost());
      }
      catch (UnknownHostException e)
      {
        return DbusConstants.UNKNOWN_HOST;
      }
      catch (RuntimeException e)
      {
        return DbusConstants.UNKNOWN_HOST;
      }
    }

    public static String getHostName(InetAddress ia)
    {
       return ia.getHostName();
    }

}
