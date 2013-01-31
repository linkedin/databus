package com.linkedin.databus2.core.container.request;
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


public class RegisterResponseEntry 
{
    private long _id;
    private String _schema;
    private short _version;

    public RegisterResponseEntry(long id, short version, String schema)
    {
      super();
      _id = id;
      _version = version;
      _schema = schema;
    }

    public RegisterResponseEntry()
    {
      this(0, (short)0,"N/A");
    }

    public long getId()
    {
      return _id;
    }

    public void setId(long id)
    {
      _id = id;
    }

    public short getVersion()
    {
      return _version;
    }

    public void setVersion(short version)
    {
      _version = version;
    }

    public String getSchema()
    {
      return _schema;
    }

    public void setSchema(String schema)
    {
      _schema = schema;
    }

}
