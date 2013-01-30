package com.linkedin.databus.core.util;
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


public class IdNamePair
{
  private static final Long DEFAULT_ID = Long.valueOf(-1);
  private static final String DEFAULT_NAME = "NoName";

  private Long _id;
  private String _name;

  public IdNamePair(Long id, String name)
  {
    super();
    _id = id;
    _name = name;
  }

  public IdNamePair()
  {
    this(DEFAULT_ID, DEFAULT_NAME);
  }

  public Long getId()
  {
    return _id;
  }

  public void setId(Long id)
  {
    _id = id;
  }

  public String getName()
  {
    return _name;
  }

  public void setName(String name)
  {
    _name = name;
  }
  
  @Override
  public boolean equals(Object other)
  {
    if (null == other || ! (other instanceof IdNamePair)) return false;
    return equalsPair((IdNamePair)other);
  }

  public boolean equalsPair(IdNamePair other)
  {
    return _id.equals(other._id) && _name.equals(other._name);
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder(_name.length() + 20);
    sb.append("{\"id\":");
    sb.append(_id);
    sb.append(", \"name\":\"");
    sb.append(_name);
    sb.append("\"}");
    return sb.toString();
  }

  @Override
  public int hashCode()
  {
    return _name.hashCode() ^ _id.intValue();
  }

}
