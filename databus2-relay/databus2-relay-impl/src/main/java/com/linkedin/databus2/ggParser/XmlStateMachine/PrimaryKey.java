package com.linkedin.databus2.ggParser.XmlStateMachine;

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

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus2.core.DatabusException;

/**
 * A helper class to help deal with PrimaryKeys They can be a simple key or a
 * composite key. In the case of a composite key, helper methods for parsing out
 * individual keys, search for presence of a given key
 *
 */
public class PrimaryKey
{
  private final String _pKey;
  private final List<String> _pKeyList = new ArrayList<String>(3);

  public PrimaryKey(String pKey) throws DatabusException
  {
    if (pKey == null)
    {
      throw new DatabusException("PrimaryKey cannot be null");
    }
    _pKey = pKey;
    String[] pKeyList = _pKey.split(DbusConstants.COMPOUND_KEY_SEPARATOR);
    assert (pKeyList.length >= 1);
    for (String s: pKeyList)
    {
      _pKeyList.add(s.trim());
    }
  }

  public String getPKey()
  {
    return _pKey;
  }

  public List<String> getPKeyList()
  {
    return _pKeyList;
  }

  /**
   * Check if the given key has more than one primary key elements inside
   *
   * @return true if it is a composite key ( more than one key element )
   */
  public boolean isCompositeKey()
  {
    return _pKeyList.size() == 1 ? false : true;
  }

  /**
   * Computes the number of subkeys
   * @return the number of subkeys
   */
  public int getNumKeys()
  {
    return _pKeyList.size();
  }

  /**
   * Given a schema field name, to check if it is a ( part of a ) primary key
   *
   * @param field
   * @return
   */
  protected boolean isPartOfPrimaryKey(Schema.Field field)
  {
    String trimmedFName = field.name().trim();
    return _pKeyList.contains(trimmedFName);
  }

  /**
   * A static helper method which is used for a very common use-case Given a
   * primary key field name, and a schema field check if it is a primary key
   *
   * @param pkFieldName
   * @param field
   * @return
   */
  static public boolean isPrimaryKey(String pkFieldName, Schema.Field field)
  {
    boolean isPrimaryKey = false;
    try
    {
      PrimaryKey pk = new PrimaryKey(pkFieldName);
      isPrimaryKey = pk.isPartOfPrimaryKey(field);
    } catch (DatabusException de)
    {
      isPrimaryKey = false;
    }
    return isPrimaryKey;
  }


}

