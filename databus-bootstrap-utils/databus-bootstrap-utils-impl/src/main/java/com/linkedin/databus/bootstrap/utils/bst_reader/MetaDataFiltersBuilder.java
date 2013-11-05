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
package com.linkedin.databus.bootstrap.utils.bst_reader;

public class MetaDataFiltersBuilder
{
  private String _minKey;
  private String _maxKey;
  private long _minScn = -1;
  private long _maxScn = -1;

  public MetaDataFilters build()
  {
    if (_minKey != null && _maxKey != null && _maxKey.compareTo(_minKey) < 0)
    {
      throw new IllegalArgumentException("minimum key " + _minKey + " > maximum key " + _maxKey);
    }
    if (_minScn > 0 && _maxScn >0 && _maxScn > _minScn)
    {
      throw new IllegalArgumentException("minimum scn " + _minScn + " > maximum scn " + _maxScn);
    }
    return new MetaDataFilters(_minKey, _maxKey, _minScn, _maxScn);
  }

  public String minKey()
  {
    return _minKey;
  }
  public MetaDataFiltersBuilder minKey(String minKey)
  {
    _minKey = minKey;
    return this;
  }
  public String maxKey()
  {
    return _maxKey;
  }
  public MetaDataFiltersBuilder maxKey(String maxKey)
  {
    _maxKey = maxKey;
    return this;
  }
  public long minScn()
  {
    return _minScn;
  }
  public MetaDataFiltersBuilder minScn(long minScn)
  {
    _minScn = minScn;
    return this;
  }
  public long maxScn()
  {
    return _maxScn;
  }
  public MetaDataFiltersBuilder maxScn(long maxScn)
  {
    _maxScn = maxScn;
    return this;
  }
}