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


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * A collection that keeps track of object generations (age). Each object is associated with an
 * integer age. Objects' age increases with each use and they get removed once they reach a max age.
 *
 * One use for this is to keep track of retries.
 *
 * @author cbotev
 *
 * @param <T>       the type of objects to track
 */
public class ObjectGenerationTracker<T>
{

  private static final int DEFAULT_SIZE = 16;

  private static class AgedObject<U>
  {
    private final U _object;
    private int _age;

    public AgedObject(U o)
    {
      _age = 0;
      _object = o;
    }

    public int getAge()
    {
      return _age;
    }

    public int age(int delta)
    {
      _age += delta;
      return _age;
    }

    @Override
    public boolean equals(Object o)
    {
      if (null == o || !(o instanceof AgedObject<?>)) return false;
      return _object == ((AgedObject<?>)o)._object;
    }

    @Override
    public int hashCode()
    {
      return _object.hashCode();
    }

    @Override
    public String toString()
    {
      StringBuilder result = new StringBuilder();
      result.append("{\"age\":");
      result.append(_age);
      result.append(", \"object\":");
      result.append(_object.toString());
      result.append('}');

      return result.toString();
    }

  }

  private final IdentityHashMap<T, AgedObject<T>> _objects;
  private final List<Set<AgedObject<T>>> _generations;
  private final int _maxAge;

  public ObjectGenerationTracker(int maxAge)
  {
    assert maxAge >= 0 : "negative max age";

    _maxAge = maxAge;
    _objects = new IdentityHashMap<T, AgedObject<T>>(DEFAULT_SIZE);
    _generations = new ArrayList<Set<AgedObject<T>>>(_maxAge + 1);
    for (int i = 0; i <= _maxAge; ++i)
    {
      _generations.add(new HashSet<AgedObject<T>>(DEFAULT_SIZE));
    }
  }

  /**
   * Adds a new object with age 0. If the object exists, nothing happens.
   * @param o       the object to add
   */
  public void add(T o)
  {
    AgedObject<T> agedObj = _objects.get(o);
    if (null == agedObj)
    {
      agedObj = new AgedObject<T>(o);
      _objects.put(o, agedObj);
      _generations.get(0).add(agedObj);
    }
  }

  /**
   * Adds all objects to the collection.
   *
   * @see #add(Object)
   */
  public void addAll(T... objs)
  {
    for (T o: objs) add(o);
  }

  /**
   * Adds all objects to the collection.
   *
   * @see #add(Object)
   */
  public void addAll(Collection<T> objs)
  {
    for (T o: objs) add(o);
  }

  /**
   * Ages the object in the collection with 1.
   * @param  o              the object to age
   * @return the new age of the object; if that age is greater than the max age, the object has been
   *         removed from the collection; -1 if the object was not in the collection
   */
  public int age(T o)
  {
    return age(o, 1);
  }

  /**
   * Ages the object in the collection with the specified delta.
   * @param  o              the object to age
   * @param  delta          the increment in age
   * @return the new age of the object; if that age is greater than the max age, the object has been
   *         removed from the collection; -1 if the object was not in the collection
   */
  public int age(T o, int delta)
  {
    assert delta > 0 : "non-positive delta";

    AgedObject<T> agedObj = _objects.get(o);
    if (null == agedObj) return -1;

    _generations.get(agedObj.getAge()).remove(agedObj);
    int newAge = agedObj.age(delta);
    if (newAge <= _maxAge) _generations.get(newAge).add(agedObj);

    return newAge;
  }

  /** Returns the maximum allowed age*/
  public int getMaxAge()
  {
    return _maxAge;
  }

  /** Returns the number of objects in the collection */
  public int size()
  {
    return _objects.size();
  }
}
