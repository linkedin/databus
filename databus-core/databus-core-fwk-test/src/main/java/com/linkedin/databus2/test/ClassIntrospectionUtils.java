/*
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
 */

package com.linkedin.databus2.test;


import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.log4j.Logger;


/**
 * The class is used to introspect all member variables of a class (private, public, protected).
 */
public class ClassIntrospectionUtils
{

  public static final Logger LOG = Logger.getLogger(ClassIntrospectionUtils.class.getName());

  Object _classInstance;
  Class<?> _classType;

  /**
   * The constructor to use introspection utils on a particular class
   * @param classInstance Instance of the particular class you want to introspect
   */
  public ClassIntrospectionUtils(Object classInstance)
  {
    _classInstance = classInstance;
    _classType = classInstance.getClass();
  }

  // ==================== Setter and getter method specific to the instance of the class ==============================
  /**
   * Get the value of the field from the class.
   * @param fieldName The name of the field
   * @param fieldClassType Class type to which the return value is cast
   * @param <T>
   * @return
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   */
  public<T> T getFieldValue(String fieldName, Class<T> fieldClassType)
      throws NoSuchFieldException, IllegalAccessException
  {
    Field field = getField(fieldName);
    Object value = field.get(_classInstance);
    return fieldClassType.cast(value);
  }

  /**
   * Set the value of the field which is not accessible[private]
   * @param fieldName The name of the field to be set
   * @param fieldValue Value to set for the field
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   */
  public void setFieldValue(String fieldName, Object fieldValue)
      throws NoSuchFieldException, IllegalAccessException
  {
    getField(fieldName).set(_classInstance, fieldValue);
  }

  /**
   * Invoke any method (including private) of the class using this method.
   * @param methodName The method name that needs to be invoked
   * @param args The parameters that are needed for the invokation of the method
   */
  public Object invokeMethod(String methodName, Object... args)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
  {
    Method method = getMethod(methodName);
    return method.invoke(_classInstance,args);
  }

  //=================== Getter and setter method for the class type (not the instance) ================================

  /**
   * Get access to the method for future invocations. The method changes the visibility of private members as well.
   * Get the 'Method' (Java type Method, not the actual field from the instance).
   * @param methodName The name of the method
   * @return
   * @throws NoSuchMethodException
   */
  public Method getMethod(String methodName)
      throws NoSuchMethodException
  {
    return methodUnwinder(methodName,_classType);
  }

  /**
   * Get the 'Field' (Java type Field, not the actual field from the instance).
   * @param fieldName The name of the field whose visibility is to be changed
   * @return
   * @throws NoSuchFieldException
   */
  public Field getField(String fieldName)
      throws NoSuchFieldException
  {
    return fieldUnwinder(fieldName, _classType);
  }

  //==================================== Helper methods for the introspection class ===================================

  /**
   * Recursively goes through the class's hierarchy to search for the particular field
   */
  private static<T> Field fieldUnwinder(String fieldName, Class<T> classType)
      throws NoSuchFieldException
  {
    Field field = null;

    if(LOG.isDebugEnabled())
      LOG.debug("Peeking into classInstance: " + classType);

    try
    {
      field = classType.getDeclaredField(fieldName);
    }
    catch (NoSuchFieldException e)
    {
      //If the field does not exist, let's check if it exists in the parent class.
      Class<?> superClass = classType.getSuperclass();
      if(superClass == null)
        throw e;

      return fieldUnwinder(fieldName, superClass);
    }

    field.setAccessible(true);
    return field;
  }

  /**
   * Recursively goes through the class's hierarchy to search for the particular method
   */
  private<T> Method methodUnwinder(String fieldName, Class<T> classType)
      throws NoSuchMethodException
  {
    Method method= null;

    if(LOG.isDebugEnabled())
      LOG.debug("Peeking into classInstance: " + classType);

    try
    {
      method = classType.getDeclaredMethod(fieldName);
    }
    catch (NoSuchMethodException e)
    {
      //If the field does not exist, let's check if it exists in the parent class.
      Class<?> superClass = classType.getSuperclass();
      if(superClass == null)
        throw e;

      return methodUnwinder(fieldName, superClass);
    }

    method.setAccessible(true);
    return method;
  }
}
