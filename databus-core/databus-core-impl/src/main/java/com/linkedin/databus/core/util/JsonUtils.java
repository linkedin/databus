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


import java.io.IOException;
import java.io.StringWriter;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/** Helper JSON functions */
public class JsonUtils
{
  public static final String JSON_SERIALIZATION_FAILURE_STRING = "JSON serialization failed";

  /**
   * Serializes a bean as JSON
   * @param <T>     the bean type
   * @param bean    the bean to serialize
   * @param pretty  a flag if the output is to be pretty printed
   * @return        the JSON string
   */
  public static <T>String toJsonString(T bean, boolean pretty)
         throws JsonGenerationException, JsonMappingException, IOException
  {
    JsonFactory jsonFactory = new JsonFactory(new ObjectMapper());

    StringWriter out = new StringWriter(1000);
    JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(out);
    if (pretty) jsonGenerator.useDefaultPrettyPrinter();
    jsonGenerator.writeObject(bean);
    out.flush();

    return out.toString();
  }

  /**
   * Serializes a bean as JSON. This method will not throw an exception if the serialization fails
   * but it will instead return the string {@link #JSON_SERIALIZATION_FAILURE_STRING}
   * @param <T>     the bean type
   * @param bean    the bean to serialize
   * @param pretty  a flag if the output is to be pretty printed
   * @return        the JSON string or {@link #JSON_SERIALIZATION_FAILURE_STRING} if serialization
   *                fails
   */
  public static <T>String toJsonStringSilent(T bean, boolean pretty)
  {
    try
    {
      return toJsonString(bean, pretty);
    }
    catch (IOException ioe)
    {
      return JSON_SERIALIZATION_FAILURE_STRING;
    }
  }
}
