package com.linkedin.databus2.schemas;
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


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * Loads versioned schemas from Java resources. Tries to load a resource named index.schemas_registry.
 * The file should contain the names of the resources with the schemas, one per line.
 **/
public class ResourceVersionedSchemaSetProvider implements VersionedSchemaSetProvider
{
  public static final String MODULE = ResourceVersionedSchemaSetProvider.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String INDEX_RESOURCE_NAME = "index.schemas_registry";

  private final ClassLoader _classLoader;

  public ResourceVersionedSchemaSetProvider(ClassLoader classLoader)
  {
    _classLoader = null != classLoader ? classLoader : ClassLoader.getSystemClassLoader();
  }

  @Override
  public VersionedSchemaSet loadSchemas()
  {
    VersionedSchemaSet result = new VersionedSchemaSet();

    InputStream indexIns = null;
    indexIns = _classLoader.getResourceAsStream(INDEX_RESOURCE_NAME);
    if (null == indexIns)
    {
      LOG.info("resource not found: " + INDEX_RESOURCE_NAME + "; no schemas will be loaded");
    }
    else
    {
      BufferedReader indexReader = null;
      try
      {
        indexReader = new BufferedReader(new InputStreamReader(indexIns, "UTF-8"));

        List<String> resourceNames = readIndex(indexReader);
        LOG.info("schema resources found: " + resourceNames);
        for (String resource: resourceNames)
        {
          VersionedSchema schema = readSchemaFromResource(resource);
          if (null != schema) result.add(schema);
        }
      }
      catch (IOException ioe)
      {
        LOG.error("i/o error: " + ioe.getMessage(), ioe);
      }
      finally
      {
        try {
          indexIns.close();
          if (null != indexReader) indexReader.close();
        }
        catch (IOException e1)
        {
          LOG.error("cleanup failed: " + e1.getMessage(), e1);
        }
      }
    }
    return result;
  }

  private VersionedSchema readSchemaFromResource(String resource) throws IOException
  {
    LOG.info("loading schema resource: " + resource);
    VersionedSchemaId schemaId = FileSystemVersionedSchemaSetProvider.parseSchemaVersion(resource);
    if (null == schemaId) return null;

    InputStream schemaInput = _classLoader.getResourceAsStream(resource);
    try
    {
      String schemaJson = IOUtils.toString(schemaInput);
      VersionedSchema schema = new VersionedSchema(schemaId, Schema.parse(schemaJson), null);
      return schema;
    }
    finally
    {
      if (null != schemaInput) schemaInput.close();
    }
  }

  private List<String> readIndex(BufferedReader indexReader) throws IOException
  {
    ArrayList<String> result = new ArrayList<String>();
    String line = null;
    while (null != (line = indexReader.readLine()))
    {
      line = line.trim();
      if (0 != line.length()) result.add(line);
    }

    return result;
  }

}
