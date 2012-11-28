package com.linkedin.databus2.schemas;

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
    BufferedReader indexReader = null;
    try
    {
      indexIns = _classLoader.getResourceAsStream(INDEX_RESOURCE_NAME);
      indexReader = new BufferedReader(new InputStreamReader(indexIns));

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
        if (null != indexIns) indexIns.close();
        if (null != indexReader) indexReader.close();
      }
      catch (IOException e1)
      {
        LOG.error("cleanup failed: " + e1.getMessage(), e1);
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
      VersionedSchema schema = new VersionedSchema(schemaId, Schema.parse(schemaJson));
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
