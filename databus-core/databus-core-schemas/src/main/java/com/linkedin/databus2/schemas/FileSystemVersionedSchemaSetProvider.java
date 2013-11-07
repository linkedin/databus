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


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * "Borrowed"  largely from com.linkedin.avro.FileSystemSchemaSetProvider
 */
public class FileSystemVersionedSchemaSetProvider implements VersionedSchemaSetProvider
{
  public static final String MODULE = FileSystemVersionedSchemaSetProvider.class.getName();
  private static final Logger logger = Logger.getLogger(MODULE);
  static final String DEFAULT_SCHEMA_SUFFIX = ".avsc";
  static final Pattern FILE_NAME_PATTERN = Pattern.compile("(.*)\\.(\\d+).avsc");

  private final String schemaSuffix;
  private final File[] dirs;

  public FileSystemVersionedSchemaSetProvider(List<File> dirs)
  {
    this(dirs.toArray(new File[dirs.size()]));
  }

  public FileSystemVersionedSchemaSetProvider(File... dirs)
  {
    this(DEFAULT_SCHEMA_SUFFIX, dirs);
  }

  public FileSystemVersionedSchemaSetProvider(String schemaSuffix, File... dirs)
  {
    this.dirs = Arrays.copyOf(dirs, dirs.length);
    this.schemaSuffix = schemaSuffix;
  }

  @Override
  public VersionedSchemaSet loadSchemas()
  {
    try
    {
      VersionedSchemaSet schemas = new VersionedSchemaSet();
      for(File f: dirs) loadSchemas(f, schemas);
      return schemas;
    }
    catch(IOException e)
    {
      throw new RuntimeException(e);
    }
  }

  private void loadSchemas(File f, VersionedSchemaSet schemas) throws IOException
  {
    if(!f.exists() || !f.canRead())
    {
      throw new IllegalArgumentException("File does not exist or cannot be read: " + f.getAbsolutePath());
    }

    if(f.isDirectory())
    {
      File[] files = f.listFiles();
      if(files != null)
      {
        for(File child: files) loadSchemas(child, schemas);
      }
    }
    else if(f.getName().endsWith(schemaSuffix))
    {
      VersionedSchemaId verSchemaId = parseSchemaVersion(f.getName());

      if (null != verSchemaId)
      {
        logger.info("Loading schema " + verSchemaId + " from file: " + f);
        InputStream input = new FileInputStream(f);
        try
        {
          String schemaJson = IOUtils.toString(input);
          schemas.add(new VersionedSchema(verSchemaId, Schema.parse(schemaJson), null));
        }
        finally
        {
          IOUtils.closeQuietly(input);
        }
      }
    }
    else
    {
      logger.debug("Ignoring file " + f + " as it does not end with the required suffix '" + schemaSuffix + "'.");
    }
  }

  static VersionedSchemaId parseSchemaVersion(String fileName) throws IOException
  {
    Matcher matcher = FILE_NAME_PATTERN.matcher(fileName);
    if (matcher.matches())
    {
      String baseName = matcher.group(1);
      short version = Short.parseShort(matcher.group(2));
      return new VersionedSchemaId(baseName, version);
    }
    else
    {
      logger.warn("Invalid file name: " + fileName);
      return null;
     }
  }

}
