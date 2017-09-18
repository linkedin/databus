package com.linkedin.databus.container.request;
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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.util.CompressUtil;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.ChunkedWritableByteChannel;
import com.linkedin.databus2.core.container.DatabusHttpHeaders;
import com.linkedin.databus2.core.container.monitoring.mbean.HttpStatisticsCollector;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import com.linkedin.databus2.core.container.request.InvalidRequestParamValueException;
import com.linkedin.databus2.core.container.request.RegisterResponseEntry;
import com.linkedin.databus2.core.container.request.RegisterResponseMetadataEntry;
import com.linkedin.databus2.core.container.request.RequestProcessingException;
import com.linkedin.databus2.core.container.request.RequestProcessor;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaSet;


public class RegisterRequestProcessor implements RequestProcessor
{
  public static final String MODULE = RegisterRequestProcessor.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  public final static String COMMAND_NAME = "register";
  public final static String SOURCES_PARAM = "sources";

  private final ExecutorService _executorService;
  private final HttpRelay _relay;

  public RegisterRequestProcessor(ExecutorService executorService, HttpRelay relay)
  {
    super();
    _executorService = executorService;
    _relay = relay;
  }

  @Override
  public ExecutorService getExecutorService()
  {
    return _executorService;
  }

  @Override
  public DatabusRequest process(DatabusRequest request) throws IOException,
                                                               RequestProcessingException
  {
    try
    {
      // fail early if optional version param is included but isn't valid
      int registerRequestProtocolVersion = 3;  // 2 and 3 are same for us; 4 is a superset only newer clients understand
      String registerRequestProtocolVersionStr =
          request.getParams().getProperty(DatabusHttpHeaders.PROTOCOL_VERSION_PARAM);
      if (registerRequestProtocolVersionStr != null)
      {
        try
        {
          registerRequestProtocolVersion = Integer.parseInt(registerRequestProtocolVersionStr);
        }
        catch (NumberFormatException e)
        {
          LOG.error("Could not parse /register request protocol version: " + registerRequestProtocolVersionStr);
          throw new InvalidRequestParamValueException(COMMAND_NAME, DatabusHttpHeaders.PROTOCOL_VERSION_PARAM,
                                                      registerRequestProtocolVersionStr);
        }
        if (registerRequestProtocolVersion < 2 || registerRequestProtocolVersion > 4)
        {
          LOG.error("Out-of-range /register request protocol version: " + registerRequestProtocolVersionStr);
          throw new InvalidRequestParamValueException(COMMAND_NAME, DatabusHttpHeaders.PROTOCOL_VERSION_PARAM,
                                                      registerRequestProtocolVersionStr);
        }
      }

      Collection<LogicalSource> logicalSources = null;
      HttpStatisticsCollector relayStatsCollector = _relay.getHttpStatisticsCollector();

      String sources = request.getParams().getProperty(SOURCES_PARAM);
      if (null == sources)
      {
        // need to return all schemas, so first get all sources
        logicalSources = _relay.getSourcesIdNameRegistry().getAllSources();
      }
      else
      {
        String[] sourceIds = sources.split(",");
        logicalSources = new ArrayList<LogicalSource>(sourceIds.length);

        for (String sourceId: sourceIds)
        {
          int srcId;
          String trimmedSourceId = sourceId.trim();
          try
          {
            srcId = Integer.valueOf(trimmedSourceId);
            LogicalSource lsource = _relay.getSourcesIdNameRegistry().getSource(srcId);
            if (null != lsource) logicalSources.add(lsource);
            else
            {
              LOG.error("No source name for source id: " + srcId);
              throw new InvalidRequestParamValueException(COMMAND_NAME, SOURCES_PARAM, sourceId);
            }
          }
          catch (NumberFormatException nfe)
          {
            if (relayStatsCollector != null)
            {
              relayStatsCollector.registerInvalidRegisterCall();
            }
            throw new InvalidRequestParamValueException(COMMAND_NAME, SOURCES_PARAM, sourceId);
          }
        }
      }

      SchemaRegistryService schemaRegistry = _relay.getSchemaRegistryService();
      ArrayList<RegisterResponseEntry> registeredSources = new ArrayList<RegisterResponseEntry>(20);

      for (LogicalSource lsource: logicalSources)
      {
        getSchemas(schemaRegistry, lsource.getName(), lsource.getId(), sources, registeredSources);
      }

      // Note that, as of April 2013, the Espresso sandbox's schema registry
      // (in JSON format) is 4.5 MB and growing.  But 100 KB is probably OK
      // for regular production cases.
      StringWriter out = new StringWriter(102400);
      ObjectMapper mapper = new ObjectMapper();

      // any circumstances under which we might want to override this?
      int registerResponseProtocolVersion = registerRequestProtocolVersion;

      if (registerRequestProtocolVersion == 4)  // DDSDBUS-2009
      {
        LOG.debug("Got version 4 /register request; fetching metadata schema.");
        // Get (replication) metadata schema from registry; format it as list
        // of schemas (multiple only if more than one version exists).  Per
        // https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Espresso+Metadata+Schema,
        // name of replication metadata is simply "metadata".
        ArrayList<RegisterResponseMetadataEntry> registeredMetadata = new ArrayList<RegisterResponseMetadataEntry>(2);
        getMetadataSchemas(schemaRegistry, registeredMetadata);

        // Set up the v4 response as a map:  one entry is the existing list of source
        // schemas, and the others (if present) are the new lists of metadata schema(s)
        // and (TODO) key schemas.
        HashMap<String, List<Object>> responseMap = new HashMap<String, List<Object>>(4);
        responseMap.put(RegisterResponseEntry.SOURCE_SCHEMAS_KEY, (List<Object>)(List<?>)registeredSources);
        if (registeredMetadata.size() > 0)
        {
          LOG.debug("Sending v4 /register response with metadata schema.");
          responseMap.put(RegisterResponseMetadataEntry.METADATA_SCHEMAS_KEY,
                          (List<Object>)(List<?>)registeredMetadata);
        }
        else
        {
          LOG.debug("No metadata schema available; sending v4 /register response without.");
        }
        // TODO:  figure out how to retrieve key schemas and include via RegisterResponseEntry.KEY_SCHEMAS_KEY
        mapper.writeValue(out, responseMap);
      }
      else // fall back to old style (v2/v3 response)
      {
        mapper.writeValue(out, registeredSources);
      }
      String outStr = out.toString();
      String compress = request.getParams().getProperty(DatabusHttpHeaders.PROTOCOL_COMPRESS_PARAM);
      if ("true".equals(compress))
      {
        outStr = CompressUtil.compress(outStr);
      }

      ChunkedWritableByteChannel responseContent = request.getResponseContent();
      byte[] resultBytes = outStr.getBytes(Charset.defaultCharset());
      responseContent.addMetadata(DatabusHttpHeaders.DBUS_CLIENT_RELAY_PROTOCOL_VERSION_HDR,
                                  registerResponseProtocolVersion);
      responseContent.write(ByteBuffer.wrap(resultBytes));

      if (null != relayStatsCollector)
      {
        HttpStatisticsCollector connStatsCollector = (HttpStatisticsCollector)
        request.getParams().get(relayStatsCollector.getName());
        if (null != connStatsCollector)
        {
          connStatsCollector.registerRegisterCall(registeredSources);
        }
        else
        {
          relayStatsCollector.registerRegisterCall(registeredSources);
        }
      }

      return request;
    }
    catch (InvalidRequestParamValueException e)
    {
      HttpStatisticsCollector relayStatsCollector = _relay.getHttpStatisticsCollector();
      if (null != relayStatsCollector)
        relayStatsCollector.registerInvalidRegisterCall();
      throw e;
    }
  }

  /**
   * Returns list of versioned source (or key?) schemas for the given source name and associates
   * them with the specified ID.
   * TODO:  either add support for key schemas or rename method to getSourceSchemas()
   */
  private void getSchemas(SchemaRegistryService schemaRegistry,                // IN
                          String name,                                         // IN:  source name
                          Integer sourceId,                                    // IN
                          String sources,                                      // IN (for error-logging only)
                          ArrayList<RegisterResponseEntry> registeredSources)  // OUT
  throws RequestProcessingException
  {
    Map<Short, String> versionedSchemas = null;
    try
    {
      versionedSchemas = schemaRegistry.fetchAllSchemaVersionsBySourceName(name);
    }
    catch (DatabusException ie)
    {
      HttpStatisticsCollector relayStatsCollector = _relay.getHttpStatisticsCollector();
      if (relayStatsCollector != null) relayStatsCollector.registerInvalidRegisterCall();
      throw new RequestProcessingException(ie);
    }

    if ((null == versionedSchemas) || (versionedSchemas.isEmpty()))
    {
      HttpStatisticsCollector relayStatsCollector = _relay.getHttpStatisticsCollector();
      if (relayStatsCollector != null) relayStatsCollector.registerInvalidRegisterCall();
      LOG.error("Problem fetching schema for sourceId " + sourceId + "; sources string = " + sources);
    }
    else
    {
      for (Entry<Short, String> e : versionedSchemas.entrySet())
      {
        registeredSources.add(new RegisterResponseEntry(sourceId.longValue(), e.getKey(), e.getValue()));
      }
    }
  }

  /**
   * Returns list of versioned metadata schemas.
   * TODO (DDSDBUS-2093):  implement this.
   */
  private void getMetadataSchemas(SchemaRegistryService schemaRegistry,                         // IN
                                  ArrayList<RegisterResponseMetadataEntry> registeredMetadata)  // OUT
  throws RequestProcessingException
  {
    Map<SchemaId, VersionedSchema> versionedSchemas = null;
    try
    {
      VersionedSchemaSet schemaSet = schemaRegistry.fetchAllMetadataSchemaVersions();
      if (schemaSet != null)
      {
        versionedSchemas = schemaSet.getAllVersionsWithSchemaId(SchemaRegistryService.DEFAULT_METADATA_SCHEMA_SOURCE);
      }
    }
    catch (DatabusException ie)
    {
      HttpStatisticsCollector relayStatsCollector = _relay.getHttpStatisticsCollector();
      if (relayStatsCollector != null) relayStatsCollector.registerInvalidRegisterCall();
      throw new RequestProcessingException(ie);
    }

    if (versionedSchemas != null && !versionedSchemas.isEmpty())
    {
      for (SchemaId id:  versionedSchemas.keySet())
      {
        VersionedSchema entry = versionedSchemas.get(id);
        if (entry.getOrigSchemaStr() == null)
        {
          throw new RequestProcessingException("Null schema string for metadata version " + entry.getVersion());
        }
        registeredMetadata.add(new RegisterResponseMetadataEntry((short)entry.getVersion(),
                                                                 entry.getOrigSchemaStr(),
                                                                 id.getByteArray()));
      }
    }
  }
}
