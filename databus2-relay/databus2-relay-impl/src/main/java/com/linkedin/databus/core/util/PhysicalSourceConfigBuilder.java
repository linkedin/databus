package com.linkedin.databus.core.util;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;


public class PhysicalSourceConfigBuilder implements ConfigBuilder<PhysicalSourceStaticConfig[]>
{
  private static final Logger LOG = Logger.getLogger(PhysicalSourceConfigBuilder.class.getName());
  File[] _fileNames = null;

  public PhysicalSourceConfigBuilder(String[] fileNames) {
    _fileNames = new File[fileNames.length];
    for(int i = 0; i < fileNames.length; ++i) _fileNames[i] = new File(fileNames[i]);
  }

  public PhysicalSourceConfigBuilder(String baseDir, String[] fileNames) {
    _fileNames = new File[fileNames.length];
    for(int i = 0; i < fileNames.length; ++i) _fileNames[i] = new File(baseDir, fileNames[i]);
  }

  @Override
  public PhysicalSourceStaticConfig[] build() throws InvalidConfigException {
    ObjectMapper mapper = new ObjectMapper();

    PhysicalSourceStaticConfig[] list =
        new PhysicalSourceStaticConfig[null == _fileNames ? 0 : _fileNames.length];
    if(_fileNames == null) return list;

    for(int i = 0; i < _fileNames.length; ++i) {
      File sourceJson = _fileNames[i];
      PhysicalSourceConfig pConfig = null;
      Exception e = null;
      try
      {
        pConfig = mapper.readValue(sourceJson, PhysicalSourceConfig.class);
      }
      catch (JsonParseException jpe) {
        e = jpe;
      } catch (JsonMappingException jme) {
        e = jme;
      } catch (IOException ioe) {
        e = ioe;
      }
      if(e != null || pConfig == null) {
        throw new InvalidConfigException(e);
      }
      pConfig.checkForNulls();
      LOG.info("Generated Physical source config: name= " + pConfig.getId());

      list[i] = pConfig.build();
    }
    /*
    for(PhysicalSourceStaticConfig pCfg : pConfigs) {
      for(LogicalSourceStaticConfig lSC : pCfg.getSources()) {
        config.setSourceName("" + lSC.getId(), lSC.getName());
      }
    }
    */
    return list;
  }
}
