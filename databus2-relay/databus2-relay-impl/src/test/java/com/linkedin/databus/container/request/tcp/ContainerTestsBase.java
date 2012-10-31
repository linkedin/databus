package com.linkedin.databus.container.request.tcp;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.DbusEventBuffer;
import com.linkedin.databus.core.DbusEventBufferMult;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriterStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryStaticConfig;
import com.linkedin.databus2.schemas.VersionedSchemaSetBackedRegistryService;

public class ContainerTestsBase
{
  PhysicalSourceStaticConfig[] _psourcesConfig;
  PhysicalSourceStaticConfig[] _psourcesConfigV1;
  DbusEventBuffer.StaticConfig _defaultBufferConf;

  public void setUp() throws Exception
  {

    //default buffer configuration
    DbusEventBuffer.Config bufferConfigBuilder = new DbusEventBuffer.Config();
    bufferConfigBuilder.setMaxSize(1000000);
    bufferConfigBuilder.setReadBufferSize(10000);
    bufferConfigBuilder.setScnIndexSize(10000);
    _defaultBufferConf = bufferConfigBuilder.build();

    //create standard physical sources configuration
    _psourcesConfig = new PhysicalSourceStaticConfig[3];
    _psourcesConfigV1 = new PhysicalSourceStaticConfig[3];

    PhysicalSourceConfig pConfBuilder = new PhysicalSourceConfig();
    short lsrcId = 1;
    for (int psrcId = 1; psrcId <= 3; ++psrcId )
    {
      String partName = "psource";// + psrcId;
      pConfBuilder.setName(partName);
      pConfBuilder.setId(psrcId);
      pConfBuilder.setUri("jdbc://psource/" + psrcId);

      for (int i = 0; i < 2; ++i, ++lsrcId)
      {

        pConfBuilder.getSource(i).setId((short)(100 + lsrcId));
        pConfBuilder.getSource(i).setName(partName + ".source" + lsrcId);
        pConfBuilder.getSource(i).setUri("source" + lsrcId);
        pConfBuilder.getSource(i).setPartitionFunction("constant:1");
      }

      _psourcesConfig[psrcId - 1] = pConfBuilder.build();
      pConfBuilder.setName(PhysicalSourceConfig.DEFAULT_PHYSICAL_PARTITION_NAME);
      _psourcesConfigV1[psrcId-1] = pConfBuilder.build();
    }
  }
  DbusEventBufferMult createStandardEventBufferMult() throws Exception
  {
    return createStandardEventBufferMult(3);
  }

  DbusEventBufferMult createStandardEventBufferMult(int v) throws Exception
  {
    DbusEventBufferMult bufMult = new DbusEventBufferMult();
    PhysicalSourceStaticConfig [] staticConfig = (v==1)? _psourcesConfigV1 : _psourcesConfig;
    for (PhysicalSourceStaticConfig psourceConfig: staticConfig)
    {
      bufMult.addNewBuffer(psourceConfig, _defaultBufferConf);
    }
    bufMult.setDropOldEvents(true);
    return bufMult;
  }

  DbusEventBuffer getBufferForPartition(DbusEventBufferMult eventBufMult, int ppartId, String ppName)
  {
    PhysicalPartition ppart = new PhysicalPartition(ppartId, ppName);

    DbusEventBuffer eventBuffer = eventBufMult.getOneBuffer(ppart);
    return eventBuffer;
  }

  DbusEventBufferMult createOnePhysicalSourceEventBufferMult(int v) throws Exception
  {
    DbusEventBufferMult bufMult = new DbusEventBufferMult();
    PhysicalSourceStaticConfig [] staticConfig = (v==1) ? _psourcesConfigV1 : _psourcesConfig;
    bufMult.addBuffer(staticConfig[0], new DbusEventBuffer(_defaultBufferConf));
    return bufMult;
  }

  HttpRelay.Config createStandardRelayConfigBuilder() throws Exception
  {
    HttpRelay.Config relayConfigBuilder = new HttpRelay.Config();

    relayConfigBuilder.getEventBuffer().setMaxSize(_defaultBufferConf.getMaxSize());
    relayConfigBuilder.getEventBuffer().setReadBufferSize(_defaultBufferConf.getReadBufferSize());
    relayConfigBuilder.getEventBuffer().setScnIndexSize(_defaultBufferConf.getScnIndexSize());

    relayConfigBuilder.getSchemaRegistry().setType(SchemaRegistryStaticConfig.RegistryType.EXISTING.toString());
    relayConfigBuilder.getSchemaRegistry().useExistingService(
        new VersionedSchemaSetBackedRegistryService());

    relayConfigBuilder.setSourceName("101", "psource.source1");
    relayConfigBuilder.setSourceName("102", "psource.source2");
    relayConfigBuilder.setSourceName("103", "psource.source3");
    relayConfigBuilder.setSourceName("104", "psource.source4");
    relayConfigBuilder.setSourceName("105", "psource.source5");

    relayConfigBuilder.getDataSources().getSequenceNumbersHandler()
                      .setType(MaxSCNReaderWriterStaticConfig.Type.IN_MEMORY.toString());

    return relayConfigBuilder;
  }

}
