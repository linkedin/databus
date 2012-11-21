package com.linkedin.databus2.schemas.mbean;

import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaId;
import com.linkedin.databus2.schemas.VersionedSchemaSet;

public class VersionedSchemaSetStatus implements VersionedSchemaSetStatusMBean
{
  private final VersionedSchemaSet _schemaSet;

  public VersionedSchemaSetStatus(VersionedSchemaSet schemaSet)
  {
    super();
    _schemaSet = schemaSet;
  }

  @Override
  public String[] getLoadedSchemas()
  {
    Set<String> schemaBaseNames = _schemaSet.getSchemaBaseNames();
    String[] results = new String[schemaBaseNames.size()];
    int idx = 0;
    for (String baseName: schemaBaseNames)
    {
      StringBuilder b = new StringBuilder(1000);
      b.append(baseName);
      b.append(": ");
      SortedMap<VersionedSchemaId, VersionedSchema> versions = _schemaSet.getAllVersionsByName(baseName);
      boolean isFirst = true;
      for (Entry<VersionedSchemaId, VersionedSchema> vs: versions.entrySet())
      {
        if (!isFirst) b.append(", ");
        isFirst = false;
        b.append(vs.getKey().getVersion());
      }

      results[idx++] = b.toString();
    }

    return results;
  }

  @Override
  public String getSchema(String baseName, short version)
  {
    VersionedSchema vs = _schemaSet.getSchemaByNameVersion(baseName, version);
    return null == vs ? null : vs.getSchema().toString();
  }

}
