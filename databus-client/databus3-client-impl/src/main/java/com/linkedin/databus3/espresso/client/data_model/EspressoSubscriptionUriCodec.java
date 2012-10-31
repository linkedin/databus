package com.linkedin.databus3.espresso.client.data_model;

import java.net.URI;
import java.net.URISyntaxException;

import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus.core.data_model.LogicalSourceId;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.data_model.PhysicalSource;
import com.linkedin.databus.core.data_model.SubscriptionUriCodec;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus3.espresso.schema.EspressoSchemaName;

/**
 * A codec for espresso {@link DatabusSubscription} URIs.
 * Format is: espresso:[//authority]/dbName/partition[/table]
 *
 *  <ul>
 *     <li><i>authority</i> can be MASTER, SLAVE, ANY; by default is ANY
 *     <li><i>dbName</i> is the espresso DB name
 *     <li><i>partition</i> is the partition number of * for all partitions
 *     <li><i>table</i> is the espresso table name or * for all tables; default is *
 *  </ul>
 */
public class EspressoSubscriptionUriCodec implements SubscriptionUriCodec
{
  public static final String SCHEME = "espresso";
  private static final EspressoSubscriptionUriCodec THE_INSTANCE = new EspressoSubscriptionUriCodec();

  static
  {
    //make sure the instance is registered
    getInstance();
  }

  private EspressoSubscriptionUriCodec()
  {
    DatabusSubscription.registerUriCodec(this);
  }

  public static EspressoSubscriptionUriCodec getInstance()
  {
    return THE_INSTANCE;
  }

  @Override
  public String getScheme()
  {
    return SCHEME;
  }

  @Override
  public DatabusSubscription decode(URI uri) throws DatabusException
  {
    if (!SCHEME.equals(uri.getScheme()))
      throw new DatabusException("expected " + SCHEME + " subscription scheme; found " +
                                 uri.getScheme());
    if (null == uri.getPath()) throw new DatabusException("path expected in subscription URI: " + uri);

    PhysicalSource pSrc = PhysicalSource.createSlaveSourceWildcard();
    String authority = uri.getAuthority();
    if (PhysicalSource.PHYSICAL_SOURCE_MASTER.equals(authority))
      pSrc = PhysicalSource.createMasterSourceWildcard();
    else if (PhysicalSource.PHYSICAL_SOURCE_ANY.equals(authority))
      pSrc = PhysicalSource.createAnySourceWildcard();
    else if (null != authority && !PhysicalSource.PHYSICAL_SOURCE_SLAVE.equals(authority))
      throw new DatabusException("invalid authority: " + authority);

    String[] path = uri.getPath().split("/", 5);
    if (path.length > 4) throw new DatabusException("unrecognized path: " + path[4]);

    String dbName = path.length > 1 ? path[1] : null;
    if (null == dbName || 0 == dbName.length()) throw new DatabusException("espresso DB name expected: " + uri);

    String partStr = path.length > 2 ? path[2] : null;
    if (null == partStr || 0 == partStr.length()) throw new DatabusException("partition expected: " + uri);
    PhysicalPartition pp = null;
    if (partStr.equals("*")) pp = PhysicalPartition.createAnyPartitionWildcard(dbName);
    else
    {
      try
      {
        int partNum = Integer.parseInt(partStr);
        pp = new PhysicalPartition(partNum, dbName);
      }
      catch (NumberFormatException e)
      {
        throw new DatabusException("invalid partition: " + uri, e);
      }
    }

    String lsourceName = path.length > 3 ? path[3] : "*";
    if (null == lsourceName || 0 == lsourceName.length())
      throw new DatabusException("table name expected: " + uri);
    LogicalSource lsource = null;
    if (lsourceName.equals("*")) lsource = LogicalSource.createAllSourcesWildcard();
    else
    {
      EspressoSchemaName ename = EspressoSchemaName.create(pp.getName(), lsourceName);
      lsource = new LogicalSource(ename.getDatabusSourceName());
    }

    DatabusSubscription sub = new DatabusSubscription(pSrc, pp,
                                                      new LogicalSourceId(lsource, pp.getId().shortValue()));
    return sub;
  }

  @Override
  public URI encode(DatabusSubscription sub)
  {
    String authority = sub.getPhysicalSource().getUri();
    if (sub.getPhysicalSource().isAnySourceWildcard())
      authority = PhysicalSource.PHYSICAL_SOURCE_ANY;
    else if (sub.getPhysicalSource().isMasterSourceWildcard())
      authority = PhysicalSource.PHYSICAL_SOURCE_MASTER;
    else if (sub.getPhysicalSource().isSlaveSourceWildcard())
      authority = PhysicalSource.PHYSICAL_SOURCE_SLAVE;
    else throw new RuntimeException("non wildcard physical sources are not supported yet");
    StringBuilder path = new StringBuilder();
    path.append('/');
    path.append(sub.getPhysicalPartition().getName());
    path.append('/');
    if (sub.getPhysicalPartition().isAnyPartitionWildcard()) path.append('*');
    else path.append(sub.getPhysicalPartition().getId());
    path.append('/');
    if (sub.getLogicalPartition().getSource().isAllSourcesWildcard()) path.append('*');
    else path.append(sub.getLogicalPartition().getSource().getName());
    try
    {
      URI res = new URI(SCHEME, authority, path.toString(), null, null);
      return res;
    }
    catch (URISyntaxException e)
    {
      throw new RuntimeException("unable to generate subscription URI: " + e.getMessage(), e);
    }
  }

}
