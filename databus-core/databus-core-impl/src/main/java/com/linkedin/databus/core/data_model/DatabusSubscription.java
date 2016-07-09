package com.linkedin.databus.core.data_model;
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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.util.IdNamePair;
import com.linkedin.databus2.core.DatabusException;


public class DatabusSubscription
{
  public static final Logger LOG = Logger.getLogger(DatabusSubscription.class);

  private final PhysicalSource _physicalSource;
  private final PhysicalPartition _physicalPartition;
  private final LogicalSourceId _logicalPartition;

  private static volatile SubscriptionUriCodec _defaultCodec = LegacySubscriptionUriCodec.getInstance();
  private static final Map<String, SubscriptionUriCodec> _uriCodecs
      = new ConcurrentHashMap<String, SubscriptionUriCodec>(3);
  private  static ServiceLoader<SubscriptionUriCodec> _codecSetLoader = ServiceLoader.load(SubscriptionUriCodec.class);

  static
  {
    loadAndRegisterCodecs();
  }

  /**
   * An API method for use by external clients to create a subscription object from a URI string
   * The format of the URI string is described below
   *
   * @param subUriString : A string of the form
   *                       espresso://[MASTER|SLAVE|ANY]/EspressoDBName/PartitionNumber/TableName
   *                       It is possible to specify a wildcard(*) for Partition number and tableName
   *
   * @return A DatabusSubscription object that may be used to register a databus consumer to a DatabusV3 Client
   * @throws DatabusException
   * @throws URISyntaxException
   */
  public static DatabusSubscription createFromUri(String subUriString)
         throws DatabusException, URISyntaxException
  {
    //a hack for the default URI decoder where there may be a colon as a partition separator
    //this will make the URI parser try to decoded it as a scheme separator
    int colonIdx = subUriString.indexOf(':');
    if (colonIdx >= 0)
    {
      String prefix = subUriString.substring(0, colonIdx);
      if (! _uriCodecs.containsKey(prefix) && !_defaultCodec.getScheme().equals(prefix))
        subUriString = _defaultCodec.getScheme() + ":" + subUriString;
    }
    URI subUri = new URI(subUriString);
    return createFromUri(subUri);
  }

  /**
   * Given a list of subscription strings, creates a list of DatabusSubscription objects
   *
   * @param subUriStringList : Decodes a list of subscription URIs
   * @return List<DatabusSubscription> : List of associated subscription objects in the corresponding order
   */
  public static List<DatabusSubscription> createFromUriList(Collection<String> subUriStringList)
      throws DatabusException, URISyntaxException
  {
    List<DatabusSubscription> subList = new ArrayList<DatabusSubscription>(subUriStringList.size());
    for (String subUriString: subUriStringList)
    {
      subList.add(createFromUri(subUriString));
    }
    return subList;
  }

  /**
   * DO NOT USE externally. This is meant for internal databus usage. Please use {@link createFromUri(String)}
   * instead.
   * @see <a href="https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Databus+2.0+and+Databus+3.0+Data+Model"/>
   *
   * A DatabusSubscription object is represents the smallest unit of subscription in Databus client.
   * @param physicalSource - Describes the server which physically stores co-located logical sources.
   *                         Typically this is a Oracle or MySQL server instance
   * @param physicalPartition - In Databus 2.0, it represents the database instance. When it is "master"
   *                            it repsents database instance in our primary colo.
   *                            In Databus 3.0, it represents the instance to which all updates (writes)
   *                            are routed to
   * @param logicalPartition- Represents a logical source ( and its representation with an id ). It is a
   *                          collection of data records with the same record schema.
   */
  public DatabusSubscription(PhysicalSource physicalSource,
                             PhysicalPartition physicalPartition,
                             LogicalSourceId logicalPartition)
  {
    super();
    if (null == physicalSource) throw new NullPointerException("physical source");
    if (null == physicalPartition) throw new NullPointerException("physical partition");
    if (null == logicalPartition) throw new NullPointerException("logical partition");
    _physicalSource = physicalSource;
    _physicalPartition = physicalPartition;
    _logicalPartition = logicalPartition;
  }

  /**
   * DO NOT USE externally. This is meant for internal databus usage
   * This may be removed in a future release
   * @deprecated
   */
  @Deprecated
  public static DatabusSubscription createSubscription(IdNamePair pair, short lPartitionId)
  {
    LogicalSource ls = new LogicalSource(pair);
    // this is ESPRESSO specific code - TODO needs to be moved ? (DDSDBUS-107)
    String name = pair.getName();
    String[] idx = name.split("\\.");
    if(idx.length != 2 && !name.equals("*")) //v2 mode may have source of form com.linkedin.databus.member2 for e.g.
    	return createSimpleSourceSubscription(pair.getName());
    // v3 case
    String dbName = idx[0];
    PhysicalPartition pPart = new PhysicalPartition((int)lPartitionId, dbName);
    LogicalSourceId lSrcId = new LogicalSourceId(ls, lPartitionId);

    return new DatabusSubscription(PhysicalSource.createAnySourceWildcard(),
                                   pPart,
                                   lSrcId);
  }


  public static DatabusSubscription createMasterSourceSubscription(LogicalSource source)
  {
    return new DatabusSubscription(PhysicalSource.createMasterSourceWildcard(),
                                   PhysicalPartition.createAnyPartitionWildcard(),
                                   LogicalSourceId.createAllPartitionsWildcard(source));
  }

  public static DatabusSubscription createSlaveSourceSubscription(LogicalSource source)
  {
    return new DatabusSubscription(PhysicalSource.createSlaveSourceWildcard(),
                                   PhysicalPartition.createAnyPartitionWildcard(),
                                   LogicalSourceId.createAllPartitionsWildcard(source));
  }

  /**
   * DO NOT USE externally. This is meant for internal databus usage
   * This may be removed in a future release
   * @deprecated
   */
  @Deprecated
  public static DatabusSubscription createSimpleSourceSubscription(LogicalSource source)
  {
    return new DatabusSubscription(PhysicalSource.createAnySourceWildcard(),
                                   PhysicalPartition.createAnyPartitionWildcard(),
                                   LogicalSourceId.createAllPartitionsWildcard(source));
  }

   /**
   * DO NOT USE externally. This is meant for internal databus usage
   * This may be removed in a future release
   * TODO Make private and/or change name when we are sure nobody is using these.
   */
  @Deprecated
  private static DatabusSubscription createSimpleSourceSubscription(String source)
  {
    LogicalSource ls = new LogicalSource(source);
    return new DatabusSubscription(PhysicalSource.createAnySourceWildcard(),
                               PhysicalPartition.createAnyPartitionWildcard(),
                               LogicalSourceId.createAllPartitionsWildcard(ls));
  }

  /**
   * A method to convert from a subscription to a string representation of a source
   * TODO Look like this method also uses old-style epsresso subscriptions strings?
   */
  public String generateSubscriptionString()
  {
	  String name = getLogicalSource().getName();
	  String[] idx = name.split("\\.");
	  //v2 mode may have source of form com.linkedin.databus.member2.
	  // Also wild card logical sources are only supported in V3.
	  return name;
    // FIXME Incorrect base implementation
	  //if(idx.length != 2 && !name.equals("*"))
		//  return name;

	  // v3 case
    //  SubscriptionUriCodec codec = DatabusSubscription.getUriCodec("espresso");
	  //URI u = codec.encode(this);
	  //return u.toString();
  }

  /**
   * Given a subscription, the method below constructs a pretty name
   *
   * The expected input/output is of the following formats:
   * 1. subs = ["com.linkedin.events.db.dbPrefix.tableName"]
   *    prettyName = "dbPrefix_tableName"
   *
   * 2. subs = ["com.linkedin.events.db.dbPrefix1.tableName1","com.linkedin.events.db.dbPrefix2.tableName2"],
   *    prettyName = "dbPrefix1_tableName1_dbPrefix2_tableName2"
   *
   * 3. subs =["espresso:/db/1/tableName1"
   *    prettyName = "db_tableName1_1"
   *
   * 4. subs =["espresso:/db/<wildcard>/tableName1"]. where wildcard=*
   *    prettyName = "db_tableName1"
   *
   * 5. subs =["espresso:/db/1/<wildcard>"]. where wildcard=*
   *    prettyName = "db_1"
   */
  public String createPrettyNameFromSubscription()
  {
    String s = generateSubscriptionString();
    URI u = null;
    try
    {
      u = new URI(s);
    } catch (URISyntaxException e){
      throw new DatabusRuntimeException("Unable to decode a URI from the string s = " + s + " subscription = " + toString());
    }


    if (null == u.getScheme())
    {
      // TODO: Have V2 style subscriptions have an explicit codec type. Make it return "legacy" codec
      // here. Given a subscription string, we should be able to convert it to DatabusSubscription
      // in an idempotent way. That is, converting back and forth should give the same value

      // Subscription of type com.linkedin.databus.events.dbName.tableName
      String[] parts = s.split("\\.");
      int len = parts.length;
      if (len == 0)
      {
        // Error case
        String errMsg = "Unexpected format for subscription. sub = " + toString() + " string = " + s;
        throw new DatabusRuntimeException(errMsg);
      }
      else if (len == 1)
      {
        // Unit-tests case: logicalSource is specified as "source1"
        return parts[0];
      }
      else
      {
        // Expected case. com.linkedin.databus.events.dbName.tableName
        String pn = parts[len-2] + "_" + parts[len-1];
        return pn;
      }
    }
    else if (u.getScheme().equals("espresso"))
    {
      // Given that this subscription conforms to EspressoSubscriptionUriCodec,
      // logicalSourceName (DBName.TableName) and partitionNumber(1) are guaranteed to be non-null
      String dbName = getPhysicalPartition().getName();
      boolean isWildCardOnTables = getLogicalSource().isAllSourcesWildcard();
      String name = getLogicalPartition().getSource().getName();
      boolean isWildCardOnPartitions = getPhysicalPartition().isAnyPartitionWildcard();
      String pId =  getPhysicalPartition().getId().toString();

      StringBuilder sb = new StringBuilder();
      sb.append(dbName);

      if (! isWildCardOnTables)
      {
        sb.append("_");
        String[] parts = name.split("\\.");
        assert(parts.length == 2);
        sb.append(parts[1]);
      }

      if (!isWildCardOnPartitions)
      {
        sb.append("_");
        sb.append(pId);
      }

      s = sb.toString();
    }
    else
    {
      String errMsg = "The subscription object described as " + toString() + " is not of null or espresso type codec";
      throw new DatabusRuntimeException(errMsg);
    }
    return s;
  }

  /**
   * A utility method provided to compute a pretty name when a list of subscriptions are provided.
   * This is a fairly common use-case when multiple subscriptions are specified in a registration
   * for Databus V2
   *
   * Each of the individual subscriptions prettyNames are concatenated with an "_" in between if they
   * are different
   *
   */
  public static String getPrettyNameForListOfSubscriptions(List<DatabusSubscription> subscriptions)
  {
    if (null == subscriptions)
    {
      return "";
    }

    Set<String> prettyNames = new TreeSet<String>();
    // Collect all prettyNames
    for(DatabusSubscription sub: subscriptions)
    {
        String curPartName = sub.createPrettyNameFromSubscription();
        prettyNames.add(curPartName);
    }

    StringBuilder sb = new StringBuilder();
    Iterator<String> iter = prettyNames.iterator();
    while (iter.hasNext())
    {
      sb.append(iter.next());
      if (iter.hasNext())
      {
        sb.append("_");
      }
    }
    return sb.toString();
  }

  /**
   * DO NOT USE externally. This is meant for internal databus usage
   * This may be removed in a future release
   */
  public static DatabusSubscription createPhysicalPartitionReplicationSubscription
      (PhysicalPartition physicalPartition)
  {
    return new DatabusSubscription(PhysicalSource.createMasterSourceWildcard(),
                                   physicalPartition,
                                   LogicalSourceId.createAllPartitionsWildcard(
                                       LogicalSource.createAllSourcesWildcard()));
  }

  /**
   * DO NOT USE externally. This is meant for internal databus usage
   * Convert a list of sources specified in V2 format (String) to V3 format (DatabusSubscription)
   */
  public static List<DatabusSubscription> createSubscriptionList(List<String> sources)
  {
	  List<DatabusSubscription> subsSources = new ArrayList<DatabusSubscription>();
	  for (String s : sources)
	  {
      DatabusSubscription sub = null;
      try
      {
        sub = DatabusSubscription.createFromUri(s);
        subsSources.add(sub);
      } catch (DatabusException d)
      {
        LOG.error("Error processing subscription " + sub + " with exception "
            + d);
      } catch (URISyntaxException e)
      {
        LOG.error(e);
      }
	  }
	  return subsSources;
  }

  /**
   * DO NOT USE externally. This is meant for internal databus usage
   * Convert a list of sources specified in V3 format (DatabusSubscription) to V2 format (String)
   */
  public static List<String> getStrList(List<DatabusSubscription> sources)
  {
	  List<String> strSources = new ArrayList<String>();
	  for (DatabusSubscription sub : sources)
	  {
		  String s = sub.generateSubscriptionString();
		  strSources.add(s);
	  }
	  return strSources;
  }

  /**
   * Create a DatabusSubscription object from a JSON string
   * @param  json           the string with JSON serialization of the DatabusSubscription
   */
  public static DatabusSubscription createFromJsonString(String json)
         throws JsonParseException, JsonMappingException, IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    Builder result = mapper.readValue(json, Builder.class);
    return result.build();
  }

  public PhysicalSource getPhysicalSource()
  {
    return _physicalSource;
  }

  public PhysicalPartition getPhysicalPartition()
  {
    return _physicalPartition;
  }

  public LogicalSourceId getLogicalPartition()
  {
    return _logicalPartition;
  }

  public LogicalSource getLogicalSource()
  {
    return _logicalPartition.getSource();
  }

  public boolean equalsSubscription(DatabusSubscription other)
  {
    // subscriptions - we don't check one to one
    boolean eq = _physicalSource.isAnySourceWildcard() || other._physicalSource.isAnySourceWildcard() ||
        _physicalSource.equals(other._physicalSource);

    if(!eq)
      return false;

    eq = _physicalPartition.isAnyPartitionWildcard() || other._physicalPartition.isAnyPartitionWildcard() ||
        _physicalPartition.equals(other._physicalPartition);
    if(!eq)
      return false;

    eq = _logicalPartition.equals(other._logicalPartition);
    if(!eq)
      return false;

    return true;
  }

  @Override
  public boolean equals(Object other)
  {
    if (null == other || !(other instanceof DatabusSubscription)) return false;
    return equalsSubscription((DatabusSubscription)other);
  }

  @Override
  public int hashCode()
  {
    return _physicalSource.hashCode() ^ _physicalPartition.hashCode() ^
           _logicalPartition.hashCode();
  }

  public String toJsonString()
  {
    StringBuilder sb = new StringBuilder(200);
    sb.append("{\"physicalSource\":");
    sb.append(_physicalSource.toJsonString());
    sb.append(",\"physicalPartition\":");
    sb.append(_physicalPartition.toJsonString());
    sb.append(",\"logicalPartition\":");
    sb.append(_logicalPartition.toJsonString());
    sb.append("}");

    return sb.toString();
  }

  /**
   * generate a uniq string representation per subscription
   * @return string.
   */
  public String uniqString(){
    StringBuilder sb = new StringBuilder();
    sb.append(_physicalPartition.getName());
    sb.append('_');
    sb.append(_physicalPartition.getId());
    sb.append('_');
    sb.append(_logicalPartition.getSource().getId());
    sb.append('_');
    sb.append(_logicalPartition.getSource().getName());
    sb.append('_');
    sb.append(_logicalPartition.getId());
    return sb.toString();
  }

  public static DatabusSubscription createFromUri(URI subUri) throws DatabusException
  {
    SubscriptionUriCodec codec = (null == subUri.getScheme() || 0 == subUri.getScheme().length()
        || subUri.getScheme().equals(_defaultCodec.getScheme()) ) ?
        _defaultCodec : _uriCodecs.get(subUri.getScheme());
    if (null == codec) codec = _defaultCodec;
    return codec.decode(subUri);
  }

  /**
   * Given a comma-separated list of subscription URIs specified as strings, returns a list of
   * DatabusSubscription objects
   *
   * @param subUriListString : Comma-separated list of subscription URIs
   * @return List<DatabusSubscription> : List of associated subscription objects in the corresponding order
   */
  public static List<DatabusSubscription> createFromUriListString(String subUriListString)
          throws DatabusException, URISyntaxException
  {
    String[] subUriStringList = subUriListString.split(",");
    List<DatabusSubscription> subList = new ArrayList<DatabusSubscription>(subUriStringList.length);
    for (String subUriString: subUriStringList)
    {
      subList.add(createFromUri(subUriString));
    }
    return subList;
  }

  /**
   * DO NOT USE externally. This is meant for internal databus usage
   */
  public static List<String> createUriStringList(Collection<DatabusSubscription> subs,
                                           SubscriptionUriCodec codec)
  {
    ArrayList<String> result = new ArrayList<String>(subs.size());
    for (DatabusSubscription sub: subs)
    {
      String uri = codec.encode(sub).toString();
      result.add(uri);
    }

    return result;
  }

  /**
   * loads and registers codecs
   */
  public static void loadAndRegisterCodecs()
  {
    LOG.info("Registering URI codecs.");
    for (SubscriptionUriCodec codec: _codecSetLoader)
    {
        LOG.info("Registering URI codec:" + codec.getScheme());
        registerUriCodec(codec);
    }
  }

  /**
   * Registers a new subscription URI codec. If a codec for that scheme already exists, it will be
   * replaced.
   * @param codec   the codec to register
   */
  public static void registerUriCodec(SubscriptionUriCodec codec)
  {
    if (null == codec.getScheme() || 0 == codec.getScheme().length())
    {
      _defaultCodec = codec;
    }
    else
    {
      SubscriptionUriCodec old = _uriCodecs.put(codec.getScheme(), codec);
      if (null != old)
      {
        LOG.warn("replacing existing codec for scheme " + old.getScheme() + ": " + old);
      }
    }
  }

  /**
   * Unregisters the specified codec
   */
  public static void unregisterUriCodec(SubscriptionUriCodec codec)
  {
    if(codec.equals(_uriCodecs.get(codec.getScheme()))) _uriCodecs.remove(codec.getScheme());
  }

  /**
   * Unregisters the codec for the specified scheme
   */
  public static void unregisterUriCodec(String codecScheme)
  {
    _uriCodecs.remove(codecScheme);
  }

  /**
   * Obtains the subscription URI codec for a given scheme (e.g. oracle or espresso).
   * @return the codec or null if none exists */
  public static SubscriptionUriCodec getUriCodec(String scheme)
  {
    return _uriCodecs.get(scheme);
  }

  @Override
  public String toString()
  {
    return toJsonString();
  }

  public StringBuilder toSimpleString(StringBuilder sb)
  {
    if (null == sb)
    {
      sb = new StringBuilder(100);
    }
    sb.append("[ps=");
    _physicalSource.toSimpleString(sb).append(", pp=");
    _physicalPartition.toSimpleString(sb).append(", ls=");
    _logicalPartition.getSource().toSimpleString(sb).append("]");

    return sb;
  }

  public String toSimpleString()
  {
    return toSimpleString(null).toString();
  }

  public static class Builder
  {
    private PhysicalSource.Builder _physicalSource = new PhysicalSource.Builder();;
    private PhysicalPartition.Builder _physicalPartition = new PhysicalPartition.Builder();
    private LogicalSourceId.Builder _logicalPartition = new LogicalSourceId.Builder();

    public PhysicalSource.Builder getPhysicalSource()
    {
      return _physicalSource;
    }

    public void setPhysicalSource(PhysicalSource.Builder physicalSource)
    {
      _physicalSource = physicalSource;
    }

    public PhysicalPartition.Builder getPhysicalPartition()
    {
      return _physicalPartition;
    }

    public void setPhysicalPartition(PhysicalPartition.Builder physicalPartition)
    {
      _physicalPartition = physicalPartition;
    }

    public LogicalSourceId.Builder getLogicalPartition()
    {
      return _logicalPartition;
    }

    public void setLogicalPartition(LogicalSourceId.Builder logicalSourceId)
    {
      _logicalPartition = logicalSourceId;
    }

    public DatabusSubscription build()
    {
      return new DatabusSubscription(_physicalSource.build(), _physicalPartition.build(),
                                     _logicalPartition.build());
    }
    public boolean isEqualToSource(String source) {
      LogicalSource ls = _logicalPartition.getSource().build();
      if(ls.isAllSourcesWildcard())
        return true;

      return ls.getName().equals(source);
    }
  }

  public static SubscriptionUriCodec getDefaultCodec()
  {
    return _defaultCodec;
  }

}
