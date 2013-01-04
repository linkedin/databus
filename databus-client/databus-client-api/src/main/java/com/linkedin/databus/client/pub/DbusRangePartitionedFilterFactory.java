package com.linkedin.databus.client.pub;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LegacySubscriptionUriCodec;
import com.linkedin.databus.core.data_model.SubscriptionUriCodec;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.filter.DbusKeyCompositeFilterConfig;
import com.linkedin.databus2.core.filter.KeyFilterConfigHolder;
import com.linkedin.databus2.core.filter.KeyFilterConfigHolder.PartitionType;

public class DbusRangePartitionedFilterFactory implements
	DbusServerSideFilterFactory 
{
	public static final String MODULE = DbusModPartitionedFilterFactory.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);
	
	private final List<DatabusSubscription> _subscriptions;
	private final long _rangeWidth;
	
	
	/** 
	 * Constructs a Range-Partitioned Filter using legacy V2 subscription syntax containing only fully qualified source name
	 * E.g : srcUri can be "com.databus.example.person.Person" for Person table
	 * 
	 * @param rangeWidth : Range Size (e.g : 5000000 for 5M shards)
	 * @param srcUris Source URIs to be range partitioned.
	 * 
	 */
	public DbusRangePartitionedFilterFactory(long rangeWidth, String ... srcUris) throws DatabusException
	{
		this(rangeWidth, toLegacyUris(srcUris));
	}
	
	/** 
	 *  Constructs a Range-Partitioned Filter using URI containing legacy V2 subscription syntax with fully qualified source name.
	 *  E.g : srcUri can be new URI("com.databus.example.person.Person") for Person table
	 *  
	 *  @param srcUris Source URIs to be range partitioned.
	 */
	public DbusRangePartitionedFilterFactory(long rangeWidth, URI ... srcUris) throws DatabusException
	{
		this(rangeWidth, LegacySubscriptionUriCodec.getInstance(),srcUris);
	}
	
	/** 
	 *  Constructs a Range-Partitioned Filter using codec passed to decode the subscription
	 * 
	 *  @param codec SubscriptionURICodec to extract subscription for filtering.
	 *  @param srcUris Source URIs to be range partitioned.
	 */
	public DbusRangePartitionedFilterFactory(long rangeWidth, SubscriptionUriCodec codec, URI ... srcUris) throws DatabusException
	{
		_subscriptions =  new ArrayList<DatabusSubscription>();
		_rangeWidth = rangeWidth;
		for(URI u : srcUris)
		{
			DatabusSubscription s1 =  codec.decode(u);
			_subscriptions.add(s1);
		}
	}
	
	@Override
	public DbusKeyCompositeFilterConfig createServerSideFilter(
			DbusClusterInfo cluster, DbusPartitionInfo partition)
			throws InvalidConfigException 
	{
	    DbusKeyCompositeFilterConfig.Config compositeConfig = new DbusKeyCompositeFilterConfig.Config();
	    
		for (DatabusSubscription s : _subscriptions)
		{
		    KeyFilterConfigHolder.Config filterConfig = new KeyFilterConfigHolder.Config();
		    filterConfig.setType(PartitionType.RANGE.toString());
		    filterConfig.getRange().setSize(_rangeWidth);
		    filterConfig.getRange().setPartitions("[" + partition.getPartitionId() + "]");
		    compositeConfig.setFilter(s.getLogicalSource().getName(), filterConfig);
		}
		
		DbusKeyCompositeFilterConfig c = new DbusKeyCompositeFilterConfig(compositeConfig.build());
		LOG.info("Generated Mod Partitioned Config for partition (" + partition + ") of cluster (" + cluster + ") is :" + c);
		return c;
	
	}
	
	private static URI[] toLegacyUris(String... srcUris) throws DatabusException 
	{
		URI[] uris = new URI[srcUris.length];
		int i = 0;
		try
		{
			for (String s : srcUris)
				uris[i++] = new URI(s);
		} catch (URISyntaxException e) {
			throw new DatabusException(e);
		}
		return uris;
	}

}
