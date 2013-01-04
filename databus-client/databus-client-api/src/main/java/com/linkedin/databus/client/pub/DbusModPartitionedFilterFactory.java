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

public class DbusModPartitionedFilterFactory implements
		DbusServerSideFilterFactory 
{
	public static final String MODULE = DbusModPartitionedFilterFactory.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);
	
	private final List<DatabusSubscription> _subscriptions;
	
	/** 
	 * Constructs a Mod-Partitioned Filter using legacy V2 subscription syntax containing only fully qualified source name
	 * E.g : srcUri can be "com.databus.example.person.Person" for Person table
	 * 
	 * @param srcUris Source URIs to be mod partitioned.
	 * 
	 */
	public DbusModPartitionedFilterFactory(String ... srcUris) throws DatabusException
	{
		this(toLegacyUris(srcUris));
	}
	
	/** 
	 *  Constructs a Mod-Partitioned Filter using URI containing legacy V2 subscription syntax with fully qualified source name.
	 *  E.g : srcUri can be new URI("com.databus.example.person.Person") for Person table
	 *  
	 *  @param srcUris Source URIs to be mod partitioned.
	 */
	public DbusModPartitionedFilterFactory(URI ... srcUris) throws DatabusException
	{
		this(LegacySubscriptionUriCodec.getInstance(),srcUris);
	}
	
	/** 
	 *  Constructs a Mod-Partitioned Filter using codec passed to decode the subscription
	 * 
	 *  @param codec  SubscriptionURICodec to extract subscription for filtering.
	 *  @param srcUris Source URIs to be mod partitioned.
	 */
	public DbusModPartitionedFilterFactory(SubscriptionUriCodec codec, URI ... srcUris) throws DatabusException
	{
		_subscriptions =  new ArrayList<DatabusSubscription>();
		for(URI u : srcUris)
		{
			DatabusSubscription s1 = codec.decode(u);
			_subscriptions.add(s1);
		}
	}
	
	@Override
	public DbusKeyCompositeFilterConfig createServerSideFilter( DbusClusterInfo cluster, DbusPartitionInfo partition) 
		throws InvalidConfigException
	{
	    DbusKeyCompositeFilterConfig.Config compositeConfig = new DbusKeyCompositeFilterConfig.Config();
	    
		for (DatabusSubscription s : _subscriptions)
		{
		    KeyFilterConfigHolder.Config filterConfig = new KeyFilterConfigHolder.Config();
		    filterConfig.setType(PartitionType.MOD.toString());
		    filterConfig.getMod().setNumBuckets(cluster.getNumTotalPartitions());
		    filterConfig.getMod().setBuckets("[" + partition.getPartitionId() + "]");
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
