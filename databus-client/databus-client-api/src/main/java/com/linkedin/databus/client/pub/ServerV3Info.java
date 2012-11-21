package com.linkedin.databus.client.pub;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.linkedin.databus.core.cmclient.ResourceKey;

public class ServerV3Info extends ServerInfo {
	
	/**
	 * Gives a mapping between this relay and a list of ResourceKeys
	 * see @ResourceKey for more info
	 */
	private List<ResourceKey> _partitionInfo;
	
	/**
	 * 
	 * @param name
	 * @param address
	 * @param sources
	 */
	public ServerV3Info(String name, String state, InetSocketAddress address, List<String> sources)
	{
		super(name, state, address, sources);
		_partitionInfo = new ArrayList<ResourceKey>();
	}

	/**
	 * 
	 * @param name
	 * @param address
	 * @param sources
	 */
	public ServerV3Info(String name, String state, InetSocketAddress address, String... sources)
	{
		this(name, state, address, Arrays.asList(sources));
	}
	
	/**
	 * Add a list of ResourceKeys to this relay server
	 * @param rks
	 */
	public void addResourceKey(List<ResourceKey> rks)
	{
		_partitionInfo.addAll(rks);
	}
	
	/**
	 * Utility function to add a single ResourceKey
	 * @param rk
	 */
	public void addResourceKey(ResourceKey rk)
	{	
		List<ResourceKey> rks = new ArrayList<ResourceKey>();
		rks.add(rk);
		addResourceKey( rks );
	}
	
	/**
	 * Obtain list of ResourceKeys for this relay server
	 */
	public List<ResourceKey> getResourceKeys()
	{
		return _partitionInfo;
	}

	/*
	 * Overriding equals and hashCode to keep findBugs happy
	 */
	@Override
	public int hashCode() 
	{
		return super.hashCode();
	}

	@Override
	public boolean equals(Object obj) 
	{
		return super.equals(obj);
	}
	
	
}
