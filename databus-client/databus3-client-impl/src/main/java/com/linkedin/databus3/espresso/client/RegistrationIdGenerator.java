package com.linkedin.databus3.espresso.client;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.core.data_model.DatabusSubscription;

public class RegistrationIdGenerator
{
    public static final Logger LOG = Logger.getLogger(RegistrationIdGenerator.class);

	/**
	 * Database of Ids for registrations created so far
	 */
	private static Set<String> _registrationIdDatabase;
	static {
		_registrationIdDatabase = new HashSet<String>();
	}

	/**
	 * Queries the existing database of registrations inside the client and generates a new id different from
	 * any of them
	 *
	 * @param prefix : A String prefix which can be specified to precede the id to be generated. Typically this
	 *                 is the name of the Consumer class
	 * @param subsSources : List of subscriptions the consumer is interested in
	 * @return RegistrationId : Generated based on the prefix _ 8 byte md5 hash ( subscriptions ) _ count
	 */
	public static RegistrationId generateNewId(String prefix, List<DatabusSubscription> subsSources)
	{
		String subscription = new String();
		for (DatabusSubscription ds : subsSources)
		{
			if (ds != null)
				subscription += DatabusSubscription.createStringFromSubscription(ds);
		}
		String id = generateUniqueString(prefix, subscription);
		RegistrationId rid = new RegistrationId(id);
		return rid;
	}

	/**
	 * Checks if the input rid can be used as a RegistrationId.
	 * Specifically, checks to see if the underlying id is already in use
	 * @return
	 */
	public static boolean isIdValid(RegistrationId rid)
	{
		String id = rid.getId();
		synchronized (RegistrationIdGenerator.class)
		{
    		if (_registrationIdDatabase.contains(id))
    		{
    			return false;
    		}
    		else
    		{
    			return true;
    		}
		}
	}

	/**
	 * Adds an id into the RegistrationId database.
	 * This is useful for unit-testing / inserting an id out-of-band into the database, so that such an id
	 * would not get generated again
	 *
	 * @param id
	 */
	public static void insertId(RegistrationId rid)
	{
		String id = rid.getId();
		synchronized (RegistrationIdGenerator.class)
		{
		  _registrationIdDatabase.add(id);
		}
	}

	/**
	 * Creates a unique string given a prefix ( which must appear as is ), a subscription string ( which is converted
	 * to an 8 byte string ) and a count if there are duplicates with the previous two
	 */
	private static String generateUniqueString(String prefix, String subscription)
	{
		final String delimiter = "_";
		String baseId = prefix + delimiter + generateByteHash(subscription);
		String id = baseId;

		boolean success = false;
		boolean debugEnabled = LOG.isDebugEnabled();
		synchronized (RegistrationIdGenerator.class)
        {
	        int count = _registrationIdDatabase.size();
		    while (! success)
    		{
    			if (_registrationIdDatabase.contains(id))
    			{
    				if (debugEnabled)
    				  LOG.debug("The generated id " + id + " already exists. Retrying ...");
                    id = baseId + delimiter + count;
    				count++;
    			}
    			else
    			{
    				if (debugEnabled)
    				  LOG.debug("Obtained a new ID " + id);
    				_registrationIdDatabase.add(id);
    				success = true;
    			}
    		}
        }
		return id;
	}

	/**
	 * Generate a hash out of the String id
	 *
	 * @param id
	 * @return
	 */
	private static String generateByteHash(String id)
	{
		try {
			final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
			messageDigest.reset();
			messageDigest.update(id.getBytes(Charset.forName("UTF8")));
			final byte[] resultsByte = messageDigest.digest();
			String hash = new String(Hex.encodeHex(resultsByte));

			final int length = 8;
			if (hash.length() > length)
				hash = hash.substring(0, length);

			return hash;
		}
		catch (NoSuchAlgorithmException nse)
		{
			LOG.error("Unexpected error : Got NoSuchAlgorithm exception for MD5" );
			return new String();
		}
	}

}
