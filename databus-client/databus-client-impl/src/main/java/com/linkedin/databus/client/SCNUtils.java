package com.linkedin.databus.client;

import org.apache.log4j.Logger;

import com.linkedin.databus.client.pub.SCN;

public class SCNUtils 
{
	public static final String MODULE = SCNUtils.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);

	/**
	 * Utility to compare the sequence fields alone for the passed in SCNs
	 * If both of them are not of SingleSourceSCN type, the behavior will be equivalent to first.compareTo(second)
	 * @param first :  First Scn 
	 * @param second : Second SCN
	 * @return
	 */
	public static int compareOnlySequence(SCN first, SCN second)
	{
		if ((first instanceof SingleSourceSCN) && (second instanceof SingleSourceSCN))
		{
			SingleSourceSCN scn1 = (SingleSourceSCN)first;
			SingleSourceSCN scn2 = (SingleSourceSCN)second;
			
			Long s1 = scn1.getSequence();
			Long s2 = scn2.getSequence();
			
			return s1.compareTo(s2);
		} else {
			LOG.debug("SCN are not of type SingleSourceSCN. Reverting to standard compareTo");
			return first.compareTo(second);
		}
	}
}
