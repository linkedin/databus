package com.linkedin.databus.client;
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
