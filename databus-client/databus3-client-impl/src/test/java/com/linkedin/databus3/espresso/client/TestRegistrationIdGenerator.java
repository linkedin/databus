package com.linkedin.databus3.espresso.client;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.client.pub.RegistrationId;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LogicalSource;
import com.linkedin.databus2.test.TestUtil;

public class TestRegistrationIdGenerator {

    @BeforeClass
    public void setupClass()
    {
      TestUtil.setupLogging(true, null, Level.ERROR);
    }

    @Test(groups={"unit", "fast"})
	public void testGeneration1()
	throws Exception
	{
		List<DatabusSubscription> ds = new ArrayList<DatabusSubscription>();
		ds.add(DatabusSubscription.createMasterSourceSubscription(LogicalSource.createAllSourcesWildcard()));

		String id1 = RegistrationIdGenerator.generateNewId(new String(), ds).getId();
		String id2 = RegistrationIdGenerator.generateNewId(new String(), ds).getId();
		AssertJUnit.assertFalse(id1.equals(id2));

		// The second RegistrationId for a consumer with the same subscription will be the same id, with a suffix
		// for the count at the end.
		AssertJUnit.assertTrue(id2.startsWith(id1));
		return;
	}

    @Test(groups={"unit", "fast"})
	public void testInvalidity()
	throws Exception
	{
		RegistrationId rid = new RegistrationId("test123");
		RegistrationIdGenerator.insertId(rid);
		AssertJUnit.assertFalse(RegistrationIdGenerator.isIdValid(rid));
		return;
	}

}
