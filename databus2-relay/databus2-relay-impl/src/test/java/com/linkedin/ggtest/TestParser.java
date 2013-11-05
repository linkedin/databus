package com.linkedin.ggtest;


import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.monitoring.mbean.GGParserStatistics.TransactionInfo;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.ggParser.XmlStateMachine.DbUpdateState;
import com.linkedin.databus2.ggParser.XmlStateMachine.TransactionState;
import com.linkedin.databus2.ggParser.XmlStateMachine.TransactionSuccessCallBack;
import com.linkedin.databus2.ggParser.staxparser.StaxBuilderTest;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;


/*
 *
 *  *
 *  * Copyright 2013 LinkedIn Corp. All rights reserved
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
public class TestParser
{

  String SchemaRegistry = "databus2-relay/databus2-relay-impl/src/test/TestData/SchemaRegistry/";
  String XmlData = "databus2-relay/databus2-relay-impl/src/test/TestData/XmlData/";
  FileSystemSchemaRegistryService service;
  private static final Logger LOG = Logger.getLogger(TestParser.class.getName());


  public boolean checkNonEmptyFields(List<TransactionState.PerSourceTransactionalUpdate> dbUpdates)
  {
      for(TransactionState.PerSourceTransactionalUpdate dbUpdate : dbUpdates)
      {
        HashSet<DbUpdateState.DBUpdateImage> DBUpdateImage = dbUpdate.getDbUpdatesSet();
        Iterator<DbUpdateState.DBUpdateImage> it =  DBUpdateImage.iterator();
        while(it.hasNext())
        {
          DbUpdateState.DBUpdateImage image = it.next();
          GenericRecord record = image.getGenericRecord();
          Iterator<Schema.Field> fieldIt =  record.getSchema().getFields().iterator();
          while(fieldIt.hasNext())
          {
            String fieldName = fieldIt.next().name();
            if(record.get(fieldName) == null)
              return false;
          }
        }
      }

      return true;
  }



  @BeforeClass
  public void loadSchema()
      throws InvalidConfigException
  {


    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.DEBUG);
    FileSystemSchemaRegistryService.Config configBuilder = new FileSystemSchemaRegistryService.Config();
    configBuilder.setFallbackToResources(true);
    configBuilder.setSchemaDir(SchemaRegistry);
    FileSystemSchemaRegistryService.StaticConfig config = configBuilder.build();
    service = FileSystemSchemaRegistryService.build(config);
  }


  private class BasicOperationsCheckCallback implements TransactionSuccessCallBack
  {
    int iteration = 0;
    @Override
    public void onTransactionEnd(List<TransactionState.PerSourceTransactionalUpdate> dbUpdates, TransactionInfo ti)
    {
      long fieldValue = ((Long)dbUpdates.get(0).getDbUpdatesSet().iterator().next().getKeyPairs().get(0).getKey()).longValue();
      switch(iteration)
      {
        case 0:
          Assert.assertEquals(dbUpdates.size(),1);
          Assert.assertEquals(fieldValue, 492441);
          Assert.assertEquals(checkNonEmptyFields(dbUpdates), true);
          break;
        case 1:
          Assert.assertEquals(dbUpdates.size(),1);
          Assert.assertEquals(fieldValue, 2);
          Assert.assertEquals(checkNonEmptyFields(dbUpdates), true);
          break;
        case 2:
          Assert.assertEquals(dbUpdates.size(),1);
          Assert.assertEquals(fieldValue, 23);
          Assert.assertEquals(checkNonEmptyFields(dbUpdates), true);
          break;
        default:
          break;
      }

      iteration++;
    }
  }

  /**
   * Makes sure there are no exceptions is thrown on parsing xml, verify the keys processed.
   * @throws DatabusException
   * @throws FileNotFoundException
   */
  @Test
  public void basicOperationsTest()
      throws Exception
  {
    BasicOperationsCheckCallback callback = new BasicOperationsCheckCallback();
    StaxBuilderTest test = new StaxBuilderTest(XmlData+"basicprocessing.xml",service, callback);
    test.processXml();
  }


  //TODO pending test to complete to capture insert/deletes
  /**
   *  Verify if the parser is able to capture the type of event (UPDATE, DELETE, INSERT)
   */
  @Test
  public void OperationsCheck(){

  }

  /**
   *  Verify if the parser is able to capture all oracle data types
   */
  @Test
  public void verifyAllOracleDataTypes(){

  }

  /**
   *  Checks if the parser it able to construct events with extra fields
   */
  @Test
  public void extraFieldsTest()
      throws Exception
  {
    BasicOperationsCheckCallback callback = new BasicOperationsCheckCallback();
    StaxBuilderTest test = new StaxBuilderTest(XmlData+"extrafields.xml",service, callback);
    test.processXml();
  }

  /**
   *  Check if the parser is able to construct event if there spaces, new lines between elements/tags
   */
  @Test
  public void formattingTest()
      throws Exception
  {
    BasicOperationsCheckCallback callback = new BasicOperationsCheckCallback();
    StaxBuilderTest test = new StaxBuilderTest(XmlData+"extraspaces.xml",service, callback);
    test.processXml();
  }


  private class keyCompressionCallback implements TransactionSuccessCallBack
  {
    int iteration = 0;
    @Override
    public void onTransactionEnd(List<TransactionState.PerSourceTransactionalUpdate> dbUpdates, TransactionInfo ti)
    {
      long fieldValue = ((Long)dbUpdates.get(0).getDbUpdatesSet().iterator().next().getKeyPairs().get(0).getKey()).longValue();
      switch(iteration)
      {
        case 0:
          Assert.assertEquals(dbUpdates.size(),1);
          Assert.assertEquals(fieldValue, 492441);
          Assert.assertEquals(checkNonEmptyFields(dbUpdates), true);
          break;
        case 1:
          Assert.assertEquals(dbUpdates.size(),1);
          Assert.assertEquals(fieldValue, 2);
          Assert.assertEquals(checkNonEmptyFields(dbUpdates), true);
          Assert.assertEquals(dbUpdates.get(0).getDbUpdatesSet().iterator().next().getGenericRecord().get("lname"), "lastmodified");
          Assert.assertEquals(dbUpdates.get(0).getDbUpdatesSet().size(),1);
          break;
        case 2:
          Assert.assertEquals(dbUpdates.size(),1);
          Assert.assertEquals(fieldValue, 23);
          Assert.assertEquals(checkNonEmptyFields(dbUpdates), true);
          break;
        default:
          break;
      }

      iteration++;
    }
  }

  /**
   * Check key compression. If there are multiple updates on the same key, choose the last one
   */
  @Test
  public void keyCompressionTest()
      throws Exception
  {
    keyCompressionCallback callback = new keyCompressionCallback();
    StaxBuilderTest test = new StaxBuilderTest(XmlData+"keyCompression.xml",service, callback);
    test.processXml();
  }

  /**
   * Missing scn should throw an exception
   */
  @Test
  public void missingScnCheck()
      throws Exception
  {
    BasicOperationsCheckCallback callback = new BasicOperationsCheckCallback();
    try{
      StaxBuilderTest test = new StaxBuilderTest(XmlData+"missingscn.xml",service, callback);
      test.processXml();
      Assert.fail("Test has not detected failure on missing scn");
    }
    catch(DatabusException e)
    {
      LOG.info("Caught databus exception, verifying if it's for missing scn..");
      String error = e.getMessage();
      String expected = "Unable to find scn for the given dbUpdate, terminating the parser";
      Assert.assertEquals(error, expected);
    }
  }

  /**
   * Missing tokens fields, this should terminate the parser
   */
  @Test
  public void missingTokensCheck()
      throws Exception
  {
    BasicOperationsCheckCallback callback = new BasicOperationsCheckCallback();
    try{
      StaxBuilderTest test = new StaxBuilderTest(XmlData+"missingtokens.xml",service, callback);
      test.processXml();
      Assert.fail("Test has not detected failure on missing tokens");
    }
    catch(DatabusException e)
    {
      LOG.info("Caught databus exception, verifying if it's for missing tokens..");
      String error = e.getMessage();
      String expected = "The current state is : columns(ENDELEMENT) the expected state was: [tokens(STARTELEMENT)]. The next state found was: dbupdate(ENDELEMENT)";
      Assert.assertEquals(error,expected);
    }
  }


  private class NullFieldsCallback implements TransactionSuccessCallBack
  {
    int iteration = 0;
    @Override
    public void onTransactionEnd(List<TransactionState.PerSourceTransactionalUpdate> dbUpdates, TransactionInfo ti)
    {
      long fieldValue = ((Long)dbUpdates.get(0).getDbUpdatesSet().iterator().next().getKeyPairs().get(0).getKey()).longValue();
      switch(iteration)
      {
        case 0:
          Assert.assertEquals(dbUpdates.size(),1);
          Assert.assertEquals(fieldValue, 492441);
          Assert.assertEquals(dbUpdates.get(0).getDbUpdatesSet().iterator().next().getGenericRecord().get("X1"), null);
          //Assert.assertEquals(checkNonEmptyFields(dbUpdates), true);
          break;
        case 1:
          Assert.assertEquals(dbUpdates.size(),1);
          Assert.assertEquals(fieldValue, 2);
          //Assert.assertEquals(checkNonEmptyFields(dbUpdates), true);
          break;
        default:
          break;
      }

      iteration++;
    }
  }


  /**
   * Test for null/empty fields, this should not terminate the parser. The test will verify is the field returns null
   */
  @Test
  public void nullFieldsCheck()
      throws Exception
  {
    NullFieldsCallback callback = new NullFieldsCallback();
    StaxBuilderTest test = new StaxBuilderTest(XmlData+"nullfields.xml",service, callback);
    test.processXml();
  }

  private class SortCheckCallback implements TransactionSuccessCallBack
  {

    private boolean checkIfSetContainsKey(HashSet<DbUpdateState.DBUpdateImage> dbUpdatesSet, Long[] keys){

      if(dbUpdatesSet.size() == 0)
        return false;

      for(int i = 0 ; i< keys.length; i++)
      {
        boolean foundCurrentKey = false;
        Iterator<DbUpdateState.DBUpdateImage> it = dbUpdatesSet.iterator();
        while(it.hasNext())                           //Search all updates for the current key
        {
           if(it.next().getKeyPairs().get(0).getKey().equals(keys[i]))   //TODO change on introducting composite keys
              foundCurrentKey = true;
        }

        if(!foundCurrentKey) return false;
      }

      return true;
   }

    @Override
    public void onTransactionEnd(List<TransactionState.PerSourceTransactionalUpdate> dbUpdates, TransactionInfo ti)
    {
      Assert.assertEquals(dbUpdates.size(),3);
      Assert.assertEquals(dbUpdates.get(0).getSourceId(), 401);
      Assert.assertEquals(dbUpdates.get(1).getSourceId(), 402);
      Assert.assertEquals(dbUpdates.get(2).getSourceId(), 403);
      HashSet<DbUpdateState.DBUpdateImage> dbUpdatesSet = dbUpdates.get(0).getDbUpdatesSet();
      Assert.assertTrue(dbUpdatesSet.size() == 2 && checkIfSetContainsKey(dbUpdatesSet,new Long[]{Long.valueOf(10), Long.valueOf(
          11)}));
      dbUpdatesSet = dbUpdates.get(1).getDbUpdatesSet();
      Assert.assertTrue(dbUpdatesSet.size() == 2 && checkIfSetContainsKey(dbUpdatesSet,new Long[]{Long.valueOf(20), Long.valueOf(
          21)}));
      dbUpdatesSet = dbUpdates.get(2).getDbUpdatesSet();
      Assert.assertTrue(dbUpdatesSet.size() == 1 && checkIfSetContainsKey(dbUpdatesSet,new Long[]{Long.valueOf(30)}));

    }
  }

  /**
   * Test to see if all the updates within a given transaction are clubbed to together by source, and is sorted by source.
   */
  @Test
  public void sortMultipleSourcesCheck()
      throws Exception
  {
    SortCheckCallback callback = new SortCheckCallback();
    StaxBuilderTest test = new StaxBuilderTest(XmlData+"sortMultipleSources.xml",service, callback);
    test.processXml();
  }

  /**
   *  Checks if the parser it able to advance SCNs when there are no events for the current source
   */
  @Test
  public void nullTransactionsTest()
      throws Exception
  {
    BasicOperationsCheckCallback callback = new BasicOperationsCheckCallback();
    StaxBuilderTest test = new StaxBuilderTest(XmlData+"nullTransactions.xml",service, callback);
    test.processXml();
  }

}



//TODO Unit test to peek into the primary key type (verify it's non null and NOT of type schema)
//TODO test addToBuffer Method
