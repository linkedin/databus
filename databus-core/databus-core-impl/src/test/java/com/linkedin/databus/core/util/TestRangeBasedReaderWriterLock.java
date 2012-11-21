package com.linkedin.databus.core.util;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.AssertJUnit;
import com.linkedin.databus.core.util.RangeBasedReaderWriterLock.LockToken;

public class TestRangeBasedReaderWriterLock {

	BufferPositionParser _parser = new BufferPositionParser(Integer.MAX_VALUE, Integer.MAX_VALUE);
	
	@BeforeMethod
  public void setUp() throws Exception {
	}

	@AfterMethod
  public void tearDown() throws Exception {
	}

	@Test
	public void testAcquireReaderLock() {
		RangeBasedReaderWriterLock lock = new RangeBasedReaderWriterLock();
		LockToken token = lock.acquireReaderLock(10, 200,_parser);
		AssertJUnit.assertEquals(10, token._id.start);
		AssertJUnit.assertEquals(200, token._id.end);
		
	}
	
	@Test
	public void testAcquireWriterLock() 
	{
	   final RangeBasedReaderWriterLock lock = new RangeBasedReaderWriterLock();
	   
	   {
	     /*
          * When Readers with same GenIds present
          */
         LockToken token1 = lock.acquireReaderLock(_parser.setGenId(1000, 1), _parser.setGenId(20000, 1), _parser);
         LockToken token2 = lock.acquireReaderLock(_parser.setGenId(1000,1), _parser.setGenId(20000,1), _parser);
         LockToken token3 = lock.acquireReaderLock(_parser.setGenId(10000,1), _parser.setGenId(20000,1), _parser);

         Runnable writer = new Runnable() {
           public void run()
           {
             lock.acquireWriterLock(_parser.setGenId(0,1), _parser.setGenId(1001,1), _parser);
           }
         };
         
         Thread writerThread = new Thread(writer);
         writerThread.start();

         try
         {
           Thread.sleep(100);
         } catch ( InterruptedException ie) { throw new RuntimeException(ie); }

         AssertJUnit.assertTrue(lock.isWriterWaiting());
         AssertJUnit.assertFalse(lock.isWriterIn());
         lock.releaseReaderLock(token1);
  
         try
         {
           Thread.sleep(100);
         } catch ( InterruptedException ie) { throw new RuntimeException(ie); }
         AssertJUnit.assertTrue(lock.isWriterWaiting());
         AssertJUnit.assertFalse(lock.isWriterIn());
         lock.releaseReaderLock(token2);  
         
         try
         {
           writerThread.join(1000);
         } catch (InterruptedException ie) {throw new RuntimeException(ie); }

         AssertJUnit.assertTrue(lock.isWriterIn());
         AssertJUnit.assertFalse(lock.isWriterWaiting());
         AssertJUnit.assertFalse(writerThread.isAlive());
         lock.releaseReaderLock(token3);
         lock.releaseWriterLock(_parser);
	   }
	  
	   
	   {
	     /*
	      * When Readers with different GenIds present
	      */
	     LockToken token1 = lock.acquireReaderLock(_parser.setGenId(10, 1), _parser.setGenId(200, 1), _parser);
	     LockToken token2 = lock.acquireReaderLock(_parser.setGenId(10,1), _parser.setGenId(2000,1), _parser);
	     LockToken token3 = lock.acquireReaderLock(2000, 3000, _parser);

	     Runnable writer = new Runnable() {
	       public void run()
	       {
	         lock.acquireWriterLock(_parser.setGenId(2000,1), _parser.setGenId(11000,1), _parser);
	       }
	     };

	     Thread writerThread = new Thread(writer);
	     writerThread.start();

	     try
	     {
	       Thread.sleep(100);
	     } catch ( InterruptedException ie) { throw new RuntimeException(ie); }

	     AssertJUnit.assertTrue(lock.isWriterWaiting());
	     AssertJUnit.assertFalse(lock.isWriterIn());
	     lock.releaseReaderLock(token3);

	     try
	     {
	       writerThread.join(1000);
	     } catch (InterruptedException ie) {throw new RuntimeException(ie); }

	     AssertJUnit.assertTrue(lock.isWriterIn());
	     AssertJUnit.assertFalse(lock.isWriterWaiting());
	     AssertJUnit.assertFalse(writerThread.isAlive());
	     lock.releaseReaderLock(token1);
	     lock.releaseReaderLock(token2);
         lock.releaseWriterLock(_parser);
	   }
	}
	
	@Test
	public void testShiftReaderLock() {
		RangeBasedReaderWriterLock lock = new RangeBasedReaderWriterLock();
		LockToken token = lock.acquireReaderLock(10, 200,_parser);
		lock.shiftReaderLockStart(token, 50,_parser);
		AssertJUnit.assertEquals(50, token._id.start);
		AssertJUnit.assertEquals(200, token._id.end);
		lock.acquireWriterLock(0, 40,_parser);
		lock.releaseWriterLock(_parser);
		lock.releaseReaderLock(token);
	}

}
