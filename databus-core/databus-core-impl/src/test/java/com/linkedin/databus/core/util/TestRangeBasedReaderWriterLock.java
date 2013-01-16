package com.linkedin.databus.core.util;

import java.util.concurrent.TimeoutException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.util.RangeBasedReaderWriterLock.LockToken;
import com.linkedin.databus2.test.TestUtil;

public class TestRangeBasedReaderWriterLock {

	BufferPositionParser _parser = new BufferPositionParser(Integer.MAX_VALUE, Integer.MAX_VALUE);

	@BeforeClass
    public void setUpClass() throws Exception {
	  TestUtil.setupLogging(true, null, Level.ERROR);
	}

	@Test
	public void testAcquireReaderLock() throws Exception {
		RangeBasedReaderWriterLock lock = new RangeBasedReaderWriterLock();
		LockToken token = lock.acquireReaderLock(10, 200,_parser, "testAcquireReaderLock");
		AssertJUnit.assertEquals(10, token._id.start);
		AssertJUnit.assertEquals(200, token._id.end);

	}

	@Test
	public void testAcquireWriterLock() throws InterruptedException, TimeoutException
	{
	  final Logger log = Logger.getLogger("TestRangeBasedReaderWriterLock.testAcquireWriterLock");
	   final RangeBasedReaderWriterLock lock = new RangeBasedReaderWriterLock();

	   {
	     /*
          * When Readers with same GenIds present
          */
         LockToken token1 = lock.acquireReaderLock(_parser.setGenId(1000, 1),
                                                   _parser.setGenId(20000, 1),
                                                   _parser,
                                                   "testAcquireWriterLock1");
         LockToken token2 = lock.acquireReaderLock(_parser.setGenId(1000,1),
                                                   _parser.setGenId(20000,1),
                                                   _parser,
                                                   "testAcquireWriterLock2");
         LockToken token3 = lock.acquireReaderLock(_parser.setGenId(10000,1),
                                                   _parser.setGenId(20000,1),
                                                   _parser,
                                                   "testAcquireWriterLock3");

         Runnable writer = new Runnable() {
           @Override
          public void run()
           {
             try
            {
              lock.acquireWriterLock(_parser.setGenId(0,1), _parser.setGenId(1001,1), _parser);
            }
            catch (InterruptedException e)
            {
              log.error(e);
            }
            catch (TimeoutException e)
            {
              log.error(e);
            }
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
	     LockToken token1 = lock.acquireReaderLock(_parser.setGenId(10, 1),
	                                               _parser.setGenId(200, 1),
	                                               _parser,
	                                               "testAcquireWriterLock4");
	     LockToken token2 = lock.acquireReaderLock(_parser.setGenId(10,1),
	                                               _parser.setGenId(2000,1),
	                                               _parser,
	                                               "testAcquireWriterLock5");
	     LockToken token3 = lock.acquireReaderLock(2000, 3000, _parser, "testAcquireWriterLock6");

	     Runnable writer = new Runnable() {
	       @Override
        public void run()
	       {
	         try
	         {
	           lock.acquireWriterLock(_parser.setGenId(2000,1), _parser.setGenId(11000,1), _parser);
	         }
	         catch (InterruptedException e)
	         {
	           log.error(e);
	         }
	         catch (TimeoutException e)
	         {
	           log.error(e);
	         }
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
	public void testShiftReaderLock() throws Exception {
		RangeBasedReaderWriterLock lock = new RangeBasedReaderWriterLock();
		LockToken token = lock.acquireReaderLock(10, 200,_parser, "testShiftReaderLock");
		lock.shiftReaderLockStart(token, 50,_parser);
		AssertJUnit.assertEquals(50, token._id.start);
		AssertJUnit.assertEquals(200, token._id.end);
		lock.acquireWriterLock(0, 40,_parser);
		lock.releaseWriterLock(_parser);
		lock.releaseReaderLock(token);
	}

}
