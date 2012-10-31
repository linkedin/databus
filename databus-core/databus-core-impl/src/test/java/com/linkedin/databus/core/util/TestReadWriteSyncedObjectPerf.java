package com.linkedin.databus.core.util;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.AssertJUnit;
import java.util.Formatter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

public class TestReadWriteSyncedObjectPerf {
	
	public static final Logger LOG = Logger.getLogger(TestReadWriteSyncedObjectPerf.class);
	
	public static final int ITER_NUM = 10000000;
	
	public static class TestClass extends ReadWriteSyncedObject
	{
	    private boolean _threadUnsafe;
		public int _counter1;
		public int _counter2;
		private AtomicInteger _counter3;
        private AtomicInteger _counter4;
		
		public TestClass(boolean threadSafe)
		{
			super(threadSafe);
			_threadUnsafe = ! threadSafe;
			_counter1 = 1;
			_counter2 = 1;
			_counter3 = new AtomicInteger(1);
            _counter4 = new AtomicInteger(1);
		}
		
		public int getCounter1ThreadedFinally()
		{
			Lock readLock = _threadUnsafe? null : acquireReadLock();
			int result = 0;
			try
			{
				result = _counter1;
			}
			finally
			{
				releaseLock(readLock);
			}
			
			return result;
		}
		
		public int getCounter1ThreadedNoFinally()
		{
			Lock readLock = _threadUnsafe? null : acquireReadLock();
			int result = _counter1;
			releaseLock(readLock);
			
			return result;
		}
		
		public int getCounter1ThreadedFinallyDirect()
		{
			Lock readLock = acquireReadLock(null);
			try
			{
				return _counter1;
			}
			finally
			{
				releaseLock(readLock);
			}
		}
		
		public int getCounter1ThreadedNoFinallyDirect()
		{
			Lock readLock = acquireReadLock(null);
			int result = _counter1;
			releaseLock(readLock);
			
			return result;
		}
		
		public int getCounter2ThreadedFinally()
		{
			Lock readLock = _threadUnsafe? null : acquireReadLock();
			try
			{
				return _counter2;
			}
			finally
			{
				releaseLock(readLock);
			}
		}
		
		public int getCounter2ThreadedNoFinally()
		{
			Lock readLock = _threadUnsafe? null : acquireReadLock();
			int result = _counter2;
			releaseLock(readLock);
			
			return result;
		}
		
		public int getCounter2ThreadedFinallyDirect()
		{
			Lock readLock = acquireReadLock(null);
			int result = 0;
			try
			{
				result = _counter2;
			}
			finally
			{
				releaseLock(readLock);
			}
			
			return result;
		}
		
		public int getCounter2ThreadedNoFinallyDirect()
		{
			Lock readLock = acquireReadLock(null);
			int result = _counter2;
			releaseLock(readLock);
			
			return result;
		}
		
		public int getCounter1NoLock()
		{
			return _threadUnsafe ? _counter1 : _counter1;
		}
		
		public int getCounter2NoLock()
		{
			 return _threadUnsafe ? _counter2 : _counter2;
		}
        
        public int getCounter3()
        {
            return _counter3.get();
        }
        
        public int getCounter4()
        {
            return _counter4.get();
        }
		
		// MUTATORS
		public void incCounter1ThreadedFinally()
		{
			Lock writeLock = acquireWriteLock();
			try
			{
				++_counter1;
			}
			finally
			{
				releaseLock(writeLock);
			}
		}
		
		public void incCounter1ThreadedNoFinally()
		{
			Lock writeLock = acquireWriteLock();
			++ _counter1;
			releaseLock(writeLock);
		}
		
		public void incCounter1ThreadedFinallyDirect()
		{
			Lock writeLock = acquireWriteLock(null);
			try
			{
				++_counter1;
			}
			finally
			{
				releaseLock(writeLock);
			}
		}
		
		public void incCounter1ThreadedNoFinallyDirect()
		{
			Lock writeLock = acquireWriteLock(null);
			++_counter1;
			releaseLock(writeLock);
		}
		
		public void incCounter2ThreadedFinally()
		{
			Lock writeLock = acquireWriteLock();
			try
			{
				++ _counter2;
			}
			finally
			{
				releaseLock(writeLock);
			}
		}
		
		public void incCounter2ThreadedNoFinally()
		{
			Lock writeLock = acquireWriteLock();
			++_counter2;
			releaseLock(writeLock);
		}
		
		public void incCounter2ThreadedFinallyDirect()
		{
			Lock writeLock = acquireWriteLock(null);
			try
			{
				++_counter2;
			}
			finally
			{
				releaseLock(writeLock);
			}
		}
		
		public void incCounter2ThreadedNoFinallyDirect()
		{
			Lock writeLock = acquireWriteLock(null);
			++ _counter2;
			releaseLock(writeLock);
		}
		
		public void incCounter1NoLock()
		{
			++_counter1;
		}
		
		public void incCounter2NoLock()
		{
			++_counter2;
		}
		
		public void intCounter3()
		{
		  _counter3.incrementAndGet();
		}
        
        public void intCounter4()
        {
          _counter4.incrementAndGet();
        }
	}

	@BeforeMethod
  public void setUp() throws Exception {
	}

	@AfterMethod
  public void tearDown() throws Exception {
	}

	@Test
	public void testAcquireReadLock() {
		TestClass safeObject = new TestClass(true);
		TestClass unsafeObject = new TestClass(false);
		
		AssertJUnit.assertTrue(doReadTests("Reads thread-safe", safeObject) > 0);
		AssertJUnit.assertTrue(doReadTests("Reads thread-unsafe", unsafeObject) > 0);
	}

	private void doWriteTests(String expName, TestClass testObject)
	{
		testObject.incCounter1ThreadedFinally(); 
		testObject.incCounter2ThreadedFinally(); 

		//thread-safe finally easy
		long threadSafeFinallyEasyStart = System.nanoTime();
		for (int i = 0; i < ITER_NUM; ++i)
		{
			testObject.incCounter1ThreadedFinally(); 
			testObject.incCounter2ThreadedFinally(); 
		}
		long threadSafeFinallyEasyFinish = System.nanoTime();
		
		//thread-safe finally direct
		long threadSafeFinallyDirectStart = System.nanoTime();
		for (int i = 0; i < ITER_NUM; ++i)
		{
			testObject.incCounter1ThreadedFinallyDirect(); 
			testObject.incCounter2ThreadedFinallyDirect(); 
		}
		long threadSafeFinallyDirectFinish = System.nanoTime();

		//thread-safe finally direct
		long threadSafeNoFinallyDirectStart = System.nanoTime();
		for (int i = 0; i < ITER_NUM; ++i)
		{
			testObject.incCounter1ThreadedNoFinallyDirect(); 
			testObject.incCounter2ThreadedNoFinallyDirect(); 
		}
		long threadSafeNoFinallyDirectFinish = System.nanoTime();

		//thread-safe finally easy
		long threadSafeNoFinallyEasyStart = System.nanoTime();
		for (int i = 0; i < ITER_NUM; ++i)
		{
			testObject.incCounter1ThreadedNoFinally(); 
			testObject.incCounter2ThreadedNoFinally(); 
		}
		long threadSafeNoFinallyEasyFinish = System.nanoTime();

		//no-lock method
		long nolockMethodStart = System.nanoTime();
		for (int i = 0; i < ITER_NUM; ++i)
		{
			testObject.incCounter1NoLock(); 
			testObject.incCounter2NoLock(); 
		}
		long nolockMethodFinish = System.nanoTime();

		//no-lock attribute
		long nolockAttrStart = System.nanoTime();
		for (int i = 0; i < ITER_NUM; ++i)
		{
			++testObject._counter1; 
			++testObject._counter2; 
		}
		long nolockAttrFinish = System.nanoTime();

        //atomic method
        long atomicMethodStart = System.nanoTime();
        for (int i = 0; i < ITER_NUM; ++i)
        {
            testObject.intCounter3(); 
            testObject.intCounter4(); 
        }
        long atomicMethodFinish = System.nanoTime();
		
		Formatter fmt = new Formatter();
		fmt.format("%s (in ns/call)\n" +
				   "  finally direct    : %f\n" +
				   "  finally easy      : %f\n" +
				   "  no-finally direct : %f\n" +
				   "  no-finally easy   : %f\n" +
				   "  no-lock method    : %f\n" +
				   "  no-lock attribute : %f\n" +
				   "  atomic method     : %f\n",
				   expName,
				   (threadSafeFinallyDirectFinish - threadSafeFinallyDirectStart) * 1.0 / ITER_NUM,
				   (threadSafeFinallyEasyFinish - threadSafeFinallyEasyStart) * 1.0 / ITER_NUM,
				   (threadSafeNoFinallyDirectFinish - threadSafeNoFinallyDirectStart) * 1.0 / ITER_NUM,
				   (threadSafeNoFinallyEasyFinish - threadSafeNoFinallyEasyStart) * 1.0 / ITER_NUM,
				   (nolockMethodFinish - nolockMethodStart) * 1.0 / ITER_NUM,
				   (nolockAttrFinish - nolockAttrStart) * 1.0 / ITER_NUM,
				   (atomicMethodFinish - atomicMethodStart) * 1.0 / ITER_NUM);
		
		LOG.info(fmt.toString());
	}

	private int doReadTests(String expName, TestClass testObject)
	{
		
		int sum = testObject.getCounter1ThreadedFinally() + testObject.getCounter2ThreadedFinally();

		//thread-safe finally easy
		long threadSafeFinallyEasyStart = System.nanoTime();
		for (int i = 0; i < ITER_NUM; ++i)
		{
			sum += testObject.getCounter1ThreadedFinally() + 
			       testObject.getCounter2ThreadedFinally(); 
		}
		long threadSafeFinallyEasyFinish = System.nanoTime();

		//thread-safe finally direct
		long threadSafeFinallyDirectStart = System.nanoTime();
		for (int i = 0; i < ITER_NUM; ++i)
		{
			sum += testObject.getCounter1ThreadedFinallyDirect() + 
			       testObject.getCounter2ThreadedFinallyDirect(); 
		}
		long threadSafeFinallyDirectFinish = System.nanoTime();

		//thread-safe finally direct
		long threadSafeNoFinallyDirectStart = System.nanoTime();
		for (int i = 0; i < ITER_NUM; ++i)
		{
			sum += testObject.getCounter1ThreadedNoFinallyDirect() + 
			       testObject.getCounter2ThreadedNoFinallyDirect(); 
		}
		long threadSafeNoFinallyDirectFinish = System.nanoTime();

		//thread-safe finally easy
		long threadSafeNoFinallyEasyStart = System.nanoTime();
		for (int i = 0; i < ITER_NUM; ++i)
		{
			sum += testObject.getCounter1ThreadedNoFinally() + 
			       testObject.getCounter2ThreadedNoFinally(); 
		}
		long threadSafeNoFinallyEasyFinish = System.nanoTime();

		//no-lock method
		long nolockMethodStart = System.nanoTime();
		for (int i = 0; i < ITER_NUM; ++i)
		{
			sum += testObject.getCounter1NoLock() + 
			       testObject.getCounter2NoLock(); 
		}
		long nolockMethodFinish = System.nanoTime();

		//no-lock attribute
		long nolockAttrStart = System.nanoTime();
		for (int i = 0; i < ITER_NUM; ++i)
		{
			sum += testObject._counter1 + 
			       testObject._counter2; 
		}
		long nolockAttrFinish = System.nanoTime();

        //no-lock method
        long atomicMethodStart = System.nanoTime();
        for (int i = 0; i < ITER_NUM; ++i)
        {
            sum += testObject.getCounter3() + 
                   testObject.getCounter4(); 
        }
        long atomicMethodFinish = System.nanoTime();
		
		Formatter fmt = new Formatter();
		fmt.format("%s (in ns/call)\n" +
				   "  finally direct    : %f\n" +
				   "  finally easy      : %f\n" +
				   "  no-finally direct : %f\n" +
				   "  no-finally easy   : %f\n" +
				   "  no-lock method    : %f\n" +
				   "  no-lock attribute : %f\n" +
				   "  atomic method     : %f\n",
				   expName,
				   (threadSafeFinallyDirectFinish - threadSafeFinallyDirectStart) * 1.0 / ITER_NUM,
				   (threadSafeFinallyEasyFinish - threadSafeFinallyEasyStart) * 1.0 / ITER_NUM,
				   (threadSafeNoFinallyDirectFinish - threadSafeNoFinallyDirectStart) * 1.0 / ITER_NUM,
				   (threadSafeNoFinallyEasyFinish - threadSafeNoFinallyEasyStart) * 1.0 / ITER_NUM,
				   (nolockMethodFinish - nolockMethodStart) * 1.0 / ITER_NUM,
				   (nolockAttrFinish - nolockAttrStart) * 1.0 / ITER_NUM,
				   (atomicMethodFinish - atomicMethodStart) * 1.0 / ITER_NUM);
		
		LOG.info(fmt.toString());
		
		return sum;
	}

	@Test
	public void testAcquireWriteLock() {
		TestClass safeObject = new TestClass(true);
		TestClass unsafeObject = new TestClass(false);
		
		doWriteTests("Write thread-safe", safeObject);
		doWriteTests("Write thread-unsafe", unsafeObject);
	}

}
