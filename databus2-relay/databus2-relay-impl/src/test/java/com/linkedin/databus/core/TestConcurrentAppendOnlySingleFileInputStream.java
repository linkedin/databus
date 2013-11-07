package com.linkedin.databus.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.security.SecureRandom;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.test.ConditionCheck;
import com.linkedin.databus2.test.TestUtil;

public class TestConcurrentAppendOnlySingleFileInputStream
{
	public static final String MODULE = TestConcurrentAppendOnlySingleFileInputStream.class.getName();
	public static final Logger LOG = Logger.getLogger(MODULE);
	@BeforeClass
	public void setUpClass() throws InvalidConfigException
	{
		//setup logging
		TestUtil.setupLogging(true, null, Level.INFO);
		InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
	}

	@Test
	public void testStaticStream()
			throws Exception
	{
		File srcFile = File.createTempFile("src_trail_", null);
		File destFile = File.createTempFile("dest_trail_", null);

		LOG.info("Src File :" + srcFile);
		LOG.info("Dest File :" + destFile);

		WriterThread w1 = new WriterThread("Writer1",srcFile, 10,null);
		w1.start();

		Thread.sleep(1000);
		w1.shutdown();

		BufferedReader r = new BufferedReader(new InputStreamReader(ConcurrentAppendableSingleFileInputStream.createStaticFileInputStream(srcFile.getAbsolutePath(),0)));
		WriterThread w2 = new WriterThread("Writer2",destFile, 0,	r);

		w2.start();
		w2.join();

		r.close();

		FileComparator cmp = new FileComparator(srcFile.getAbsolutePath(),destFile.getAbsolutePath());
		cmp.compare();
	}

	@Test
	/**
	 * Test when reader starts immediately after writer
	 * @throws Exception
	 */
	public void testConcurrentAppendStream1()
			throws Exception
	{
		File srcFile = File.createTempFile("src_trail_", null);
		File destFile = File.createTempFile("dest_trail_", null);

		LOG.info("Src File :" + srcFile);
		LOG.info("Dest File :" + destFile);

		WriterThread w1 = new WriterThread("Writer1",srcFile, 10,null);
		w1.start();

		ConcurrentAppendableSingleFileInputStream i = ConcurrentAppendableSingleFileInputStream.createAppendingFileInputStream(srcFile.getAbsolutePath(),0,100);
		BufferedReader r = new BufferedReader(new InputStreamReader(i));
		WriterThread w2 = new WriterThread("Writer2",destFile, 0,	r);
		w2.start();

		Thread.sleep(10000);
		w1.shutdown();

		i.appendDone();
		w2.join();

		r.close();
		LOG.info("FileInputStream state :" + i);
		FileComparator cmp = new FileComparator(srcFile.getAbsolutePath(),destFile.getAbsolutePath());
		cmp.compare();
	}

	@Test
	/**
	 * Test when reader sees intermediate NULL characters
	 * @throws Exception
	 */
	public void testConcurrentAppendStreamNullCharacters()
			throws Exception
	{
		File srcFile = File.createTempFile("src_trail_", null);
		File destFile = File.createTempFile("dest_trail_", null);

		LOG.info("Src File :" + srcFile);
		LOG.info("Dest File :" + destFile);

		// Writer Thread will insert 1 set (5) of NULL characters after inserting 100 lines and pause
		// The test thread will wait for number of NULL characters inserted to be >= 1 before unpausing Writer.
		// Writer will then remove the NULL bytes and start appending valid data.
		// Reader is expected to read all the data as in the source file.
		final WriterThread w1 = new WriterThread("Writer1",srcFile, 10,null, true, 1, 100);
		w1.start();

		ConcurrentAppendableSingleFileInputStream i = ConcurrentAppendableSingleFileInputStream.createAppendingFileInputStream(srcFile.getAbsolutePath(),0,100);
		BufferedReader r = new BufferedReader(new InputStreamReader(i));
		WriterThread w2 = new WriterThread("Writer2",destFile, 0,	r);
		w2.start();

        TestUtil.assertWithBackoff(new ConditionCheck()
        {
          @Override
          public boolean check()
          {
            return w1.getNumNullsInserted() >= 1;
          }
        }, "wait for writer to write atleast one NULL ", 10000, LOG);

        w1.unpause();
		Thread.sleep(1000);
		w1.shutdown();

		i.appendDone();
		w2.join();

		r.close();
		LOG.info("FileInputStream state :" + i);
		FileComparator cmp = new FileComparator(srcFile.getAbsolutePath(),destFile.getAbsolutePath());
		cmp.compare();

		// Ensure no NULL characters are seen in file created by the reader.
		FileInputStream fis = new FileInputStream(destFile);
		try
		{
			int retVal = 0;

			while (retVal != -1)
			{
				retVal = fis.read();
				Assert.assertTrue(retVal != 0x0);
			}
		} finally {
			fis.close();
		}
	}


	@Test
	/**
	 * Test when reader sees intermediate NULL characters are seen multiple times during the stream read
	 * @throws Exception
	 */
	public void testConcurrentAppendStreamMultipleNullCharacters()
			throws Exception
	{
		File srcFile = File.createTempFile("src_trail_", null);
		File destFile = File.createTempFile("dest_trail_", null);

		LOG.info("Src File :" + srcFile);
		LOG.info("Dest File :" + destFile);

		// Writer Thread will insert 3 sets (5 each time) of NULL characters after inserting 100 lines and pause
		// The test thread will wait for number of NULL characters inserted to be >= 1 before unpausing Writer.
		// Writer will then remove the NULL bytes and start appending valid data.
		// Reader is expected to read all the data as in the source file.
		final WriterThread w1 = new WriterThread("Writer1",srcFile, 10,null, true, 3, 100);
		w1.start();

		ConcurrentAppendableSingleFileInputStream i = ConcurrentAppendableSingleFileInputStream.createAppendingFileInputStream(srcFile.getAbsolutePath(),0,100);
		BufferedReader r = new BufferedReader(new InputStreamReader(i));
		WriterThread w2 = new WriterThread("Writer2",destFile, 0,	r);
		w2.start();

		for ( int j = 1; j <= 3; j++)
		{
			final int nullInsertTimes = j;
			TestUtil.assertWithBackoff(new ConditionCheck()
			{
				@Override
				public boolean check()
				{
					return w1.getNumNullsInserted() == nullInsertTimes;
				}
			}, "wait for writer to write atleast one NULL ", 10000, LOG);

			w1.unpause();
			Thread.sleep(3000);
		}

		w1.shutdown();

		i.appendDone();
		w2.join();

		r.close();
		LOG.info("FileInputStream state :" + i);
		FileComparator cmp = new FileComparator(srcFile.getAbsolutePath(),destFile.getAbsolutePath());
		cmp.compare();

		// Ensure no NULL characters are seen in file created by the reader.
		FileInputStream fis = new FileInputStream(destFile);
		try
		{
			int retVal = 0;

			while (retVal != -1)
			{
				retVal = fis.read();
				Assert.assertTrue(retVal != 0x0);
			}
		} finally {
			fis.close();
		}
	}
	/**
	 * Test when reader starts before writer
	 * @throws Exception
	 */
	@Test
	public void testConcurrentAppendStream2()
			throws Exception
	{
		File srcFile = File.createTempFile("src_trail_", null);
		File destFile = File.createTempFile("dest_trail_", null);

		LOG.info("Src File :" + srcFile);
		LOG.info("Dest File :" + destFile);

		ConcurrentAppendableSingleFileInputStream i = ConcurrentAppendableSingleFileInputStream.createAppendingFileInputStream(srcFile.getAbsolutePath(),0,100);
		BufferedReader r = new BufferedReader(new InputStreamReader(i));
		WriterThread w2 = new WriterThread("Writer2",destFile, 0,	r);
		w2.start();

		WriterThread w1 = new WriterThread("Writer1",srcFile, 10,null);
		w1.start();
		Thread.sleep(10000);
		w1.shutdown();

		i.appendDone();
		w2.join();

		r.close();

		FileComparator cmp = new FileComparator(srcFile.getAbsolutePath(),destFile.getAbsolutePath());
		cmp.compare();
	}


	@Test
	/**
	 * Test when reader starts after delay after writer
	 * @throws Exception
	 */
	public void testConcurrentAppendStream3()
			throws Exception
	{
		File srcFile = File.createTempFile("src_trail_", null);
		File destFile = File.createTempFile("dest_trail_", null);

		LOG.info("Src File :" + srcFile);
		LOG.info("Dest File :" + destFile);

		WriterThread w1 = new WriterThread("Writer1",srcFile, 10,null);
		w1.start();

		Thread.sleep(3000);

		ConcurrentAppendableSingleFileInputStream i = ConcurrentAppendableSingleFileInputStream.createAppendingFileInputStream(srcFile.getAbsolutePath(),0,100);
		BufferedReader r = new BufferedReader(new InputStreamReader(i));
		WriterThread w2 = new WriterThread("Writer2",destFile, 0,	r);
		w2.start();

		Thread.sleep(10000);
		w1.shutdown();

		i.appendDone();
		w2.join();

		r.close();

		FileComparator cmp = new FileComparator(srcFile.getAbsolutePath(),destFile.getAbsolutePath());
		cmp.compare();
	}

	public static class FileComparator
	{
		private final String _file1;
		private final String _file2;

		public FileComparator(String file1, String file2)
		{
			_file1 = file1;
			_file2 = file2;
		}

		public void compare()
			throws Exception
		{
			BufferedReader br1 = null;
			BufferedReader br2 = null;

			try
			{
				FileInputStream fstream1 = new FileInputStream(_file1);
				FileInputStream fstream2 = new FileInputStream(_file2);

				DataInputStream in1= new DataInputStream(fstream1);
				DataInputStream in2= new DataInputStream(fstream2);

				br1 = new BufferedReader(new InputStreamReader(in1));
				br2 = new BufferedReader(new InputStreamReader(in2));

				String strLine1 = null;
				String strLine2 = null;


				long line = 0;
				while((strLine1 = br1.readLine()) != null & (strLine2 = br2.readLine()) != null)
				{
					line++;
					if(! strLine1.equals(strLine2))
					{
						throw new RuntimeException("Unmatched : Line :" + line + ", Str 1: (" + strLine1 + "), Str 2: (" + strLine2 + ")");
					}
				}

				if ((strLine1 != null) || (strLine2 != null))
					throw new RuntimeException("Unmatched : Line :" + line + ", Str 1: (" + strLine1 + "), Str 2: (" + strLine2 + ")");
			} finally {
				if ( null != br1)
					br1.close();

				if ( null != br2)
					br2.close();
			}
		}
	}

	public static class WriterThread
		extends DatabusThreadBase
	{
		private final SecureRandom random = new SecureRandom();
		private final File _file;
		private BufferedWriter _writer;
		private final long _intervalMs;
		private final BufferedReader _reader;
		private FileOutputStream _fileOutputStream;
		private String _lastLine;


		/** Do we need to insert NULL characters **/
		private final boolean _insertNullCharacters;

		/** Number of times NULL character has to be inserted **/
		private final int _numNullInsertions;

		/** Number of valid lines to appear between successive NULL insertion **/
		private final int _nullInterval;

		//Counters
		private int _numNullInserted = 0;
		private int _numLinesInserted = 0;

		public WriterThread(String name, File file, long intervalMs, BufferedReader reader)
				throws IOException
		{
			this(name, file, intervalMs, reader, false, 0,1);
		}

		public WriterThread(String name,
				            File file,
				            long intervalMs,
				            BufferedReader reader,
				            boolean insertNullCharacters,
				            int numNullInsertions,
				            int nullInterval)
				throws IOException
		{
			super(name);
			_file = file;
			_intervalMs = intervalMs;
			_fileOutputStream = new FileOutputStream(file);
			_writer = new BufferedWriter(new OutputStreamWriter(_fileOutputStream));
			_reader = reader;
			_insertNullCharacters = insertNullCharacters;
			_numNullInsertions = numNullInsertions;
			_nullInterval = nullInterval;
		}
		@Override
    public boolean runOnce()
				throws DatabusException
		{
			try
			{
				if ( _reader == null)
				{
					// Random generation
					String randomString = new BigInteger(200, random).toString(32);
					_writer.append(randomString);
					_lastLine = randomString;
					_writer.append("\n");
					//LOG.info("Written String :" + randomString);
				} else {
					// Copy from reader
					String line = _reader.readLine();
					_lastLine = line;
					LOG.debug("Read String is :" + line);

					if ( line != null)
					{
						_writer.append(line);
						_writer.append("\n");
					} else {
						return false;
					}
				}

				_numLinesInserted ++;

				if ( _insertNullCharacters
						&& ( _numNullInserted < _numNullInsertions)
						&& ((_numLinesInserted % _nullInterval) == 0))
				{
					_writer.flush();
					_fileOutputStream.flush();
					long position = _fileOutputStream.getChannel().position();

					// Append some 0x0 characters
					byte[] b = { 0x0, 0x0, 0x0, 0x0, 0x0 };
					_writer.append(new String(b));

					_writer.flush();
					_fileOutputStream.flush();

					_numNullInserted ++;

					try { awaitUnPauseRequest(); } catch (InterruptedException e) {}
					LOG.info("Writer resuming. Starting to write valid data from position " + position);
					_fileOutputStream.getChannel().position(position);
					_writer.append("VALID DATA HERE");
				}

				if (_intervalMs > 0 )
				{
					try
					{
						Thread.sleep(_intervalMs);
					} catch (InterruptedException ie) {
						LOG.info("Got interrupted while sleeping for :" + _intervalMs + " ms");
					}
				}
				_writer.flush();
			} catch (IOException e) {
				LOG.error("Got Exception :", e);
				throw new DatabusException(e);
			}
			return true;
		}

		@Override
		public void beforeRun()
		{}

		@Override
		public void afterRun()
		{
			try {
				_writer.close();
			} catch (IOException e) {
				LOG.error("Got Exception while closing inputStream for file :" + _file, e);
			}
		}

		public String getLastLine()
		{
		  return _lastLine;
		}

		public long getNumLinesInserted()
		{
			return _numLinesInserted;
		}

		public long getNumNullsInserted()
		{
			return _numNullInserted;
		}
	}
}
