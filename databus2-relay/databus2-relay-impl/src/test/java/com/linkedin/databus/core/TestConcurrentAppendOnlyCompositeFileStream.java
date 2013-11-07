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
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.databus.core.TrailFileNotifier.TrailFileManager;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.test.TestUtil;

public class TestConcurrentAppendOnlyCompositeFileStream
{
  public static final String MODULE = TestConcurrentAppendOnlyCompositeFileStream.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final String SRC_FILE_PREFIX = "src_trail_";
  public static final String DEST_FILE_PREFIX = "dest_trail_";

  @BeforeClass
  public void setUpClass() throws InvalidConfigException
  {
    //setup logging
    TestUtil.setupLogging(true, null, Level.INFO);
    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
  }

  private File createTempDir()
      throws IOException
  {
    File dir = File.createTempFile("dir_", null);

    if (!(dir.delete()))
      throw new IOException("Unable to delete temp file " + dir.getAbsolutePath());

    if ( !dir.mkdir())
      throw new IOException("Unable to create tempDir :" + dir.getAbsolutePath());

    dir.deleteOnExit();
    return dir;
  }


  @Test
  public void testStaticStream()
      throws Exception
  {

    File dir = createTempDir();
    File srcFile = File.createTempFile(SRC_FILE_PREFIX, null, dir);
    File destFile = File.createTempFile(DEST_FILE_PREFIX, null, dir);
    TrailFilesComparator c = null;
    WriterThread w2 = null;
    WriterThread w1 = null;
    try
    {
      LOG.info("Src File :" + srcFile);
      LOG.info("Dest File :" + destFile);

      w1 = new WriterThread("Writer1",srcFile, 10,100, null);
      w1.start();

      Thread.sleep(10005);
      w1.shutdown();

      ConcurrentAppendableCompositeFileInputStream r2 = new ConcurrentAppendableCompositeFileInputStream(dir.getAbsolutePath(), null, -1, new FileFilter(dir,srcFile.getAbsolutePath(), false, false), false);

      BufferedReader r = new BufferedReader(new InputStreamReader(r2));
      w2 = new WriterThread("Writer2", destFile, 0, 100,	r);

      LOG.info("Reader started !!");
      w2.start();

      LOG.info("About to sleep for 15 sec");
      Thread.sleep(15000);

      LOG.info("Closing reader !!");
      r2.close();
      LOG.info("Reader closed !!");
      w2.join();

      r.close();

      c = new TrailFilesComparator(srcFile.getAbsolutePath(), destFile.getAbsolutePath());
      c.compare();

      if ( null != w1)
        w1.deleteFiles();

      if ( null != w2)
        w2.deleteFiles();
    } finally {
      srcFile.delete();
      destFile.delete();
    }
  }

  @Test
  public void testErrorStaticStream()
      throws Exception
  {

    File dir = createTempDir();
    File srcFile = File.createTempFile(SRC_FILE_PREFIX, null, dir);
    File destFile = File.createTempFile(DEST_FILE_PREFIX, null, dir);
    WriterThread w1 = null;
    try
    {
      LOG.info("Src File :" + srcFile);
      LOG.info("Dest File :" + destFile);

      w1 = new WriterThread("Writer1",srcFile, 10,100, null);
      w1.start();

      Thread.sleep(10005);
      w1.shutdown();

      ConcurrentAppendableCompositeFileInputStream r2 = new ConcurrentAppendableCompositeFileInputStream(dir.getAbsolutePath(), null, -1, new FileFilter(dir,srcFile.getAbsolutePath(), true, false), false);


      int ret = 0;
      ret = r2.read();
      Assert.assertEquals("Read should freturn EOF", ConcurrentAppendableSingleFileInputStream.EOF, ret);
      Assert.assertEquals("InputStream should have been closed", true, r2.isClosed());

      if ( null != w1)
        w1.deleteFiles();
    } finally {
      srcFile.delete();
      destFile.delete();
    }
  }


  @Test
  /**
   * Test error case when onNewTrailFile notices error
   * @throws Exception
   */
  public void testErrorConcurrentAppendStream1()
      throws Exception
  {
	  File dir = createTempDir();
	  File srcFile = File.createTempFile(SRC_FILE_PREFIX, null, dir);
	  File destFile = File.createTempFile(DEST_FILE_PREFIX, null, dir);
	  WriterThread w2 = null;
	  WriterThread w1 = null;
	  try
	  {
		  LOG.info("Src File :" + srcFile);
		  LOG.info("Dest File :" + destFile);

		  w1 = new WriterThread("Writer1",srcFile, 10,100, null);
		  w1.start();

		  ConcurrentAppendableCompositeFileInputStream r2 = new ConcurrentAppendableCompositeFileInputStream(dir.getAbsolutePath(), null, -1, new FileFilter(dir,srcFile.getAbsolutePath(),false, true), false);

		  BufferedReader r = new BufferedReader(new InputStreamReader(r2));
		  w2 = new WriterThread("Writer2", destFile, 0, 100,	r);

		  LOG.info("Reader started !!");
		  w2.start();

		  LOG.info("About to sleep for 12 secs");
		  Thread.sleep(12000);
		  LOG.info("Stopping writer !!");
		  w1.shutdown();
		  LOG.info("Writer stopped !!");

		  LOG.info("About to sleep for 5 secs");
		  Thread.sleep(5000);

		  Assert.assertEquals("InputStream should be closed", true, r2.isClosed());
		  Assert.assertEquals("ShutdownOnError should be marked true", true,r2.getTrailFileNotifier().isShutdownOnError());

		  w2.join();

		  r.close();
		  r2.close();

		  if ( null != w1)
			  w1.deleteFiles();

		  if ( null != w2)
			  w2.deleteFiles();
	  } finally {
		  srcFile.delete();
		  destFile.delete();
	  }
  }

  @Test
  /**
   * Test when reader starts immediately after writer
   * @throws Exception
   */
  public void testConcurrentAppendStream1()
      throws Exception
  {
    File dir = createTempDir();
    File srcFile = File.createTempFile(SRC_FILE_PREFIX, null, dir);
    File destFile = File.createTempFile(DEST_FILE_PREFIX, null, dir);
    TrailFilesComparator c = null;
    WriterThread w2 = null;
    WriterThread w1 = null;
    try
    {
      LOG.info("Src File :" + srcFile);
      LOG.info("Dest File :" + destFile);

      w1 = new WriterThread("Writer1",srcFile, 10,100, null);
      w1.start();

      ConcurrentAppendableCompositeFileInputStream r2 = new ConcurrentAppendableCompositeFileInputStream(dir.getAbsolutePath(), null, -1, new FileFilter(dir,srcFile.getAbsolutePath(),false, false), false);

      BufferedReader r = new BufferedReader(new InputStreamReader(r2));
      w2 = new WriterThread("Writer2", destFile, 0, 100,	r);

      LOG.info("Reader started !!");
      w2.start();

      LOG.info("About to sleep for 12 secs");
      Thread.sleep(12000);
      LOG.info("Stopping writer !!");
      w1.shutdown();
      LOG.info("Writer stopped !!");

      LOG.info("About to sleep for 5 secs");
      Thread.sleep(5000);

      LOG.info("Closing reader !!");
      r2.close();
      LOG.info("Reader closed !!");

      w2.join();

      r.close();

      c = new TrailFilesComparator(srcFile.getAbsolutePath(), destFile.getAbsolutePath());
      c.compare();

      if ( null != w1)
        w1.deleteFiles();

      if ( null != w2)
        w2.deleteFiles();
    } finally {
      srcFile.delete();
      destFile.delete();
    }
  }

  @Test
  /**
   * Test when reader starts after delay after writer
   * @throws Exception
   */
  public void testConcurrentAppendStream2()
      throws Exception
  {
    File dir = createTempDir();
    File srcFile = File.createTempFile(SRC_FILE_PREFIX, null, dir);
    File destFile = File.createTempFile(DEST_FILE_PREFIX, null, dir);
    TrailFilesComparator c = null;
    WriterThread w2 = null;
    WriterThread w1 = null;
    try
    {
      LOG.info("Src File :" + srcFile);
      LOG.info("Dest File :" + destFile);

      w1 = new WriterThread("Writer1",srcFile, 10,100, null);
      w1.start();

      LOG.info("About to sleep for 3 secs");
      Thread.sleep(3000);

      ConcurrentAppendableCompositeFileInputStream r2 = new ConcurrentAppendableCompositeFileInputStream(dir.getAbsolutePath(), null, -1, new FileFilter(dir,srcFile.getAbsolutePath(),false, false), false);

      BufferedReader r = new BufferedReader(new InputStreamReader(r2));
      w2 = new WriterThread("Writer2", destFile, 0, 100,	r);

      LOG.info("Reader started !!");
      w2.start();

      LOG.info("About to sleep for 10 secs");
      Thread.sleep(10000);
      LOG.info("Stopping writer !!");
      w1.shutdown();
      LOG.info("Writer stopped !!");

      LOG.info("About to sleep for 5 secs");
      Thread.sleep(5000);

      LOG.info("Closing reader !!");
      r2.close();
      LOG.info("Reader closed !!");

      w2.join();

      r.close();

      c = new TrailFilesComparator(srcFile.getAbsolutePath(), destFile.getAbsolutePath());
      c.compare();

      if ( null != w1)
        w1.deleteFiles();

      if ( null != w2)
        w2.deleteFiles();
    } finally {
      srcFile.delete();
      destFile.delete();
    }
  }

  /**
   * Test when reader starts before writer
   * @throws Exception
   */
  @Test
  public void testConcurrentAppendStream3()
      throws Exception
  {
    File dir = createTempDir();
    File srcFile = File.createTempFile(SRC_FILE_PREFIX, null, dir);
    File destFile = File.createTempFile(DEST_FILE_PREFIX, null, dir);
    TrailFilesComparator c = null;
    WriterThread w2 = null;
    WriterThread w1 = null;
    try
    {
      LOG.info("Src File :" + srcFile);
      LOG.info("Dest File :" + destFile);

      ConcurrentAppendableCompositeFileInputStream r2 = new ConcurrentAppendableCompositeFileInputStream(dir.getAbsolutePath(), null, -1, new FileFilter(dir,srcFile.getAbsolutePath(),false, false), false);
      BufferedReader r = new BufferedReader(new InputStreamReader(r2));
      w2 = new WriterThread("Writer2", destFile, 0, 100,	r);
      w2.start();
      LOG.info("Reader started !!");

      LOG.info("About to sleep for 3 secs");
      Thread.sleep(12000);

      w1 = new WriterThread("Writer1",srcFile, 10,100, null);
      w1.start();
      LOG.info("Writer started !!");

      LOG.info("About to sleep for 10 secs");
      Thread.sleep(10000);
      LOG.info("Stopping writer !!");
      w1.shutdown();
      LOG.info("Writer stopped !!");

      LOG.info("About to sleep for 5 secs");
      Thread.sleep(5000);

      LOG.info("Closing reader !!");
      r2.close();
      LOG.info("Reader closed !!");

      w2.join();

      r.close();

      c = new TrailFilesComparator(srcFile.getAbsolutePath(), destFile.getAbsolutePath());
      c.compare();

      if ( null != w1)
        w1.deleteFiles();

      if ( null != w2)
        w2.deleteFiles();
    } finally {
      srcFile.delete();
      destFile.delete();
    }
  }

  public static class FileFilter
  implements TrailFileManager
  {

    private final String _prefix;

    private final boolean induceErrorOnFirstTrail;
    private final boolean induceErrorOnOtherTrail;

    public FileFilter(File dir,
                      String prefix,
                      boolean induceErrorOnFirstTrail,
                      boolean induceErrorOnOtherTrail)
    {
      _prefix = prefix + "_";
      this.induceErrorOnFirstTrail = induceErrorOnFirstTrail;
      this.induceErrorOnOtherTrail = induceErrorOnOtherTrail;
    }

    @Override
    public int compareFileName(File file1, File file2) {
      String[] f1 = file1.getAbsolutePath().split("_");
      String[] f2 = file2.getAbsolutePath().split("_");

      Long num1 = Long.parseLong(f1[f1.length-1]);
      Long num2 = Long.parseLong(f2[f2.length-1]);

      //System.out.println("Num1 for :" + file1 + " is :" + num1 + ", num2 for :" + file2 + " is :" + num2);
      if ( induceErrorOnFirstTrail)
    	  return num2.compareTo(num1);

      return num1.compareTo(num2);
    }

    @Override
    public boolean isTrailFile(File file)
    {
      String f1 = file.getAbsolutePath();

      if (f1.startsWith(_prefix))
        return true;

      return false;
    }

	@Override
	public boolean isNextFileInSequence(File file1, File file2)
	{
	  if (induceErrorOnOtherTrail)
		  return false;

	  String[] f1 = file1.getAbsolutePath().split("_");
	  String[] f2 = file2.getAbsolutePath().split("_");

	  long num1 = Long.parseLong(f1[f1.length-1]);
	  long num2 = Long.parseLong(f2[f2.length-1]);
	  if ((num2 - num1) == 1)
	  {
		return true;
	  }
	  return false;
	}
  }


  public static class TrailFilesComparator
  {
    private final String _file1Prefix;
    private final String _file2Prefix;

    public TrailFilesComparator(String file1Prefix, String file2Prefix)
    {
      _file1Prefix = file1Prefix;
      _file2Prefix = file2Prefix;
    }


    public void compare() throws Exception
    {
      long i = 1;

      while (true)
      {
        File f1 = new File(_file1Prefix + "_" + i);
        File f2 = new File(_file2Prefix + "_" + i);

        boolean exist1 = f1.exists();
        boolean exist2 = f2.exists();

        LOG.info("Comparing files : " + f1 + " and " + f2);

        if ( (!exist1 ) && (!exist2))
        {
          return;
        }
        else if (exist1 != exist2)
        {
          throw new Exception("File Mismatch. File (" + f1 + ") exists ? " + exist1 + ", File (" + f2 +") exists ?" + exist2 );
        }
        else
        {
          FileComparator c = new FileComparator(f1.getAbsolutePath(), f2.getAbsolutePath());
          c.compare();
        }

        i++;
      }
    }
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
    private final long _numLinesPerFile;

    private long _lineCount = 0;
    private int _nextFileNum = 1;


    private final List<File> _files = new ArrayList<File>();

    public WriterThread(String name,
                        File file,
                        long intervalMs,
                        long numLinesPerFile,
                        BufferedReader reader)
                            throws IOException
    {
      super(name);
      _file = file;
      _intervalMs = intervalMs;
      rolloverWriterFile();
      _reader = reader;
      _numLinesPerFile = numLinesPerFile;
    }

    private void rolloverWriterFile()
        throws IOException
    {
      String f = _file.getAbsolutePath() + "_" + _nextFileNum;
      _nextFileNum++;

      if ( null != _writer)
        _writer.close();

      File f1 = new File(f);
      _writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f1)));
      _files.add(f1);
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
          _writer.append("\n");
          _lineCount++;

          if ((_numLinesPerFile > 0) && ((_lineCount%_numLinesPerFile) == 0))
            rolloverWriterFile();

          LOG.debug("Written String :" + randomString);
        } else {
          // Copy from reader
          String line = _reader.readLine();

          if ( line != null)
          {
            LOG.debug("Read String is :" + line);
            _writer.append(line);
            _writer.append("\n");
            _lineCount++;
            if ((_numLinesPerFile > 0) && ((_lineCount%_numLinesPerFile) == 0))
              rolloverWriterFile();
          } else {
            return false;
          }
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

    public void deleteFiles()
    {
      for (File f : _files)
        f.delete();
    }
  }
}
