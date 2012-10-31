package com.linkedin.databus2.core.seq;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.xeril.util.Destroyable;
import org.xeril.util.config.ConfigHelper;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;

/**
 * This class reads and saves max SCN in a text file.
 *
 * @author abhasin
 */
public class FileMaxSCNHandler implements MaxSCNReaderWriter, Destroyable
{
  private static final String TEMP          = ".temp";
  public static final String SCN_SEPARATOR = ":";
  private static final String MODULE        = FileMaxSCNHandler.class.getName();
  private static final Logger LOG        = Logger.getLogger(MODULE);

  //private final String        _key;
  //private final String        _scnDir;
  private final String        _scnFileName;
  //private final Long          _flushInterval;
  //private final Long          _defaultInitVal;
  private final AtomicLong    _scn;
  private final AtomicLong    _flushCounter;
  private final StaticConfig _staticConfig;

  /**
   * Factory method for dependency injection
   *
   * @param   config   the MaxSCN handler configuration builder
   * @return  a new handler object
   */
  public static FileMaxSCNHandler create(Config config) throws IOException, DatabusException,
                                                               InvalidConfigException
  {
    return create(config.build());
  }

  /**
   * Factory method for dependency injection
   *
   * @param   config   the MaxSCN handler static configuration
   * @return  a new handler object
   */
  public static FileMaxSCNHandler create(StaticConfig config) throws IOException, DatabusException
  {
    FileMaxSCNHandler hdlr = new FileMaxSCNHandler(config);
    hdlr.loadInitialValue();
    return hdlr;
  }

  public FileMaxSCNHandler(StaticConfig config)
  {
    _staticConfig = config;
    _flushCounter = new AtomicLong(0);
    _scn = new AtomicLong(0);
    _scnFileName = _staticConfig.getScnDir().getAbsolutePath() + File.separator + _staticConfig.getKey();
    LOG.info("creatgin file:" + _scnFileName);
  }

  /**
   * Reads scn value from SCN File name according to configuration. If SCN file does not
   * exist, it creates the SCN file w/the initial value specified in the configuration. An
   * exception is thrown if we the SCN file exists but we fail to read the SCN value from
   * it.
   *
   * @throws IOException
   *           if we fail to open or read from the SCN file
   * @throws RuntimeException
   *           if the SCN value cannot be read or parsed from the file
   */
  protected void loadInitialValue() throws IOException,
      DatabusException
  {
    Long initVal = null;
    LOG.info("Trying to read initial SCN from file: " + _scnFileName);

    File file = new File(_scnFileName);
    if (file.exists())
    {
      FileReader fileReader = new FileReader(file);
      try
      {
        BufferedReader reader = new BufferedReader(fileReader);
        String scnLine = reader.readLine();
        if (null != scnLine)
        {
          try
          {
            String scnString = scnLine.substring(0, scnLine.indexOf(SCN_SEPARATOR));
            _scn.set(Long.parseLong(scnString));

            LOG.info("Starting from MAX SCN:" + scnString);
          }
          catch (Exception e)
          {
            LOG.error("Could not read initial SCN value. Value missing or not in expected format; scnLine = "
                             + scnLine,
                         e);
            throw new DatabusException("Failed to load initial SCN value. Value misisng or not in expected format.",
                                       e);
          }
        }
        else
        {
          LOG.warn("SCN file empty; defaulting to initial value from configuration:" +
                      _staticConfig.getInitVal());
          _scn.set(_staticConfig.getInitVal());
        }
        reader.close();
      }
      finally
      {
        fileReader.close();
      }
    }
    else
    {
      LOG.info("Initial max SCN does not exist. Defaulting to initial value from configuration: "
          + _staticConfig.getInitVal());
      _scn.set(_staticConfig.getInitVal());
      writeScnToFile();
    }
  }

  /**
   * Write SCN value to file. If SCN file exists, move it aside and create a new file
   * w/new value.
   */
  private void writeScnToFile() throws IOException
  {
    long scn = _scn.longValue();

    File dir = _staticConfig.getScnDir();
    if (! dir.exists() && !dir.mkdirs())
    {
      throw new IOException("unable to create SCN file parent:" + dir.getAbsolutePath());
    }

    // delete the temp file if one exists
    File tempScnFile = new File(_scnFileName + TEMP);
    if (tempScnFile.exists() && !tempScnFile.delete())
    {
      LOG.error("unable to erase temp SCN file: " + tempScnFile.getAbsolutePath());
    }

    File scnFile = new File(_scnFileName);
    if (scnFile.exists() && !scnFile.renameTo(tempScnFile))
    {
      LOG.error("unable to backup scn file");
    }

    if (!scnFile.createNewFile())
    {
      LOG.error("unable to create new SCN file:" + scnFile.getAbsolutePath());
    }
    FileWriter writer = new FileWriter(scnFile);
    writer.write(Long.toString(scn));
    writer.write(SCN_SEPARATOR + new Date().toString());
    writer.flush();
    writer.close();
    LOG.debug("scn persisted: " + scn);
  }

  @Override
  public long getMaxScn()
  {
    return _scn.get();
  }

  @Override
  public void saveMaxScn(long endOfPeriod) throws DatabusException
  {
    _scn.set(endOfPeriod);
    long ctr = _flushCounter.addAndGet(1);
    // Retain the SCN Val every now and then
    if (ctr % _staticConfig.getFlushItvl() == 0)
    {
      if (LOG.isDebugEnabled())
      {
        LOG.debug("Flushing counter:" + ctr);
      }
      try
      {
        writeScnToFile();
      }
      catch (IOException e)
      {
        LOG.error("Caught exception saving SCN = " + _scn, e);
        throw new DatabusException("Caught exception saving SCN = " + _scn, e);
      }
    }
  }

  /**
   * Make Sure We dump back the SCN VAL on shut down
   *
   */
  @Override
  public void destroy()
  {
    LOG.info("destory() called, saving scn file before shutting down.");
    try
    {
      writeScnToFile();
    }
    catch (IOException e)
    {
      LOG.error("Failed to write final SCN value to file on destroy()", e);
    }
  }

  @Override
  public String toString() {
    return _scnFileName + ":" + _scn;
  }

  public static class StaticConfig
  {
    private final String _key;
    private final File _scnDir;
    private final Long   _initVal;
    private final Long   _flushItvl;

    public StaticConfig(String key, File scnDir, Long initVal, Long flushItvl)
    {
      super();
      _key = key;
      _scnDir = scnDir;
      _initVal = initVal;
      _flushItvl = flushItvl;
    }

    /** the name of the file used for storing the SCN */
    public String getKey()
    {
      return _key;
    }

    /** the directory where the SCN will be saved */
    public File getScnDir()
    {
      return _scnDir;
    }

    /** the initial scn value */
    public Long getInitVal()
    {
      return _initVal;
    }

    /** the number of SCN updates before the SCN is persisted to disk */
    public Long getFlushItvl()
    {
      return _flushItvl;
    }


  }

  public static class Config implements ConfigBuilder<StaticConfig>
  {
    private String _key       = "MaxSCN";
    private String _scnDir    = "databus2-maxscn";
    private Long   _initVal   = 0L;
    private Long   _flushItvl = 1L;

    /** the name of the file used for storing the SCN */
    public String getKey()
    {
      return _key;
    }

    /** Changes the name of the file used for storing the SCN */
    public void setKey(String key)
    {
      this._key = key;
    }

    /** Changes the directory where the SCN will be saved */
    public void setScnDir(String scnDir)
    {
      _scnDir = scnDir;
    }

    /** Returns the directory where the SCN will be saved */
    public String getScnDir()
    {
      return _scnDir;
    }

    /** Sets the initial scn value */
    public void setInitVal(long scn)
    {
      _initVal = scn;
    }

    /** Returns the initial scn value */
    public long getInitVal()
    {
      return _initVal;
    }

    /** Sets the number of SCN updates before the SCN is persisted to disk */
    public void setFlushItvl(Long flushItvl)
    {
      _flushItvl = flushItvl;
    }

    /** Returns the number of SCN updates before the SCN is persisted to disk */
    public Long getFlushItvl()
    {
      return ConfigHelper.getRequired(_flushItvl);
    }

    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      File scnDir = new File(_scnDir);

      if (!scnDir.exists())
      {
        if (!scnDir.mkdirs()) throw new InvalidConfigException("Unable to create scn dir:" + _scnDir);
      }

      if (! scnDir.isDirectory()) throw new InvalidConfigException("Not an scn dir:" + _scnDir);

      if (_flushItvl <= 0) throw new InvalidConfigException("Invalid flush interval:" + _flushItvl);

      return new StaticConfig(_key, scnDir, _initVal, _flushItvl);
    }
  }
}
