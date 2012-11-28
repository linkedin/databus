package com.linkedin.databus.bootstrap.utils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.log4j.Logger;

import com.linkedin.databus.bootstrap.common.BootstrapConfig;
import com.linkedin.databus.bootstrap.common.BootstrapConn;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO;
import com.linkedin.databus.bootstrap.common.BootstrapReadOnlyConfig;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;

public class BootstrapDbControlMain
{
  public static final Logger LOG = Logger.getLogger(BootstrapDbControlMain.class.getName());

  public static final String ACTION_OPTION_NAME = "action";
  public static final char ACTION_OPTION_CHAR = 'a';
  public static final String SRCID_OPTION_NAME = "srcid";
  public static final char SRCID_OPTION_CHAR = 'S';

  public static enum Action
  {
    CREATE_LOG_TABLE,
    CREATE_TAB_TABLE;

    public static String generateActionList()
    {
      return Arrays.toString(new Action[]{CREATE_LOG_TABLE, CREATE_TAB_TABLE});
    }
  }

  public static class Cli extends ServerContainer.Cli
  {
    private Action _action = null;
    private Integer _srcId = null;

    public Cli()
    {
      super();
    }

    public Cli(String usage)
    {
      super(usage);
    }

    @Override
    public void processCommandLineArgs(String[] cliArgs) throws IOException, DatabusException
    {
      super.processCommandLineArgs(cliArgs);
      CommandLine cmdLine = getCmdLine();

      if (! cmdLine.hasOption(ACTION_OPTION_CHAR))
      {
        throw new DatabusException("action expected; see --help for usage");
      }
      String actionString = cmdLine.getOptionValue(ACTION_OPTION_CHAR).toUpperCase();

      try
      {
        _action = Action.valueOf(actionString);
      }
      catch (IllegalArgumentException e)
      {
        throw new DatabusException("invalid action: " + actionString);
      }

      if (_action.equals(Action.CREATE_LOG_TABLE) || _action.equals(Action.CREATE_TAB_TABLE))
      {
        if (! cmdLine.hasOption(SRCID_OPTION_CHAR))
        {
          throw new DatabusException("srcid expected; see --help for usage");
        }
        String srcidString = cmdLine.getOptionValue(SRCID_OPTION_CHAR);

        try
        {
          _srcId = Integer.parseInt(srcidString);
        }
        catch (NumberFormatException e)
        {
          throw new DatabusException("invalid srcid: " + srcidString);
        }
      }
    }

    @SuppressWarnings("static-access")
    @Override
    protected void constructCommandLineOptions()
    {
      super.constructCommandLineOptions();

      Option actionOption = OptionBuilder.withArgName("action")
                                         .hasArg()
                                         .withLongOpt(ACTION_OPTION_NAME)
                                         .withDescription("action:" + Action.generateActionList())
                                         .create(ACTION_OPTION_CHAR);
      Option srcidOption = OptionBuilder.withArgName("srcid")
                                        .hasArg()
                                        .withLongOpt(SRCID_OPTION_NAME)
                                        .withDescription("action:")
                                        .create(SRCID_OPTION_CHAR);

      _cliOptions.addOption(actionOption);
      _cliOptions.addOption(srcidOption);
    }

    public Action getAction()
    {
      return _action;
    }

    public Integer getSrcId()
    {
      return _srcId;
    }

  }

  public static void main(String[] args)
         throws IOException, DatabusException,  InstantiationException, IllegalAccessException,
                ClassNotFoundException, SQLException
  {
    Cli cli = new Cli("java " + BootstrapDbControlMain.class.getSimpleName() + " [options]");
    cli.processCommandLineArgs(args);

    Properties cfgProps = cli.getConfigProps();
    BootstrapConfig cfgBuilder = new BootstrapConfig();
    ConfigLoader<BootstrapReadOnlyConfig> configLoad =
        new ConfigLoader<BootstrapReadOnlyConfig>("databus.bootstrap.", cfgBuilder);
    configLoad.loadConfig(cfgProps);

    BootstrapReadOnlyConfig cfg = cfgBuilder.build();
    BootstrapConn conn = new BootstrapConn();
    try
    {
      conn.initBootstrapConn(true, cfg.getBootstrapDBUsername(), cfg.getBootstrapDBPassword(),
                             cfg.getBootstrapDBHostname(), cfg.getBootstrapDBName());
      BootstrapDBMetaDataDAO dao = new BootstrapDBMetaDataDAO(conn,        		
    		    					cfg.getBootstrapDBHostname(),
    		    					cfg.getBootstrapDBUsername(),
    		    					cfg.getBootstrapDBPassword(),
    		    					cfg.getBootstrapDBName(),
    		    					false);
      switch (cli.getAction())
      {
      case CREATE_LOG_TABLE: dao.createNewLogTable(cli.getSrcId()); break;
      case CREATE_TAB_TABLE: dao.createNewSrcTable(cli.getSrcId()); break;
      default: throw new RuntimeException("unknown action: " + cli.getAction());
      }
    }
    finally
    {
      conn.close();
    }
  }

}
