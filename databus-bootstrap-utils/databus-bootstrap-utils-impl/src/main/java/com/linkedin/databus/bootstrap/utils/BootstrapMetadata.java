package com.linkedin.databus.bootstrap.utils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.linkedin.databus.bootstrap.common.BootstrapConn;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO;
import com.linkedin.databus.bootstrap.common.BootstrapDBMetaDataDAO.SourceStatusInfo;
import com.linkedin.databus2.util.DBHelper;

public class BootstrapMetadata
{

  public final static String DEFAULT_BOOTSTRAP_HOST = "localhost";
  public final static String DEFAULT_BOOTSTRAP_DATABASE = "bootstrap";
  public final static String DEFAULT_BOOTSTRAP_USER = "bootstrap";
  public final static String DEFAULT_BOOTSTRAP_PASSWORD = "bootstrap";
  public final static String DEFAULT_MINSCN_TYPE = "catchup-purged";

  private static BootstrapConn _bsConn = null;
  private static BootstrapDBMetaDataDAO _dbao = null;


  /**
   */
  public static void main(String[] args)
  {
    int exitStatus = 0;

    GnuParser cmdLineParser = new GnuParser();
    Options options = new Options();
    options
        .addOption("d", "dbname", true, "database name : [bootstrap]")
        .addOption("s", "dbhost", true, "database hostname: [localhost]")
        .addOption("p", "dbpassword", true, "database password: [bootstrap]")
        .addOption("u", "dbuser", true, "database user: [bootstrap] ")
        .addOption("S", "sources", true,
            " <srcid,..>|<srcName substring,..,>  [all]")
        .addOption(
            "m",
            "minScnType",
            true,
            "type of minScn algorithm: [catchup-forever] catchup-forever|catchup-purged|snapshot. " +
            "'snapshot':        use when snapshot table has been purged and the size of snapshot is not large.Times-out in 5 minutes if query cannot complete. " +
            "'catchup-purged':  use when snapshot table has been purged and a conservative value of minScn is acceptable or when 'snapshot' isn't feasible. " +
            "'catchup-forever': use when snapshot table has *never* been purged . "+
            "Note: If a source has been seeded, regardless of the minScnType, the value returned is 0.")
        .addOption("h", "help", false, "help");

    // add descriptions used in usage() 'functionName' , 'description-pairs'
    String[] functionNames = {
        "getSources",
        "get sourceName, sourceId, sourceStatus from bootstrap db",
        "createMinScnTable",
        "create metadata required for minScn feature in bootstrap db if none exists",
        "createAllTables",
        "create all metadata in bootstrap db if they don't exist",
        "getMinScn",
        "[--sources <>] return minScn as seen by bootstrap service for specified sources",
        "computeMinScn",
        "[--sources <>] [--minScnType <>] return minScn using one of the algorithms specified. "
            + "INIT could mean failure to find a minSCN ",
        "computeAndSetMinScn",
        "[--sources <>] [--minScnType<>] set minScn using one of the algorithms specified. ",
        "setMinScn",
        "[--sources <>] INFINITY|INIT|<scn> set minScn to a desired value. ", };

    try
    {
      CommandLine cmdLineArgs = cmdLineParser.parse(options, args, false);

      if (cmdLineArgs.hasOption('h'))
      {
        usage(options,functionNames);
        System.exit(0);
      }


      String[] fns = cmdLineArgs.getArgs();
      if (fns.length < 1)
      {
        usage(options,functionNames);
        throw new Exception("Missing argument");
      }

      String function = fns[0];
      String arg1 = (fns.length > 1) ? fns[1] : null;
      if (!legitFunction(functionNames,function))
      {
        usage(options,functionNames);
        throw new Exception("Unknown function");
      }

      String bootstrapHost = cmdLineArgs.getOptionValue("dbhost",
          DEFAULT_BOOTSTRAP_HOST);
      String bootstrapDbName = cmdLineArgs.getOptionValue("dbname",
          DEFAULT_BOOTSTRAP_DATABASE);
      String bootstrapPassword = cmdLineArgs.getOptionValue("dbpassword",
          DEFAULT_BOOTSTRAP_PASSWORD);
      String bootstrapUser = cmdLineArgs.getOptionValue("dbuser",
          DEFAULT_BOOTSTRAP_USER);

      _bsConn = new BootstrapConn();
      _bsConn.initBootstrapConn(false, bootstrapUser, bootstrapPassword,
          bootstrapHost, bootstrapDbName);
      _dbao = new BootstrapDBMetaDataDAO(_bsConn, bootstrapHost, bootstrapUser,
          bootstrapPassword, bootstrapDbName, false);

      String minScnType = cmdLineArgs.getOptionValue("minScnType",
          DEFAULT_MINSCN_TYPE);

      if (function.equals("createMinScnTable"))
      {
        createBootstrapMinScn();
        return;
      }
      else if (function.equals("createAllTables"))
      {
        _dbao.setupDB();
        return;
      }

      List<SourceStatusInfo> sourceInfoList = _dbao.getSrcStatusInfoFromDB();
      if (function.equalsIgnoreCase("getSources"))
      {
        for (SourceStatusInfo s : sourceInfoList)
        {
          StringBuilder sb = new StringBuilder();
          sb.append(s.getSrcName()).append('\t').append(s.getSrcId())
              .append('\t').append(s.getStatus());
          System.out.println(sb.toString());
        }
      }
      else
      {
        // not getSources()
        // expect srcId list; for brevity;
        String mySources = cmdLineArgs.getOptionValue("sources");
        String mySrcList[] = null;
        if (mySources != null)
        {
          mySrcList = mySources.split(",");
        }

        List<SourceStatusInfo> finalSrcIdList = (mySrcList != null) ? getFinalSrcIdList(
            sourceInfoList, mySrcList) : sourceInfoList;

        if (function.equalsIgnoreCase("getMinScn"))
        {
          // read minScn from minscn table;
          createBootstrapMinScn();
          for (SourceStatusInfo sInfo : finalSrcIdList)
          {
            long minScn = _dbao.getMinScnOfSnapshots(sInfo.getSrcId());
            DBHelper.commit(_bsConn.getDBConn());
            if (minScn == Long.MIN_VALUE)
            {
              System.err.println("Error: Cannot get minScn for "
                  + sInfo.getSrcId());
            }
            else
            {
              printScn(minScn, sInfo, '\t');
            }
          }
        }
        else if (function.equalsIgnoreCase("computeMinScn"))
        {
          // run a query against snapshot table/log table meta using --type
          // snapshot|catchup-purged|catchup-forever
          // database not affected
          for (SourceStatusInfo sInfo : finalSrcIdList)
          {
            long minScn = computeMinScn(minScnType, sInfo.getSrcId());
            // commit select;
            DBHelper.commit(_bsConn.getDBConn());
            if (minScn == Long.MIN_VALUE)
            {
              System.err.println("Error: Cannot get minScn for "
                  + sInfo.getSrcId());
            }
            else
            {
              printScn(minScn, sInfo, '\t');
            }

          }
        }
        else if (function.equalsIgnoreCase("setMinScn"))
        {
          if (arg1 == null)
          {
            throw new Exception(
                "Missing argument for setMinScn: <scn>, INFINITY or INIT");
          }
          // setMinScn <scn>
          // override existing scn; never fail unless update fails ; show
          // warning that you should know what you are doing
          createBootstrapMinScn();
          long scn;
          if (arg1.equalsIgnoreCase("INFINITY"))
          {
            scn = Long.MAX_VALUE;
          }
          else if (arg1.equalsIgnoreCase("INIT"))
          {
            scn = BootstrapDBMetaDataDAO.DEFAULT_WINDOWSCN;
          }
          else
          {
            scn = Long.parseLong(arg1);
          }
          for (SourceStatusInfo sInfo : finalSrcIdList)
          {
            // calls commit
            _dbao.updateMinScnOfSnapshot(sInfo.getSrcId(), scn);
          }
        }
        else if (function.equalsIgnoreCase("computeAndSetMinScn"))
        {
          // set scn iff minScn has been initialized; ensure minScn is updated
          // safely according to --type
          // if seeded; --type is ignored;
          createBootstrapMinScn();
          for (SourceStatusInfo sInfo : finalSrcIdList)
          {
            long minScn = computeMinScn(minScnType, sInfo.getSrcId());
            if (minScn != Long.MIN_VALUE)
            {
              _dbao.updateMinScnOfSnapshot(sInfo.getSrcId(), minScn);
            }
            DBHelper.commit(_bsConn.getDBConn());
          }
        }
      }

    }
    catch (ParseException e)
    {
      usage(options,functionNames);
      exitStatus = 1;
    }
    catch (Exception e)
    {
      System.err.println("Error: " + e);
      exitStatus = 1;
    }
    finally
    {
      if (_dbao != null)
        _dbao.close();
      System.exit(exitStatus);
    }
  }

  private static boolean legitFunction(String[] functionNames, String fn)
  {
    for (String f:functionNames)
    {
      if (fn.equalsIgnoreCase(f))
        return true;
    }
    return false;
  }

  static private void printScn(long scn, SourceStatusInfo s, char delim)
  {
    StringBuilder sb = new StringBuilder();
    if (scn == BootstrapDBMetaDataDAO.INFINITY_WINDOWSCN)
    {
      sb.append("INFINITY");
    }
    else if (scn == BootstrapDBMetaDataDAO.DEFAULT_WINDOWSCN)
    {
      sb.append("INIT");
    }
    else
    {
      sb.append(scn);
    }
    sb.append(delim).append(s.getSrcId()).append(delim).append(s.getSrcName());
    System.out.println(sb.toString());
  }

  private static long computeMinScn(String minScnType, int sourceId)
  {
    long minScn = Long.MIN_VALUE;
    try
    {
      if (_dbao.isSeeded(sourceId))
      {
        return 0;
      }
      if (minScnType.equalsIgnoreCase("snapshot"))
      {
        minScn = _dbao.getMinWindowScnFromSnapshot(sourceId, Integer.MAX_VALUE, 5*60);
      }
      else if (minScnType.equalsIgnoreCase("catchup-forever"))
      {
        minScn = _dbao.getMinScnFromLogTable(sourceId, false);
      }
      else if (minScnType.equalsIgnoreCase("catchup-purged"))
      {
        minScn = _dbao.getMinScnFromLogTable(sourceId, true);
      }
      else
      {
        System.err.println("Error: unknown minScnType: " + minScnType);
      }
    }
    catch (SQLException e)
    {
      System.err.println("Error in computeMinScn" + e);
    }
    return minScn;
  }

  // TODO: make it more optimal
  private static List<SourceStatusInfo> getFinalSrcIdList(
      List<SourceStatusInfo> sourceInfoList, String[] mySrcList)
  {
    List<SourceStatusInfo> srcList = new ArrayList<SourceStatusInfo>();
    for (String mysrc : mySrcList)
    {
      int srcid = -1;
      try
      {
        srcid = Integer.parseInt(mysrc);
      }
      catch (NumberFormatException e)
      {

      }
      for (SourceStatusInfo s : sourceInfoList)
      {
        if ((s.getSrcId() == srcid)
            || (srcid < 0 && s.getSrcName().contains(mysrc)))
        {
          srcList.add(s);
        }
      }

    }
    return srcList;
  }

  static void createBootstrapMinScn() throws SQLException
  {
    if (!_dbao.doesMinScnTableExist())
    {
      _dbao.setupMinScnTable();
    }
  }

  public static void usage(Options options,String[] functionNames)
  {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + BootstrapMetadata.class.getName(),options);
    System.out.println("");
    for (int i=0; i < functionNames.length;i+=2)
    {
      String s = String.format("%-25s%s",functionNames[i],functionNames[i+1]);
      System.out.println(s);
    }
  }

}
