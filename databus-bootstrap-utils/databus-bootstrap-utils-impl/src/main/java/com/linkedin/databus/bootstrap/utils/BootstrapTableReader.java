package com.linkedin.databus.bootstrap.utils;

import com.linkedin.databus.bootstrap.common.BootstrapConn;
import com.linkedin.databus.client.DbusEventAvroDecoder;
import com.linkedin.databus.core.DbusEventInternalReadable;
import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaSet;
import com.linkedin.databus2.schemas.utils.SchemaHelper;
import com.linkedin.databus2.util.DBHelper;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.PropertyConfigurator;

public class BootstrapTableReader 
{

    public static final String MODULE = BootstrapDBReader.class.getName();
    public static final Logger LOG = Logger.getLogger(MODULE);

    public static final String HELP_OPT_LONG_NAME = "help";
    public static final String BOOTSTRAP_DB_PROPS_OPT_LONG_NAME = "bootstrap_db_props";
    public static final String QUERY_CONFIG_OPT_LONG_NAME = "query_config";
    public static final String SOURCE_ID_PARAM = "source_id";
    public static final String LOG4J_PROPS_OPT_LONG_NAME = "log_props";
    public static final char   HELP_OPT_CHAR = 'h';
    public static final char   BOOTSTRAP_DB_PROP_OPT_CHAR = 'p';
    public static final char   QUERY_CONFIG_OPT_CHAR = 'q';
    public static final char   LOG4J_PROPS_OPT_CHAR = 'l';
    
    private static Properties  _sBootstrapConfigProps = null;
    private static Properties  _sQueryConfigProps = null;
   
    private static BootstrapSeederMain.StaticConfig _bsStaticConfig;
    private static StaticConfig _queryStaticConfig;
    private static DbusEventAvroDecoder _decoder;
    private static Map<Field, Type>  _fieldTypes;
    private static Schema _schema;
    
    private BootstrapReaderEventHandler _eventHandler;
    
    public BootstrapTableReader(BootstrapReaderEventHandler eventHandler)
    {
    	_eventHandler = eventHandler;
    }
    
    private String getTableName()
    {
      String table = null;
      if (_queryStaticConfig.getTablePrefix().equalsIgnoreCase("tab_"))
      {
        table = "tab_" + _queryStaticConfig.getSourceId();
      } else {
        table = "log_" + _queryStaticConfig.getSourceId() + "_" + _queryStaticConfig.getLogId();
      }
      return table;
    }
    
    private BootstrapConn _bootstrapConn = null;
    
    public void execute()
      throws Exception
    {
      String query = getQuery();
      Connection conn = null;
      Statement stmt = null;
      ResultSet rs = null;
      try
     {
        conn = getConnection();
        
        stmt = conn.createStatement();
        
        LOG.info("Executing query : " + query);
        rs = stmt.executeQuery(query);
        
        byte[] b1 = new byte[1024 * 1024];
        ByteBuffer buffer = ByteBuffer.wrap(b1);
        DbusEventInternalReadable event = new DbusEventV1(buffer,0);

        int count = 0;
        _eventHandler.onStart(query);
        
        while (rs.next())
        {
          buffer.clear();
          buffer.put(rs.getBytes("val"));
          event.reset(buffer, 0);
          GenericRecord record = _decoder.getGenericRecord(event);
          _eventHandler.onRecord(event, record);
          count++;
        } 
        _eventHandler.onEnd(count);
      } finally {
        DBHelper.close(rs,stmt,conn);
      }
    }
    
    public Connection getConnection()
    {
      Connection conn = null;

      if (_bootstrapConn == null)
      {
        LOG.info("<<<< Creating Bootstrap Connection!! >>>>");
        _bootstrapConn = new BootstrapConn();
        try
       {
           _bootstrapConn.initBootstrapConn(false,
                                            _bsStaticConfig.getBootstrap().getBootstrapDBUsername(),
                                            _bsStaticConfig.getBootstrap().getBootstrapDBPassword(),
                                            _bsStaticConfig.getBootstrap().getBootstrapDBHostname(),
                                            _bsStaticConfig.getBootstrap().getBootstrapDBName());
        } catch (Exception e) {
            LOG.fatal("Unable to open BootstrapDB Connection !!", e);
            throw new RuntimeException("Got exception when getting bootstrap DB Connection.",e);
        }
      }

      try
     {
        conn = _bootstrapConn.getDBConn();
      } catch ( SQLException e) {
        LOG.fatal("Not able to open BootstrapDB Connection !!", e);
        throw new RuntimeException("Got exception when getting bootstrap DB Connection.",e);
      }
      return conn;
    }
    
    
    public String getQuery()
    {
      String table = getTableName();
      
      StringBuilder sql = new StringBuilder();
      
      sql.append("select val from ");
      sql.append(table);
      sql.append(" where ").append(_queryStaticConfig.getQueryKey());
      
      if (_queryStaticConfig.isRangeQuery())
      {
        sql.append(" >= ").append(_queryStaticConfig.getMinKey());
        sql.append(" and ").append(_queryStaticConfig.getQueryKey()).append(" <= ").append(_queryStaticConfig.getMaxKey());
      } else {
        sql.append(" = ").append(_queryStaticConfig.getValue());
      }
        
      return sql.toString();
    }
    
    public static void init(String[] args)
      throws Exception
    {
      parseArgs(args);
      
      BootstrapSeederMain.Config bsConf = new BootstrapSeederMain.Config();
      ConfigLoader<BootstrapSeederMain.StaticConfig> configLoader =
                    new ConfigLoader<BootstrapSeederMain.StaticConfig>("databus.reader.", bsConf);
      _bsStaticConfig = configLoader.loadConfig(_sBootstrapConfigProps);
      
      Config qConf = new Config();
      ConfigLoader<StaticConfig> configLoader2 =
                    new ConfigLoader<StaticConfig>("databus.query.", qConf);
      _queryStaticConfig = configLoader2.loadConfig(_sQueryConfigProps);
      
      
      SchemaRegistryService schemaRegistry = FileSystemSchemaRegistryService.build(_bsStaticConfig.getSchemaRegistry().getFileSystem());
      LOG.info("Schema =" + schemaRegistry.fetchLatestSchemaByType(_queryStaticConfig.getSourceName()));

      _schema = Schema.parse(schemaRegistry.fetchLatestSchemaByType(_queryStaticConfig.getSourceName()));
      VersionedSchema vs = new VersionedSchema(_schema.getFullName(), (short)1, _schema);

      VersionedSchemaSet schemaSet = new VersionedSchemaSet();
      schemaSet.add(vs);
      _decoder = new DbusEventAvroDecoder(schemaSet);
      
      List<Field> fields = _schema.getFields();

      _fieldTypes = new HashMap<Field, Type>();
      for (Field field : fields)
      {
         LOG.info("initSchema : Field=" + field + ",Type=" + field.schema().getType() + ",dbFieldName=" + SchemaHelper.getMetaField(field,"dbFieldName"));
         Type t =  SchemaHelper.getAnyType(field);
         _fieldTypes.put(field, t);
      }
      
    }
    
    
    public static Schema getSchema()
    {
    	return _schema;
    }
    
    @SuppressWarnings("static-access")
    public static void parseArgs(String[] args) throws IOException
    {
        CommandLineParser cliParser = new GnuParser();


        Option helpOption = OptionBuilder.withLongOpt(HELP_OPT_LONG_NAME)
                                       .withDescription("Help screen")
                                       .create(HELP_OPT_CHAR);

        Option sourcesOption = OptionBuilder.withLongOpt(QUERY_CONFIG_OPT_LONG_NAME)
                                       .withDescription("Query Config")
                                       .hasArg()
                                       .withArgName("property_file")
                                       .create(QUERY_CONFIG_OPT_CHAR);

        Option dbOption = OptionBuilder.withLongOpt(BOOTSTRAP_DB_PROPS_OPT_LONG_NAME)
                                    .withDescription("Bootstrap DB properties to use")
                                    .hasArg()
                                    .withArgName("property_file")
                                    .create(BOOTSTRAP_DB_PROP_OPT_CHAR);

        Option log4jPropsOption = OptionBuilder.withLongOpt(LOG4J_PROPS_OPT_LONG_NAME)
                                            .withDescription("Log4j properties to use")
                                            .hasArg()
                                            .withArgName("property_file")
                                            .create(LOG4J_PROPS_OPT_CHAR);
            
        Options options = new Options();
        options.addOption(helpOption);
        options.addOption(sourcesOption);
        options.addOption(dbOption);
        options.addOption(log4jPropsOption);

        CommandLine cmd = null;
        try
       {
          cmd = cliParser.parse(options, args);
        }
        catch (ParseException pe)
        {
          LOG.fatal("Bootstrap Physical Config: failed to parse command-line options.", pe);
          throw new RuntimeException("Bootstrap Physical Config: failed to parse command-line options.", pe);
        }

        if (cmd.hasOption(LOG4J_PROPS_OPT_CHAR))
        {
          String log4jPropFile = cmd.getOptionValue(LOG4J_PROPS_OPT_CHAR);
          PropertyConfigurator.configure(log4jPropFile);
          LOG.info("Using custom logging settings from file " + log4jPropFile);
        }
        else
        {
          PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
          ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);

          Logger.getRootLogger().removeAllAppenders();
          Logger.getRootLogger().addAppender(defaultAppender);

          LOG.info("Using default logging settings");
        }
        
        if (cmd.hasOption(HELP_OPT_CHAR))
        {
          printCliHelp(options);
          System.exit(0);
        }

        if (! cmd.hasOption(QUERY_CONFIG_OPT_CHAR))
            throw new RuntimeException("Query Config is not provided; use --help for usage");

        if (!cmd.hasOption(BOOTSTRAP_DB_PROP_OPT_CHAR) )
            throw new RuntimeException("Bootstrap config is not provided; use --help for usage");

        String propFile1 = cmd.getOptionValue(QUERY_CONFIG_OPT_CHAR);
        String propFile2 = cmd.getOptionValue(BOOTSTRAP_DB_PROP_OPT_CHAR);
        LOG.info("Loading bootstrap DB config from properties file " + propFile2);

        _sQueryConfigProps = new Properties();
		FileInputStream f1 = new FileInputStream(propFile1);
		try
		{
			_sQueryConfigProps.load(f1);
		} finally {
			f1.close();
		}
		
        _sBootstrapConfigProps = new Properties();
		FileInputStream f2 = new FileInputStream(propFile2);
		try
		{
			_sBootstrapConfigProps.load(f2);
		} finally {
			f2.close();
		}   
    }
    
    private static void printCliHelp(Options cliOptions)
    {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp("java " + BootstrapSeederMain.class.getName(), cliOptions);
    }
    
    public static class StaticConfig
    {
        private final Integer sourceId;
        private final String sourceName;
        private final String tablePrefix;
        private final String queryKey;
        private final boolean isRangeQuery;
        private final Long value;
        private final Long minKey;
        private final Long maxKey;
        private final Long logId;
        
        public Long getLogId() {
          return logId;
        }
        
        public Integer getSourceId() {
            return sourceId;
        }
        
        public String getSourceName() {
          return sourceName;
        }
        
        public String getTablePrefix() {
            return tablePrefix;
        }
        
        public String getQueryKey() {
            return queryKey;
        }
        
        public boolean isRangeQuery() {
            return isRangeQuery;
        }
        
        public Long getValue() {
            return value;
        }
        
        public Long getMinKey() {
            return minKey;
        }
        public Long getMaxKey() {
            return maxKey;
        }
        
        public StaticConfig(Integer sourceId, String sourceName, String tablePrefix,
                String queryKey, boolean isRangeQuery, Long value, Long minKey,
                Long maxKey, Long logId) {
            super();
            this.sourceId = sourceId;
            this.sourceName = sourceName;
            this.tablePrefix = tablePrefix;
            this.queryKey = queryKey;
            this.isRangeQuery = isRangeQuery;
            this.value = value;
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.logId = logId;
        }
        
        @Override
       public String toString()
        {
          return "StaticConfig [sourceId=" + sourceId + ", sourceName=" + sourceName
             + ", tablePrefix=" + tablePrefix + ", queryKey=" + queryKey
             + ", isRangeQuery=" + isRangeQuery + ", value=" + value + ", minKey="
             + minKey + ", maxKey=" + maxKey + "]";
        }
    }

    public static class Config implements ConfigBuilder<StaticConfig>
    {
        private Integer sourceId;
        private String sourceName;
        private String tablePrefix;
        private String field;
        private boolean isRangeQuery;
        private Long value;
        private Long minValue;
        private Long maxValue;
        private Long logId;
        
        public Long getLogId()
        {
          return logId;
        }
        
        public void setLogId(Long logId)
        {
          this.logId = logId;
        }
        
        public String getSourceName()
        {
          return sourceName;
        }
        public void setSourceName(String sourceName)
        {
          this.sourceName = sourceName;
        }
        public Integer getSourceId() {
            return sourceId;
        }
        public void setSourceId(Integer sourceId) {
            this.sourceId = sourceId;
        }
        public String getTablePrefix() {
            return tablePrefix;
        }
        public void setTablePrefix(String tablePrefix) {
            this.tablePrefix = tablePrefix;
        }
        public String getField() {
            return field;
        }
        public void setField(String queryKey) {
            this.field = queryKey;
        }
        public boolean isRangeQuery() {
            return isRangeQuery;
        }
        public void setRangeQuery(boolean isRangeQuery) {
            this.isRangeQuery = isRangeQuery;
        }
        public Long getValue() {
            return value;
        }
        public void setValue(Long value) {
            this.value = value;
        }
        public Long getMinValue() {
            return minValue;
        }
        public void setMinValue(Long minKey) {
            this.minValue = minKey;
        }
        public Long getMaxValue() {
            return maxValue;
        }
        public void setMaxValue(Long maxKey) {
            this.maxValue = maxKey;
        }
        
        @Override
       public StaticConfig build() throws InvalidConfigException
        {
            return new StaticConfig(sourceId, sourceName, tablePrefix, field, isRangeQuery, value, minValue, maxValue, logId);
        }
    }

}
