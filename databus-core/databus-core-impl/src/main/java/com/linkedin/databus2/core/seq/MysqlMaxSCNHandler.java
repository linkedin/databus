package com.linkedin.databus2.core.seq;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.log4j.Logger;

import java.beans.PropertyVetoException;
import java.sql.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class MysqlMaxSCNHandler  implements MaxSCNReaderWriter {

    private static final Logger LOG = Logger.getLogger(MysqlMaxSCNHandler.class.getName());

    private final StaticConfig staticConfig;
    private final AtomicLong _flushCounter;
    private final AtomicLong    _scn;
    private final ComboPooledDataSource cpds;

    public MysqlMaxSCNHandler(StaticConfig staticConfig, ComboPooledDataSource cpds){
        this.staticConfig = staticConfig;
        _flushCounter = new AtomicLong(0);
        _scn = new AtomicLong(0);
        this.cpds = cpds;
    }


    public static MysqlMaxSCNHandler create(StaticConfig config) throws DatabusException {
        ComboPooledDataSource cpds = createConnectionPool(config);
        MysqlMaxSCNHandler handler = new MysqlMaxSCNHandler(config, cpds);
        handler.loadInitialValue(cpds);
        return handler;
    }

    private void loadInitialValue(ComboPooledDataSource cpds) throws DatabusException {
        Statement stmt = null;
        Connection connection = null;
        try {
            connection = cpds.getConnection();
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(staticConfig.getGetSCNQuery());
            if(!rs.isBeforeFirst()){
                LOG.info("Initial max SCN does not exist in datastore. Defaulting to initial value from configuration: "
                        + staticConfig.getInitVal());
                _scn.set(staticConfig.getInitVal());
                writeScnToDataStore();
                return;
            }
            while(rs.next()) {
                _scn.set(rs.getLong(staticConfig.getScnColumnName()));
            }
            rs.close();
        } catch (SQLException e) {
            throw new DatabusException("unable to load initial SCN value",e);
        } finally {
            if(stmt!=null){
                try {
                    stmt.close();
                } catch (SQLException e) {
                }
            }
            if(connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    private static ComboPooledDataSource createConnectionPool(StaticConfig staticConfig) throws DatabusException {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass( staticConfig.getDriverClass() );
        } catch (PropertyVetoException e) {
            throw new DatabusException("Unable to create connection pool",e);
        }
        cpds.setJdbcUrl(staticConfig.getJdbcUrl());
        cpds.setUser(staticConfig.getDbUser());
        cpds.setPassword(staticConfig.getDbPassword());
        //TODO : Default connection pool setting, make it configurable
        return cpds;
    }


    @Override
    public long getMaxScn() throws DatabusException {
        return _scn.get();
    }

    @Override
    public void saveMaxScn(long scn) throws DatabusException {
        _scn.set(scn);

       long ctr =  _flushCounter.addAndGet(1);
       if(ctr % staticConfig.getFlushItvl() == 0) {
           LOG.info("Flushing counter:" + ctr);
           writeScnToDataStore();
        }
    }

    private void writeScnToDataStore() throws DatabusException {
        LOG.info("save max scn to mysql");
        PreparedStatement stmt = null;
        Connection connection = null;
        try {
            connection = cpds.getConnection();
            stmt = connection.prepareStatement(staticConfig.getUpsertSCNQuery());
            stmt.setLong(1,_scn.get());
            int i = stmt.executeUpdate();
            LOG.info("scn inserted "+_scn.get() +", success : "+i);
        } catch (SQLException e) {
            LOG.error("Could not persist SCN value "+_scn.get(),e);
            throw new DatabusException("Could not persist SCN value "+_scn.get(),e);
        }finally {
            if(stmt!=null){
                try {
                    stmt.close();
                } catch (SQLException e) {
                }
            }
            if(connection!=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    public void destroy() throws DatabusException {
        LOG.info("destory() called, saving scn file before shutting down.");
        writeScnToDataStore();
        cpds.close();
    }

    public static class Config implements ConfigBuilder<StaticConfig>
    {
        private String jdbcUrl= "jdbc:mysql://localhost:3306/databus";

        private String scnTable = "databus_scn_store";

        private String driverClass = "com.mysql.jdbc.Driver";

        private String dbUser = "root";

        private String dbPassword = "";

        private Long flushItvl = 1L;

        private Long initVal = 0L;

        private String upsertSCNQuery = "insert into databus_scn_store (max_scn) values (?)";

        private String getSCNQuery = "select max_scn from databus_scn_store order by updated_at desc limit 1";

        private String scnColumnName = "max_scn";

        public String getScnColumnName() {
            return scnColumnName;
        }

        public void setScnColumnName(String scnColumnName) {
            this.scnColumnName = scnColumnName;
        }

        public Long getInitVal() {
            return initVal;
        }

        public void setInitVal(Long initVal) {
            this.initVal = initVal;
        }

        public String getUpsertSCNQuery() {
            return upsertSCNQuery;
        }

        public void setUpsertSCNQuery(String upsertSCNQuery) {
            this.upsertSCNQuery = upsertSCNQuery;
        }

        public String getGetSCNQuery() {
            return getSCNQuery;
        }

        public void setGetSCNQuery(String getSCNQuery) {
            this.getSCNQuery = getSCNQuery;
        }

        public Long getFlushItvl() {
            return flushItvl;
        }

        public void setFlushItvl(Long flushItvl) {
            this.flushItvl = flushItvl;
        }

        public String getDbUser() {
            return dbUser;
        }

        public void setDbUser(String dbUser) {
            this.dbUser = dbUser;
        }

        public String getDbPassword() {
            return dbPassword;
        }

        public void setDbPassword(String dbPassword) {
            this.dbPassword = dbPassword;
        }

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public void setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

        public String getScnTable() {
            return scnTable;
        }

        public void setScnTable(String scnTable) {
            this.scnTable = scnTable;
        }

        public String getDriverClass() {
            return driverClass;
        }

        public void setDriverClass(String driverClass) {
            this.driverClass = driverClass;
        }

        @Override
        public StaticConfig build() throws InvalidConfigException {
            //TODO : verify
            return new StaticConfig(jdbcUrl,scnTable, driverClass, dbUser, dbPassword, flushItvl, initVal ,
                    upsertSCNQuery, getSCNQuery ,scnColumnName);
        }
    }
    public static class StaticConfig{

        private String driverClass = "com.mysql.jdbc.Driver";

        private String jdbcUrl;

        private String scnTable;

        private String dbUser;

        private String dbPassword;

        private Long  flushItvl;

        private Long initVal;

        private String upsertSCNQuery;

        private String getSCNQuery;

        private String scnColumnName;

        public String getScnColumnName() {
            return scnColumnName;
        }

        public Long getInitVal() {
            return initVal;
        }

        public String getUpsertSCNQuery() {
            return upsertSCNQuery;
        }

        public String getGetSCNQuery() {
            return getSCNQuery;
        }

        public Long getFlushItvl() {
            return flushItvl;
        }

        public String getDriverClass() {
            return driverClass;
        }

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public String getScnTable() {
            return scnTable;
        }

        public String getDbUser() {
            return dbUser;
        }

        public String getDbPassword() {
            return dbPassword;
        }

        public StaticConfig(String host, String table, String driverClass, String dbUser, String dbPassword, long flushItvl, long initVal,
        String upsertSCNQuery, String getSCNQuery, String scnColumnName){
            this.jdbcUrl = host;
            this.scnTable = table;
            this.driverClass = driverClass;
            this.dbUser = dbUser;
            this.dbPassword = dbPassword;
            this.flushItvl = flushItvl;
            this.initVal = initVal;
            this.upsertSCNQuery = upsertSCNQuery;
            this.getSCNQuery = getSCNQuery;
            this.scnColumnName = scnColumnName;
        }

    }

}

