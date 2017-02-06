package com.linkedin.databus2.core.seq;

import com.linkedin.databus2.core.DatabusException;

/**
 *
 */
public class MysqlMaxSCNHandlerFactory implements SequenceNumberHandlerFactory {
    private final MysqlMaxSCNHandler.Config _configBuilder;

    public MysqlMaxSCNHandlerFactory(MysqlMaxSCNHandler.Config configBuilder)
    {
        _configBuilder = configBuilder;
    }

    @Override
    public MaxSCNReaderWriter createHandler(String id) throws DatabusException {
        MysqlMaxSCNHandler maxSCNHandler;
        MysqlMaxSCNHandler.StaticConfig config;
        synchronized (_configBuilder) {
            config = _configBuilder.build();
            maxSCNHandler = MysqlMaxSCNHandler.create(config);
        }
        return maxSCNHandler;
    }
}
