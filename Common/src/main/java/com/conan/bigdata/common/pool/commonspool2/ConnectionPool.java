package com.conan.bigdata.common.pool.commonspool2;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Created by Administrator on 2019/5/6.
 */
public class ConnectionPool extends GenericObjectPool<Connection> {

    public ConnectionPool(PooledObjectFactory<Connection> factory) {
        super(factory);
    }

    public ConnectionPool(PooledObjectFactory<Connection> factory, GenericObjectPoolConfig<Connection> config) {
        super(factory, config);
    }

    public ConnectionPool(PooledObjectFactory<Connection> factory, GenericObjectPoolConfig<Connection> config, AbandonedConfig abandonedConfig) {
        super(factory, config, abandonedConfig);
    }
}