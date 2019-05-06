package com.conan.bigdata.common.pool.commonspool2;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2019/5/6.
 */
public class ConnectionFactory extends BasePooledObjectFactory<Connection> {

    private AtomicInteger id=new AtomicInteger(1);

    @Override
    public Connection create() throws Exception {
        return new Connection(id.getAndAdd(1));
    }

    @Override
    public PooledObject<Connection> wrap(Connection connection) {
        return new DefaultPooledObject<>(connection);
    }
}