package com.conan.bigdata.common.pool.commonspool1;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.util.Random;

/**
 * Created by Administrator on 2019/5/6.
 */
public class ConnectionFactory implements PooledObjectFactory<Connection> {

    @Override
    public PooledObject<Connection> makeObject() throws Exception {
        return new DefaultPooledObject<>(new Connection());
    }

    @Override
    public void destroyObject(PooledObject<Connection> p) throws Exception {
        p.getObject().destory();
    }

    @Override
    public boolean validateObject(PooledObject<Connection> p) {
        return new Random().nextInt() % 2 == 0;
    }

    @Override
    public void activateObject(PooledObject<Connection> p) throws Exception {
    }

    @Override
    public void passivateObject(PooledObject<Connection> p) throws Exception {

    }
}