package com.conan.bigdata.common.pool.rdbmspool;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.sql.Connection;

/**
 * Created by Administrator on 2019/5/6.
 */
public class TestRDBMSPool1 {

    public static GenericObjectPool<Connection> getInstance() {
        String driverClassName = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/test";
        String userName = "root";
        String userPass = "123";

        JDBCFactory factory = new JDBCFactory(driverClassName, url, userName, userPass);
        GenericObjectPoolConfig<Connection> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(4);
        config.setMaxIdle(1);
        config.setMaxIdle(1);
        // 链接超时异常
        config.setMaxWaitMillis(1000000);
        config.setTestOnBorrow(false);
        config.setTestOnReturn(false);

        GenericObjectPool<Connection> pool = new GenericObjectPool<>(factory, config);
        return pool;
    }

    public static void main(String[] args) throws Exception {

        GenericObjectPool<Connection> pool = getInstance();
        for (int i = 0; i < 20; i++) {
            Connection conn = pool.borrowObject();
            System.out.println("类[" + TestRDBMSPool1.class.getName() + "]活跃的连接数 : " + pool.getNumActive());
            if (i == 3) {
                Thread.sleep(1000000);
            }
            pool.returnObject(conn);
        }

    }
}