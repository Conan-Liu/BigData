package com.conan.bigdata.spark.rdbms;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Conan on 2019/5/11.
 * 构造一个 MySql 访问的连接池
 */
class JDBCFactory extends BasePooledObjectFactory<Connection> {

    private String driverClassName;
    private String url;
    private String userName;
    private String passWord;

    public JDBCFactory(String driverClassName, String url, String userName, String passWord) {
        this.driverClassName = driverClassName;
        this.url = url;
        this.userName = userName;
        this.passWord = passWord;
    }

    @Override
    public Connection create() throws Exception {
        Connection conn = null;
        try {
            Class.forName(driverClassName);
            conn = DriverManager.getConnection(url, userName, passWord);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    @Override
    public PooledObject<Connection> wrap(Connection obj) {
        return new DefaultPooledObject<>(obj);
    }

    @Override
    public void destroyObject(PooledObject<Connection> p) throws Exception {
        Connection conn = p.getObject();
        conn.close();
    }
}

public class JDBCPool {

    public static GenericObjectPool<Connection> getInstance() {
        String driverClassName = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/spark";
        String userName = "root";
        String userPass = "123";

        JDBCFactory factory = new JDBCFactory(driverClassName, url, userName, userPass);
        GenericObjectPoolConfig<Connection> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(5);
        config.setMaxIdle(2);
        config.setMinIdle(2);
        // 链接超时异常
        config.setMaxWaitMillis(1000000);
        config.setTestOnBorrow(false);
        config.setTestOnReturn(false);

        return new GenericObjectPool<>(factory, config);
    }

}