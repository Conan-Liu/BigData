package com.conan.bigdata.common.pool.rdbmspool;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Administrator on 2019/5/6.
 */
public class JDBCFactory extends BasePooledObjectFactory<Connection> {

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