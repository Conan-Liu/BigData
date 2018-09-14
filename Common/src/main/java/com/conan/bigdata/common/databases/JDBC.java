package com.conan.bigdata.common.databases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Administrator on 2018/9/14.
 */
public class JDBC {
    private static Connection conn = null;

    static {
        try {
            Class.forName(DB.TEST_MYSQL_19_6.driver);
            conn = DriverManager.getConnection(DB.TEST_MYSQL_19_6.url, DB.TEST_MYSQL_19_6.user, DB.TEST_MYSQL_19_6.password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        System.out.println(conn.isClosed());
    }
}