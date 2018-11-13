package com.conan.bigdata.common.databases;

import java.sql.*;

/**
 * Created by Administrator on 2018/9/14.
 */
public class ImpalaJDBC {
    private static Connection conn = null;

    private static final String CLASS_NAME = "com.cloudera.impala.jdbc41.Driver";
    private static final String URL = "jdbc:impala://dn2.hadoop.pdbd.mwbyd.cn:21050/default";
    private static final String USER = "";
    private static final String PASSWORD = "";

    static {
        try {
            Class.forName(CLASS_NAME);
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        System.out.println(conn.isClosed());
        PreparedStatement ps = conn.prepareStatement("select count(*) from ads.user_tag");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        conn.close();
    }
}