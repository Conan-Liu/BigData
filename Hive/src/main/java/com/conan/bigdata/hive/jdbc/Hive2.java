package com.conan.bigdata.hive.jdbc;

import java.sql.*;

/**
 * Created by Administrator on 2018/8/1.
 */
public class Hive2 {

    public static Connection conn = null;

    static {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection("jdbc:hive2://dn1.hadoop.pdbd.prod:10000/default", "hive", "hive");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        PreparedStatement ps = conn.prepareStatement("select default.dining_destination_tags('1,4,5')");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
}