package com.conan.bigdata.common.databases;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * Created by Administrator on 2018/9/14.
 */
public class JDBC {
    private static Connection conn = null;

    public void getConnection(String className, String url, String user, String passwd) {
        try {
            Class.forName(className);
            conn = DriverManager.getConnection(url, user, passwd);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void show(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            // 读所有的schema和数据
            for (int i = 1; i <= columnCount; i++) {
                System.out.println(metaData.getColumnName(i) + "\t" + metaData.getColumnTypeName(i) + "\t" + rs.getObject(i));
            }

            // 读DATE类型的数据
            TimeZone timeZone = TimeZone.getTimeZone("GMT+8");
            long time = rs.getDate("birthday").getTime();
            System.out.println(rs.getString("birthday"));
            System.out.println(time);
            System.out.println(new Date(time));
            String timeFormat = DateFormatUtils.format(new Date(time), "yyyy-MM-dd HH:mm:ss", timeZone);
            System.out.println(timeFormat);
        }
    }

    public static void main(String[] args) throws SQLException {
        JDBC jdbc = new JDBC();
        jdbc.getConnection(DB.MYSQL_18_81_COUPONS.driver, DB.MYSQL_18_81_COUPONS.url, DB.MYSQL_18_81_COUPONS.user, DB.MYSQL_18_81_COUPONS.password);
        System.out.println(conn);
        // 000094195827   000000074053
        PreparedStatement ps = conn.prepareStatement("select * from cp_card_member where m_shopid = 43 and card_no = '000094195827'");
        ResultSet rs = ps.executeQuery();
        jdbc.show(rs);
        System.out.println(conn.isClosed());
    }
}