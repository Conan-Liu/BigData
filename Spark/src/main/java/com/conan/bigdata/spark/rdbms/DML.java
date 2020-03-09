package com.conan.bigdata.spark.rdbms;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DML {
    public static List<String> getAllBlackListData(Connection conn, String tableName) throws SQLException {
        List<String> list = new ArrayList<>();

        String sql = String.format("select word from %s where flag = 1", tableName);
        ResultSet rs = conn.createStatement().executeQuery(sql);
        while (rs.next()) {
            list.add(rs.getString("word"));
        }

        return list;
    }

    public static int insertBlackListData(Connection conn, List<String> list) throws SQLException {
        StringBuilder sb = new StringBuilder("insert into black_list(word, flag) values ");
        for (String s : list) {
            sb.append("('" + s + "', 1), ");
        }
        System.out.println("sql = " + sb.substring(0, sb.lastIndexOf(",")));
        return conn.createStatement().executeUpdate(sb.substring(0, sb.lastIndexOf(",")));
    }

    public static void main(String[] args) throws Exception {
        System.out.println(getAllBlackListData(JDBCPool.getInstance().borrowObject(), "black_list"));

        StringBuilder sb = new StringBuilder("insert into black_list(word, flag) values ");

        sb.append("('a', 1), ");
        sb.append("('b', 1), ");
        String s = sb.substring(0, sb.lastIndexOf(","));
        System.out.println(s);
    }
}