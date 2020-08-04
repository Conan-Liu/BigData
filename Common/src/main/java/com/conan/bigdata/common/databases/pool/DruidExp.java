package com.conan.bigdata.common.databases.pool;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 */
public class DruidExp {

    private static DataSource dataSource;

    static {
        try {
            Properties properties = new Properties();
            InputStream in = DruidExp.class.getResourceAsStream("/application.properties");
            properties.load(in);
            dataSource = DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class InsertData implements Runnable {

        private int i;

        public InsertData(int i) {
            this.i = i;
        }

        @Override
        public void run() {
            try {
                // 内部类共享外部类对象
                Connection conn = dataSource.getConnection();
                String sql = "INSERT INTO test (f1, f2) VALUES (?, ?)";
                PreparedStatement ps = conn.prepareStatement(sql);
                conn.setAutoCommit(false);
                ps.setInt(1, 99);
                ps.setString(2, "zhangsan-" + i);
                ps.addBatch();
                ps.setInt(1, 999);
                ps.setString(2, "lisi-" + i);
                ps.addBatch();
                ps.executeBatch();
                conn.commit();
                // 关闭连接，归还到连接池中
                conn.close();
                System.out.println("提交成功-" + i);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Connection conn = dataSource.getConnection();
        System.out.println(conn);

        for(int i=0;i<10;i++){
            Thread t=new Thread(new InsertData(i));
            t.start();
        }

        Thread.sleep(40000);
        System.out.println("=======");
        System.out.println(conn.getAutoCommit());
        Connection conn1=dataSource.getConnection();
        System.out.println(conn1.getAutoCommit());
    }
}
