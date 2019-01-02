package com.conan.bigdata.hive.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by Administrator on 2018/8/1.
 */
public class Hive2 {

    public static Connection conn = null;

//    static {
//        try {
//            Class.forName("org.apache.hive.jdbc.HiveDriver");
//            conn = DriverManager.getConnection("jdbc:hive2://dn1.hadoop.pdbd.test.cn:10000/default;saslQop=auth;principal=hive/dn1.hadoop.pdbd.test.cn@MWEER.COM", "deploy", "");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    public static void main(String[] args) throws Exception {
        System.setProperty("java.security.krb5.conf", "C:\\krb5.conf");
        Configuration conf=new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("deploy@MWEER.COM","C:\\deploy.keytab");

        Class.forName("org.apache.hive.jdbc.HiveDriver");
        conn = DriverManager.getConnection("jdbc:hive2://dn1.hadoop.pdbd.test.cn:10000/;principal=hive/dn1.hadoop.pdbd.test.cn@MWEER.COM;kerberosAuthType=kerberos");
        System.out.println(conn);

    }
}