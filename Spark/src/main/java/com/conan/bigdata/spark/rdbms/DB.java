package com.conan.bigdata.spark.rdbms;

import java.util.Properties;

public enum DB {

    MYSQL_TEST_19_6("10.0.19.6", 30242, "business_olap", "pd_test_dev", "VFR5rgdf");

    public String ip;
    public int port;
    public String database;
    public String user;
    public String pass;
    public String url;
    public Properties prop;

    DB(String ip, int port, String database, String user, String pass) {
        this.ip = ip;
        this.port = port;
        this.database = database;
        this.user = user;
        this.pass = pass;
        this.prop = new Properties();
        if (this.toString().startsWith("MYSQL")) {
            this.url = String.format("jdbc:mysql://%s:%s/%s", ip, port, database);
            String driver = "com.mysql.jdbc.Driver";
            this.prop.put("driver", driver);
        }
        this.prop.put("user", user);
        this.prop.put("pass", pass);
    }
}