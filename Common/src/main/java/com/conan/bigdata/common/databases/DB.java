package com.conan.bigdata.common.databases;

import java.util.Properties;


public enum DB {
    TEST_MYSQL_19_6("10.0.19.6", "business_olap", "pd_test_dev", "VFR5rgdf"),
    MYSQL_24_5_TRANSFORM("10.0.24.5", "transform", "bigdata", "u0j@>uRFsjJi0uExv[yZ"),
    MYSQL_24_6_TRANSFORM("10.0.24.6", "transform", "bigdata", "u0j@>uRFsjJi0uExv[yZ"),
    MYSQL_24_7_TRANSFORM("10.0.24.7", "transform", "bigdata", "u0j@>uRFsjJi0uExv[yZ"),
    MYSQL_231_TRANSFORM("10.0.146.231", "transform", "root", "qianbiyinghoubiyingfengbuqin9"),
    MYSQL_231_SETTING("10.0.146.231", "setting", "root", "qianbiyinghoubiyingfengbuqin9"),
    MYSQL_231_SPARK("10.0.146.231", "spark", "root", "qianbiyinghoubiyingfengbuqin9"),
    MYSQL_231_MARKETING("10.0.146.231", "marketing", "root", "qianbiyinghoubiyingfengbuqin9"),
    MYSQL_211_ALFRED_MOBILE("10.1.30.245", "alfred-mobile", "pd_hadoop_dev", "4_hadoop_D32F"),
    MYSQL_197_SPARK("10.1.29.49", "winpos", "pd_hadoop_dev", "4_hadoop_D32F"),
    MYSQL_4_PAYN("10.1.31.20", "payn", "big.data", "Lud?~%n7vVus3ErgEdZc"),
    MYSQL_220_COUPONS("10.1.27.60", "coupons", "pd_hadoop_dev", "4_hadoop_D32F"), //10.0.146.218下線，改成10.0.146.32
    MYSQL_38_OA_STORAGE("10.0.35.6", "oa_storage", "pd_hadoop_dev", "DFDEFEl3454mlWncT"),
    MYSQL_10_QBT("10.1.25.60", "qbt", "pd_hadoop_dev", "4_hadoop_D32F"),
    MYSQL_100_QUEUES("10.1.25.120", "queues", "pd_hadoop_dev", "DFDEFEl3454mlWncT"),
    MYSQL_100_RESTAURANT("10.1.22.44", "restaurant", "pd_hadoop_dev", "4_hadoop_D32F"),
    MYSQL_36_NOWWARNING("10.1.22.241", "nowwarning", "pd_hadoop_dev", "chuxrbzvhpIMbue3"),
    MYSQL_32_RESTAURANT("10.1.25.60", "restaurant", "pd_hadoop_dev", "4_hadoop_D32F"),
    MYSQL_249_RESTAURANT("10.0.146.37", "restaurant", "pd_hadoop_dev", "4_hadoop_D32F"),
    MYSQL_80_ASSOCIATOR("10.0.146.80", "associator", "bigdata", "v(Yrbh2ekwBDEIu(]ue3"),
    MYSQL_50_ASSOCIATOR("10.1.32.185", "associator", "rdpususertag_dev", "dcff73ec05d73a4d5ae0ea47a0030541"), //阿里云迁移，从10.0.32.5 改为 10.1.32.150 by Sui 20170313
    MYSQL_181_ASSOCIATOR("10.1.32.181", "associator", "devuser2", "LC2q@dM=T5!pmoAwf5k"), //阿里云迁移，从10.0.32.5 改为 10.1.32.150 by Sui 20170313
    MYSQL_100_ASSOCIATOR("10.0.146.33", "associator", "devuser2", "LC2q@dM=T5!pmoAwf5k"),
    MYSQL_66_ASSOCIATOR("10.0.21.66", "associator", "associator", "LC2q@dM=T5!pmoAwf5k"),  //测试
    MYSQL_24_ASSOCIATOR("10.0.24.24", "estimate", "devuser2", "LC2q@dM=T5!pmoAwf5k"),  //预估模型用
    MYSQL_33_ASSOCIATOR("10.0.146.33", "associator", "associator", "LC2q@dM=T5!pmoAwf5k"), //测试  13307
    MYSQL_18_81_COUPONS("10.0.18.81", "coupons", "devuser2", "LC2q@dM=T5!pmoAwf5k"), // 会员测试库
    ORACLE_166_BWSWD("10.0.21.166", "ORCL", "pmadw", "pmadev021"),
    MYSQL_13_PLAT("10.1.36.13", "plat_db", "pd_hadoop_dev", "Flh5atLxtw4kak"),
    ORACLE_37_BWSWD("10.0.24.10", "BQSWD", "pmadw", "pmadev021"),
    ORACLE_7_BWSWD("10.0.24.7", "BQSWD", "pmadw", "pmadev021"),
    ORACLE_9_BWSWD("10.0.24.9", "BQSWD", "pmadw", "pmadev120"),
    ORACLE_12_BWSWD("10.0.24.12", "BQSWD", "pmadw", "pmadev021");

    public String ip;
    public String user;
    public String password;
    public Properties properties = new Properties();
    public String url;
    public String driver;

    private String MYSQL_PARAMETERS = "tinyInt1isBit=false&useUnicode=true&characterEncoding=gbk&zeroDateTimeBehavior=convertToNull";
    private String ORACLE_PARAMETERS = "";

    DB(String ip, String database, String user, String password) {
        this.ip = ip;
        this.user = user;
        this.password = password;
        if (this.toString().startsWith("MYSQL_239_RESTAURANT")) {
            this.url = String.format("jdbc:mysql://%s:3307/%s?%s", ip, database, this.MYSQL_PARAMETERS);
            this.driver = "com.mysql.jdbc.Driver";
            this.properties.put("driver", this.driver);
        } else if (this.toString().startsWith("MYSQL")) {
            this.url = String.format("jdbc:mysql://%s:3306/%s?%s", ip, database, this.MYSQL_PARAMETERS);
            this.driver = "com.mysql.jdbc.Driver";
            this.properties.put("driver", this.driver);
        } else if (this.toString().startsWith("TEST")) {
            this.url = String.format("jdbc:mysql://%s:30242/%s?%s", ip, database, this.MYSQL_PARAMETERS);
            this.driver = "com.mysql.jdbc.Driver";
            this.properties.put("driver", this.driver);
        }

        this.properties.put("user", user);
        this.properties.put("password", password);
    }
}
