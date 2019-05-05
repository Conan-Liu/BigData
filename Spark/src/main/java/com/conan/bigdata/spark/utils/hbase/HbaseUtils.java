package com.conan.bigdata.spark.utils.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Conan on 2019/5/3.
 * HBase 操作工具类
 */
public class HbaseUtils {

    private static Admin admin = null;
    private static Configuration conf = null;
    private static HbaseUtils hbaseUtils = null;
    private static Connection connection = null;

    /**
     * 单例类， 私有构造方法
     */
    private HbaseUtils() {
        conf = HBaseConfiguration.create();
        ;
        conf.set("hbase.zookeeper.quorum", "CentOS");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.rootdir", "hdfs://CentOS:8020/hbase");
    }

    /**
     * 连接池方式一
     * 这个是官方推荐使用的方式
     *
     * @return
     * @throws IOException
     */
    public static Connection getConnection() throws IOException {
        if (connection == null) {
            ExecutorService pool= Executors.newFixedThreadPool(10);
            // 这里有个重载的方法，可以传入连接池参数
            // 所有进程共用一个connection对象， 这样就能保证Table 和 Admin 操作是线程安全的
            connection = ConnectionFactory.createConnection(conf,pool);
        }
        return connection;
    }


    public static Admin getHbaseAdmin() throws IOException {
        if (admin == null) {
            admin = getConnection().getAdmin();
        }
        return admin;
    }

    public static synchronized HbaseUtils getInstance() {
        if (hbaseUtils == null) {
            hbaseUtils = new HbaseUtils();
        }
        return hbaseUtils;
    }

    /**
     * 获取hbase的表名
     *
     * @param tableName
     * @return
     */
    public HTable getTable(String tableName) {
        HTable table = null;
        try {
            table = (HTable) getConnection().getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 添加记录到Hbase
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @param value
     */
    public void put(String tableName, String rowKey, String familyName, String columnName, String value) {
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        HTable table = HbaseUtils.getInstance().getTable("course_clickcount");
        System.out.println(table.getName().getNameAsString());

        HbaseUtils.getInstance().put("course_clickcount", "201711_18", "info", "click_count", "2");
    }
}