package com.conan.bigdata.hbase.mwee;

import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by Administrator on 2018/11/5.
 */
public class TestHbase {

    private static Configuration hbaseConf = null;
    private static Connection connection = null;
    private static Admin hbaseAdmin = null;

    public static Configuration getHBaseConf() {
        if (hbaseConf == null) {
            hbaseConf = HBaseConfiguration.create();
            // 必须指定， 否则会报 schema 不一致
            hbaseConf.set("fs.defaultFS", "hdfs://nameservice1/");
            hbaseConf.set("dfs.nameservices", "nameservice1");
            hbaseConf.set("dfs.ha.namenodes.nameservice1", "nn1,nn2");
//            hbaseConf.set("dfs.namenode.rpc-address.nameservice1.nn1", "nn1.hadoop.pdbd.mwbyd.cn:8020");
//            hbaseConf.set("dfs.namenode.rpc-address.nameservice1.nn2", "nn2.hadoop.pdbd.mwbyd.cn:8020");
            hbaseConf.set("dfs.namenode.rpc-address.nameservice1.nn1", "nn1.hadoop.pdbd.test.cn:8020");
            hbaseConf.set("dfs.namenode.rpc-address.nameservice1.nn2", "nn2.hadoop.pdbd.test.cn:8020");
            hbaseConf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            hbaseConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            hbaseConf.set("mapreduce.reduce.memory.mb", "4096");
            hbaseConf.set("mapreduce.reduce.shuffle.input.buffer.percent", "0.3");
            hbaseConf.set("mapreduce.reduce.shuffle.parallelcopies", "3");
            hbaseConf.set("mapreduce.map.maxattempts", "2");
//            hbaseConf.set("hbase.zookeeper.quorum", "10.1.39.98,10.1.39.99,10.1.39.100");
            hbaseConf.set("hbase.zookeeper.quorum", "10.0.24.41,10.0.24.42,10.0.24.43");
            hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
//            hbaseConf.set("hadoop.security.authentication","kerberos");
//            hbaseConf.set("keytab.file" , "C:\\Users\\Administrator\\Desktop\\testKerberos\\hive.keytab" );
//            hbaseConf.set("kerberos.principal" , "hive/_HOST@MWEER.COM" );
//            UserGroupInformation.setConfiguration(hbaseConf);
//            try{
//                UserGroupInformation.loginUserFromKeytab("hive/_HOST@MWEER.COM","C:\\Users\\Administrator\\Desktop\\testKerberos\\hive.keytab");
//            }catch (Exception e){
//                e.printStackTrace();
//            }
        }
        return hbaseConf;
    }

    public static Connection getConnection() throws IOException {
        if (connection == null) {
            connection = ConnectionFactory.createConnection(getHBaseConf());
        }
        return connection;
    }

    public static Admin getHbaseAdmin() throws IOException {
        if (hbaseAdmin == null) {
            hbaseAdmin = getConnection().getAdmin();
        }
        return hbaseAdmin;
    }

    public static void show(ResultScanner rs) {
        int sum = 0;
        try {
            for (Result r : rs) {
//                for (Cell cell : r.rawCells()) {
////                    System.out.print(new String(CellUtil.cloneQualifier(cell)) + ":" + new String(CellUtil.cloneValue(cell), "UTF-8") + "\t");
//                    System.out.println(new String(CellUtil.cloneRow(cell)));
//                }
//                System.out.println("row_key = " + new String(r.getRow(), "UTF-8"));
                // r.size() 的值由 batch 来决定 ， 当 batch = 字段数 的时候， 每一次Result遍历就是一条记录
//                System.out.println(r.size());
                sum++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("总数: " + sum);
    }

    public static void getScanner(String searchKey) throws Exception {
//        String startKey = reverseWithLpad(searchKey);
//        String stopKey = reverseWithLpad(searchKey) + "B";
        String startKey = searchKey;
        String stopKey = searchKey + "B";

        System.out.println(startKey);

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(stopKey));

        // 如果设置了 逆序查找， 那么startrow 和 endrow 需要调换下才行
//        scan.setReversed(true);
//        scan.setStartRow(Bytes.toBytes(stopKey));
//        scan.setStopRow(Bytes.toBytes(startKey));

        scan.setBatch(19);
        scan.setCaching(100);
        scan.setCacheBlocks(true);

        System.out.println(getConnection().isClosed());

        ResultScanner rs = getConnection().getTable(TableName.valueOf("user_tag_detail")).getScanner(scan);
        show(rs);
    }

    public static String reverseWithLpad(String searchKey) {
        int len = searchKey.length();
        String key = "";
        switch (len) {
            case 1:
                key = "000000000" + searchKey;
                break;
            case 2:
                key = "00000000" + searchKey;
                break;
            case 3:
                key = "0000000" + searchKey;
                break;
            case 4:
                key = "000000" + searchKey;
                break;
            case 5:
                key = "00000" + searchKey;
                break;
            case 6:
                key = "0000" + searchKey;
                break;
            case 7:
                key = "000" + searchKey;
                break;
            case 8:
                key = "00" + searchKey;
                break;
            case 9:
                key = "0" + searchKey;
                break;
            case 10:
                key = searchKey;
                break;
            default:
                key = "";
        }
        return new StringBuilder(key).reverse().toString();
    }


    public static void main(String[] args) throws Exception {
//        System.setProperty("java.security.krb5.conf", "C:\\Users\\Administrator\\Desktop\\testKerberos\\krb5.conf");
        getScanner("0948316000280");
    }
}