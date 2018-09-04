package com.conan.bigdata.hbase.api;

import com.conan.bigdata.hbase.common.CONSTANT;
import com.conan.bigdata.hbase.util.HBaseUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created by Administrator on 2018/9/3.
 */
public class SimpleAPI {

    private static final String OUT_PATH = "D:\\opt\\aaa.txt";

    public static void getResult(TableName tableName, String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = HBaseUtils.getConnection().getTable(tableName);
        Result result = table.get(get);

        System.out.println("返回记录数: " + result.size());
        for (Cell cell : result.listCells()) {
            System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
    }

    public static void getResultScan(TableName tableName, String rowKey) {
        String startKey = rowKey;
        String stopKey = String.valueOf(Long.valueOf(rowKey) + 1);
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(stopKey));

        scan.setBatch(1000);
        scan.setCaching(1000);
        scan.setCacheBlocks(true);

        File f = new File(OUT_PATH);
        ResultScanner rs = null;
        try {
            Table table = HBaseUtils.getConnection().getTable(tableName);
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (Cell cell : r.listCells()) {
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()) + "\n";
//                    System.out.print(value);
                    FileUtils.write(f, value, Charset.defaultCharset(), true);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rs.close();
        }
    }

    public static void main(String[] args) throws IOException {
//        getResult(TableName.valueOf(CONSTANT.TABLE_NAME), "162175342");
        getResultScan(TableName.valueOf(CONSTANT.TABLE_NAME), new StringBuilder("162175342").reverse().toString());
    }
}