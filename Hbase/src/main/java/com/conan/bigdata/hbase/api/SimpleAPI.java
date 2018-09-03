package com.conan.bigdata.hbase.api;

import com.conan.bigdata.hbase.common.CONSTANT;
import com.conan.bigdata.hbase.util.HBaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Administrator on 2018/9/3.
 */
public class SimpleAPI {

    public static void getResult(TableName tableName, String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = HBaseUtils.getConnection().getTable(tableName);
        Result result = table.get(get);

        for (Cell cell : result.listCells()) {
            System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
    }

    public static void main(String[] args) throws IOException {
        getResult(TableName.valueOf(CONSTANT.TABLE_NAME),"48972075101501072577e969");
    }
}