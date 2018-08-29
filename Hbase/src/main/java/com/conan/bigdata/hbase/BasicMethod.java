package com.conan.bigdata.hbase;

import com.conan.bigdata.hbase.common.HBaseAdminOperate;
import com.conan.bigdata.hbase.util.HBaseUtils;

import java.io.IOException;

/**
 * Created by Administrator on 2018/8/28.
 */
public class BasicMethod {

    private static final String TABLE_NAME = "user_action";

    public static void main(String[] args) throws IOException {
        HBaseAdminOperate.createTable(TABLE_NAME, HBaseUtils.getSplitKey("10,20,30,40,50,60,70,80,90", ","));
//        HBaseAdminOperate.getTableRegionInfo(TABLE_NAME);
//        HBaseAdminOperate.deleteTable(TABLE_NAME);
    }

}