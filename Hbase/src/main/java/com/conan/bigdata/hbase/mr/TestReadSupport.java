package com.conan.bigdata.hbase.mr;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;

/**
 * Created by Administrator on 2018/8/31.
 */
public class TestReadSupport extends DelegatingReadSupport<Group> {
//
//    public TestReadSupport(ReadSupport<Group> delegate) {
//        super(delegate);
//    }

    public TestReadSupport(){
        super(new GroupReadSupport());
    }

    @Override
    public ReadContext init(InitContext context) {
        return super.init(context);
    }
}