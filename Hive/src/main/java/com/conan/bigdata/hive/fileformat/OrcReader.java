package com.conan.bigdata.hive.fileformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.io.IOException;
import java.util.List;

/**
 * Created by Administrator on 2018/8/1.
 */
public class OrcReader {
    public static void main(String[] args) throws IOException {
        String path = "hdfs://ns1/user/hive/warehouse/ods.db/resta_citytable";
        Configuration conf = new Configuration();
        conf.set("dfs.nameservices", "ns1");
        conf.set("dfs.ha.namenodes.ns1", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.ns1.nn1", "m01.dpbu.mwee.prd:8020");
        conf.set("dfs.namenode.rpc-address.ns1.nn2", "m02.dpbu.mwee.prd:8020");
        conf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Path filePath = new Path(path);

        FileSystem fs = FileSystem.get(conf);
        Reader reader = OrcFile.createReader(fs, filePath);

        StructObjectInspector inspector = (StructObjectInspector) reader.getObjectInspector();
        RecordReader records = reader.rows();

        Object row = null;

        System.out.println("下面开始输出内容：");
        while (records.hasNext()) {
            row = records.next(row);
            List<Object> value_list = inspector.getStructFieldsDataAsList(row);
            System.out.println(value_list);
        }
    }
}