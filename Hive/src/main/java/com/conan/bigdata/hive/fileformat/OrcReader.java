package com.conan.bigdata.hive.fileformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Created by Administrator on 2018/8/1.
 */
public class OrcReader {
    public static void main(String[] args) throws IOException {
        String path = "hdfs://ns1/user/hive/warehouse/ods.db/resta_citytable";
        Configuration conf = new Configuration();
        Path filePath = new Path(path);

        FileSystem fs = FileSystem.get(conf);
        Reader reader = OrcFile.createReader(fs, filePath);

        StructObjectInspector inspector = (StructObjectInspector) reader.getObjectInspector();
        RecordReader records = reader.rows();

        Object row=null;

        System.out.println("下面开始输出内容：");
        while (records.hasNext()){
            row=records.next(row);
            List<Object> value_list=inspector.getStructFieldsDataAsList(row);
            System.out.println(value_list);
        }
    }
}