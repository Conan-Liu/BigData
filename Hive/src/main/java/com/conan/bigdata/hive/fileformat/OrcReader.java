package com.conan.bigdata.hive.fileformat;

import com.conan.bigdata.hive.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.io.IOException;
import java.util.List;

public class OrcReader {
    public static void main(String[] args) throws IOException {
        orcFile_Reader();

    }

    public static void orcFile_Reader() throws IOException {
        // conf中指定了 FS 的schema ， 所以这里不需要指定
        String path = "/user/hive/warehouse/ods.db/resta_citytable/000__c2928301_53c9_4415_a790_a6c37ddfe4fe";
        Configuration conf = HadoopConf.getInstance();
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