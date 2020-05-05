package com.conan.bigdata.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.*;

import java.io.IOException;
import java.util.List;

/**
 * com.twitter.parquet-hadoop  这个包是老版本的jar
 * org.apache.parquet-hadoop   parquet从1.7.0开始，重构了包名， 也就是说这个是新的
 * <p>
 * 老版本jar包的package是parquet开头
 * 新版本jar包的package是org.apache.parquet开头
 *
 * 此例子是基于 1.7.0 版本
 */
public class ReadParquetJava {

    private final static String PARQUET_PATH = "D:\\city_info.parquet";

    public static void main(String[] args) throws IOException {
//        readMetadata(PARQUET_PATH);
//        readParquet(PARQUET_PATH);
        messageType();
    }

    // 读取parquet的元数据
    private static void readMetadata(String path) throws IOException {
        Configuration conf = new Configuration();
        ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(conf, new Path(path), ParquetMetadataConverter.NO_FILTER);
        MessageType schema = parquetMetadata.getFileMetaData().getSchema();
        System.out.println(schema);
        System.out.println("=======这是字段名=========================");
        List<Type> fields = schema.getFields();
        for (Type field : fields) {
            System.out.println(field.getName());
        }

        System.out.println("=======这是字段的数据类型=========================");
        List<ColumnDescriptor> columnDescriptor = schema.getColumns();
        for (ColumnDescriptor col : columnDescriptor) {
            System.out.println(col.getType().javaType);
        }
    }

    // 读取parquet的内容
    private static void readParquet(String path) throws IOException {
        GroupReadSupport groupReadSupport = new GroupReadSupport();
        ParquetReader<Group> reader = ParquetReader.builder(groupReadSupport, new Path(path)).build();
        Group line = null;
        int count = 0;
        while ((line = reader.read()) != null) {
            // 0 开头，  第二个好像默认写 0 ， 原因未知, 用字段名称可读性好一点, 注意读取数据的格式
            System.out.println(line.getString("province", 0));
            count++;
            if (count >= 10) {
                break;
            }
        }
    }

    // 写parquet
    private static void writeParquet(String path) throws IOException{
        GroupFactory group =new SimpleGroupFactory(messageType());
        group.newGroup().add("provinceid",12);
    }

    // 构建 MessageType
    public static MessageType messageType(){
        MessageType schema= Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("cityid")
                .required(PrimitiveType.PrimitiveTypeName.INT64).named("provinceid")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("province")
                .named("example"); // 这个example， 就相当于表的名称， 之前定义的字段就是表的field
        System.out.println(schema);
        return schema;
    }
}