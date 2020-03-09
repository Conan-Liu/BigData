package com.conan.bigdata.hive.fileformat;

import com.conan.bigdata.hive.util.SupportHiveDataType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrcWriter {
    public static void main(String[] args) {
        Map<String, String> columns = new HashMap<String, String>();
        columns.put("id", "int");
        columns.put("name", "string");
        columns.put("height", "float");


    }

//    public List<ObjectInspector> getColumnTypeInspectors(Map<String, String> map) {
//        for (Map.Entry<String, String> entry : map.entrySet()) {
//            SupportHiveDataType columnType = SupportHiveDataType.valueOf(entry.getValue());
//            ObjectInspector objectInspector=null;
//            switch (columnType){
//
//            }
//        }
//    }
}