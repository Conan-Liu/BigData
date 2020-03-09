package com.conan.bigdata.hadoop.basic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.conan.bigdata.hadoop.util.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;

public class FileSystemShell extends Configured implements Tool {
    public static void main(String[] args) {
        try {
            int result = ToolRunner.run(HadoopConf.getHAInstance(), new FileSystemShell(), args);
            System.out.println("result = " + result);
        } catch (Exception e) {
            System.out.println("出错啦");
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        updateDataFlow(conf);
        return 0;
    }

    public int testFS(Configuration conf) throws IOException {
        System.out.println(conf.get("fs.defaultFS"));
        FsShell shell = new FsShell(conf);
        Path trashDir = shell.getCurrentTrashDir();
        System.out.println("trashDir : " + trashDir.getName());
        try {
            shell.run(new String[]{"-ls", "/user"});    // 这条命令相当于 hdfs dfs -ls /user
            return 0;
        } catch (Exception e) {
            return 1;
        }
    }

    // 更新dataflow的数据
    public void updateDataFlow(Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] files = fs.listStatus(new Path("/backup/dataflow/20191021/"), new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return !path.getName().startsWith(".");
                }
            });
            CompressionCodecFactory factory = new CompressionCodecFactory(conf);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path("/meimeng/activity/2019/10/20/20191021.log"), true, 4096)));
            for (FileStatus file : files) {
                System.out.println("****************  " + file.getPath().toString());
                CompressionCodec codec = factory.getCodec(file.getPath());
                FSDataInputStream in = fs.open(file.getPath(), 4096);
                CompressionInputStream codecIn = codec.createInputStream(in);
                BufferedReader br = new BufferedReader(new InputStreamReader(codecIn));
                String line = null;
                while ((line = br.readLine()) != null) {
                    JSONArray jsonArray = JSON.parseArray(line);
                    for (int i = 0; i < jsonArray.size(); i++) {
                        JSONObject j = jsonArray.getJSONObject(i);
                        j.put("timestamp", Double.parseDouble(j.getString("timestamp")));
                        j.put("build", 0);
                        String userid = j.getString("userid");
                        if (userid != null) {
                            j.put("userid", "".equals(userid.trim()) ? 0 : Double.parseDouble(j.getString("userid")));
                        }
                        JSONObject biz = j.getJSONObject("biz");
                        if (biz != null) {
                            j.put("biz", biz.toJSONString());
                        }
                        bw.write(j.toJSONString());
                        bw.write("\n");
                    }
                }
                in.close();
                codecIn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//
//    private String transform(JSONObject jsonObj) {
//
//        //2.biz:
//        JSONObject bizJsonObj = jsonObj.getJSONObject("biz");
//        if (bizJsonObj != null) {
//            //biz子元素:
//            for (String key : bizJsonObj.keySet()) {
//                val fieldName = s"${BIZ}_${key}"
//                val typeOption: Option[String] = DATA_TYPE.get(key)
//                if (typeOption.nonEmpty) {
//                    typeOption.get match {
//                        //类型匹配
//                        case INTEGER => addNotNullValue(jsonObj, fieldName, bizJsonObj.getInteger(key))
//                        case LONG => addNotNullValue(jsonObj, fieldName, bizJsonObj.getLong(key))
//                        case STRING => addNotNullValue(jsonObj, fieldName, bizJsonObj.getString(key))
//                        case DOUBLE => addNotNullValue(jsonObj, fieldName, bizJsonObj.getDouble(key))
//                    }
//                } else {
//                    //DATA_TYPE中不存在（默认String）:
//                    addNotNullValue(jsonObj, fieldName, bizJsonObj.getString(key))
//                }
//
//            }
//            //最后将biz json对象转为 string ==> Hive:
//            jsonObj.put(BIZ, bizJsonObj.toJSONString)
//        }
//
//        //3.Druid
//        transform4Druid(jsonObj) //Druid
//        //:
//        jsonObj.toJSONString
//    }
}
