package com.conan.hive.udf;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Administrator on 2017/4/18.
 */
public class StringToList extends GenericUDF {

    private static int mapTasks = 0;
    private static String init = "";
    private transient List ret = new ArrayList();

    @Override
    public void configure(MapredContext context) {
        // 这个可以获取job执行时的Configuration
        // 只会运行在map task
        System.out.println(new Date() + "############## configure");
        if (context != null) {
            mapTasks = context.getJobConf().getNumMapTasks();
        }
        System.out.println(new Date() + "############# mapTasks[" + mapTasks + "]...");
        super.configure(context);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 这个方法只会在初始化时执行，主要用于定义返回值类型
        System.out.println(new Date() + "############## initialize");
        init = "init";
        // 定义函数的返回类型为java的List
        ObjectInspector returnOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
        return ObjectInspectorFactory.getStandardListObjectInspector(returnOI);
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        // 必选，函数处理的核心方法，用途和UDF中的evaluate一样
        ret.clear();
        if (args.length < 1)
            return ret;
        String str = args[0].get().toString();
        String[] s = str.split(",", -1);
        for (String word : s) {
            ret.add(word);
        }
        return ret;
    }

    @Override
    public String getDisplayString(String[] children) {
        // 显示函数帮助信息
        return "Usage: string_to_List(String str)";
    }
}
