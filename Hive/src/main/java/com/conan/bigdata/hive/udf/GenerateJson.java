package com.conan.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import java.util.Arrays;

/**
 * Created by Administrator on 2018/8/7.
 */
public class GenerateJson extends GenericUDF {

    private MapObjectInspector mapObjectInspector = null;

    //这个方法只调用一次，并且在evaluate()方法之前调用。该方法接受的参数是一个ObjectInspectors数组。该方法检查接受正确的参数类型和参数个数。
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length % 2 != 0 || objectInspectors.length == 0) {
            throw new UDFArgumentException("The number of parameters must be even number, Every two parameters make up K-V from begin to end !");
        }

//        ObjectInspector returnOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
//        return returnOI;
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    //这个方法类似UDF的evaluate()方法。它处理真实的参数，并返回最终结果。接受任意参数的个数
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        Text[] argument = (Text[]) deferredObjects;

        return getJson(argument);
    }

    //这个方法用于当实现的GenericUDF出错的时候，打印出提示信息。而提示信息就是你实现该方法最后返回的字符串。
    @Override
    public String getDisplayString(String[] strings) {
        return Arrays.toString(strings);
    }

    private Text getJson(Text[] s) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < s.length; i += 2) {
            sb.append("\"").append(s[i]).append("\":\"").append(s[i + 1]).append("\"").append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return new Text("{" + sb.toString() + "}");
    }
}