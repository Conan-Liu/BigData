package com.conan.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.UUID;

/**
 * Created by Administrator on 2017/7/27.
 */
// 一定要加上下面这段注解， rand()函数也有这个， 否则得到的所有记录数将是一样的UUID
@UDFType(deterministic = false)
public class GenericUUID extends GenericUDF {

    private static final Text uuid = new Text();

    // 初始化动作，只执行一次
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length > 0) {
            throw new UDFArgumentException("there should not pass arguments !");
        }
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        UUID u = UUID.randomUUID();
        uuid.set(u.toString().replace("-", ""));
        return uuid;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "help.................";
    }

}
