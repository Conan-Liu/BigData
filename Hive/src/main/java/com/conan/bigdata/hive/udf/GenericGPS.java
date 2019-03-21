package com.conan.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * Created by Administrator on 2019/3/20.
 */
public class GenericGPS extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2) {
            throw new UDFArgumentException("The number of parameters must be two, the first is longitude, the second is latitude...");
        }

        for (ObjectInspector objectInspector : objectInspectors) {
            if (!(objectInspector instanceof DoubleObjectInspector)) {
                throw new UDFArgumentException("The data type of parameters must be Double...");
            }
        }
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        double x = (double) deferredObjects[0].get();
        double y = (double) deferredObjects[1].get();
        double x_pi = 3.14159265358979324 * 3000.0 / 180.0;

        double z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * x_pi);
        double theTa = Math.atan2(y, x) + 0.000003 * Math.cos(x * x_pi);
        double bd_lng = z * Math.cos(theTa) + 0.0065;
        double bd_lat = z * Math.sin(theTa) + 0.006;
        String bd_gps = String.valueOf(bd_lng) + "," + String.valueOf(bd_lat);

        return bd_gps;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "it is wrong, silly b !!!";
    }
}