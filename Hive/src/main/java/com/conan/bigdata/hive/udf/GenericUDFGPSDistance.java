package com.conan.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 计算两个GPS之间的距离
 */
public class GenericUDFGPSDistance extends GenericUDF {

    private static final double EARTH_RADIUS = 6378137;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 4 & objectInspectors.length != 2) {
            throw new UDFArgumentException("The number of parameters must be contain two or four parameters. \n" +
                    "two parameters is two pair of latitude,longitude,  with comma seperate\n" +
                    "four parameters is latitude1, longitude1, latitude2, longitude2");
        }

        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        try {
            if (deferredObjects.length == 4) {
                double latitude1 = Double.valueOf(deferredObjects[0].get().toString().trim());
                double longitude1 = Double.valueOf(deferredObjects[1].get().toString().trim());
                double latitude2 = Double.valueOf(deferredObjects[2].get().toString().trim());
                double longitude2 = Double.valueOf(deferredObjects[3].get().toString().trim());

                return new DoubleWritable(getDistance(latitude1, longitude1, latitude2, longitude2));
            } else if (deferredObjects.length == 2) {
                String[] gps1 = deferredObjects[0].get().toString().split(",");
                String[] gps2 = deferredObjects[1].get().toString().split(",");
                double latitude1 = Double.valueOf(gps1[0].trim());
                double longitude1 = Double.valueOf(gps1[1].trim());
                double latitude2 = Double.valueOf(gps2[0].trim());
                double longitude2 = Double.valueOf(gps2[1].trim());
                return new DoubleWritable(getDistance(latitude1, longitude1, latitude2, longitude2));
            } else {
                return new DoubleWritable(0);
            }
        }catch (Exception e){
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "this is wrong infomation!!!";
    }

    private double getDistance(double latitude1, double longitude1, double latitude2, double longitude2) {
        double radLat1 = rad(latitude1);
        double radLat2 = rad(latitude2);
        double a = radLat1 - radLat2;
        double b = rad(longitude1) - rad(longitude2);

        double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
                + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
        s = s * EARTH_RADIUS;
        s = Math.round(s * 10000d) / 10000d;
        return s;
    }

    private double rad(double d) {
        return d * Math.PI / 180.0;
    }


    public static void main(String[] args) {
        // 单位 米 m
        System.out.println(new GenericUDFGPSDistance().getDistance(31.274001,121.452356,31.270813,121.477135));
    }
}