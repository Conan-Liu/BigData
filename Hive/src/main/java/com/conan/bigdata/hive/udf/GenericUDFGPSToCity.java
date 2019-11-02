package com.conan.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * 根据GPS定位到某个城市，这个需要调用第三方的地图接口
 * 基础服务可以封装成公司内部的接口
 * udf 不知道可不可以调用http接口
 */
public class GenericUDFGPSToCity extends GenericUDF {

    private HttpURLConnection httpConn = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        try {
            URL restURL = new URL("http://10.0.21.214:3051/services/api/city.locationCity");
            httpConn = (HttpURLConnection) restURL.openConnection();
            httpConn.setRequestMethod("POST");
            httpConn.setRequestProperty("Content-Type", "application/json");
            httpConn.setDoOutput(true);
            httpConn.setAllowUserInteraction(false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String gpsJson = deferredObjects[0].get().toString();
        StringBuilder sb = new StringBuilder();
        try {
            OutputStream out = httpConn.getOutputStream();
            out.write(gpsJson.getBytes());
            out.flush();
            InputStream in = httpConn.getInputStream();
            int resultCode = httpConn.getResponseCode();
            if (HttpURLConnection.HTTP_OK == resultCode) {
                byte[] b = new byte[1024];
                int len;
                while ((len = in.read(b)) != -1) {
                    sb.append(new String(b, 0, len, "UTF-8"));
                }
                System.out.println(sb);
            } else {
                return new Text();
            }
            out.close();
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Text(sb.toString());
    }

    @Override
    public String getDisplayString(String[] children) {
        return "这是调http请求的， 大数据量的情况下， 可能会撑爆接口";
    }
}