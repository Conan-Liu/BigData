package com.conan.hive.udtf;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by Administrator on 2017/4/16.
 */
public class HourList_Simple extends GenericUDTF {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    private static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHH");
    private static Calendar c_begin = Calendar.getInstance();
    private static Calendar c_end = Calendar.getInstance();
    private static String start_hour;
    private static String end_hour;

    @Override
    public void configure(MapredContext mapredContext) {
        // 这个可以获取job执行时的Configuration
        super.configure(mapredContext);
    }

    @SuppressWarnings("deprecation")
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 5) {
            throw new UDFArgumentLengthException("hour_list takes only five argument");
        }
        List<String> fieldName = new ArrayList<String>(4);
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(4);
        // 这里为了演示，返回两列，我的实际应用只需要一列
        // ArrayIndexOutOfBoundsException 如果定义字段和forward输出的数组长度不一致，就会报这个错
        fieldName.add("HH");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String begin_time = args[0].toString();
        String end_time = args[1].toString();
        int hour_diff = 0;
        int begin = Integer.parseInt(begin_time.substring(8, 10));
        if (begin_time.length() != 14 || end_time.length() != 14 || begin_time.compareTo(end_time) > 0)
            return;

        hour_diff = Integer.parseInt(end_time.substring(8, 10)) - begin;

        String hour_list = begin_time.substring(8, 10);
        for (int i = 0; i < hour_diff; i++) {
            hour_list += "," + String.format("%02d", begin + 1);
        }

        forward(hour_list.split(","));
    }

    @Override
    public void close() throws HiveException {
    }


    public static void main(String[] args) throws ParseException, InterruptedException {
        Date date = sdf.parse("20170102560200");
        System.out.println(date.getTime());

//        System.out.println("20170102030405".substring(0, 8));
//        System.out.println("ab".compareTo("ac"));
//
        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
//        c.set(2017, 04, 16, 0, 10);
        c1.setTime(sdf.parse("20170101000000"));
        c2.setTime(sdf.parse("20170101235959"));
//        System.out.println(c1.get(Calendar.HOUR));
//        System.out.println(c1.get(Calendar.HOUR_OF_DAY));
//        System.out.println(Arrays.toString(new HourList().hourLeftZero(c1)));
//
//        while (sdf1.format(c1.getTime()).compareTo(sdf1.format(c2.getTime())) <= 0) {
//            System.out.println("hour :" + Arrays.toString(new HourList().hourLeftZero(c1)));
//            c1.add(Calendar.HOUR_OF_DAY, 1);
//        }

        System.out.println(c2.get(Calendar.MINUTE) + "\t" + c2.get(Calendar.SECOND));

//        float[] result = new float[]{1000, 1000};
//        System.out.println(Arrays.toString(new HourList().hourLeftZero(c1, result[0], result[1])));


        float a = 1000f;
        long b = 3l;
        System.out.println(a / b);
    }
}
