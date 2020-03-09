package com.conan.bigdata.hive.udtf;

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

public class HourList extends GenericUDTF {

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
        fieldName.add("time_avg");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldName.add("flow_avg");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String begin_time = args[0].toString();
        String end_time = args[1].toString();
        float duration_sec = Float.parseFloat(args[2].toString());
        float flow_kb = Float.parseFloat(args[3].toString());
        String src_file_day = args[4].toString();
        if (src_file_day.length() != 8) {
            throw new UDFArgumentException("第五个参数一定要是8位的日期");
        }

        Date begin = null;
        Date end = null;
        int valid;
        try {
            begin = sdf.parse(begin_time);
            valid = 1;
        } catch (ParseException e) {
            valid = 0;
        }

        try {
            end = sdf.parse(end_time);
            if (valid == 0)                // 这里 valid, 1 开始有效，2 结束有效， 3 两个有效， 4 两个无效;
                valid = 2;
            else if (valid == 1)
                valid = 3;
        } catch (ParseException e) {
            if (valid == 0)
                valid = 4;
        }

        if ((valid == 3 || valid == 1) && src_file_day.compareTo(begin_time.substring(0, 8)) > 0)
            begin_time = src_file_day + "000000";
        if ((valid == 3 || valid == 2) && src_file_day.compareTo(end_time.substring(0, 8)) < 0)
            end_time = src_file_day + "235959";


//        System.out.println("valid : " + valid);
//        System.out.println("start_time : " + begin_time);
//        System.out.println("end_time : " + end_time);

        if (begin_time.compareTo(end_time) > 0) {
            return;
        } else if (valid == 3) {
            try {
                c_begin.setTime(sdf.parse(begin_time));
                c_end.setTime(sdf.parse(end_time));
            } catch (ParseException e) {
                return;
            }
            start_hour = sdf1.format(c_begin.getTime());
            end_hour = sdf1.format(c_end.getTime());
            long interval = (c_end.getTimeInMillis() - c_begin.getTimeInMillis()) / 1000;
            while (sdf1.format(c_begin.getTime()).compareTo(end_hour) <= 0) {
                float[] avg = time_Flow_Avg(c_begin, c_end, duration_sec, flow_kb, interval == 0 ? 1 : interval);
                forward(hourLeftZero(c_begin, avg[0], avg[1]));
                c_begin.add(Calendar.HOUR_OF_DAY, 1);
            }
        } else if (valid == 1) {
            try {
                c_begin.setTime(sdf.parse(begin_time));
            } catch (ParseException e) {
                return;
            }
            forward(hourLeftZero(c_begin, duration_sec, flow_kb));
        } else if (valid == 2) {
            try {
                c_end.setTime(sdf.parse(end_time));
            } catch (ParseException e) {
                return;
            }
            forward(hourLeftZero(c_end, duration_sec, flow_kb));
        }
    }

    @Override
    public void close() throws HiveException {
    }

    private String[] hourLeftZero(Calendar c_begin, float time_avg, float flow_avg) {
        int hour = c_begin.get(Calendar.HOUR_OF_DAY);
        if (hour < 10)
            return new String[]{"0" + hour, String.valueOf(time_avg), String.valueOf(flow_avg)};
        else
            return new String[]{String.valueOf(hour), String.valueOf(time_avg), String.valueOf(flow_avg)};
    }

    private float[] time_Flow_Avg(Calendar c_begin, Calendar c_end, float duration_sec, float flow_kb, long interval) {
        String hour1 = sdf1.format(c_begin.getTime());
        String hour2 = sdf1.format(c_end.getTime());
        float time_avg = duration_sec / interval;
        float flow_avg = flow_kb / interval;
        float[] result = new float[2];
        int compareValue = hour1.compareTo(hour2);
        if (compareValue < 0 && start_hour.compareTo(hour1) == 0) {
            long temp_interval = 3600 - (c_begin.get(Calendar.MINUTE) * 60 + c_begin.get(Calendar.SECOND));
            result[0] = time_avg * temp_interval;
            result[1] = flow_avg * temp_interval;
        } else if (compareValue == 0 && start_hour.compareTo(hour1) == 0) {
            result[0] = duration_sec;
            result[1] = flow_kb;
        } else if (compareValue == 0 && start_hour.compareTo(hour1) < 0) {
            long temp_interval = c_end.get(Calendar.MINUTE) * 60 + c_end.get(Calendar.SECOND);
            result[0] = time_avg * temp_interval;
            result[1] = flow_avg * temp_interval;
        } else {
            result[0] = time_avg * 3600;
            result[1] = flow_avg * 3600;
        }
        return result;
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
