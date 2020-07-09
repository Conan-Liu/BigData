package com.conan.bigdata.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class TestAPI {

    /**
     * kafka 消费者组对应到 __consumer_offsets 的分区信息
     */
    @Test
    public void kafkaPartitionId() {
        int i = Math.abs("meimeng_spark_20190102".hashCode()) % 50;
        System.out.println(i);
    }

    @Test
    public void test1() {
        System.out.println("aaa");
    }

    @Test
    public void test2() {
        System.out.println("abc".substring(1));
    }

    @Test
    public void test3() {
        System.out.println(Integer.MIN_VALUE);
        System.out.println(Math.abs(Integer.MIN_VALUE));
        System.out.println(Integer.MAX_VALUE + 1);
        System.out.println(Integer.MIN_VALUE - 1);

        System.out.println(0.2f > 0.2);   // 返回true，因为float和double精度的问题
    }

    @Test
    public void test4() {
        Map<String, String> map = new HashMap<>();
        map.put("1", "liu");
        map.put("2", "fei");
        String a = map.get("3");
        System.out.println(a.length());
    }

    // 测试一个catch 捕捉多个异常
    @Test
    public void test5() {
//        try{
//            throw new IOException("aaa");
//            throw new SQLException("bbb");
//            throw new InterruptedException("ccc");
//                    // 这种多异常的写法， 必须类不相关， 也就是说不能互为父子类
//        }catch (SQLException | IOException | InterruptedException e){
//
//        }
    }

    @Test
    public void test6() {
        Pattern pattern = Pattern.compile("^[0-9]*$");
        System.out.println(pattern.matcher("123").matches());
    }

    @Test
    public void test7() {
        TreeSet<Integer> set = new TreeSet<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                if (o1 > o2) {
                    return 1;
                } else {
                    return -1;
                }
            }
        });
        set.add(3);
        set.add(-1);
        set.add(5);
        set.add(2);
        set.add(0);
        set.pollFirst();
        set.add(100);
        set.pollFirst();
        set.pollLast();
        for (Integer val : set)
            System.out.println(val);
    }

    @Test
    public void test8() {
        System.out.println(Integer.parseInt("0000001100001000", 2));
        System.out.println(Integer.toBinaryString(776));

        byte[] s = "abc".getBytes();
        System.out.println(Arrays.toString(s));
    }

    @Test
    public void test9() {
        int n1 = 99999;
        int n2 = n1;
        String hex = "0123456789abcdef";
        StringBuilder sb = new StringBuilder();
        while (n1 > 0) {
            int i = n1 & 0xf;
            sb.append(hex.charAt(i));
            n1 >>= 4;
        }

        System.out.println(sb.reverse());
        System.out.println(Integer.toHexString(n2));
    }

    @Test
    public void test10() {
        String[] ss = " a b".split("\\s+");
        System.out.println(ss.length);
        for (String s : ss)
            System.out.println(s);
    }

    @Test
    public void test11() {
        JSONObject json = JSON.parseObject("{\"name\":\"liu\",\"age\":null}");
        System.out.println(json.toJSONString());
        System.out.println(json.get("age") == null ? "空" : json.get("age").getClass().getSimpleName());
    }

    @Test
    public void test12() {
        System.out.println(-1 << 5);
        System.out.println(1 << 5);
        System.out.println(-1 ^ -1 << 5);

        System.out.println("c".getBytes().length);

        System.out.println(Integer.toBinaryString(Integer.MIN_VALUE));
        System.out.println(Math.abs(Integer.MIN_VALUE));
    }

    @Test
    public void test13() {
        System.out.println(getClass().getName());
        System.out.println(getClass().getSimpleName());
        System.out.println(getClass().getCanonicalName());
    }

    @Test
    public void test14() {
        System.out.println(Runtime.getRuntime().availableProcessors());
        System.out.println(Integer.MAX_VALUE);
        System.out.println((int) (4 / 0.75));
        Integer i=null;
        Integer y=i+1;
        System.out.println(y);
    }

    /**
     * 字符串存储详解
     */
    @Test
    public void test15() {
        String s1 = "hello";
        String s2 = "hello";
        String s3 = "he" + "llo";
        String s4 = "hel" + new String("lo");
        String s5 = new String("hello");
        String s6 = s5.intern();
        String s7 = "h";
        String s8 = "ello";
        String s9 = s7 + s8;
        System.out.println(s1 == s2);//true  可以看到字符串常量共享了常量池
        System.out.println(s1 == s3);//true  JVM的优化，共享常量池
        System.out.println(s1 == s4);//false
        System.out.println(s1 == s9);//false
        System.out.println(s4 == s5);//false
        System.out.println(s1 == s6);//true
    }

    @Test
    public void test16() throws Exception{
        System.out.println(0.2f == 0.2);

        // URL 编码屏蔽特殊字符
        System.out.println(URLEncoder.encode("%{module}", "UTF-8"));

        // UUID 唯一标识符， 目前全球通用的是微软（GUID）的 8-4-4-4-12 的标准, 占36个字符
        String uuid = UUID.randomUUID().toString();
        System.out.println(uuid);
        System.out.println(uuid.getBytes().length);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        System.out.println(format.parse("2019-01-01").getTime());

        System.out.println(-1 ^ (-1 << 12));

        System.out.println(Math.pow(2, -2));

        System.out.println(100 / 200 / 365);  // 0
    }
}