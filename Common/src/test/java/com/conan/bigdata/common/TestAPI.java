package com.conan.bigdata.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

public class TestAPI {

    /**
     * kafka 消费者组对应到 __consumer_offsets 的分区信息
     */
    @Test
    public void kafkaPartitionId(){
        int i=Math.abs("meimeng_spark_20190102".hashCode()) % 50;
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
    public void test12(){
        System.out.println(-1<<5);
        System.out.println(1<<5);
        System.out.println(-1^-1<<5);

        System.out.println("c".getBytes().length);

        System.out.println(Integer.toBinaryString(Integer.MIN_VALUE));
        System.out.println(Math.abs(Integer.MIN_VALUE));
    }

    @Test
    public void test13(){
        System.out.println(getClass().getName());
        System.out.println(getClass().getSimpleName());
        System.out.println(getClass().getCanonicalName());
    }

    @Test
    public void test14(){
        System.out.println(Runtime.getRuntime().availableProcessors());
    }
}