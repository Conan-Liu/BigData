package com.conan.bigdata.common;

import org.junit.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2019/3/30.
 */
public class TestAPI {

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
}