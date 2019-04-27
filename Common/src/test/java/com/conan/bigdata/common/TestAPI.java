package com.conan.bigdata.common;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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
    public void test4(){
        Map<String,String> map=new HashMap<>();
        map.put("1","liu");
        map.put("2","fei");
        String a=map.get("3");
        System.out.println(a.length());
    }
}