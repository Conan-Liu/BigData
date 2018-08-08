package com.conan.bigdata.common.javaapi;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2018/8/8.
 * 获取接口或抽象类的具体实现类
 */
public class GetSpecifiedImplClass {

    public static void main(String[] args) {
        List<String> lisi=new ArrayList<>();

        Class clz=lisi.getClass();

        System.out.println(clz.getName());
    }

}