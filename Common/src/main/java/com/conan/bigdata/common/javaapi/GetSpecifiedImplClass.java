package com.conan.bigdata.common.javaapi;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2018/8/8.
 * 获取接口或抽象类的具体实现类
 */
public class GetSpecifiedImplClass {

    public GetSpecifiedImplClass() {
        System.out.println("无参构造函数");
    }

    public GetSpecifiedImplClass(String name) {
        System.out.println("有参构造函数， 参数为:" + name);
    }

    public static void main(String[] args) {
        List<String> lisi = new ArrayList<>();

        Class clz = lisi.getClass();

        System.out.println(clz.getName());
    }

    public void show() {
        System.out.println("这是JavaReflect类反射调用的方法");
    }

}