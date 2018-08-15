package com.conan.bigdata.common.javaapi;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

/**
 * Created by Administrator on 2018/8/14.
 */
public class JavaReflect {
    public static void main(String[] args) throws Exception {
        //Demo1.  通过Java反射机制得到类的包名和类名
        Demo1();
        System.out.println("===============================================");

        //Demo2.  验证所有的类都是Class类的实例对象
        Demo2();
        System.out.println("===============================================");

        //Demo3.  通过Java反射机制，用Class 创建类对象[这也就是反射存在的意义所在]，无参构造
        Demo3();
        System.out.println("===============================================");

        //Demo4:  通过Java反射机制得到一个类的构造函数，并实现构造带参实例对象
        Demo4();
        System.out.println("===============================================");

        //Demo5:  通过Java反射机制操作成员变量, set 和 get
//        Demo5();
//        System.out.println("===============================================");
//
//        //Demo6: 通过Java反射机制得到类的一些属性： 继承的接口，父类，函数信息，成员信息，类型等
//        Demo6();
//        System.out.println("===============================================");
//
//        //Demo7: 通过Java反射机制调用类中方法
//        Demo7();
//        System.out.println("===============================================");
//
        //Demo8: 通过Java反射机制获得类加载器
        Demo8();
        System.out.println("===============================================");
    }

    /**
     * 通过Java的反射机制得到包名和类名
     */
    public static void Demo1() {

        GetSpecifiedImplClass get = new GetSpecifiedImplClass();
        System.out.println("Demo1: 包名 ======= " + get.getClass().getPackage().getName());
        System.out.println("Demo1: 完整类名 === " + get.getClass().getName());
    }

    /**
     * 验证所有的类都是Class类的实例对象
     */
    public static void Demo2() throws ClassNotFoundException {
        Class<?> clz1 = null;
        Class<?> clz2 = null;

        clz1 = Class.forName("com.conan.bigdata.common.javaapi.GetSpecifiedImplClass");
        System.out.println("Demo2(写法1): 包名 ======= " + clz1.getPackage().getName());
        System.out.println("Demo2(写法1): 完整类名 === " + clz1.getName());

        clz2 = GetSpecifiedImplClass.class;
        System.out.println("Demo2(写法2): 包名 ======= " + clz2.getPackage().getName());
        System.out.println("Demo2(写法2): 完整类名 === " + clz2.getName());
    }

    /**
     * 通过java反射机制， 用Class 创建对象
     */
    public static void Demo3() throws Exception {
        Class clz1 = Class.forName("com.conan.bigdata.common.javaapi.GetSpecifiedImplClass");

        GetSpecifiedImplClass get = (GetSpecifiedImplClass) clz1.newInstance();
        get.show();
    }

    /**
     * 通过java反射获得类的构造函数， 并实现创建带参实例对象
     */
    public static void Demo4() throws Exception {
        Class<?> clz1 = Class.forName("com.conan.bigdata.common.javaapi.GetSpecifiedImplClass");
        GetSpecifiedImplClass get1 = null;
        GetSpecifiedImplClass get2 = null;

        Constructor<?>[] constructors = clz1.getConstructors();

        get1 = (GetSpecifiedImplClass) constructors[0].newInstance();
        get1.show();

        get2 = (GetSpecifiedImplClass) constructors[1].newInstance("liufeiqiang");
        get2.show();
    }

    /**
     * 通过java反射机制操作成员变量， set和get
     */
    public static void Demo5() throws Exception {
        Class<?> clz1 = Class.forName("com.conan.bigdata.common.javaapi.GetSpecifiedImplClass");

        Field field = clz1.getDeclaredField("name");
    }

    /**
     * 获取类加载器的信息
     */
    public static void Demo8() throws Exception {
        Class<?> clz1 = Class.forName("com.conan.bigdata.common.javaapi.GetSpecifiedImplClass");
        System.out.println("Demo8: 类加载器类名 ======== " + clz1.getClassLoader().getClass().getName());
    }
}