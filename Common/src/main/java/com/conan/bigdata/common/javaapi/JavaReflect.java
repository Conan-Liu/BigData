package com.conan.bigdata.common.javaapi;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class JavaReflect {
    public static void main(String[] args) throws Exception {
        //demo1.  通过Java反射机制得到类的包名和类名
//        demo1();
//        System.out.println("===============================================");

        //demo2.  验证所有的类都是Class类的实例对象
//        demo2();
//        System.out.println("===============================================");

        //demo3.  通过Java反射机制，用Class 创建类对象[这也就是反射存在的意义所在]，无参构造
//        demo3();
//        System.out.println("===============================================");

        //demo4:  通过Java反射机制得到一个类的构造函数，并实现构造带参实例对象
//        demo4();
//        System.out.println("===============================================");

        //demo5:  通过Java反射机制操作成员变量, set 和 get
//        demo5();
//        System.out.println("===============================================");
//
//        //demo6: 通过Java反射机制得到类的一些属性： 继承的接口，父类，函数信息，成员信息，类型等
//        demo6();
//        System.out.println("===============================================");
//
//        //demo7: 通过Java反射机制调用类中方法
//        demo7();
//        System.out.println("===============================================");
//
        //demo8: 通过Java反射机制获得类加载器
//        demo8();
//        System.out.println("===============================================");

        demo9();
    }

    /**
     * 通过Java的反射机制得到包名和类名
     */
    public static void demo1() {

        GetSpecifiedImplClass get = new GetSpecifiedImplClass();
        System.out.println("demo1: 包名 ======= " + get.getClass().getPackage().getName());
        System.out.println("demo1: 完整类名 === " + get.getClass().getName());
    }

    /**
     * 验证所有的类都是Class类的实例对象
     */
    public static void demo2() throws ClassNotFoundException {
        Class<?> clz1 = null;
        Class<?> clz2 = null;

        clz1 = Class.forName("com.conan.bigdata.common.javaapi.GetSpecifiedImplClass");
        System.out.println("demo2(写法1): 包名 ======= " + clz1.getPackage().getName());
        System.out.println("demo2(写法1): 完整类名 === " + clz1.getName());

        clz2 = GetSpecifiedImplClass.class;
        System.out.println("demo2(写法2): 包名 ======= " + clz2.getPackage().getName());
        System.out.println("demo2(写法2): 完整类名 === " + clz2.getName());
    }

    /**
     * 通过java反射机制， 用Class创建对象
     * newInstance() 方法创建实例，意味着该类必须有无参构造方法，否则运行异常
     */
    public static void demo3() throws Exception {
        Class clz1 = Class.forName("com.conan.bigdata.common.javaapi.GetSpecifiedImplClass");

        GetSpecifiedImplClass get = (GetSpecifiedImplClass) clz1.newInstance();
        get.show();
    }

    /**
     * 通过java反射获得类的构造函数， 并实现创建带参实例对象
     */
    public static void demo4() throws Exception {
        Class<?> clz1 = Class.forName("com.conan.bigdata.common.javaapi.GetSpecifiedImplClass");
        GetSpecifiedImplClass get1 = null;
        GetSpecifiedImplClass get2 = null;

        // Constructor的newInstance()方法是接受可变长参数，用于创建带参数的实例
        Constructor<?>[] constructors = clz1.getConstructors();

        get1 = (GetSpecifiedImplClass) constructors[0].newInstance();
        get1.show();

        get2 = (GetSpecifiedImplClass) constructors[1].newInstance("liufeiqiang");
        get2.show();
    }

    /**
     * 通过java反射机制操作成员变量， set和get
     */
    public static void demo5() throws Exception {
        Class<?> clz1 = Class.forName("com.conan.bigdata.common.javaapi.GetSpecifiedImplClass");

        Field field = clz1.getDeclaredField("name");
    }

    /**
     * 获取类加载器的信息
     */
    public static void demo8() throws Exception {
        Class<?> clz1 = Class.forName("com.conan.bigdata.common.javaapi.GetSpecifiedImplClass");
        System.out.println("demo8: 类加载器类名 ======== " + clz1.getClassLoader().getClass().getName());
    }

    /**
     * 反射调用类的方法
     */
    private static void demo9() throws Exception {
        Class clz=Class.forName("com.conan.bigdata.common.javaapi.GetSpecifiedImplClass");
        Object o = clz.newInstance();
        Method show = clz.getMethod("show");
        Method show1=clz.getMethod("show1",String.class);
        show.invoke(o);
        show1.invoke(o,"flag");
    }
}