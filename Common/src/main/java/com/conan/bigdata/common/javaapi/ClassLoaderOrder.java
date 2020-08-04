package com.conan.bigdata.common.javaapi;

import com.conan.bigdata.common.jvm.ClassLoaderExp;

/**
 * 类内容加载顺序
 * 父类静态变量 -> 父类静态代码块 -> 子类静态变量 -> 子类静态代码块 -> 父类构造函数 -> 子类构造函数
 */
class SuperClassLoaderOrder {
    static {
        System.out.println("SuperClassLoaderOrder - static1");
    }
    {
        System.out.println("SuperClassLoaderOrder - static2");
    }

    public SuperClassLoaderOrder(){
        System.out.println("SuperClassLoaderOrder - constructor");
    }

    public void overrideMethod(){
        System.out.println("SuperClassLoaderOrder - overrideMethod");
    }
}
public class ClassLoaderOrder extends SuperClassLoaderOrder {
    static {
        System.out.println("ClassLoaderOrder - static1");
    }
    {
        System.out.println("ClassLoaderOrder - static2");
    }

    public ClassLoaderOrder(){
        System.out.println("ClassLoaderOrder - constructor");
    }

    public void subClassMethod(){
        System.out.println("ClassLoaderOrder - subClassMethod");
    }

    public void overrideMethod(int i){
        System.out.println("ClassLoaderOrder - overrideMethod");
    }

    public static void main(String[] args) {
        ClassLoaderOrder classLoaderOrder = new ClassLoaderOrder();
        classLoaderOrder.overrideMethod(10);

        SuperClassLoaderOrder superClassLoaderOrder=new ClassLoaderOrder();
        // 父类引用指向子类对象，如果子类定义了更多的方法，则是无法通过父类引用来调用的
        // superClassLoaderOrder.subClassMethod();
        // 可以强制转化后再调用
        ((ClassLoaderOrder)superClassLoaderOrder).subClassMethod();

        System.out.println(classLoaderOrder.getClass().getSuperclass().getName());
    }
}
