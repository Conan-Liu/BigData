package com.conan.bigdata.common.jvm;

import sun.misc.Launcher;

import java.net.URL;

public class ClassLoaderApp {

    class Test{}
    public static void main(String[] args) {
        System.out.println("******** 本类加载 ******");
        // 这里打印sun.misc.Launcher$AppClassLoader，也就是自己写的类是从AppClassLoader加载的
        ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
        System.out.println(systemClassLoader);
        ClassLoader parent = systemClassLoader.getParent();
        System.out.println(parent);
        ClassLoader parent1 = parent.getParent();
        System.out.println(parent1);
        // 这里报NullPointerException，说明parent1已经是最顶层的加载器
        // ClassLoader parent2 = parent1.getParent();
        // System.out.println(parent2);

        System.out.println("\n\n");
        System.out.println("******** 加载其它类 ******");
        // 同样是AppClassLoader来加载
        ClassLoader classLoader = Test.class.getClassLoader();
        System.out.println(classLoader);
        // 这里 int 类型的加载器没有，原因如下
        ClassLoader classLoader1 = int.class.getClassLoader();
        System.out.println(classLoader1);
        ClassLoader classLoader2 = String.class.getClassLoader();
        System.out.println(classLoader2);

        System.out.println();
        System.out.println("bootstrapLoader加载以下文件：");
        URL[] urls = Launcher.getBootstrapClassPath().getURLs();
        for (int i = 0; i < urls.length; i++) {
            System.out.println(urls[i]);
        }

        System.out.println();
        System.out.println("extClassloader加载以下文件：");
        System.out.println(System.getProperty("java.ext.dirs"));

        System.out.println();
        System.out.println("appClassLoader加载以下文件：");
        System.out.println(System.getProperty("java.class.path"));

    }
}
