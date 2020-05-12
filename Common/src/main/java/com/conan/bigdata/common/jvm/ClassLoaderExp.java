package com.conan.bigdata.common.jvm;

/**
 * 这些类加载器都只是加载指定的目录下的jar包或者资源。如果在某种情况下，我们需要动态加载一些东西呢？
 * 比如从D盘某个文件夹加载一个class文件，或者从网络上下载class主内容然后再进行加载
 */
public class ClassLoaderExp extends ClassLoader {

    private String mlibPath;
    public ClassLoaderExp(String path){
        this.mlibPath=path;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        return super.findClass(name);
    }

    public static void main(String[] args) throws ClassNotFoundException {
        Class aClass = Class.forName("com.mw.Test1");
    }
}
